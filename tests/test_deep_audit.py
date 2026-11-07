"""Cross-component regressions found in the 3.1.5 source audit; all offline."""
import copy
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from test_live_transport import live, run, runtime_with_transport
from exchange.orders import ExchangeError, safe_create_order
from exchange.reconciler import reconcile
from risk.config import load_config
from risk.kill_switch import KillSwitch

S = 'BTC/USDT:USDT'


def test_periodic_save_cannot_revert_new_accounting(live, tmp_path):
    async def check():
        bot, _ = await runtime_with_transport(live, tmp_path)
        assert await bot.enter(S, 'buy', 100)
        record = live.executions[live.execution_key(S, 1)]
        # Strategy snapshot can predate a userTrades response during another await.
        bot.positions[S] = copy.deepcopy(bot.positions[S])
        record['position']['entry_fee'] = .123
        record['position']['accounting_status'] = 'complete'
        bot.positions[S]['entry_fee'] = 0
        bot.positions[S]['accounting_status'] = 'pending'
        bot.positions[S]['sl'] += .01
        bot.save()
        assert record['position']['entry_fee'] == .123
        assert record['position']['accounting_status'] == 'complete'
        assert record['position']['sl'] == bot.positions[S]['sl']
        await live.close()
    run(check())


def test_reconcile_keeps_original_entry_facts_on_partial_remote_close(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        assert await bot.enter(S, 'buy', 100)
        original = copy.deepcopy(bot.positions[S])
        server.qty = .5
        report = await reconcile(live, bot, bot.safety.journal)
        assert report['size_mismatches'] == [S]
        assert bot.positions[S]['amount'] == original['amount']
        bot.save()
        assert live.executions[live.execution_key(S, 1)]['position']['amount'] == original['amount']
        await live.close()
    run(check())


def test_reconcile_does_not_replace_protection_before_size_check(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        assert await bot.enter(S, 'buy', 100)
        server.qty = .5
        live.protect = AsyncMock()
        bot.safety.trip = AsyncMock()
        await live.refresh_account()
        assert not await bot.safety.reconcile_once()
        live.protect.assert_not_awaited()
        bot.safety.trip.assert_awaited_once_with('reconcile_mismatch')
        await live.close()
    run(check())


def test_malformed_successful_post_queries_same_id_before_resolving(live):
    calls = []
    async def handler(request):
        calls.append(request)
        if request.method == 'POST': return httpx.Response(200, text='<html>upstream fault</html>')
        return httpx.Response(200, json={'orderId': 42, 'clientOrderId': request.url.params['origClientOrderId'], 'status': 'FILLED'})
    async def check():
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        result = await safe_create_order(live, {'symbol': S, 'type': 'market', 'side': 'buy', 'amount': 1}, live.journal)
        assert result['orderId'] == 42
        assert [r.method for r in calls] == ['POST', 'GET']
        assert calls[0].url.params['newClientOrderId'] == calls[1].url.params['origClientOrderId']
        await live.close()
    run(check())


def test_transient_get_timeout_is_retried_but_post_is_not(live):
    calls = []
    async def handler(request):
        calls.append(request.method)
        if len(calls) == 1: raise httpx.ReadTimeout('never log signed URL', request=request)
        return httpx.Response(200, json={'ok': True})
    async def check():
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock):
            assert await live._request('GET', '/fapi/v1/accountConfig', retry=True) == {'ok': True}
        assert calls == ['GET', 'GET']
        calls.clear()
        with pytest.raises(ExchangeError):
            await live._request('POST', '/fapi/v1/order', retry=True)
        assert calls == ['POST']
        await live.close()
    run(check())


def test_daily_rollover_precedes_drawdown_evaluation_and_preserves_health():
    kill = KillSwitch(load_config()['kill_switch'])
    kill.day = '2000-01-01'
    kill.day_start_equity = 100
    kill.state.daily_pnl = -4.9
    kill.state.consecutive_losses = 2
    kill.on_api_error()
    heartbeat = kill.state.last_ws_heartbeat
    assert kill.check(SimpleNamespace(equity=94), None) is None
    assert kill.day != '2000-01-01' and kill.state.daily_pnl == 0
    assert kill.state.consecutive_losses == 2 and kill.state.api_errors_1m == 1
    assert kill.state.last_ws_heartbeat == heartbeat


def test_nonfinite_equity_cannot_pass_exposure_guard():
    from risk.exposure_limits import can_open
    state = SimpleNamespace(equity=float('nan'), open_positions={})
    assert can_open(state, S, 'buy', 10, load_config()['exposure']) == (False, 'invalid_equity_or_notional')


def test_actual_liquidation_boundary_tightens_stop_instead_of_discarding_fill(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        original = server.__call__
        async def handler(request):
            response = await original(request)
            if request.url.path == '/fapi/v3/positionRisk' and server.qty:
                data = response.json()
                data[0]['liquidationPrice'] = '99.15'
                return httpx.Response(200, json=data)
            return response
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        assert await bot.enter(S, 'buy', 100)
        assert server.qty == 1
        pos = bot.positions[S]
        assert float(pos['protection']['sl']['trigger']) > 99.15 * 1.003
        assert pos['sl'] >= pos['stop_initial']
        await live.close()
    run(check())


def test_restart_adopts_pending_native_stop_by_same_client_id(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        assert await bot.enter(S, 'buy', 100)
        pos = bot.positions[S]
        previous = pos['protection']['sl']['clientOrderId']
        # Crash after Binance accepted the SL, before its response was saved.
        intent = live.intents[previous]
        intent['status'] = 'pending'
        intent.pop('order_id', None)
        intent.pop('response', None)
        intent.pop('protection_committed', None)
        del pos['protection']['sl']
        for record in live.executions.values():
            if record.get('position'): record['position']['protection'].pop('sl', None)
        before = len(server.algos)
        assert await live.recover_execution()
        bot.merge_execution_state()
        assert live.intents[previous]['status'] == 'observed'
        assert bot.positions[S]['protection']['sl']['clientOrderId'] == previous
        assert len(server.algos) == before
        await live.close()
    run(check())


def test_restart_with_pending_protection_absent_must_block(live, tmp_path):
    async def check():
        bot, _ = await runtime_with_transport(live, tmp_path)
        assert await bot.enter(S, 'buy', 100)
        live.persist_intent('kh_lost_protection', {'symbol': S, 'type': 'STOP_MARKET', 'side': 'sell',
            'amount': 1, 'params': {'newClientOrderId': 'kh_lost_protection', 'reduceOnly': True,
            'stopPrice': 99, 'round_up': True}})
        assert not await live.recover_execution()
        await live.close()
    run(check())


def test_exit_checker_does_not_swallow_live_management_failure(live, tmp_path):
    async def check():
        bot, _ = await runtime_with_transport(live, tmp_path)
        assert await bot.enter(S, 'buy', 100)
        live.fetch_ticker = AsyncMock(side_effect=RuntimeError('secret must not be logged'))
        # Paper retains compatibility, live must propagate to the latched exit watcher.
        with pytest.raises(RuntimeError): await bot.strategy.CascadeHunter.manage_exits(bot)
        await live.close()
    run(check())


@pytest.mark.parametrize('fill_data', [[{'qty': '1'}], [{'qty': '1', 'commissionAsset': 'USDT', 'commission': 'NaN', 'realizedPnl': '0'}]])
def test_bad_accounting_payload_keeps_filled_entry_protected(live, tmp_path, fill_data):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        original = server.__call__
        async def handler(request):
            if request.url.path.endswith('/userTrades'): return httpx.Response(200, json=fill_data)
            return await original(request)
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        assert await bot.enter(S, 'buy', 100)
        assert server.qty == 1 and bot.safety.kill.triggered is None
        assert live.trades[0]['accounting_status'] == 'pending'
        assert set(bot.positions[S]['protection']) == {'sl', 'tp'}
        await live.close()
    run(check())


def test_reviewed_reset_retains_daily_loss_and_streak_and_blocks_pending(tmp_path):
    from infra.journal import atomic_json
    from tools.reset_safety import reviewed_reset_state
    cfg = load_config()
    kill = KillSwitch(cfg['kill_switch'])
    kill.check(SimpleNamespace(equity=100), None)
    kill.on_trade_closed(-2)
    kill.on_api_error()
    kill.triggered = 'monitor_failure'
    data = dict(account_positions={}, strategy_positions={}, equity_usdt=98,
                operations=dict(kill=kill.dump(), storage_failed=False))
    saved = reviewed_reset_state(copy.deepcopy(data), tmp_path, cfg)
    state = saved['operations']['kill']
    assert state['triggered'] is None and state['state']['daily_pnl'] == -2
    assert state['state']['consecutive_losses'] == 1 and state['state']['api_errors_1m'] == 1
    assert state['day_start_equity'] == 100 and state['errors'] == kill.dump()['errors']
    atomic_json(tmp_path / 'order_intents.json', {'kh_x': dict(symbol=S, type='market', status='pending')})
    with pytest.raises(ValueError, match='Unresolved'): reviewed_reset_state(data, tmp_path, cfg)


def test_native_close_after_known_partial_collects_both_executions(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        assert await bot.enter(S, 'buy', 100)
        server.partial_close = True
        with pytest.raises(ExchangeError): await bot.close_position(S, 'audit_partial')
        native = server.fill('SELL', server.qty, 'native_remaining')
        native['reduceOnly'] = True
        sl = server.algos[bot.positions[S]['protection']['sl']['orderId']]
        sl.update(actualOrderId=str(native['orderId']), algoStatus='FINISHED')
        await live.refresh_account()
        assert await bot.adopt_native_close(S)
        assert not bot.positions and len(server.orders) == 3
        final = next(t for t in live.trades if t.get('exit_detail'))
        assert final['exit_detail']['net_pnl_usdt'] == pytest.approx(server.wallet - 100)
        assert len(bot.safety.accounted_closes) == 1
        await live.close()
    run(check())


def test_signed_get_clock_recovery_is_bounded(live):
    calls = []
    async def handler(request):
        calls.append(request.url.path)
        if request.url.path.endswith('/time'):
            return httpx.Response(200, json={'serverTime': int(time.time() * 1000) + 1234})
        if calls.count('/fapi/v1/accountConfig') == 1: return httpx.Response(400, json={'code': -1021})
        return httpx.Response(200, json={'canTrade': True})
    async def check():
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        assert (await live._request('GET', '/fapi/v1/accountConfig', retry=True))['canTrade']
        assert calls == ['/fapi/v1/accountConfig', '/fapi/v1/time', '/fapi/v1/accountConfig']
        assert 1200 < live.clock_offset_ms < 1300
        await live.close()
    run(check())


def test_failed_position_read_cannot_commit_half_account_snapshot(live):
    async def handler(request):
        if request.url.path.endswith('/account'):
            return httpx.Response(200, json={'assets': [{'asset': 'USDT', 'walletBalance': '5000',
                'marginBalance': '5001', 'availableBalance': '4900', 'initialMargin': '101'}]})
        return httpx.Response(401, json={'code': -2015})
    async def check():
        live.balance, live._equity, live._free, live._used = 100, 101, 90, 11
        live._last_sync = time.monotonic()
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        with pytest.raises(ExchangeError): await live.refresh_account()
        assert (live.balance, live.equity, live.free, live.used) == (100, 101, 90, 11)
        await live.close()
    run(check())


def test_live_runtime_paths_are_independent_of_cwd(tmp_path, monkeypatch):
    from pathlib import Path
    before = load_config()
    monkeypatch.chdir(tmp_path)
    after = load_config()
    assert Path(after['exchange']['state_dir']).is_absolute()
    assert before['exchange']['state_dir'] == after['exchange']['state_dir']
    assert before['rollout']['operator_policy_file'] == after['rollout']['operator_policy_file']


def test_unprotectable_price_tick_blocks_entry_without_technical_kill(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        live.markets[S]['tick'] = '1'
        assert not await bot.enter(S, 'buy', 100)
        assert not server.orders and not server.qty
        assert bot.safety.kill.triggered is None and live.execution_fault is None
        await live.close()
    run(check())


def test_journal_write_failure_before_post_blocks_entry(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        def failed_log(*args, **kwargs):
            live.journal.failed = True
            return False
        live.journal.log = failed_log
        with patch('asyncio.sleep', new_callable=AsyncMock):
            assert not await bot.enter(S, 'buy', 100)
        assert not server.orders, 'A known pre-POST storage failure must prevent entry submission'
        assert bot.safety.kill.triggered == 'storage_failure'
        await live.close()
    run(check())
