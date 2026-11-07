"""Separate liquidation safety, configured trigger price and normal exits."""
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch
import copy
import httpx

import pytest

from futures_simulator import FuturesSimulator, production_bot
from test_live_transport import live, run


def test_stored_stop_is_safe_despite_mark_below_stop(live):
    # Entirely synthetic values preserve the liquidation/trigger relationship.
    symbol = 'BTC/USDT:USDT'
    pos = dict(side='buy', entry=100, amount=10, leverage=20,
               sl=99.2, tp=101.2, signal_id='kh_synthetic_regression')
    actual = dict(liquidationPrice='97', markPrice='99.1', _trigger_price=99.3)
    live.constrain_actual_liquidation(symbol, pos, actual)
    assert pos['sl'] == 99.2
    assert D('99.2') > D(actual['liquidationPrice']) * D('1.003')
    run(live.close())


@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_mark_crossing_contract_stop_does_not_kill_valid_protection(tmp_path, monkeypatch, side):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        symbol, raw = bot.exchange.symbol('AIOUSDT'), 'AIOUSDT'
        assert await bot.enter(symbol, side, .05)
        stop = D(bot.positions[symbol]['protection']['sl']['trigger'])
        sign = D(1 if side == 'buy' else -1)
        server.prices[raw] = stop + sign * D('.00004')
        server.mark_prices[raw] = stop - sign * D('.00001')
        assert await bot.safety.reconcile_once()
        assert bot.safety.kill.triggered is None
        assert symbol in bot.positions and len(server.orders) == 1
        assert all(a['algoStatus'] == 'NEW' for a in server.algos.values())
        await bot.exchange.close()
    run(check())


@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_reached_stop_uses_normal_exit_path_not_protection_failure(tmp_path, monkeypatch, side):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        symbol, raw = bot.exchange.symbol('AIOUSDT'), 'AIOUSDT'
        assert await bot.enter(symbol, side, .05)
        stop = D(bot.positions[symbol]['protection']['sl']['trigger'])
        sign = D(1 if side == 'buy' else -1)
        server.prices[raw] = stop - sign * D('.00001')
        with patch('asyncio.sleep', new_callable=AsyncMock):
            await bot.manage_exits()
        assert not server.position_rows() and not bot.positions
        assert bot.safety.kill.triggered is None
        result = next(t['exit_detail'] for t in bot.exchange.trades if t.get('exit_detail'))
        assert result['exit_reason'] == 'HARD_SL'
        assert D(result['net_pnl_exact']) == server.wallet - server.start_wallet
        await bot.exchange.close()
    run(check())


def test_actual_liquidation_buffer_breach_still_latches(tmp_path, monkeypatch):
    async def check():
        import json
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server, hard_policy=True)
        symbol, raw = bot.exchange.symbol('AIOUSDT'), 'AIOUSDT'
        assert await bot.enter(symbol, 'buy', .05)
        liquidation = D(server.position_rows(raw)[0]['liquidationPrice'])
        server.mark_prices[raw] = liquidation * D('1.0029')
        with patch('asyncio.sleep', new_callable=AsyncMock):
            assert not await bot.safety.reconcile_once()
        assert bot.safety.kill.triggered and not server.position_rows()
        records = [json.loads(s) for s in (bot.safety.journal.root / 'events.log').read_text().splitlines()]
        failure = next(r for r in records if r['event'] == 'protection_recovery_exception')
        assert failure['stage'] == 'liquidation_validation'
        assert failure['error_type'] == 'ProtectionGuardError'
        assert failure['report']['reason'] == 'mark_liquidation_buffer_breach'
        await bot.exchange.close()
    run(check())


@pytest.mark.parametrize('role', ['sl', 'tp'])
def test_native_fill_with_stale_position_snapshot_is_adopted_without_second_close(tmp_path, monkeypatch, role):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server, legacy=(role == 'tp'))
        live, raw, symbol = bot.exchange, 'AIOUSDT', bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        stale = copy.deepcopy(server.position_rows())
        protection = bot.positions[symbol]['protection'][role]
        server.prices[raw] = D(protection['trigger'])
        child = server.trigger_algo(protection['orderId'])
        reads = 0
        async def handler(request):
            nonlocal reads
            if request.url.path == '/fapi/v3/positionRisk':
                reads += 1
                if reads == 1: return httpx.Response(200, json=stale)
            return await server(request)
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        assert await bot.safety.reconcile_once()
        assert not bot.positions and not server.position_rows()
        assert bot.safety.kill.triggered is None
        assert len(server.orders) == 2
        assert sum(m == 'POST' and p == '/fapi/v1/order' for m,p,_ in server.requests) == 1
        record = live.executions[live.execution_key(symbol, child['orderId'])]
        assert record['native_exit'] and record['options']['native_parent_id'] == protection['orderId']
        result = next(t['exit_detail'] for t in live.trades if t.get('exit_detail'))
        assert result['exit_reason'] == ('HARD_SL' if role == 'sl' else 'take_profit')
        assert D(result['net_pnl_exact']) == server.wallet - server.start_wallet
        await live.close()
    run(check())


def test_native_partial_then_filled_uses_same_child_id_and_exact_fees(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        live, raw, symbol = bot.exchange, 'AIOUSDT', bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        sl = bot.positions[symbol]['protection']['sl']
        server.prices[raw] = D(sl['trigger'])
        child = server.trigger_algo(sl['orderId'], partial=True)
        child_id, reads = child['orderId'], 0
        async def handler(request):
            nonlocal reads
            if request.url.path == '/fapi/v1/order' and request.method == 'GET' and request.url.params.get('orderId') == str(child_id):
                reads += 1
                if reads == 2: server.complete_native_order(child_id)
            return await server(request)
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock): assert await bot.safety.reconcile_once()
        assert reads >= 2 and len(server.orders) == 2
        assert not bot.positions and bot.safety.kill.triggered is None
        final = next(t for t in live.trades if t.get('exit_detail'))
        assert len(final['execution_ids']) == 2
        assert D(final['exit_detail']['net_pnl_exact']) == server.wallet - server.start_wallet
        await live.recover_execution(); bot.merge_execution_state(); await bot._drain_notifications()
        assert len(bot.safety.accounted_closes) == 1
        await live.close()
    run(check())


def test_terminal_partial_native_exit_closes_only_proven_remainder(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        live, raw, symbol = bot.exchange, 'AIOUSDT', bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        original = D(str(bot.positions[symbol]['amount']))
        sl = bot.positions[symbol]['protection']['sl']
        server.prices[raw] = D(sl['trigger'])
        child = server.trigger_algo(sl['orderId'], partial=True)
        child['status'] = 'EXPIRED'
        remaining = abs(server.positions[raw]['qty'])
        assert remaining > 0
        assert await bot.safety.reconcile_once()
        assert bot.safety.kill.triggered is None and not server.position_rows()
        posted = [v for m,p,v in server.requests if m == 'POST' and p == '/fapi/v1/order']
        assert len(posted) == 2 and D(posted[-1]['quantity']) == remaining
        assert D(child['executedQty']) + remaining == original
        result = next(t['exit_detail'] for t in live.trades if t.get('exit_detail'))
        assert result['exit_reason'] == 'HARD_SL'
        assert D(result['net_pnl_exact']) == server.wallet - server.start_wallet
        await live.close()
    run(check())


def test_unknown_native_remainder_blocks_fallback_post_and_keeps_latch(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        live, symbol = bot.exchange, bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        sl = bot.positions[symbol]['protection']['sl']
        # A known child ID exists but neither its completion nor cancellation
        # can be established. No actual fill is invented in the model.
        cid = 7777
        server.orders[cid] = dict(orderId=cid, clientOrderId=sl['clientOrderId'], symbol='AIOUSDT',
            side='SELL', status='NEW', avgPrice='0', executedQty='0', origQty=str(bot.positions[symbol]['amount']), reduceOnly=True)
        server.algos[sl['orderId']].update(algoStatus='FINISHED', actualOrderId=str(cid))
        async def handler(request):
            if request.method == 'DELETE' and request.url.path == '/fapi/v1/order':
                raise httpx.ReadTimeout('signed URL must never be logged', request=request)
            return await server(request)
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock): assert not await bot.safety.reconcile_once()
        assert bot.safety.kill.triggered and server.position_rows()
        assert sum(m == 'POST' and p == '/fapi/v1/order' for m,p,_ in server.requests) == 1
        assert symbol in bot.positions and bot.positions[symbol]['protection']['sl'] == sl
        await live.close()
    run(check())


def test_native_child_snapshot_survives_abort_before_accounting(tmp_path, monkeypatch):
    class Abort(BaseException): pass
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        live, symbol = bot.exchange, bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        bot.save()
        sl = bot.positions[symbol]['protection']['sl']
        server.prices['AIOUSDT'] = D(sl['trigger'])
        child = server.trigger_algo(sl['orderId'])
        with patch.object(live, '_accept_fill', side_effect=Abort()):
            with pytest.raises(Abort): await bot.safety.reconcile_once()
        import json
        stored = json.loads(live.execution_path.read_text())[live.execution_key(symbol, child['orderId'])]
        assert stored['native_exit'] and stored['options']['native_parent_id'] == sl['orderId']
        await live.close()
        bot = await production_bot(tmp_path, monkeypatch, server, startup=False)
        assert await bot.exchange.recover_execution()
        assert await bot.adopt_native_close(symbol)
        assert not bot.positions and len(server.orders) == 2
        assert sum(m == 'POST' and p == '/fapi/v1/order' for m,p,_ in server.requests) == 1
        assert len(bot.safety.accounted_closes) == 1
        await bot.exchange.close()
    run(check())


def test_lost_native_cancel_response_confirms_child_then_closes_remaining(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        live, symbol = bot.exchange, bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        sl = bot.positions[symbol]['protection']['sl']
        server.prices['AIOUSDT'] = D(sl['trigger'])
        child = server.trigger_algo(sl['orderId'], partial=True)
        remaining = abs(server.positions['AIOUSDT']['qty'])
        async def handler(request):
            response = await server(request)
            if request.method == 'DELETE' and request.url.path == '/fapi/v1/order':
                raise httpx.ReadTimeout('private signed request', request=request)
            return response
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock): assert await bot.safety.reconcile_once()
        assert child['status'] == 'CANCELED' and not server.position_rows()
        assert bot.safety.kill.triggered is None
        posted = [v for m,p,v in server.requests if m == 'POST' and p == '/fapi/v1/order']
        assert len(posted) == 2 and D(posted[-1]['quantity']) == remaining
        await live.close()
    run(check())


def test_native_fill_between_account_and_open_order_reads_is_normal_exit(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        live, symbol = bot.exchange, bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        sl = bot.positions[symbol]['protection']['sl']
        fired = False
        async def handler(request):
            nonlocal fired
            if request.url.path == '/fapi/v1/openAlgoOrders' and not fired:
                fired = True
                server.prices['AIOUSDT'] = D(sl['trigger'])
                server.trigger_algo(sl['orderId'])
            return await server(request)
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        assert await bot.safety.reconcile_once()
        assert fired and not bot.positions and not live.positions
        assert bot.safety.kill.triggered is None and len(server.orders) == 2
        assert next(t['exit_detail']['exit_reason'] for t in live.trades if t.get('exit_detail')) == 'HARD_SL'
        await live.close()
    run(check())


def test_configured_mark_trigger_ignores_contract_only_touch(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        bot.cfg['exchange']['working_type'] = 'MARK_PRICE'
        symbol = bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        stop = D(bot.positions[symbol]['protection']['sl']['trigger'])
        server.prices['AIOUSDT'] = stop - D('.00001')
        server.mark_prices['AIOUSDT'] = stop + D('.00004')
        await bot.manage_exits()
        assert symbol in bot.positions and len(server.orders) == 1
        assert bot.safety.kill.triggered is None
        await bot.exchange.close()
    run(check())


def test_native_price_pending_restart_keeps_origin_and_no_duplicate_exit(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        live, symbol = bot.exchange, bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        sl = bot.positions[symbol]['protection']['sl']
        server.prices['AIOUSDT'] = D(sl['trigger'])
        child = server.trigger_algo(sl['orderId'])
        async def handler(request):
            response = await server(request)
            if request.url.path == '/fapi/v1/order' and request.url.params.get('orderId') == str(child['orderId']):
                data = response.json(); data.update(avgPrice='0', cumQuote='0')
                return httpx.Response(200, json=data)
            return response
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock): assert await bot.safety.reconcile_once()
        assert not server.position_rows() and len(server.orders) == 2
        assert not any(t.get('exit_detail') for t in live.trades)
        await live.close()
        bot = await production_bot(tmp_path, monkeypatch, server, startup=False)
        assert await bot.exchange.recover_execution()
        bot.exchange.management_faults[symbol]['last_attempt']-=10
        assert await bot.safety.reconcile_once()
        assert len(server.orders) == 2 and not bot.safety.kill.triggered
        result = next(t['exit_detail'] for t in bot.exchange.trades if t.get('exit_detail'))
        assert result['exit_reason'] == 'HARD_SL'
        assert D(result['net_pnl_exact']) == server.wallet - server.start_wallet
        await bot.exchange.close()
    run(check())


@pytest.mark.parametrize('mark', [None, '0', 'NaN'])
def test_missing_or_invalid_mark_stays_fail_closed_and_safe_to_serialize(live, mark):
    import json
    from exchange.protection_state import ProtectionGuardError
    pos = dict(side='buy', entry=100, sl=99.2, leverage=20, signal_id='guard_invalid_mark')
    with pytest.raises(ProtectionGuardError) as caught:
        live.constrain_actual_liquidation('BTC/USDT:USDT', pos,
            {'liquidationPrice':'95', 'markPrice':mark, '_trigger_price':100})
    assert caught.value.reason == 'invalid_mark_price'
    json.dumps(caught.value.details, allow_nan=False)
    run(live.close())


def test_no_valid_replacement_stop_before_trigger_still_fails_closed(live):
    from exchange.protection_state import ProtectionGuardError
    pos = dict(side='buy', entry=100, sl=99.2, leverage=20, signal_id='guard_no_clearance')
    with pytest.raises(ProtectionGuardError) as caught:
        live.constrain_actual_liquidation('BTC/USDT:USDT', pos,
            {'liquidationPrice':'99.5', 'markPrice':'100', '_trigger_price':99.7})
    assert caught.value.reason == 'no_valid_stop_before_trigger'
    assert pos['sl'] == 99.2
    run(live.close())


@pytest.mark.parametrize('mismatch', ['side', 'entry'])
def test_identity_mismatch_cannot_be_hidden_by_native_reconciliation(tmp_path, monkeypatch, mismatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        symbol = bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        if mismatch == 'side': server.positions['AIOUSDT']['qty'] *= -1
        else: server.positions['AIOUSDT']['entry'] += D('.001')
        bot.safety.trip = AsyncMock()
        bot.exchange.inspect_native_exits = AsyncMock()
        assert not await bot.safety.reconcile_once()
        bot.safety.trip.assert_awaited_once_with('reconcile_mismatch')
        bot.exchange.inspect_native_exits.assert_not_awaited()
        await bot.exchange.close()
    run(check())


def test_flat_close_settles_known_child_even_when_open_orders_snapshot_misses_it(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        live, symbol = bot.exchange, bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        sl = bot.positions[symbol]['protection']['sl']
        child_id = 8888
        async def handler(request):
            response = await server(request)
            if request.method == 'POST' and request.url.path == '/fapi/v1/order' and request.url.params.get('reduceOnly') == 'true':
                server.orders[child_id] = dict(orderId=child_id, clientOrderId=sl['clientOrderId'], symbol='AIOUSDT',
                    side='SELL', status='NEW', avgPrice='0', executedQty='0', origQty=sl['amount'], reduceOnly=True)
                server.algos[sl['orderId']].update(algoStatus='FINISHED', actualOrderId=str(child_id))
            # The model's openOrders intentionally omits the child. Its known
            # parent actualOrderId must still prevent leaving a live remainder.
            return response
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock): await bot.close_position(symbol, 'manual_exit')
        assert not bot.positions and not server.position_rows()
        assert server.orders[child_id]['status'] == 'CANCELED'
        assert sum(m == 'POST' and p == '/fapi/v1/order' for m,p,_ in server.requests) == 2
        assert bot.safety.kill.triggered is None
        await live.close()
    run(check())


@pytest.mark.parametrize('liquidation', ['NaN', '1e1000'])
def test_invalid_liquidation_cannot_poison_guard_journal_with_nonfinite_numbers(live, liquidation):
    import json
    from exchange.protection_state import ProtectionGuardError
    pos = dict(side='buy', entry=100, sl=99.2, leverage=20, signal_id='bad_liq')
    with pytest.raises(ProtectionGuardError) as caught:
        live.constrain_actual_liquidation('BTC/USDT:USDT', pos,
            {'liquidationPrice':liquidation, 'markPrice':'100', '_trigger_price':100})
    assert caught.value.reason == 'invalid_liquidation_price'
    json.dumps(caught.value.details, allow_nan=False)
    assert not live.journal.failed
    run(live.close())
