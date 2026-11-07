import asyncio
import copy
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch

import pytest

from futures_simulator import FuturesSimulator, production_bot
from test_live_transport import run
from exchange.orders import ExchangeError


@pytest.mark.parametrize('equity', ['1000', '4912.19949871', '25000'])
@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_real_constructor_three_symbols_and_exact_round_trip(tmp_path, monkeypatch, equity, side):
    async def check():
        server = FuturesSimulator(equity)
        bot = await production_bot(tmp_path, monkeypatch, server)
        assert bot.exchange.on_api_error.__self__ is bot.safety.kill
        assert bot.safety.evidence is not None and bot.safety.ready
        for raw in server.specs:
            symbol = bot.exchange.symbol(raw)
            before = bot.exchange.equity
            current_side = ('sell' if side == 'buy' else 'buy') if raw == 'AIOUSDT' else side
            assert await bot.enter(symbol, current_side, float(server.prices[raw]))
            pos = bot.positions[symbol]
            assert pos['leverage'] == server.leverage[raw] == bot.exchange.selected_leverage[symbol]['leverage']
            assert pos['margin'] <= before * .1 + 1e-8
            assert pos['margin'] == pytest.approx(pos['amount'] * pos['entry'] / pos['leverage'])
            assert pos['entry_fee'] == pytest.approx(float(server.fills[-1]['commission']))
            assert all(o['is_algo'] for o in pos['protection'].values())
            assert await bot.safety.reconcile_once()
        bot.save()
        original = copy.deepcopy(bot.positions)
        await bot.exchange.close()
        # Restore the same account into an entirely new, correctly wired runtime.
        bot = await production_bot(tmp_path, monkeypatch, server, startup=False)
        assert await bot.exchange.recover_execution()
        await bot.exchange.refresh_account()
        bot.merge_execution_state()
        for symbol, pos in original.items():
            assert bot.positions[symbol]['leverage'] == pos['leverage']
            assert bot.positions[symbol]['amount'] == pos['amount']
        for raw in server.specs:
            server.prices[raw] *= D('1.001') if server.positions[raw]['qty'] > 0 else D('.999')
            await bot.close_position(bot.exchange.symbol(raw), 'audit_exit')
        assert not bot.positions and not server.position_rows()
        assert not await bot.exchange.get_open_orders()
        net = sum((D(t['exit_detail']['net_pnl_exact']) for t in bot.exchange.trades if t.get('exit_detail')), D(0))
        assert net == server.wallet - server.start_wallet
        before = bot.safety.kill.state.daily_pnl
        await bot.exchange.recover_execution()
        bot.merge_execution_state()
        await bot._drain_notifications()
        assert bot.safety.kill.state.daily_pnl == before
        assert len(bot.safety.accounted_closes) == 3
        await bot.exchange.close()
    run(check())


@pytest.mark.parametrize('fault', ['lost_entry', 'lost_close', 'late_fills', 'partial_close', 'sl_reject'])
def test_production_wiring_fault_matrix(tmp_path, monkeypatch, fault):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server, hard_policy=True)
        symbol = bot.exchange.symbol('AIOUSDT')
        server.lose_entry_response = fault == 'lost_entry'
        server.delay_accounting = fault == 'late_fills'
        server.reject_role = {'sl_reject': 'STOP_MARKET', 'tp_reject': 'TAKE_PROFIT_MARKET'}.get(fault)
        with patch('asyncio.sleep', new_callable=AsyncMock):
            entered = await bot.enter(symbol, 'buy', .05)
            if fault in ('sl_reject'):
                assert not entered and not bot.safety.kill.triggered
                assert bot.exchange.management_faults[symbol]['reason'] == 'entry_confirmed_management_failed'
                assert not server.position_rows()
            else:
                assert entered and bot.positions[symbol]['protection']
                assert len(server.orders) == 1
                if fault == 'late_fills':
                    assert bot.exchange.trades[0]['accounting_status'] == 'pending'
                    assert await bot.safety.reconcile_once()
                    server.delay_accounting = False
                    await bot.exchange.sync_accounting()
                    bot.merge_execution_state()
                server.lose_close_response = fault == 'lost_close'
                server.partial_close = fault == 'partial_close'
                if fault == 'partial_close':
                    initial = bot.positions[symbol]['amount']
                    with pytest.raises(ExchangeError): await bot.close_position(symbol, 'audit_partial')
                    assert bot.positions[symbol]['amount'] == initial
                    await bot.safety.trip('audit_partial_close')
                    assert len(server.orders) == 3
                else:
                    await bot.close_position(symbol, 'audit_exit')
                    assert len(server.orders) == 2
        assert not server.position_rows() and not await bot.exchange.get_open_orders()
        assert len([t for t in bot.exchange.trades if t.get('exit_detail')]) == 1
        net = next(D(t['exit_detail']['net_pnl_exact']) for t in bot.exchange.trades if t.get('exit_detail'))
        assert net == server.wallet - server.start_wallet
        await bot.exchange.close()
    run(check())


@pytest.mark.parametrize('when', ['entry', 'close'])
def test_restart_after_abrupt_abort_between_post_and_response(tmp_path, monkeypatch, when):
    class AbruptAbort(BaseException): pass
    async def check():
        import httpx
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        symbol = bot.exchange.symbol('AIOUSDT')
        if when == 'close': assert await bot.enter(symbol, 'buy', .05)
        bot.save()
        # Execute each HTTP request once, then interrupt before its response.
        async def transport(request):
            response = await server(request)
            if request.method == 'POST' and request.url.path == '/fapi/v1/order': raise AbruptAbort()
            return response
        await bot.exchange.client.aclose()
        bot.exchange.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(transport))
        with pytest.raises(AbruptAbort):
            if when == 'entry': await bot.enter(symbol, 'buy', .05)
            else: await bot.close_position(symbol, 'audit_exit')
        await bot.exchange.close()
        restored = await production_bot(tmp_path, monkeypatch, server, startup=False)
        await restored.safety.startup()
        assert restored.safety.ready and not restored.safety.kill.blocks_entries
        assert len(server.orders) == (1 if when == 'entry' else 2)
        if when == 'entry':
            assert symbol in restored.positions and restored.positions[symbol]['protection']
            assert not await restored.enter(symbol, 'buy', .05)
        else:
            assert not server.position_rows()
            assert len([t for t in restored.exchange.trades if t.get('exit_detail')]) == 1
            assert not await restored.exchange.get_open_orders()
        await restored.exchange.close()
    run(check())


def test_actual_monitor_cycle_with_evidence_and_all_guards(tmp_path, monkeypatch):
    class EndCycle(BaseException): pass
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        assert await bot.enter(bot.exchange.symbol('AIOUSDT'), 'buy', .05)
        bot.safety.heartbeat()
        # Interrupt only the outer monitor interval, after one complete cycle.
        async def interval(delay):
            assert delay == bot.cfg['kill_switch']['monitor_interval_sec']
            raise EndCycle()
        with patch('infra.supervisor.asyncio.sleep', side_effect=interval):
            with pytest.raises(EndCycle): await bot.safety.monitor()
        assert bot.safety.kill.triggered is None and bot.safety.ready
        assert bot.safety.last_account and bot.safety.last_recon and bot.safety.last_funding
        assert bot.safety.evidence.data['samples']
        assert bot.state_path.exists()
        await bot.exchange.close()
    run(check())
