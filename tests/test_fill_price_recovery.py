"""Incomplete first order responses are hypotheses tested offline, not AIO proof."""
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from futures_simulator import FuturesSimulator, production_bot
from test_live_transport import run
from exchange.fill_price import execution_price
from tools.reset_safety import reviewed_reset_state
from exchange.orders import ExecutionUnknown


@pytest.mark.parametrize('reducing', [False, True])
@pytest.mark.parametrize('quote_available', [False, True])
def test_filled_first_response_missing_average_recovers_without_extra_post(tmp_path, monkeypatch, reducing, quote_available):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        symbol = bot.exchange.symbol('AIOUSDT')
        if reducing: assert await bot.enter(symbol, 'sell', .05)
        async def handler(request):
            response = await server(request)
            if request.method == 'POST' and request.url.path == '/fapi/v1/order':
                order = response.json()
                order['cumQuote'] = str(Decimal(order['avgPrice']) * Decimal(order['executedQty'])) if quote_available else '0'
                order['avgPrice'] = '0.000000'
                return httpx.Response(200, json=order)
            return response
        await bot.exchange.client.aclose()
        bot.exchange.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock):
            if reducing: await bot.close_position(symbol, 'price_recovery_test')
            else: assert await bot.enter(symbol, 'sell', .05)
        assert bot.safety.kill.triggered is None and bot.exchange.execution_fault is None
        assert len(server.orders) == (2 if reducing else 1)
        if reducing:
            assert not server.position_rows() and not bot.positions
        else:
            assert set(bot.positions[symbol]['protection']) == {'sl'}
            assert bot.exchange.trades[0]['price'] == .05
        # The initial zero average is retained even after a later priced query.
        intent = next(i for i in bot.exchange.intents.values() if i['type'] == 'market' and bool(i['params'].get('reduceOnly')) == reducing)
        assert Decimal(intent['observations'][0]['order']['avgPrice']) == 0
        if not quote_available: assert Decimal(intent['observations'][-1]['order']['avgPrice']) == Decimal('.05')
        await bot.exchange.close()
    run(check())


@pytest.mark.parametrize('restart', [False, True])
def test_unavailable_entry_price_keeps_fill_fact_closes_and_later_accounts_once(tmp_path, monkeypatch, restart):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server, hard_policy=True)
        live, symbol = bot.exchange, bot.exchange.symbol('AIOUSDT')
        async def handler(request):
            response = await server(request)
            if request.url.path == '/fapi/v1/order' and request.method in ('GET', 'POST'):
                order = response.json()
                if order.get('orderId') == 1000:
                    order.update(avgPrice='0', cumQuote='0')
                    return httpx.Response(200, json=order)
            return response
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock):
            assert not await bot.enter(symbol, 'sell', .0499)
        assert bot.safety.kill.triggered == 'fill_price_unavailable'
        assert not server.position_rows() and len(server.orders) == 2
        entry = live.executions[live.execution_key(symbol, 1000)]
        assert entry['order']['status'] == 'FILLED' and entry['fill_price_pending']
        assert entry['entry_facts']['amount'] == float(server.orders[1000]['executedQty'])
        assert entry['entry_facts']['entry'] is None
        assert not any(t.get('exit_detail') for t in live.trades)
        # The reset tool must not mistake terminal raw orders for completed accounting.
        import json
        state = json.loads(bot.state_path.read_text())
        with pytest.raises(ValueError, match='Unresolved|Pending'):
            reviewed_reset_state(state, bot.state_path.parent, bot.cfg)
        await live.close()
        if restart:
            bot = await production_bot(tmp_path, monkeypatch, server, startup=False, hard_policy=True)
            live = bot.exchange
        else:
            live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(server))
        assert await live.recover_execution()
        bot.merge_execution_state()
        await bot._drain_notifications()
        assert len(server.orders) == 2 and not server.position_rows()
        final = [t for t in live.trades if t.get('exit_detail')]
        assert len(final) == 1
        assert Decimal(final[0]['exit_detail']['net_pnl_exact']) == server.wallet - server.start_wallet
        assert final[0]['exit_detail']['tp'] is None  # Never invent historical protection.
        assert final[0]['exit_detail']['expected_entry'] == .0499
        assert final[0]['exit_detail']['actual_entry'] == .05
        assert final[0]['exit_detail']['entry_slippage_bps'] != 0
        previous = bot.safety.kill.state.daily_pnl
        assert await live.recover_execution()
        bot.merge_execution_state()
        await bot._drain_notifications()
        assert bot.safety.kill.state.daily_pnl == previous
        assert bot.safety.kill.triggered == 'fill_price_unavailable'
        assert next(t for t in live.trades if not t['reduceOnly'])['emergency']
        assert final[0]['margin'] == next(t for t in live.trades if not t['reduceOnly'])['margin']
        await live.close()
    run(check())


@pytest.mark.parametrize('restart', [False, True])
def test_filled_close_price_pending_never_resends_close(tmp_path, monkeypatch, restart):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        live, symbol = bot.exchange, bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'sell', .05)
        async def handler(request):
            response = await server(request)
            if request.url.path == '/fapi/v1/order' and request.method in ('GET', 'POST'):
                order = response.json()
                if order.get('orderId') == 1001:
                    order.update(avgPrice='0', cumQuote='0')
                    return httpx.Response(200, json=order)
            return response
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock):
            with pytest.raises(ExecutionUnknown): await bot.close_position(symbol, 'exit_price_pending')
            await bot.safety.trip('fill_price_unavailable')
        assert not server.position_rows() and len(server.orders) == 2
        assert live.executions[live.execution_key(symbol, 1001)]['fill_price_pending']
        assert not any(t.get('exit_detail') for t in live.trades)
        await live.close()
        if restart:
            bot = await production_bot(tmp_path, monkeypatch, server, startup=False)
            live = bot.exchange
        else:
            live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(server))
        assert await live.recover_execution()
        await live.refresh_account()
        assert await bot.adopt_native_close(symbol)
        assert not bot.positions and len(server.orders) == 2
        final = [t for t in live.trades if t.get('exit_detail')]
        assert len(final) == 1
        assert Decimal(final[0]['exit_detail']['net_pnl_exact']) == server.wallet - server.start_wallet
        previous = bot.safety.kill.state.daily_pnl
        assert await live.recover_execution()
        bot.merge_execution_state()
        await bot._drain_notifications()
        assert bot.safety.kill.state.daily_pnl == previous
        await live.close()
    run(check())


def test_late_pricing_of_partial_exit_keeps_actual_last_exit_reason(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server, hard_policy=True)
        live, symbol = bot.exchange, bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'sell', .05)
        server.partial_close = True
        async def handler(request):
            response = await server(request)
            if request.url.path == '/fapi/v1/order' and request.method in ('GET', 'POST'):
                order = response.json()
                if order.get('orderId') == 1001:
                    order.update(avgPrice='0', cumQuote='0')
                    return httpx.Response(200, json=order)
            return response
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep', new_callable=AsyncMock):
            with pytest.raises(ExecutionUnknown): await bot.close_position(symbol, 'partial_exit')
            server.prices['AIOUSDT'] = Decimal('.0499')
            await bot.safety.trip('fill_price_unavailable')
        assert not server.position_rows() and len(server.orders) == 3
        assert not any(t.get('exit_detail') for t in live.trades)
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(server))
        assert await live.recover_execution()
        bot.merge_execution_state()
        await bot._drain_notifications()
        final = [t for t in live.trades if t.get('exit_detail')]
        assert len(final) == 1 and str(final[0]['id']) == '1002'
        assert final[0]['exit_detail']['exit_reason'] == 'kill_switch'
        assert Decimal(final[0]['exit_detail']['net_pnl_exact']) == server.wallet - server.start_wallet
        assert len(bot.safety.accounted_closes) == 1
        await live.close()
    run(check())


def test_actual_cumulative_quote_supplies_vwap_not_signal_price():
    price, source = execution_price(dict(avgPrice='0', executedQty='276043', cumQuote='9749.8387600', price='0.1'))
    assert price == Decimal('.0353200') and source == 'cumQuote/executedQty'


@pytest.mark.parametrize('value', ['0', '-1', 'NaN', 'Infinity', None])
def test_invalid_price_with_no_quote_never_becomes_an_estimate(value):
    price, _ = execution_price(dict(avgPrice=value, executedQty='276043', cumQuote='0', price='0.03532'))
    assert price is None
