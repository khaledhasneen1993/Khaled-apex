"""Offline exchange-filter and definitive-rejection regressions."""
from decimal import Decimal
from unittest.mock import AsyncMock, patch
import httpx
import pytest
from test_live_transport import live, run, runtime_with_transport
from exchange.quantity import quantity_rules, validate_quantity, OrderValidationError
from exchange.orders import safe_create_order, ExchangeError, ExecutionUnknown, OrderRejected

S = 'BTC/USDT:USDT'


def rules(market_step='0', lot_step='0.001', market_max='0.5'):
    return {'LOT_SIZE': {'minQty': '0.001', 'maxQty': '1000', 'stepSize': lot_step},
            'MARKET_LOT_SIZE': {'minQty': '0.001', 'maxQty': market_max, 'stepSize': market_step}}


def test_zero_market_step_keeps_market_maximum():
    bound = quantity_rules(rules())
    assert bound['max'] == Decimal('0.5') and bound['step'] == Decimal('.001')


def test_stricter_lot_maximum_is_also_preserved():
    data = rules(market_max='5000')
    data['LOT_SIZE']['maxQty'] = '10'
    assert quantity_rules(data)['max'] == 10


def test_common_quantity_step_covers_both_grids():
    data = rules(market_step='.003', lot_step='.002')
    data['LOT_SIZE']['minQty'] = '0'
    data['MARKET_LOT_SIZE']['minQty'] = '0'
    assert quantity_rules(data)['step'] == Decimal('.006')


@pytest.mark.parametrize('qty,valid', [(0.5, True), (.501, False), (.0005, False), (.3333, False)])
def test_bounds_and_step_are_decimal_exact(live, qty, valid):
    market = live.market(S)
    market['filters'] = rules()
    if valid: assert validate_quantity(market, qty)
    else:
        with pytest.raises(OrderValidationError): validate_quantity(market, qty)
    run(live.close())


def test_oversize_entry_skipped_without_post_or_policy_changes(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        market = live.market(S)
        market['filters'] = rules()
        market['limits']['amount']['max'] = .5
        before = live.cfg['margin_fraction_per_position']
        assert not await bot.enter(S, 'buy', 100)
        choice = live.selected_leverage[S]
        assert choice['leverage'] == 10 and choice['amount'] == 1
        assert choice['validation_reason'] == 'quantity_above_max'
        assert not choice['entry_order_valid'] and not choice['confirmed']
        assert not server.orders and not live.intents and not server.algos
        assert not bot.safety.kill.triggered and live.execution_fault is None
        assert live.cfg['margin_fraction_per_position'] == before == .1
        await live.close()
    run(check())


def test_limits_rechecked_immediately_before_order_send(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        await live.refresh_account(); await live.select_leverage(S, 10, 100)
        live.market(S)['limits']['amount']['max'] = .5
        with pytest.raises(OrderValidationError):
            await live.create_order(S, 'market', 'buy', 1, params={'expected_price': 100, 'exit_atr': .4})
        assert not server.orders and not live.intents and live.execution_fault is None
        await live.close()
    run(check())


def test_filter_failure_after_filled_entry_still_flattens(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        live.protect = AsyncMock(side_effect=OrderValidationError('quantity_above_max'))
        assert not await bot.enter(S, 'buy', 100)
        assert not server.qty and len(server.orders) == 2
        assert bot.safety.kill.triggered is None
        assert live.management_faults[S]['reason'] == 'entry_confirmed_management_failed'
        await live.close()
    run(check())


def test_4005_rejection_is_durable_and_not_network_unknown(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        original = server.__call__
        posts = []
        async def handler(request):
            if request.url.path == '/fapi/v1/order' and request.method == 'POST':
                posts.append(request.url.params['newClientOrderId'])
                return httpx.Response(400, json={'code': -4005})
            return await original(request)
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        with patch('exchange.orders.asyncio.sleep', new_callable=AsyncMock):
            assert not await bot.enter(S, 'buy', 100)
        assert len(posts) == 1 and not server.orders and not live.trades
        assert live.intents[posts[0]]['status'] == 'rejected'
        assert bot.safety.kill.triggered == 'order_rejected'
        assert not bot.safety.ready
        await live.close()
    run(check())


def test_rejected_post_with_query_failure_remains_unknown(live):
    live.submit_order = AsyncMock(side_effect=ExchangeError(400, -4005))
    live.query_order_by_client_id = AsyncMock(side_effect=ExchangeError(503, -1007))
    async def check():
        with pytest.raises(ExecutionUnknown):
            await safe_create_order(live, {'symbol': S, 'type': 'market', 'side': 'buy', 'amount': 1}, live.journal)
        assert live.submit_order.await_count == 1
        assert next(iter(live.intents.values()))['status'] == 'pending'
        await live.close()
    run(check())


def test_oversize_remote_close_uses_proven_residual_chunks(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        assert await bot.enter(S, 'buy', 100)
        original = server.__call__
        async def handler(request):
            server.qty = round(server.qty, 3)  # Binance quantities are decimal strings, not accumulated float noise.
            response = await original(request)
            server.qty = round(server.qty, 3)
            return response
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        live.market(S)['limits']['amount']['max'] = .4
        for _ in range(2):
            with pytest.raises(ExchangeError): await bot.close_position(S, 'kill_switch')
        await bot.close_position(S, 'kill_switch')
        closes = [t for t in live.trades if t['reduceOnly']]
        assert [t['amount'] for t in closes] == [.4, .4, .2]
        assert not server.qty and not bot.positions
        assert len([t for t in closes if t.get('exit_detail')]) == 1
        await live.close()
    run(check())


def test_real_loader_and_preflight_preserve_zero_step_market_cap(live):
    from tools.demo_preflight import preflight
    from test_live_transport import SimulatedBinance
    server = SimulatedBinance()
    async def handler(request):
        assert request.method == 'GET'
        path = request.url.path
        if path == '/fapi/v1/exchangeInfo':
            fs = [{'filterType': key, **value} for key, value in rules().items()]
            fs += [{'filterType': 'PRICE_FILTER', 'tickSize': '.01'}, {'filterType': 'MIN_NOTIONAL', 'notional': '5'}]
            return httpx.Response(200, json={'symbols': [{'symbol': 'BTCUSDT', 'baseAsset': 'BTC', 'quoteAsset': 'USDT',
                'marginAsset': 'USDT', 'contractType': 'PERPETUAL', 'status': 'TRADING', 'filters': fs}]})
        if path == '/fapi/v1/positionSide/dual': return httpx.Response(200, json={'dualSidePosition': False})
        if path == '/fapi/v1/multiAssetsMargin': return httpx.Response(200, json={'multiAssetsMargin': False})
        if path == '/fapi/v1/accountConfig': return httpx.Response(200, json={'canTrade': True})
        if path == '/fapi/v1/symbolConfig': return httpx.Response(200, json=[])
        if path == '/fapi/v1/userTrades': return httpx.Response(200, json=[])
        return await server(request)
    async def check():
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        report = await preflight(live, 'BTCUSDT', websocket=False)
        assert report['account_can_trade'] is True
        assert report['candidate']['quantity_limits']['max'] == .5
        assert report['candidate']['validation_reason'] == 'quantity_above_max'
        assert not report['candidate']['entry_order_valid'] and report['requires_operator_review']
        assert live.market(S)['step'] == '.001' or Decimal(live.market(S)['step']) == Decimal('.001')
        assert not server.orders
        await live.close()
    run(check())


def test_last_defensive_validation_marks_unsent_intent_rejected(live):
    async def check():
        live.market(S)['limits']['amount']['max'] = .5
        async def no_http(request): pytest.fail('No request should be sent for an invalid quantity')
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(no_http))
        with pytest.raises(OrderValidationError):
            await safe_create_order(live, {'symbol': S, 'type': 'market', 'side': 'buy', 'amount': 1}, live.journal)
        assert next(iter(live.intents.values()))['status'] == 'rejected'
        await live.close()
    run(check())
