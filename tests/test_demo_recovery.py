"""Network-free lifecycle fault injection against the real Binance HTTP adapter."""
import asyncio
import base64
import copy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode

import httpx
import pytest
from test_live_transport import live, run, SimulatedBinance, runtime_with_transport
from exchange.orders import ExchangeError, ExecutionUnknown, safe_create_order
from exchange.live_exchange import LiveExchange
from risk.config import load_config

SYMBOL = 'BTC/USDT:USDT'


def test_repro_filled_empty_trades_retains_protected_entry(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        original = server.__call__
        async def delayed(request):
            if request.url.path.endswith('/userTrades'): return httpx.Response(200, json=[])
            return await original(request)
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(delayed))
        assert await bot.enter(SYMBOL, 'buy', 100)
        assert server.qty > 0 and len(server.algos) == 2
        assert live.trades[0]['accounting_status'] == 'pending'
        assert not bot.safety.kill.triggered
        assert json.loads(live.execution_path.read_text())[SYMBOL + ':1']['order']['status'] == 'FILLED'
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(server))
        await live.sync_accounting(); await live.sync_accounting()
        assert len(live.trades) == 1 and live.trades[0]['accounting_status'] == 'complete'
        await live.close()
    run(check())


def test_repro_preserve_error_type_and_code(live):
    async def check():
        live._create_order = AsyncMock(side_effect=ExecutionUnknown(503, -1007))
        with pytest.raises(ExecutionUnknown) as error: await live.create_order(SYMBOL, 'market', 'buy', 1)
        assert error.value.code == -1007
        text = (live.journal.root / 'events.log').read_text()
        assert 'ExecutionUnknown' in text and 'reconcile_before_resend' in text
        await live.close()
    run(check())


async def prepare(live, tmp_path, handler=None):
    bot, server = await runtime_with_transport(live, tmp_path)
    if handler:
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler(server)))
    return bot, server


@pytest.mark.parametrize('reducing', [False, True])
def test_lost_post_response_adopts_same_order_once(live, tmp_path, reducing):
    counts = []
    def handler(server):
        async def send(request):
            response = await server(request)
            if request.method == 'POST' and request.url.path == '/fapi/v1/order':
                counts.append(request.url.params['newClientOrderId'])
                raise httpx.ReadTimeout('sensitive signed URL', request=request)
            return response
        return send
    async def check():
        bot, server = await prepare(live, tmp_path, handler)
        assert await bot.enter(SYMBOL, 'buy', 100)
        if reducing: await bot.close_position(SYMBOL, 'manual_close')
        assert len(counts) == (2 if reducing else 1)
        assert len(set(counts)) == len(counts)
        assert len(live.trades) == len(counts)
        assert bool(server.qty) != reducing
        assert 'sensitive' not in (live.journal.root / 'events.log').read_text()
        await live.close()
    run(check())


@pytest.mark.parametrize('partial', [False, True])
def test_delayed_filled_and_partial_then_full(live, tmp_path, partial):
    reads = 0
    def handler(server):
        async def send(request):
            nonlocal reads
            response = await server(request)
            if request.url.path == '/fapi/v1/order':
                if request.method == 'POST':
                    data = response.json(); data.update(status='PARTIALLY_FILLED' if partial else 'NEW', executedQty='.5' if partial else '0')
                    return httpx.Response(200, json=data)
                reads += 1
                if reads == 1:
                    data = response.json(); data.update(status='PARTIALLY_FILLED' if partial else 'NEW', executedQty='.5' if partial else '0')
                    return httpx.Response(200, json=data)
            return response
        return send
    async def check():
        bot, server = await prepare(live, tmp_path, handler)
        with patch('exchange.execution.asyncio.sleep', new_callable=AsyncMock):
            assert await bot.enter(SYMBOL, 'buy', 100)
        assert bot.positions[SYMBOL]['amount'] == 1
        assert len(live.trades) == 1 and reads == 2
        await live.close()
    run(check())


def test_close_succeeded_state_write_failed_recovers_without_resend(live, tmp_path):
    async def check():
        bot, server = await prepare(live, tmp_path)
        assert await bot.enter(SYMBOL, 'buy', 100)
        with patch.object(bot, 'save', side_effect=OSError('disk full')):
            with pytest.raises(OSError): await bot.close_position(SYMBOL, 'manual')
        assert not server.qty
        # Recreate state from the last saved entry, then recover canonical close.
        bot.restore()
        assert SYMBOL in bot.positions
        await live.refresh_account()
        assert await bot.adopt_native_close(SYMBOL)
        assert not bot.positions and len(server.orders) == 2
        assert len([t for t in live.trades if t['reduceOnly']]) == 1
        await live.close()
    run(check())


@pytest.mark.parametrize('observed', [False, True])
def test_restart_after_post_before_accounting(live, tmp_path, observed):
    async def check():
        bot, server = await prepare(live, tmp_path)
        options = {'reduceOnly': False, 'expected_price': 100, 'exit_atr': .4,
                   'leverage': 10, 'risk_cfg': live.cfg, 'equity': 100, 'newClientOrderId': 'kh_crash'}
        live.persist_intent('kh_crash', {'symbol': SYMBOL, 'type': 'market', 'side': 'buy', 'amount': 1, 'params': options})
        order = server.fill('BUY', 1, 'kh_crash')
        if observed: live.resolve_intent('kh_crash', order)
        # New adapter reads the files left at this crash boundary.
        recovered = LiveExchange(live.cfg, SimpleNamespace(acquire=AsyncMock()))
        recovered.markets = live.markets
        await recovered.client.aclose()
        recovered.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(server))
        await recovered.refresh_account()
        assert await recovered.recover_execution()
        assert await recovered.recover_execution()
        assert len(server.orders) == 1 and len(recovered.trades) == 1
        assert len(server.algos) == 2 and recovered.pending_protection[SYMBOL]['leverage'] == 10
        assert recovered.trades[0]['recovered']
        await recovered.close(); await live.close()
    run(check())


def test_pending_ambiguity_prevents_second_post(live):
    async def check():
        live.submit_order = AsyncMock(side_effect=ExchangeError())
        live.query_order_by_client_id = AsyncMock(return_value=None)
        params = {'symbol': SYMBOL, 'type': 'market', 'side': 'buy', 'amount': 1}
        with patch('exchange.orders.asyncio.sleep', new_callable=AsyncMock):
            for _ in range(2):
                with pytest.raises(ExecutionUnknown): await safe_create_order(live, params, live.journal)
        assert live.submit_order.await_count == 1
        await live.close()
    run(check())


@pytest.mark.parametrize('failure_kind', ['STOP_MARKET', 'TAKE_PROFIT_MARKET'])
def test_sl_or_tp_failure_closes_affected_symbol(live, tmp_path, failure_kind):
    def handler(server):
        async def send(request):
            if request.url.path == '/fapi/v1/algoOrder' and request.method == 'POST' and request.url.params['type'] == failure_kind:
                return httpx.Response(400, json={'code': -1111})
            return await server(request)
        return send
    async def check():
        bot, server = await prepare(live, tmp_path, handler)
        with patch('asyncio.sleep', new_callable=AsyncMock):
            assert not await bot.enter(SYMBOL, 'buy', 100)
        assert not server.qty and not bot.safety.kill.triggered
        assert live.management_faults[SYMBOL]['reason'] == 'entry_confirmed_management_failed'
        assert len(live.trades) == 2 and live.trades[-1]['emergency']
        await live.close()
    run(check())


def test_orphan_algo_cancelled_when_flat(live, tmp_path):
    async def check():
        bot, server = await prepare(live, tmp_path)
        server.algos[100] = {'algoId': 100, 'clientAlgoId': 'orphan', 'symbol': 'BTCUSDT', 'algoStatus': 'NEW', 'reduceOnly': True}
        from exchange.reconciler import reconcile
        report = await reconcile(live, bot, live.journal)
        assert report['orphan_orders'] == ['orphan']
        assert server.algos[100]['algoStatus'] == 'CANCELED'
        await live.close()
    run(check())


def test_rsa_http_signature(monkeypatch, tmp_path):
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives import serialization, hashes
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    path = tmp_path / 'private.pem'
    path.write_bytes(private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    path.chmod(0o600)
    monkeypatch.setenv('BINANCE_API_KEY', 'test-key')
    monkeypatch.delenv('BINANCE_API_SECRET', raising=False)
    monkeypatch.setenv('BINANCE_RSA_PRIVATE_KEY_PATH', str(path))
    cfg = load_config(); cfg['exchange'].update(mode='testnet', state_dir=str(tmp_path))
    async def handler(request):
        params = list(request.url.params.multi_items()); signature = params.pop()[1]
        private.public_key().verify(base64.b64decode(signature), urlencode(params).encode(), padding.PKCS1v15(), hashes.SHA256())
        return httpx.Response(200, json={'verified': True})
    async def check():
        ex = LiveExchange(cfg, SimpleNamespace(acquire=AsyncMock()))
        await ex.client.aclose()
        ex.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        assert (await ex._request('GET', '/fapi/v3/account'))['verified']
        await ex.close()
    run(check())


def test_demo_rejects_mainnet_and_changed_endpoints():
    from exchange.demo import validate_demo
    from risk.config import execution_mode
    cfg = load_config()
    with pytest.raises(ValueError): validate_demo(cfg)
    cfg['exchange']['mode'] = 'mainnet'
    with pytest.raises(ValueError): validate_demo(cfg)
    with pytest.raises(ValueError):
        with execution_mode('mainnet'): pass
    cfg['exchange'].update(mode='testnet', base_url_testnet='https://fapi.binance.com')
    with pytest.raises(ValueError): validate_demo(cfg)


@pytest.mark.parametrize('equity,maximum', [(12.5, 125), (100, 125), (1000, 50), (98765, 20)])
def test_highest_bracket_leverage_current_equity(equity, maximum):
    from exchange.leverage import choose_leverage, bracket_for
    brackets = [{'initialLeverage': 125, 'notionalFloor': 0, 'notionalCap': 2000},
                {'initialLeverage': 50, 'notionalFloor': 2000, 'notionalCap': 10000},
                {'initialLeverage': 20, 'notionalFloor': 10000, 'notionalCap': 1000000}]
    selected = choose_leverage(brackets, equity * .1, 100, lambda q: round(q, 8))
    assert selected['leverage'] == maximum
    assert selected['margin'] == pytest.approx(equity * .1)
    for candidate in range(maximum + 1, 126):
        notional = equity * .1 * candidate
        try: bracket = bracket_for(brackets, notional)
        except ValueError: continue
        assert candidate > bracket['initialLeverage']


def test_leverage_is_per_position_persisted_and_exposure(live, tmp_path):
    async def check():
        bot, server = await prepare(live, tmp_path)
        assert await bot.enter(SYMBOL, 'buy', 100)
        pos = bot.positions[SYMBOL]
        record = json.loads(live.execution_path.read_text())[SYMBOL + ':1']
        assert pos['leverage'] == record['position']['leverage'] == record['options']['leverage'] == live.trades[0]['leverage'] == 10
        assert pos['margin'] == pytest.approx(pos['entry'] * pos['amount'] / pos['leverage'])
        from risk.exposure_limits import can_open
        state = SimpleNamespace(equity=100, open_positions={SYMBOL: {**pos, 'leverage': 100, 'amount': 10}})
        cfg = copy.deepcopy(live.cfg['exposure'])
        assert can_open(state, 'ETHUSDT', 'buy', 100, cfg)[0]
        await live.close()
    run(check())


def test_readonly_tool_refuses_post(live):
    from tools.demo_preflight import ReadOnlyDemo
    async def check():
        with pytest.raises(ValueError, match='Read-only'):
            await ReadOnlyDemo._request(live, 'POST', '/fapi/v1/order')
        await live.close()
    run(check())




@pytest.mark.parametrize('equity,max_leverage', [(17, 50), (2500, 20)])
def test_real_account_equity_and_variable_leverage_flow(live, tmp_path, equity, max_leverage):
    def handler(server):
        server.wallet = equity
        async def send(request):
            if request.url.path == '/fapi/v1/leverageBracket':
                return httpx.Response(200, json=[{'brackets': [{'initialLeverage': max_leverage, 'notionalFloor': 0,
                    'notionalCap': 100000000, 'maintMarginRatio': .005, 'cum': 0}]}])
            return await server(request)
        return send
    async def check():
        bot, server = await prepare(live, tmp_path, handler)
        assert await bot.enter(SYMBOL, 'buy', 100)
        pos = bot.positions[SYMBOL]
        assert pos['leverage'] == max_leverage
        assert pos['margin'] == pytest.approx(equity * .1, abs=.005)
        assert pos['mmr'] == .005
        durable = json.loads(live.execution_path.read_text())[SYMBOL + ':1']
        assert durable['options']['leverage'] == durable['position']['leverage'] == pos['leverage']
        assert durable['options']['equity'] == equity
        await live.close()
    run(check())


def test_exchange_rejects_high_account_leverage_searches_down(live, tmp_path):
    attempts = []
    def handler(server):
        async def send(request):
            if request.url.path == '/fapi/v1/leverageBracket':
                return httpx.Response(200, json=[{'brackets': [{'initialLeverage': 25, 'notionalFloor': 0,
                    'notionalCap': 100000, 'maintMarginRatio': .004, 'cum': 0}]}])
            if request.url.path == '/fapi/v1/leverage':
                leverage = int(request.url.params['leverage']); attempts.append(leverage)
                if leverage > 20: return httpx.Response(400, json={'code': -4028})
            return await server(request)
        return send
    async def check():
        bot, server = await prepare(live, tmp_path, handler)
        assert await bot.enter(SYMBOL, 'buy', 100)
        assert attempts == [25, 24, 23, 22, 21, 20]
        assert bot.positions[SYMBOL]['leverage'] == 20 and server.qty == 2
        await live.close()
    run(check())


def test_pending_close_accounting_finalizes_after_restart(live, tmp_path):
    empty = False
    def handler(server):
        async def send(request):
            if request.url.path.endswith('/userTrades') and empty: return httpx.Response(200, json=[])
            return await server(request)
        return send
    async def check():
        nonlocal empty
        bot, server = await prepare(live, tmp_path, handler)
        assert await bot.enter(SYMBOL, 'buy', 100)
        empty = True; server.price = 100.2
        await bot.close_position(SYMBOL, 'manual')
        assert not bot.positions and live.trades[-1]['accounting_status'] == 'pending'
        assert 'exit_detail' not in live.trades[-1]
        empty = False
        await live.recover_execution(); bot.merge_execution_state(); bot.persist_execution_metadata()
        close = live.trades[-1]
        assert close['exit_detail']['net_pnl_usdt'] == pytest.approx(server.wallet - 100)
        await live.recover_execution(); bot.merge_execution_state()
        assert len(live.trades) == 2
        await live.close()
    run(check())


def test_interruption_inside_post_leaves_recoverable_intent(live, tmp_path):
    class ProcessInterrupted(BaseException): pass
    def handler(server):
        async def send(request):
            response = await server(request)
            if request.method == 'POST' and request.url.path == '/fapi/v1/order': raise ProcessInterrupted()
            return response
        return send
    async def check():
        bot, server = await prepare(live, tmp_path, handler)
        with pytest.raises(ProcessInterrupted): await bot.enter(SYMBOL, 'buy', 100)
        assert server.qty == 1
        assert list(json.loads(live.intent_path.read_text()).values())[0]['status'] == 'pending'
        assert await live.recover_execution()
        assert len(server.orders) == 1 and len(live.trades) == 1 and len(server.algos) == 2
        await live.close()
    run(check())


def test_order_id_collision_between_symbols_is_not_duplicate(live):
    trade = {'id': '12', 'symbol': 'BTCUSDT', 'amount': 1}
    live._upsert_trade(trade)
    live._upsert_trade({**trade, 'symbol': 'ETHUSDT'})
    live._upsert_trade({**trade, 'amount': 2})
    assert len(live.trades) == 2 and live.trades[0]['amount'] == 2
    run(live.close())


def test_remote_close_missing_from_local_intents_is_discovered(live, tmp_path):
    def handler(server):
        async def send(request):
            if request.url.path.endswith('/userTrades') and 'orderId' not in request.url.params:
                return httpx.Response(200, json=[{'orderId': 2}])
            response = await server(request)
            if request.url.path == '/fapi/v1/order' and request.method == 'GET' and request.url.params.get('orderId') == '2':
                return httpx.Response(200, json={**response.json(), 'reduceOnly': True})
            return response
        return send
    async def check():
        bot, server = await prepare(live, tmp_path, handler)
        assert await bot.enter(SYMBOL, 'buy', 100)
        server.fill('SELL', 1, 'outside_known_intents')
        await live.refresh_account()
        assert await bot.adopt_native_close(SYMBOL)
        assert not bot.positions and len(server.orders) == 2
        assert live.trades[-1]['recovered']
        await live.close()
    run(check())


def test_protection_disk_failure_does_not_cancel_original(live):
    from paper_account import make_position
    pos = make_position('buy', 100, 1, 10, .4, live.cfg, 100, .0005)
    old = {'orderId': 1, 'clientOrderId': 'old', 'trigger': '98', 'is_algo': True}
    pos['protection'] = {'sl': old}
    live.submit_order = AsyncMock(side_effect=lambda **kw: {'algoId': 2, 'clientAlgoId': kw['params']['newClientOrderId']})
    async def confirmed(symbol, client_id):
        intent = live.intents[client_id]
        return {'algoId': 2, 'clientAlgoId': client_id, 'algoStatus': 'NEW', 'symbol': 'BTCUSDT',
            'side': intent['side'].upper(), 'orderType': intent['type'], 'quantity': str(intent['amount']),
            'reduceOnly': True, 'triggerPrice': str(intent['params']['stopPrice'])}
    live.query_order_by_client_id = confirmed
    live.cancel_algo = AsyncMock()
    async def check():
        original = __import__('exchange.live_exchange', fromlist=['atomic_json']).atomic_json
        def fail_protection(path, value):
            if Path(path).name == 'protection.json': raise OSError('disk')
            original(path, value)
        with patch('exchange.live_exchange.atomic_json', fail_protection):
            with pytest.raises(OSError): await live.protect(SYMBOL, pos)
        live.cancel_algo.assert_not_awaited()
        await live.close()
    run(check())


def test_paginated_fills_are_exact_and_deduplicated(live):
    live.cfg['exchange']['trade_page_limit'] = 2
    async def request(method, path, params, **kw):
        if 'fromId' not in params:
            return [{'id': 1, 'orderId': 7, 'qty': '.3'}, {'id': 2, 'orderId': 7, 'qty': '.3'}]
        return [{'id': 3, 'orderId': 7, 'qty': '.4'}]
    live._request = request
    async def check():
        fills = await live.order_fills('BTCUSDT', 7)
        assert len(fills) == 3 and sum(float(f['qty']) for f in fills) == 1
        await live.close()
    run(check())


def test_nonterminal_partial_is_confirmed_cancelled_before_residual_close(live, tmp_path):
    partial_order = None
    def handler(server):
        async def send(request):
            nonlocal partial_order
            if request.url.path == '/fapi/v1/order':
                if request.method == 'POST' and not partial_order:
                    data = server.fill('BUY', .5, request.url.params['newClientOrderId'])
                    data['status'] = 'PARTIALLY_FILLED'; partial_order = data
                    return httpx.Response(200, json=data)
                if request.method == 'DELETE':
                    partial_order['status'] = 'CANCELED'
                    return httpx.Response(200, json=partial_order)
            return await server(request)
        return send
    async def check():
        bot, server = await prepare(live, tmp_path, handler)
        with patch('exchange.execution.asyncio.sleep', new_callable=AsyncMock):
            assert not await bot.enter(SYMBOL, 'buy', 100)
        assert not server.qty and bot.safety.kill.triggered == 'order_execution_uncertain'
        assert partial_order['status'] == 'CANCELED'
        assert sum(t['amount'] for t in live.trades if t['reduceOnly']) == .5
        assert len(server.orders) == 2
        await live.close()
    run(check())


@pytest.mark.parametrize('account_configuration,needs_review', [
    ({'canTrade': True}, False), ({'canTrade': False}, True), ({}, True),
    ({'canTrade': None}, True), ({'canTrade': 'true'}, True), ({'canTrade': 1}, True),
])
def test_preflight_only_reads_account_and_capabilities(live, tmp_path, account_configuration, needs_review):
    from tools.demo_preflight import preflight
    seen = []
    server = SimulatedBinance()
    async def send(request):
        seen.append(request.method)
        assert request.method == 'GET'
        path = request.url.path
        if path == '/fapi/v1/exchangeInfo':
            return httpx.Response(200, json={'symbols': [{'symbol': 'BTCUSDT', 'baseAsset': 'BTC',
                'quoteAsset': 'USDT', 'marginAsset': 'USDT', 'contractType': 'PERPETUAL', 'status': 'TRADING',
                'filters': [{'filterType': 'LOT_SIZE', 'stepSize': '.001', 'minQty': '.001', 'maxQty': '1000'},
                            {'filterType': 'PRICE_FILTER', 'tickSize': '.01'}, {'filterType': 'MIN_NOTIONAL', 'notional': '5'}]}]})
        if path == '/fapi/v1/positionSide/dual': return httpx.Response(200, json={'dualSidePosition': False})
        if path == '/fapi/v1/multiAssetsMargin': return httpx.Response(200, json={'multiAssetsMargin': False})
        if path == '/fapi/v1/accountConfig': return httpx.Response(200, json=account_configuration)
        if path == '/fapi/v1/symbolConfig': return httpx.Response(200, json=[{'symbol': 'BTCUSDT', 'leverage': 10}])
        if path == '/fapi/v1/userTrades': return httpx.Response(200, json=[])
        return await server(request)
    async def check():
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(send))
        report = await preflight(live, 'BTCUSDT', websocket=False)
        assert report['equity_usdt'] == 100 and report['signature_and_account_reads_verified']
        assert not report['order_submission_verified'] and not report['protection_acceptance_verified']
        assert not report['candidate']['confirmed']
        expected = account_configuration.get('canTrade')
        assert report['account_can_trade'] is (expected if type(expected) is bool else None)
        assert report['requires_operator_review'] is needs_review
        assert report['account_can_trade_source'] == '/fapi/v1/accountConfig'
        assert set(seen) == {'GET'} and not server.orders
        await live.close()
    run(check())
