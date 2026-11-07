"""Fault-path and real adapter HTTP contracts, with no network requests."""
import asyncio
import hashlib
import hmac
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import urlencode

import httpx
import pytest

from exchange.live_exchange import LiveExchange
from exchange.orders import ExchangeError, ExecutionUnknown, safe_create_order
from risk.config import load_config
from risk.kill_switch import KillSwitch
from paper_account import make_position
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from test_continuous import StreamData


@pytest.fixture
def live(tmp_path, monkeypatch):
    from legacy_config import legacy_execution_config
    cfg = legacy_execution_config()
    cfg['exchange']['mode'] = 'testnet'
    cfg['exchange']['state_dir'] = str(tmp_path)
    monkeypatch.setenv('BINANCE_API_KEY', 'unit-test-key')
    monkeypatch.setenv('BINANCE_API_SECRET', 'unit-test-secret')
    original_client = httpx.AsyncClient
    def unmocked(request):
        raise AssertionError('Unexpected HTTP request in offline fixture: ' + request.method + ' ' + request.url.path)
    def client(*args, **kwargs):
        kwargs.setdefault('transport', httpx.MockTransport(unmocked))
        return original_client(*args, **kwargs)
    monkeypatch.setattr(httpx, 'AsyncClient', client)
    exchange = LiveExchange(cfg, SimpleNamespace(acquire=AsyncMock()))
    for name in ('weights', 'orders_short', 'orders_long'):
        setattr(exchange, name, SimpleNamespace(acquire=AsyncMock()))
    exchange.markets = {'BTC/USDT:USDT': {'id': 'BTCUSDT', 'symbol': 'BTC/USDT:USDT',
        'step': '.001', 'tick': '.01', 'limits': {'amount': {'min': .001}, 'cost': {'min': 5}}}}
    return exchange


def run(coro): return asyncio.run(coro)


def test_signed_request_and_redacted_error(live):
    observed = []
    async def handler(request):
        observed.append(request)
        return httpx.Response(401, json={'code': -2015, 'msg': 'sensitive server text'})
    async def check():
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        with pytest.raises(ExchangeError) as error:
            await live._request('GET', '/fapi/v3/account')
        assert 'unit-test' not in str(error.value)
        assert 'sensitive' not in str(error.value)
        params = list(observed[0].url.params.multi_items())
        signature = params.pop()[1]
        assert signature == hmac.new(b'unit-test-secret', urlencode(params).encode(), hashlib.sha256).hexdigest()
        await live.close()
    run(check())


def test_post_503_adopts_without_second_post(live):
    requests = []
    async def handler(request):
        requests.append(request)
        if request.method == 'POST':
            return httpx.Response(503, json={'code': -1007})
        return httpx.Response(200, json={'orderId': 42, 'clientOrderId': request.url.params['origClientOrderId'], 'status': 'FILLED'})
    async def check():
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        order = await safe_create_order(live, {'symbol': 'BTC/USDT:USDT', 'type': 'market', 'side': 'buy', 'amount': 1}, live.journal)
        assert order['orderId'] == 42
        assert len([r for r in requests if r.method == 'POST']) == 1
        assert requests[0].url.params['newClientOrderId'] == requests[1].url.params['origClientOrderId']
        await live.close()
    run(check())


def test_ambiguous_timeout_absence_never_resubmits(live):
    live.submit_order = AsyncMock(side_effect=ExchangeError())
    live.query_order_by_client_id = AsyncMock(return_value=None)
    async def check():
        with patch('exchange.orders.asyncio.sleep', new_callable=AsyncMock):
            with pytest.raises(ExecutionUnknown):
                await safe_create_order(live, {'symbol': 'BTC/USDT:USDT', 'type': 'market', 'side': 'buy', 'amount': 1}, live.journal)
        assert live.submit_order.await_count == 1
        assert next(iter(live.intents.values()))['status'] == 'pending'
        await live.close()
    run(check())


def test_protection_replacement_confirms_new_before_cancel(live):
    events = []
    accepted = {}
    pos = make_position('buy', 100, 5, 50, .4, live.cfg, 100, .0005)
    pos['protection'] = {'sl': {'orderId': 1, 'clientOrderId': 'kh_old', 'trigger': '99.00', 'is_algo': True}}
    async def submit(**kw):
        events.append(('post', kw['type']))
        cid = kw['params']['newClientOrderId']
        accepted[cid] = {'algoId': len(events) + 100, 'clientAlgoId': cid, 'symbol': 'BTCUSDT',
            'side': kw['side'].upper(), 'orderType': kw['type'], 'quantity': str(kw['amount']),
            'reduceOnly': True, 'triggerPrice': str(kw['params']['stopPrice']), 'algoStatus': 'NEW'}
        return accepted[cid]
    async def query(symbol, client_id):
        events.append(('confirmed', client_id))
        return accepted[client_id]
    async def cancel(order): events.append(('cancel', order['orderId']))
    live.submit_order, live.query_order_by_client_id, live.cancel_algo = submit, query, cancel
    live.query_protection = AsyncMock(return_value={'algoStatus': 'CANCELED'})
    async def check():
        await live.protect('BTC/USDT:USDT', pos)
        assert [e[0] for e in events[:3]] == ['post', 'confirmed', 'cancel']
        assert float(pos['protection']['sl']['trigger']) >= pos['sl']
        assert float(pos['protection']['tp']['trigger']) >= pos['tp']
        await live.close()
    run(check())


def test_failed_new_stop_preserves_old_stop(live):
    pos = make_position('buy', 100, 5, 50, .4, live.cfg, 100, .0005)
    old = {'orderId': 1, 'clientOrderId': 'kh_old', 'trigger': '99.00', 'is_algo': True}
    pos['protection'] = {'sl': old.copy()}
    live.submit_order = AsyncMock(side_effect=ExchangeError(400, -1111))
    live.query_order_by_client_id = AsyncMock(return_value=None)
    live.cancel_algo = AsyncMock()
    async def check():
        with patch('exchange.orders.asyncio.sleep', new_callable=AsyncMock):
            with pytest.raises(ExchangeError): await live.protect('BTC/USDT:USDT', pos)
        live.cancel_algo.assert_not_awaited()
        assert pos['protection']['sl'] == old
        await live.close()
    run(check())


def test_mainnet_without_evidence_cannot_create_client(monkeypatch, tmp_path):
    cfg = load_config()
    cfg['exchange']['mode'] = 'mainnet'
    cfg['rollout']['evidence_file'] = str(tmp_path / 'missing.json')
    cfg['rollout']['approval_file'] = str(tmp_path / 'approval.json')
    with patch('exchange.live_exchange.httpx.AsyncClient') as client:
        with pytest.raises(ValueError, match='Mainnet locked'):
            LiveExchange(cfg, Mock())
        client.assert_not_called()


def test_rolling_api_errors_expire():
    kill = KillSwitch(load_config()['kill_switch'])
    with patch('risk.kill_switch.time.time', return_value=100):
        for _ in range(5): kill.on_api_error()
    with patch('risk.kill_switch.time.time', return_value=161):
        kill.state.last_ws_heartbeat = 161
        assert kill.check(SimpleNamespace(equity=100), None) is None
        assert kill.state.api_errors_1m == 0


def test_kill_latch_survives_restart(tmp_path):
    async def check():
        path = tmp_path / 'state.json'
        bot = V2PaperHunter(path, market_data=StreamData())
        bot.safety.kill.mode = 'hard'
        bot.safety.kill.triggered = 'api_errors'
        bot.save()
        restored = V2PaperHunter(path, market_data=StreamData())
        restored.safety.kill.mode = 'hard'
        restored.restore()
        assert restored.safety.kill.triggered == 'api_errors'
        assert not restored.safety.ready
        assert not await restored.safety.allow_entry('BTC/USDT:USDT', 'buy', 100)
    run(check())


def test_unrealized_drawdown_triggers():
    kill = KillSwitch(load_config()['kill_switch'])
    state = SimpleNamespace(equity=100.)
    assert kill.check(state, None) is None
    state.equity = 94.
    assert kill.check(state, None) == 'daily_drawdown'


def test_kill_closes_paper_positions_and_persists(tmp_path):
    async def check():
        bot = V2PaperHunter(tmp_path / 'state.json', market_data=StreamData())
        bot.safety.kill.mode = 'hard'
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100)
        await bot.safety.trip('manual_test_incident')
        assert not bot.positions and not bot.exchange.positions
        saved = json.loads(bot.state_path.read_text())
        assert saved['operations']['kill']['triggered'] == 'manual_test_incident'
        assert bot.exchange.trades[-1]['reduceOnly']
    run(check())


class SimulatedBinance:
    """Stateful HTTP exchange fake: real adapter paths, signatures and fills."""
    def __init__(self):
        self.qty = 0.
        self.price = 100.
        self.wallet = 100.
        self.orders = {}
        self.algos = {}
        self.next_algo = 100
        self.partial_close = False
        self.reject_protection = False

    def fill(self, side, qty, client_id):
        reducing = self.qty != 0
        if reducing and self.partial_close:
            qty /= 2
            self.partial_close = False
            status = 'EXPIRED'
        else:
            status = 'FILLED'
        pnl = (self.price - 100) * qty if reducing else 0.
        fee = self.price * qty * .0005
        self.wallet += pnl - fee
        self.qty += qty if side == 'BUY' else -qty
        order_id = len(self.orders) + 1
        order = {'orderId': order_id, 'clientOrderId': client_id, 'status': status,
                 'side': side, 'avgPrice': str(self.price), 'executedQty': str(qty), 'reduceOnly': reducing}
        self.orders[order_id] = (order, fee, pnl)
        return order

    async def __call__(self, request):
        p, method, path = request.url.params, request.method, request.url.path
        data = None
        if path == '/fapi/v1/time': data = {'serverTime': int(time.time() * 1000)}
        elif path == '/fapi/v1/marginType': data = {'code': 200}
        elif path == '/fapi/v1/leverageBracket': data = [{'symbol': 'BTCUSDT', 'brackets': [{'initialLeverage': 10, 'notionalFloor': 0, 'notionalCap': 1000000, 'maintMarginRatio': .004, 'cum': 0}]}]
        elif path == '/fapi/v1/leverage': data = {'leverage': int(p['leverage'])}
        elif path == '/fapi/v1/ticker/price': data = {'price': str(self.price)}
        elif path == '/fapi/v1/klines':
            data = [[i * 60000, '100', '100', '100', '100', '1'] for i in range(int(p['limit']))]
        elif path == '/fapi/v3/positionRisk':
            data = [{'symbol': 'BTCUSDT', 'positionAmt': str(self.qty), 'entryPrice': '100',
                     'markPrice': str(self.price), 'liquidationPrice': '98.4', 'positionInitialMargin': '10'}] if self.qty else []
        elif path == '/fapi/v3/account':
            data = {'assets': [{'asset': 'USDT', 'walletBalance': str(self.wallet),
                'marginBalance': str(self.wallet + (self.price - 100) * self.qty),
                'availableBalance': str(self.wallet - abs(self.qty) * self.price / load_config()["leverage"]),
                'initialMargin': str(abs(self.qty) * self.price / load_config()["leverage"])}]}
        elif path == '/fapi/v1/order' and method == 'POST':
            data = self.fill(p['side'], float(p['quantity']), p['newClientOrderId'])
        elif path == '/fapi/v1/order' and method == 'GET':
            if 'orderId' in p: data = self.orders[int(p['orderId'])][0]
            else: data = next((v[0] for v in self.orders.values() if v[0]['clientOrderId'] == p['origClientOrderId']), None)
        elif path == '/fapi/v1/userTrades':
            order, fee, pnl = self.orders[int(p['orderId'])]
            data = [{'commission': str(fee), 'commissionAsset': 'USDT', 'realizedPnl': str(pnl), 'qty': order['executedQty']}]
        elif path == '/fapi/v1/openOrders': data = []
        elif path == '/fapi/v1/openAlgoOrders': data = [a for a in self.algos.values() if a['algoStatus'] == 'NEW']
        elif path == '/fapi/v1/algoOrder':
            if method == 'POST':
                if self.reject_protection: return httpx.Response(400, json={'code': -1111})
                self.next_algo += 1
                data = {'algoId': self.next_algo, 'clientAlgoId': p['clientAlgoId'], 'symbol': 'BTCUSDT',
                    'algoStatus': 'NEW', 'reduceOnly': True, 'triggerPrice': p['triggerPrice'],
                    'side': p['side'], 'quantity': p['quantity'], 'orderType': p['type']}
                self.algos[self.next_algo] = data
            elif method == 'GET': data = next((a for a in self.algos.values() if a['clientAlgoId'] == p['clientAlgoId']), None)
            elif method == 'DELETE':
                self.algos[int(p['algoId'])]['algoStatus'] = 'CANCELED'
                data = {'code': 200}
        if data is None: return httpx.Response(400, json={'code': -2013})
        return httpx.Response(200, json=data)


async def runtime_with_transport(live, tmp_path):
    await live.client.aclose()
    server = SimulatedBinance()
    live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(server))
    bot = V2PaperHunter(tmp_path / 'lifecycle.json', market_data=StreamData())
    bot.safety.kill.mode = 'hard'
    bot.execution_mode = 'testnet'
    bot.cfg = live.cfg
    bot.exchange = live
    bot.safety.kill = KillSwitch(live.cfg['kill_switch'])
    bot.safety.cfg = live.cfg
    bot.safety.active = True
    bot.safety.ready = True
    bot.safety.live = True
    return bot, server


def test_full_entry_native_protection_and_tp_lifecycle(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100)
        assert len(server.algos) == 2
        assert bot.positions['BTC/USDT:USDT']['amount'] == 1
        server.price = 102
        await bot.manage_exits()
        assert not bot.positions and not server.qty
        trade = live.trades[-1]
        assert trade['exit_detail']['exit_reason'] == 'take_profit'
        assert trade['exit_detail']['net_pnl_usdt'] == pytest.approx(server.wallet - 100)
        assert all(a['algoStatus'] == 'CANCELED' for a in server.algos.values())
        await live.close()
    run(check())


def test_native_sl_is_adopted_without_false_phantom(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100)
        pos = bot.positions['BTC/USDT:USDT']
        protection = pos['protection']['sl']
        server.price = float(protection['trigger'])
        closed = server.fill('SELL', server.qty, 'native_close')
        algo = server.algos[protection['orderId']]
        algo.update(algoStatus='FINISHED', actualOrderId=str(closed['orderId']))
        await bot.manage_exits()
        assert not bot.positions and not server.qty
        assert bot.safety.kill.triggered is None
        assert live.trades[-1]['exit_detail']['exit_reason'] == 'stop_loss'
        assert live.trades[-1]['exit_detail']['net_pnl_usdt'] == pytest.approx(server.wallet - 100)
        await live.close()
    run(check())


def test_partial_close_aggregate_accounting_and_residual_flatten(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100)
        server.price = 100.2
        server.partial_close = True
        with pytest.raises(ExchangeError):
            await bot.close_position('BTC/USDT:USDT', 'kill_switch')
        assert server.qty == .5
        # Terminal partial close is known. A new reduce-only order closes the
        # observed remainder; realized PnL/fees include both fills exactly once.
        await bot.close_position('BTC/USDT:USDT', 'kill_switch')
        assert not server.qty and not bot.positions
        detail = live.trades[-1]['exit_detail']
        assert detail['net_pnl_usdt'] == pytest.approx(server.wallet - 100)
        assert sum(t['amount'] for t in live.trades if t['reduceOnly']) == 1
        await live.close()
    run(check())


def test_failed_initial_protection_closes_only_real_fill(live, tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live, tmp_path)
        server.reject_protection = True
        with patch('exchange.orders.asyncio.sleep', new_callable=AsyncMock):
            assert not await bot.enter('BTC/USDT:USDT', 'buy', 100)
        assert not server.qty  # possible real entry was not mistaken for a no-op
        assert bot.safety.kill.triggered is None
        assert live.management_faults['BTC/USDT:USDT']['reason'] == 'entry_confirmed_management_failed'
        assert any(t['side'] == 'buy' for t in live.trades)
        assert any(t['reduceOnly'] for t in live.trades)
        assert live.trades[-1]['exit_detail']['net_pnl_usdt'] == pytest.approx(server.wallet - 100)
        await live.close()
    run(check())


def test_operator_policy_requires_permissions_and_recent_rotation(tmp_path):
    from datetime import datetime, timezone, timedelta
    from exchange.rollout import require_operator_policy
    cfg = load_config()
    cfg['rollout']['operator_policy_file'] = str(tmp_path / '{mode}.json')
    with pytest.raises(ValueError, match='operator policy'):
        require_operator_policy(cfg, 'testnet')
    policy = {'mode': 'testnet', 'dedicated_account': True, 'futures_only': True,
              'withdrawals_disabled': True, 'ip_whitelist_verified': True,
              'last_rotated_at': datetime.now(timezone.utc).isoformat()}
    path = tmp_path / 'testnet.json'
    path.write_text(json.dumps(policy))
    require_operator_policy(cfg, 'testnet')
    policy['last_rotated_at'] = (datetime.now(timezone.utc) - timedelta(days=91)).isoformat()
    path.write_text(json.dumps(policy))
    with pytest.raises(ValueError, match='Rotate'):
        require_operator_policy(cfg, 'testnet')


def test_scaling_stays_at_one_percent_without_review(tmp_path):
    from exchange.rollout import mainnet_allocation_fraction
    cfg = load_config()
    cfg['rollout']['scale_state_file'] = str(tmp_path / 'stage.json')
    cfg['rollout']['scale_approval_file'] = str(tmp_path / 'approval.json')
    cfg['rollout']['mainnet_evidence_file'] = str(tmp_path / 'evidence.json')
    assert mainnet_allocation_fraction(cfg) == .01
    (tmp_path / 'approval.json').write_text(json.dumps({'approved': True, 'fraction': .02}))
    with pytest.raises(ValueError, match='Scaling locked'):
        mainnet_allocation_fraction(cfg)


def test_expected_already_isolated_error_is_not_an_api_incident(live):
    async def handler(request):
        if request.url.path.endswith('marginType'): return httpx.Response(400, json={'code': -4046})
        return httpx.Response(200, json={'leverage': live.cfg['leverage']})
    async def check():
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        live.on_api_error = Mock()
        await live.set_leverage(live.cfg['leverage'], 'BTC/USDT:USDT')
        live.on_api_error.assert_not_called()
        assert 'BTC/USDT:USDT' in live._leverage_set
        await live.close()
    run(check())


def test_malformed_credentials_never_reach_http_headers(monkeypatch):
    from legacy_config import legacy_execution_config
    cfg = legacy_execution_config()
    cfg['exchange']['mode'] = 'testnet'
    monkeypatch.setenv('BINANCE_API_KEY', 'unit-test-key\ninvalid')
    monkeypatch.setenv('BINANCE_API_SECRET', 'unit-test-secret')
    with patch('exchange.live_exchange.httpx.AsyncClient') as client:
        with pytest.raises(ValueError) as error:
            LiveExchange(cfg, Mock())
        assert 'unit-test' not in str(error.value)
        client.assert_not_called()


def test_technical_incident_persists_even_if_evidence_sampling_stops(tmp_path):
    from exchange.rollout import TestnetEvidence
    async def check():
        bot = V2PaperHunter(tmp_path / 'state.json', market_data=StreamData())
        bot.safety.kill.mode = 'hard'
        cfg = bot.cfg
        cfg['rollout']['evidence_file'] = str(tmp_path / 'trial.json')
        bot.safety.evidence = TestnetEvidence(cfg)
        await bot.safety.trip('api_errors')
        recorded = json.loads((tmp_path / 'trial.json').read_text())
        assert recorded['technical_kills'] == 1
        # The persisted incident must not disappear or increment per restart.
        bot.save()
        resumed = V2PaperHunter(bot.state_path, market_data=StreamData())
        assert 'kill_announced' in resumed.safety.warned
    run(check())
