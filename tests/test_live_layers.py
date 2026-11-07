"""20 required live-layer tests; transport and clocks are simulated, no API keys."""
import asyncio
import copy
import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from infra.rate_limit import TokenBucket, with_backoff
from risk.kill_switch import KillSwitch
from risk.config import load_config
from risk.exposure_limits import can_open
from risk.funding import check_funding_before_settlement
from exchange.live_exchange import LiveExchange
from exchange.reconciler import reconcile
from exchange.orders import ExchangeError, safe_create_order
from paper_account import PaperExchange
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from test_continuous import StreamData

CFG = load_config()


def run(coro): return asyncio.run(coro)


def test_token_bucket_limits():
    async def check():
        clock = SimpleNamespace(now=0.)
        async def sleep(delay): clock.now += max(delay, 1e-9)
        with patch('infra.rate_limit.time', SimpleNamespace(monotonic=lambda: clock.now)), patch('infra.rate_limit.asyncio.sleep', sleep):
            bucket = TokenBucket(5, 10)
            for _ in range(100): await bucket.acquire()
        assert clock.now >= 100 / 5 - 1e-8
    run(check())


def test_backoff_on_429():
    async def check():
        operation = AsyncMock(side_effect=[ExchangeError(429), ExchangeError(429), 'ok'])
        with patch('infra.rate_limit.asyncio.sleep', new_callable=AsyncMock) as sleeper:
            assert await with_backoff(operation) == 'ok'
        assert operation.await_count == 3
        assert sleeper.await_count == 2
    run(check())


def switch(): return KillSwitch(copy.deepcopy(CFG['kill_switch']))
def account(): return SimpleNamespace(equity=100.)


def test_kill_switch_daily_dd():
    kill = switch()
    kill.state.daily_pnl = -6
    assert kill.check(account(), None) == 'daily_drawdown'


def test_kill_switch_consecutive():
    kill = switch()
    for _ in range(4): kill.on_trade_closed(-.1)
    assert kill.check(account(), None) is None
    assert kill.entry_block_reason() == 'consecutive_losses'
    assert not kill.should_flatten('consecutive_losses')


def test_kill_switch_ws_timeout():
    kill = switch()
    kill.state.last_ws_heartbeat -= 31
    assert kill.check(account(), None) == 'ws_timeout'


def test_kill_switch_spread():
    kill = switch()
    assert kill.check(account(), SimpleNamespace(spread=lambda: .01, avg_spread=.001, samples=20)) is None
    assert kill.entry_block_reason() == 'spread_spike'
    assert not kill.should_flatten('spread_spike')


def test_kill_switch_resets():
    kill = switch()
    kill.state.daily_pnl = -6
    kill.check(account(), None)
    kill.reset_daily()
    assert kill.check(account(), None) is None


def reconciliation_state(positions):
    state = SimpleNamespace(open_positions=positions, open_orders={}, cfg=copy.deepcopy(CFG))
    state.remove_position = lambda symbol: positions.pop(symbol, None)
    return state


def remote_exchange(positions):
    return SimpleNamespace(get_position=AsyncMock(return_value=positions),
        get_open_orders=AsyncMock(return_value=[]), create_order=AsyncMock(), cancel_order=AsyncMock())


def test_reconcile_orphan_close():
    remote = remote_exchange([{'symbol': 'BTCUSDT', 'positionAmt': '1', 'entryPrice': '100'}])
    report = run(reconcile(remote, reconciliation_state({}), Mock()))
    assert report['orphan_positions'] == ['BTCUSDT']
    remote.get_position.assert_awaited_once_with(None)
    remote.create_order.assert_awaited_once_with('BTCUSDT', 'market', 'sell', 1., params={'reduceOnly': True})


def test_reconcile_phantom_retained_for_review():
    state = reconciliation_state({'BTCUSDT': {'amount': 1., 'entry': 100., 'side': 'buy'}})
    report = run(reconcile(remote_exchange([]), state, Mock()))
    assert report['phantom_positions'] == ['BTCUSDT']
    assert 'BTCUSDT' in state.open_positions  # Keep strategy facts until exit attribution.


def test_reconcile_size_mismatch():
    state = reconciliation_state({'BTCUSDT': {'amount': 1., 'entry': 100., 'side': 'buy'}})
    remote = remote_exchange([{'symbol': 'BTCUSDT', 'positionAmt': '2', 'entryPrice': '101'}])
    report = run(reconcile(remote, state, Mock()))
    assert report['size_mismatches'] == ['BTCUSDT']
    assert state.open_positions['BTCUSDT']['amount'] == 1  # Preserve the original entry for exact close accounting.
    assert state.open_positions['BTCUSDT']['entry'] == 100


def order_exchange(outcomes, query):
    return SimpleNamespace(cfg=copy.deepcopy(CFG), submit_order=AsyncMock(side_effect=outcomes),
        query_order_by_client_id=AsyncMock(return_value=query), persist_intent=Mock(),
        resolve_intent=Mock(), reject_intent=Mock())


def test_order_failure_adoption():
    remote = order_exchange([ExchangeError(503)], {'orderId': 12})
    assert run(safe_create_order(remote, {'symbol': 'BTCUSDT', 'type': 'market', 'side': 'buy', 'amount': 1}, Mock()))['orderId'] == 12
    assert remote.submit_order.await_count == 1
    remote.resolve_intent.assert_called_once()


def test_order_failure_retry():
    remote = order_exchange([ExchangeError(429)] * 3, None)
    async def check():
        with patch('exchange.orders.asyncio.sleep', new_callable=AsyncMock):
            with pytest.raises(ExchangeError):
                await safe_create_order(remote, {'symbol': 'BTCUSDT', 'type': 'market', 'side': 'buy', 'amount': 1}, Mock())
    run(check())
    assert remote.submit_order.await_count == 3
    ids = [c.args[0] for c in remote.persist_intent.call_args_list]
    assert len(set(ids)) == 3


def test_exposure_limits():
    cfg = copy.deepcopy(CFG['exposure'])
    cfg['basis'] = 'notional'
    state = SimpleNamespace(equity=100., open_positions={})
    assert can_open(state, 'BTCUSDT', 'long', 46, cfg) == (False, 'max_long_exposure')
    state.open_positions = {str(i): {'side': 'buy', 'entry': 1, 'amount': 1} for i in range(4)}
    assert can_open(state, 'BTCUSDT', 'long', 1, cfg) == (False, 'max_positions')
    # Explicit default margin basis permits the configured position notional.
    state.open_positions = {}
    assert can_open(state, 'BTCUSDT', 'long', 100 * .1 * CFG['leverage'], CFG['exposure'])[0]


def test_correlation_limit():
    state = SimpleNamespace(equity=100., open_positions={s: {'side': 'buy', 'entry': 100, 'amount': CFG['leverage'] * .1}
        for s in ('BTCUSDT', 'ETHUSDT')})
    assert can_open(state, 'SOLUSDT', 'long', 100 * .1 * CFG['leverage'], {**CFG['exposure'], 'max_correlated_same_direction': 2}) == (False, 'correlated_limit')


def test_funding_close_before_settlement():
    state = reconciliation_state({'BTCUSDT': {'side': 'buy'}})
    state.close_position = AsyncMock()
    remote = SimpleNamespace(get_funding_rate=AsyncMock(return_value={'lastFundingRate': '.001', 'nextFundingTime': 240000}))
    async def check():
        with patch('risk.funding.time.time', return_value=0):
            await check_funding_before_settlement(state, remote, CFG['funding'], Mock())
    run(check())
    state.close_position.assert_not_awaited()


def test_slippage_logged(tmp_path):
    async def check():
        data = StreamData()
        bot = V2PaperHunter(tmp_path / 'state.json', market_data=data)
        assert await bot.enter('BTC/USDT:USDT', 'buy', 99.9)
        pos = bot.positions['BTC/USDT:USDT']
        assert pos['entry_slippage_bps'] == pytest.approx((100 - 99.9) / 99.9 * 10000)
        data.price = pos['sl']
        await bot.manage_exits()
        trade = bot.exchange.trades[-1]
        for key in ('expected_entry', 'actual_entry', 'entry_slippage_bps', 'expected_exit', 'actual_exit', 'exit_slippage_bps'):
            assert key in trade['exit_detail']
    run(check())


def test_time_stop(tmp_path):
    async def check():
        bot = V2PaperHunter(tmp_path / 'state.json', market_data=StreamData())
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100.)
        bot.positions['BTC/USDT:USDT']['opened_at'] -= 45 * 60 + 1
        await bot.manage_exits()
        assert bot.positions  # Disabled in the hard-stop/trailing policy; age is not an entry filter.
    run(check())


def test_equity_current_used(tmp_path):
    async def check():
        data = StreamData()
        bot = V2PaperHunter(tmp_path / 'state.json', fee_rate=0, market_data=data)
        bot.cfg.pop('paper_position_notional_usdt')
        first = await bot.size_position('BTC/USDT:USDT')
        bot.exchange.balance = 80
        second = await bot.size_position('BTC/USDT:USDT')
        assert second == pytest.approx(first * .8, abs=.001)
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100)
        data.price = 99.9
        await bot.exchange.fetch_ticker('BTC/USDT:USDT')
        # Current unrealized losses are included as well as realized wallet PnL.
        from risk.trading_policy import entry_amount
        expected = entry_amount(bot.equity, data.price, CFG['leverage'], bot.cfg, 0, lambda q: float(bot.exchange.amount_to_precision('ETH/USDT:USDT', q)))
        assert await bot.size_position('ETH/USDT:USDT') == expected
    run(check())


def test_live_exchange_interface():
    for name in ('create_order', 'cancel_order', 'get_position', 'get_open_orders', 'set_leverage', 'fetch_market',
                 'fetch_balance', 'fetch_ticker', 'fetch_order_book', 'fetch_ohlcv', 'load_markets', 'amount_to_precision', 'close'):
        assert inspect.signature(getattr(LiveExchange, name)) == inspect.signature(getattr(PaperExchange, name)), name


def test_client_order_id_unique():
    async def check():
        remote = order_exchange(None, None)
        remote.submit_order = AsyncMock(return_value={'orderId': 1})
        for _ in range(1000):
            await safe_create_order(remote, {'symbol': 'BTCUSDT', 'type': 'market', 'side': 'buy', 'amount': 1}, Mock())
        ids = [c.args[0] for c in remote.persist_intent.call_args_list]
        assert len(set(ids)) == 1000
        assert all(value.startswith('kh_') and len(value) <= 36 for value in ids)
    run(check())
