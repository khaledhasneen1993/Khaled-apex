import asyncio
import copy
import json
from types import SimpleNamespace

import pytest

from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from paper_account import make_position
from risk.config import load_config
from risk.dynamic_stop import update_dynamic_stop
from test_continuous import StreamData


def test_planned_loss_includes_both_fees():
    cfg = load_config()
    for side in ('buy', 'sell'):
        for entry in (.001, 100, 100000):
            for equity in (50, 100, 10000):
                for atr_pct in (0, .004, .03):
                    from risk.trading_policy import entry_amount
                    amount = entry_amount(equity, entry, cfg['leverage'], cfg, cfg['fee_rate'])
                    pos = make_position(side, entry, amount, cfg['leverage'], entry * atr_pct,
                                        cfg, equity, cfg['fee_rate'])
                    planned_loss = abs(entry - pos.stop) * amount + pos['entry_fee'] + pos.stop * amount * cfg['fee_rate']
                    assert planned_loss <= equity * cfg['tp_sl']['max_equity_risk_pct'] + 1e-9
                    assert pos['stop_pct_initial'] == .08
                    assert pos['tp'] is None


def test_armed_state_and_initial_stop_survive_resume(tmp_path):
    async def check():
        data = StreamData()
        bot = V2PaperHunter(tmp_path / 'state.json', market_data=data)
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100)
        original = copy.deepcopy(bot.positions['BTC/USDT:USDT'])
        # A still-open position arms BE and tightens trailing, then resumes.
        pos = bot.positions['BTC/USDT:USDT']
        update_dynamic_stop(pos, SimpleNamespace(price=101.5, atr=.3), bot.cfg['exit'])
        bot.save()
        restored = V2PaperHunter(bot.state_path, market_data=data)
        assert restored.positions == bot.positions
        resumed = restored.positions['BTC/USDT:USDT']
        assert resumed.breakeven_armed
        assert resumed['stop_initial'] == original['stop_initial']
        assert resumed['signal_id'] == original['signal_id']
        data.price = resumed.stop
        await restored.manage_exits()
        record = restored.exchange.trades[-1]['exit_detail']
        required = {'signal_id', 'side', 'entry', 'stop_initial', 'stop_final', 'tp',
                    'stop_pct_initial', 'tp_pct', 'rr_planned', 'breakeven_armed',
                    'exit_reason', 'exit_price', 'pnl_pct', 'time_in_trade_sec',
                    'atr_at_entry', 'leverage', 'stop_capped'}
        assert required <= record.keys()
        assert record['stop_initial'] == original['stop_initial']
        assert record['stop_final'] > record['stop_initial']
        assert record['breakeven_armed']
        assert record['exit_reason'] == 'TRAILING_SL'
        again = V2PaperHunter(bot.state_path, market_data=data)
        assert again.exchange.trades == restored.exchange.trades
    asyncio.run(check())


def test_atr_outage_does_not_reject_signals(tmp_path):
    async def check():
        data = StreamData()
        async def offline(*args, **kwargs):
            raise ConnectionError('offline ATR')
        data.fetch_ohlcv = offline
        bot = V2PaperHunter(tmp_path / 'state.json', market_data=data)
        for symbol, side in (('BTC/USDT:USDT', 'buy'), ('ETH/USDT:USDT', 'sell'), ('SOL/USDT:USDT', 'buy')):
            assert await bot.enter(symbol, side, 100)
            assert bot.positions[symbol]['atr_source'] == 'not_used'
            assert bot.positions[symbol]['amount'] > 0
        assert len(bot.positions) == 3
    asyncio.run(check())


def test_levels_anchor_to_fill_not_signal(tmp_path):
    async def check():
        data = StreamData()
        data.price = 100.2
        bot = V2PaperHunter(tmp_path / 'state.json', market_data=data)
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100.2)
        pos = bot.positions['BTC/USDT:USDT']
        assert pos['entry'] == bot.exchange.positions['BTC/USDT:USDT']['entry'] == 100.2
        assert pos['sl'] == pytest.approx(100.2 * .92) and pos['tp'] is None
        assert pos['amount'] * pos['entry'] == pytest.approx(100, abs=.101)
    asyncio.run(check())


def test_gap_uses_actual_exit_fill_and_reports_excess_loss(tmp_path):
    async def check():
        data = StreamData()
        bot = V2PaperHunter(tmp_path / 'state.json', market_data=data)
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100)
        data.price = 88  # A gap can exceed the planned stop budget.
        await bot.manage_exits()
        record = bot.exchange.trades[-1]['exit_detail']
        assert record['exit_price'] == 88
        assert record['net_pnl_usdt'] == pytest.approx(bot.exchange.balance - 100)
        assert record['net_pnl_usdt'] < -3
    asyncio.run(check())


def test_legacy_state_upgrades_without_changing_account_or_size(tmp_path):
    async def check():
        data = StreamData()
        path = tmp_path / 'state.json'
        bot = V2PaperHunter(path, market_data=data)
        bot.cfg.pop('paper_position_notional_usdt')
        assert await bot.enter('BTC/USDT:USDT', 'buy', 100)
        state = json.loads(path.read_text())
        state['settings'].pop('active_exit')
        state['settings']['STOP_LOSS_PCT'] = .15
        state['settings']['TAKE_PROFIT_PCT'] = .018
        state['settings']['stop_basis'] = 'signal_price'
        old = state['strategy_positions']['BTC/USDT:USDT']
        state['strategy_positions']['BTC/USDT:USDT'] = {k: old[k] for k in ('side', 'entry', 'amount', 'tp')}
        state['strategy_positions']['BTC/USDT:USDT']['sl'] = 85
        (path.parent / 'journal.wal').unlink(missing_ok=True)
        path.write_text(json.dumps(state))
        resumed = V2PaperHunter(path, market_data=data)
        balance = resumed.exchange.balance
        amount = resumed.positions['BTC/USDT:USDT']['amount']
        await resumed.manage_exits()
        pos = resumed.positions['BTC/USDT:USDT']
        assert pos['legacy_migrated']
        assert pos['sl'] == 92
        assert pos['amount'] == amount
        assert resumed.exchange.balance == balance
    asyncio.run(check())
