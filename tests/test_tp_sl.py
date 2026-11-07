"""The 13 required acceptance tests; both sides and seeded randomized coverage."""
import asyncio
import random
from types import SimpleNamespace

import pytest

from risk.config import load_config
from risk.liq_math import compute_liq_price, max_safe_stop_pct, validate_stop_against_liq
from risk.atr_levels import compute_atr, compute_tp_sl
from risk.dynamic_stop import update_dynamic_stop
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from test_continuous import StreamData

CFG = load_config()


def test_liq_price_long():
    assert compute_liq_price('long', 100, 50, .004) == pytest.approx(98.4, abs=.01)


def test_liq_price_short():
    assert compute_liq_price('short', 100, 50, .004) == pytest.approx(101.6, abs=.01)


def test_max_safe_stop_pct():
    assert max_safe_stop_pct(50, .004, .003) == pytest.approx(.013, abs=1e-6)


def test_stop_inside_liq():
    for leverage in (10, 25, 50):
        for side, sm in (('long', 1), ('short', -1)):
            levels = compute_tp_sl(side, 100, 3, leverage, CFG['tp_sl'])
            assert validate_stop_against_liq(side, 100, levels['stop'], leverage)
            invalid = 100 * (1 - sm * (max_safe_stop_pct(leverage) + .0001))
            assert not validate_stop_against_liq(side, 100, invalid, leverage)


def test_atr_basic():
    candles = [{'high': 102., 'low': 98., 'close': 100.} for _ in range(15)]
    assert compute_atr(candles) == pytest.approx(4., abs=1e-6)
    # A gap contributes true range even when the current candle is flat.
    candles[-1] = {'high': 110., 'low': 110., 'close': 110.}
    assert compute_atr(candles) == pytest.approx((13 * 4 + 10) / 14)
    with pytest.raises(ValueError):
        compute_atr(candles[:14])


def test_tp_sl_rr_min():
    for side in ('long', 'short'):
        for entry in (.001, 1., 100., 100000.):
            for pct in (.000001, .001, .004, .015, .03, .1):
                result = compute_tp_sl(side, entry, entry * pct, 50, CFG['tp_sl'])
                assert result['rr'] >= CFG['tp_sl']['min_rr']


def test_tp_sl_capped_in_high_vol():
    for side in ('long', 'short'):
        levels = compute_tp_sl(side, 100000, 3000, 50, CFG['tp_sl'])
        assert levels['stop_pct'] <= max_safe_stop_pct(50)
        assert levels['stop_capped'] is True


def test_breakeven_after_trigger():
    for side, sm in (('long', 1), ('short', -1)):
        pos = SimpleNamespace(side=side, entry=100., stop=100 - sm, breakeven_armed=False)
        market = SimpleNamespace(price=100 * (1 + sm * CFG['exit']['be_trigger_pct']), atr=1.)
        update_dynamic_stop(pos, market, {**CFG['exit'], 'policy': 'atr'})
        assert (1 if side == 'long' else -1) * (pos.stop - 100) >= CFG['exit']['be_offset_pct'] * 100 - 1e-10
        assert pos.breakeven_armed is True


def test_trailing_follows_price():
    for side, sm in (('long', 1), ('short', -1)):
        pos = SimpleNamespace(side=side, entry=100., stop=100., breakeven_armed=True)
        stops = [pos.stop]
        for step in (1, 2, 3):
            update_dynamic_stop(pos, SimpleNamespace(price=100 + sm * step, atr=.4), {**CFG['exit'], 'policy': 'atr'})
            stops.append(pos.stop)
        assert all(sm * (b - a) > 0 for a, b in zip(stops, stops[1:]))


def test_trailing_never_loosens():
    for side, sm in (('long', 1), ('short', -1)):
        pos = SimpleNamespace(side=side, entry=100., stop=100 + sm, breakeven_armed=True)
        for price in (100.5, 100., 99.5):
            update_dynamic_stop(pos, SimpleNamespace(price=price, atr=2.), {**CFG['exit'], 'policy': 'atr'})
            assert pos.stop == 100 + sm
        # An externally tightened stop must not be loosened when BE first arms.
        pos.breakeven_armed = False
        update_dynamic_stop(pos, SimpleNamespace(price=100 + sm, atr=2.), {**CFG['exit'], 'policy': 'atr'})
        assert pos.stop == 100 + sm


def test_manage_exits_tp(tmp_path):
    async def check():
        for side in ('buy', 'sell'):
            data = StreamData()
            bot = V2PaperHunter(tmp_path / (side + '.json'), market_data=data)
            assert await bot.enter('BTC/USDT:USDT', side, 100)
            pos = bot.positions['BTC/USDT:USDT']
            sm = 1 if side == 'buy' else -1
            data.price = 100 * (1 + sm * .02)
            await bot.manage_exits()
            assert bot.positions
            data.price = bot.positions['BTC/USDT:USDT']['sl']
            await bot.manage_exits()
            assert not bot.positions
            assert bot.exchange.trades[-1]['exit_detail']['exit_reason'] == 'TRAILING_SL'
    asyncio.run(check())


def test_manage_exits_sl(tmp_path):
    async def check():
        for side in ('buy', 'sell'):
            data = StreamData()
            bot = V2PaperHunter(tmp_path / (side + '.json'), market_data=data)
            assert await bot.enter('BTC/USDT:USDT', side, 100)
            data.price = bot.positions['BTC/USDT:USDT']['sl']
            await bot.manage_exits()
            assert not bot.positions
            assert bot.exchange.trades[-1]['exit_detail']['exit_reason'] == 'HARD_SL'
    asyncio.run(check())


def test_stop_capped_never_exceeds_liq():
    rng = random.Random(20261104)
    for leverage in (10, 15, 20, 25, 50):
        for _ in range(100):
            entry = 10 ** rng.uniform(-4, 6)
            atr = entry * rng.uniform(.000001, .2)
            for side in ('long', 'short'):
                levels = compute_tp_sl(side, entry, atr, leverage, CFG['tp_sl'])
                assert validate_stop_against_liq(side, entry, levels['stop'], leverage)
                assert levels['stop_pct'] <= max_safe_stop_pct(leverage)
                assert levels['rr'] >= 1.5
