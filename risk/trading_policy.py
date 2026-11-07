"""Shared price-stop policy and fee-aware quantity budget for Paper and Demo."""
import math


def fixed_exits(cfg):
    return cfg['exit'].get('policy') == 'hard_stop_trailing'


def paper_notional_ceiling(cfg):
    if cfg['exchange']['mode'] == 'paper' and fixed_exits(cfg):
        return cfg.get('paper_position_notional_usdt')
    return None


def entry_amount(equity, price, leverage, cfg, fee_rate, round_amount=lambda q: q):
    if not all(math.isfinite(v) and v > 0 for v in (equity, price, leverage)):
        raise ValueError('Invalid sizing inputs')
    target = equity * cfg['margin_fraction_per_position'] * leverage / price
    notional_ceiling = paper_notional_ceiling(cfg)
    if notional_ceiling is not None:
        target = min(target, notional_ceiling / price)
    if not fixed_exits(cfg):
        return round_amount(target)
    # Conservative short-side stop fee, plus adverse entry-price allowance.
    stop = cfg['exit']['hard_stop_pct']
    exit_slip = cfg['slippage'].get('paper_exit_slippage_bps', 0) / 10000 if cfg['exchange']['mode'] == 'paper' else 0
    unit_loss = price * (1 + cfg['slippage']['max_expected_entry_bps'] / 10000) * (
        stop + exit_slip*(1+stop) + fee_rate*(2+stop)*(1+exit_slip))
    budgeted = equity * cfg['tp_sl']['max_equity_risk_pct'] / unit_loss
    cap = min(target, budgeted)
    rounded = round_amount(cap)
    if not 0 <= rounded <= cap + math.ulp(cap):
        raise ValueError("Quantity precision rounded above the risk ceiling")
    return rounded


def stop_reason(pos):
    if pos.get('exit_policy') == 'hard_stop_trailing':
        return 'TRAILING_SL' if pos.get('trailing_armed') else 'HARD_SL'
    return 'SL'


def expected_slippage_bps(book, side, amount, reference):
    if not math.isfinite(reference) or reference <= 0 or not math.isfinite(amount) or amount <= 0:
        raise ValueError('Invalid order-book estimate')
    remaining, quote = amount, 0.
    for price, quantity, *_ in book['asks' if side == 'buy' else 'bids']:
        price, quantity = float(price), float(quantity)
        if not math.isfinite(price) or not math.isfinite(quantity) or price <= 0 or quantity < 0:
            raise ValueError('Invalid order-book level')
        taken = min(remaining, quantity)
        quote += taken * price
        remaining -= taken
        if remaining <= math.ulp(amount): break
    if remaining > math.ulp(amount): return math.inf
    return max(0., (quote / amount / reference - 1) * (1 if side == 'buy' else -1) * 10000)
