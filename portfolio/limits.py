"""Pre-trade portfolio checks independent of model inference."""
from dataclasses import dataclass
from math import isfinite
from statistics import correlation


@dataclass(frozen=True)
class Exposure:
    symbol: str
    side: str
    notional: float
    stop_risk: float
    volatility: float

    def __post_init__(self):
        if (self.side not in ("buy", "sell") or not self.symbol
                or not all(isfinite(x) and x >= 0 for x in (self.notional, self.stop_risk, self.volatility))):
            raise ValueError("Invalid portfolio exposure")


def correlations(closed_prices, min_samples=15):
    """Align completed-candle returns by timestamp; missing data stays unknown."""
    results = {}
    for a, a_rows in closed_prices.items():
        for b, b_rows in closed_prices.items():
            if a >= b:
                continue
            keys = sorted(a_rows.keys() & b_rows.keys())
            if len(keys) >= min_samples:
                try:
                    results[a, b] = correlation([a_rows[t] for t in keys], [b_rows[t] for t in keys])
                except (ValueError, ArithmeticError):
                    continue
    return results


def risk_multiplier(equity, high_water, cfg):
    if not isfinite(equity) or not isfinite(high_water) or min(equity, high_water) <= 0:
        return 0.
    drawdown = 1-equity/high_water
    if drawdown >= cfg.drawdown_block:
        return 0.
    return .5 if drawdown >= cfg.drawdown_reduce else 1.


def check(current, candidate, equity, high_water, correlated, cfg):
    multiplier = risk_multiplier(equity, high_water, cfg)
    if not multiplier:
        return "DRAWDOWN_OR_INVALID_EQUITY"
    if any(p.symbol == candidate.symbol for p in current):
        return "SYMBOL_ALREADY_OPEN"
    all_positions = [*current, candidate]
    same_side = [p for p in all_positions if p.side == candidate.side]
    # When correlations are unavailable, conservatively group same-side exposures.
    cluster = [
        p for p in current
        if (corr := correlated.get(tuple(sorted((p.symbol, candidate.symbol))))) is None
        and p.side == candidate.side
        or corr is not None and (corr if p.side == candidate.side else -corr) >= cfg.correlation
    ]
    limits = (
        (candidate.notional, cfg.symbol_fraction, "SYMBOL_CONCENTRATION"),
        (sum(p.notional for p in all_positions), cfg.gross_fraction, "GROSS_EXPOSURE"),
        (sum(p.notional for p in same_side), cfg.direction_fraction, "DIRECTIONAL_EXPOSURE"),
        (sum(p.stop_risk for p in all_positions), cfg.aggregate_risk*multiplier, "AGGREGATE_STOP_RISK"),
        (candidate.stop_risk+sum(p.stop_risk for p in cluster), cfg.cluster_risk*multiplier,
         "HIGH_PORTFOLIO_CORRELATION"),
        (sum(p.notional*p.volatility for p in all_positions), cfg.volatility_exposure, "VOLATILITY_EXPOSURE"),
    )
    return next((reason for amount, limit, reason in limits if amount > equity*limit), None)
