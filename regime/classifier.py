"""Rule-based regimes with priority for market-data and liquidity faults."""
from enum import StrEnum


class Regime(StrEnum):
    UP = "trending_up"
    DOWN = "trending_down"
    RANGE = "ranging"
    HIGH_VOL = "high_volatility"
    LOW_VOL = "low_volatility"
    STRESS = "liquidity_stress"
    UNCERTAIN = "uncertain"


def classify(f, cfg):
    if f is None:
        return Regime.UNCERTAIN
    if f.spread >= cfg.max_spread or f.depth_usdt < cfg.min_depth_usdt:
        return Regime.STRESS
    if f.short_vol > cfg.high_volatility or f.short_vol > 2.5 * max(cfg.low_volatility, f.long_vol):
        return Regime.HIGH_VOL
    if abs(f.trend) >= cfg.trend:
        return Regime.UP if f.trend > 0 else Regime.DOWN
    if f.long_vol < cfg.low_volatility:
        return Regime.LOW_VOL
    return Regime.RANGE
