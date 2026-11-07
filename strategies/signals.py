"""Research strategies propose hypotheses. They have no exchange dependency."""
from dataclasses import dataclass
from regime.classifier import Regime


@dataclass(frozen=True)
class Signal:
    symbol: str
    side: str
    strategy: str
    asof_ms: int
    reference: float
    regime: Regime
    confidence: float
    gross_edge_proxy: float
    structure: float


def generate(f, regime, cfg):
    if regime not in (Regime.UP, Regime.DOWN, Regime.RANGE):
        return []
    signals = []
    direction = 1 if f.momentum > 0 else -1
    aligned = (direction == 1 and regime == Regime.UP) or (direction == -1 and regime == Regime.DOWN)
    funding = f.funding
    market_aligned = f.btc_momentum is None or direction * f.btc_momentum > -cfg.trend
    if (aligned and market_aligned and abs(f.momentum) >= cfg.momentum and f.spike >= cfg.volume_spike
            and direction*f.flow >= cfg.min_flow and direction*f.ofi > 0
            and (funding is None or direction*funding < cfg.fee)):
        signals.append(Signal(f.symbol, "buy" if direction == 1 else "sell", "cascade_v2", f.asof_ms, f.price,
                              regime, abs(f.flow), abs(f.momentum)/2,
                              f.recent_low if direction == 1 else f.recent_high))
    sweep_down = f.recent_low < f.range_low < f.price and f.flow > cfg.min_flow and f.ofi > 0
    sweep_up = f.recent_high > f.range_high > f.price and f.flow < -cfg.min_flow and f.ofi < 0
    if sweep_down != sweep_up and f.absorption >= cfg.min_flow:
        direction = 1 if sweep_down else -1
        if (direction == 1 and regime != Regime.DOWN) or (direction == -1 and regime != Regime.UP):
            distance = f.range_high-f.price if sweep_down else f.price-f.range_low
            signals.append(Signal(f.symbol, "buy" if sweep_down else "sell", "liquidity_reversal", f.asof_ms,
                                  f.price, regime, f.absorption, max(0, distance/f.price)/2,
                                  f.recent_low if sweep_down else f.recent_high))
    span = f.range_high-f.range_low
    if regime == Regime.RANGE and span > 0 and f.range_low <= f.price <= f.range_high:
        location = (f.price-f.range_low)/span
        if (location < .2 and f.flow > 0) or (location > .8 and f.flow < 0):
            buy = location < .2
            signals.append(Signal(f.symbol, "buy" if buy else "sell", "range_reversion", f.asof_ms, f.price,
                                  regime, min(1., abs(location-.5)*2), abs(location-.5)*span/f.price,
                                  f.range_low if buy else f.range_high))
    return signals
