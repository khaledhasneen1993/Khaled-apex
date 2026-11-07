"""Rank simultaneous hypotheses. Scores are proxies until calibrated out of sample."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Candidate:
    signal: object
    net_edge: float
    slippage: float
    stop_distance: float
    correlation: float
    depth: float

    @property
    def score(self):
        liquidity = min(1., self.depth/10000)
        return self.net_edge * self.signal.confidence * liquidity / (self.stop_distance*(1+self.correlation))


def rank(candidates, now_ms, cfg):
    return sorted(
        (c for c in candidates if 0 <= now_ms-c.signal.asof_ms <= cfg.max_age_ms and c.net_edge > 0),
        key=lambda c: (-c.score, c.signal.asof_ms, c.signal.symbol, c.signal.strategy),
    )
