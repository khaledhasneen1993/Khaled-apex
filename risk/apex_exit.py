"""State for experimental full-position exits. Used only in the Apex simulator."""
from dataclasses import dataclass
from math import isfinite

from .liq_math import direction


def breakeven(entry, side, fee, exit_slippage):
    sign = direction(side)
    return entry*(1+sign*fee)/((1-sign*fee)*(1-sign*exit_slippage))


@dataclass
class Exit:
    side: str
    entry: float
    stop: float
    target: float
    opened_ms: int
    extreme: float | None = None

    def update(self, price, atr, trend, spread, cfg, *, now_ms=None, timeout_ms=None, thesis_lost=False):
        if not all(isfinite(x) for x in (price, atr, trend, spread)) or price <= 0 or min(atr, spread) < 0:
            raise ValueError("Invalid exit observation")
        sign = direction(self.side)
        if sign*(price-self.stop) <= 0:
            return "STOP"
        if sign*(price-self.target) >= 0:
            return "TARGET"
        if timeout_ms is not None and now_ms-self.opened_ms >= timeout_ms and thesis_lost:
            return "TIME_STOP"
        old = self.extreme if self.extreme is not None else self.entry
        self.extreme = max(old, price) if sign > 0 else min(old, price)
        favorable = sign*(self.extreme-self.entry)
        be = breakeven(self.entry, self.side, cfg.fee, cfg.max_slippage)
        trail = max(atr*cfg.trail_atr*(1+min(1., abs(trend)/cfg.trend)), price*spread, price*cfg.fee*2)
        if favorable >= max(atr, abs(be-self.entry)):
            proposed = self.extreme-sign*trail
            if sign*(proposed-be) < 0:
                proposed = be
            if sign*(price-proposed) > 0 and sign*(proposed-self.stop) > 0:
                self.stop = proposed
        return None
