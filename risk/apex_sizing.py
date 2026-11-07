"""Fee and slippage-aware planning for research and shadow orders."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from math import isfinite

from .liq_math import direction, liquidation_stop_cap


@dataclass(frozen=True)
class Plan:
    quantity: float
    notional: float
    margin: float
    stop: float
    target: float
    stop_risk: float
    equity_risk: float


def plan(signal, features, cfg, equity, free, step, min_notional, slippage, risk_multiplier=1.):
    if (not all(isfinite(x) for x in (equity, free, step, min_notional, slippage, risk_multiplier))
            or min(equity, free, step, risk_multiplier) <= 0 or risk_multiplier > 1
            or min_notional < 0 or not 0 <= slippage <= cfg.max_slippage):
        raise ValueError("INVALID_SIZING_OR_SLIPPAGE")
    p = signal.reference
    sign = direction(signal.side)
    aligned_flow = sign * features.flow
    volatility_scale = 1.25 if features.short_vol > features.long_vol else 1.
    flow_scale = 1.1 if aligned_flow < cfg.min_flow else 1.
    distance = max(cfg.min_stop, cfg.atr_stop*features.atr/p*volatility_scale*flow_scale,
                   sign*(p-signal.structure)/p)
    cap = liquidation_stop_cap(signal.side, p, cfg.leverage, cfg.mmr, cfg.liquidation_buffer)
    if distance + cfg.max_slippage >= cap:
        raise ValueError("STOP_BEYOND_LIQUIDATION_BUFFER")
    worst_entry = p*(1+slippage)
    # Upper bounds include fees on the more expensive short stop and both fills.
    loss_per_unit = worst_entry*(distance+slippage+cfg.max_slippage
                                +cfg.fee*(2+distance+cfg.max_slippage))
    budget = equity*cfg.risk_fraction*risk_multiplier
    max_qty = min(budget/loss_per_unit, equity*cfg.margin_fraction*cfg.leverage/worst_entry,
                  free/(worst_entry*(1/cfg.leverage+cfg.fee)))
    quantum = Decimal(str(step))
    qty = float((Decimal(str(max_qty))/quantum).to_integral_value(rounding=ROUND_FLOOR)*quantum)
    notional = qty*p
    if qty <= 0 or notional < min_notional:
        raise ValueError("BELOW_MINIMUM_NOTIONAL")
    risk = qty*loss_per_unit
    return Plan(qty, notional, qty*worst_entry/cfg.leverage, p*(1-sign*distance),
                p*(1+sign*distance*cfg.reward_risk), risk, risk/equity)
