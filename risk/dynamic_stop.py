"""Monotone stops only. Closing remains the responsibility of manage_exits."""
import math
from .liq_math import direction


def update_dynamic_stop(pos, market, cfg: dict) -> None:
    sm = direction(pos.side)
    if not math.isfinite(market.price) or market.price <= 0:
        raise ValueError("Invalid stop-management price")
    pnl_pct = (market.price - pos.entry) / pos.entry * sm
    if cfg.get('policy') == 'hard_stop_trailing':
        arm = pos.get('trail_arm_pct', cfg['trail_arm_pct'])
        distance = pos.get('trail_distance_pct', cfg['trail_distance_pct'])
        extreme = pos.get('trailing_extreme', pos.entry)
        if sm * (market.price - extreme) > 0: extreme = market.price
        pos['trailing_extreme'] = extreme
        gain = sm * (extreme / pos.entry - 1)
        if gain >= arm or math.isclose(gain, arm, abs_tol=2 * math.ulp(extreme) / pos.entry, rel_tol=0):
            pos['trailing_armed'] = pos.breakeven_armed = True
        if pos.get('trailing_armed'):
            candidate = extreme * (1 - sm * distance)
            if sm * (candidate - pos.stop) > 0:
                pos.stop = candidate
                pos['trail_moved'] = True
        return
    # Trigger detection tolerates price multiplication rounding only.
    if not pos.breakeven_armed and (pnl_pct >= cfg["be_trigger_pct"]
            or math.isclose(pnl_pct, cfg["be_trigger_pct"], rel_tol=0, abs_tol=2 * math.ulp(market.price) / pos.entry)):
        candidate = pos.entry * (1 + sm * cfg["be_offset_pct"])
        if sm * (candidate - pos.stop) > 0:
            pos.stop = candidate
        pos.breakeven_armed = True
    if pos.breakeven_armed and cfg["trail_enabled"] and math.isfinite(market.atr) and market.atr > 0:
        candidate = market.price - sm * market.atr * cfg["trail_atr_mult"]
        if sm * (candidate - pos.stop) > 0:
            pos.stop = candidate
