"""ATR levels, with strict liquidation and optional account risk caps."""
import math
from .liq_math import direction, compute_liq_price, max_safe_stop_pct, validate_stop_against_liq, liquidation_stop_cap


def compute_atr(candles: list, period: int = 14) -> float:
    if not isinstance(period, int) or period <= 0 or len(candles) < period + 1:
        raise ValueError(f"Need >= {period + 1} candles and a positive integer period")
    trs = []
    for previous, candle in zip(candles, candles[1:]):
        h, l, prev_c = candle["high"], candle["low"], previous["close"]
        if not all(math.isfinite(x) and x > 0 for x in (h, l, prev_c)) or h < l:
            raise ValueError("Invalid ATR candle")
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return sum(trs[-period:]) / period


def compute_tp_sl(side: str, entry: float, atr: float, leverage: float, cfg: dict) -> dict:
    if not math.isfinite(entry) or entry <= 0 or not math.isfinite(atr) or atr < 0:
        raise ValueError("Invalid entry/ATR")
    if min(cfg["atr_stop_mult"], cfg["atr_tp_mult"], cfg["min_stop_pct"], cfg["min_rr"]) <= 0:
        raise ValueError("Exit distances must be positive")
    sm = direction(side)
    raw = max(atr * cfg["atr_stop_mult"], entry * cfg["min_stop_pct"])
    max_pct = (liquidation_stop_cap(side, entry, leverage, cfg['mmr'], cfg['liq_safety_margin'],
                    cfg['maintenance_amount'], cfg.get('quantity', 1)) if 'maintenance_amount' in cfg else
               max_safe_stop_pct(leverage, cfg['mmr'], cfg['liq_safety_margin']))
    max_dist = entry * max_pct
    # The multiplicative short validation is slightly stricter than the additive
    # distance cap. Respect BOTH, with an inward floating-point boundary.
    liq = compute_liq_price(side, entry, leverage, cfg["mmr"], cfg.get('maintenance_amount', 0), cfg.get('quantity', 1))
    boundary = liq * (1 + sm * cfg["liq_safety_margin"])
    strict_dist = sm * (entry - math.nextafter(boundary, entry))
    safe_dist = min(max_dist, strict_dist)
    distance = min(raw, safe_dist)
    liq_capped = raw >= safe_dist
    equity_cap = cfg.get("max_stop_distance")
    risk_capped = equity_cap is not None and distance > equity_cap
    if equity_cap is not None:
        distance = min(distance, equity_cap)
    if distance <= 0:
        raise ValueError("No positive stop distance")
    stop = entry - sm * distance
    # Round toward entry; this also keeps a capped distance within its bound.
    if sm * (entry - stop) > distance:
        stop = math.nextafter(stop, entry)
    while sm * (entry - stop) / entry > max_pct:
        stop = math.nextafter(stop, entry)
    distance = sm * (entry - stop)
    if not validate_stop_against_liq(side, entry, stop, leverage, cfg["mmr"], cfg["liq_safety_margin"], cfg.get("maintenance_amount"), cfg.get("quantity", 1)):
        raise RuntimeError("Computed stop violates liquidation constraint")
    tp_distance = max(atr * cfg["atr_tp_mult"], distance * cfg["min_rr"])
    tp = entry + sm * tp_distance
    # Rounding must not put the actual price-based R:R below the requested one.
    if sm * (tp - entry) / distance < cfg["min_rr"]:
        tp = math.nextafter(tp, math.inf if sm > 0 else -math.inf)
    return {"stop": stop, "tp": tp, "stop_pct": distance / entry,
            "tp_pct": sm * (tp - entry) / entry,
            "rr": sm * (tp - entry) / distance,
            "stop_capped": liq_capped, "risk_capped": risk_capped}
