"""Requested isolated-margin approximation, not an exchange liquidation engine."""
import math
from .config import load_config

_CFG = load_config()["tp_sl"]
MMR = _CFG["mmr"]


def direction(side):
    if side in ("long", "buy"):
        return 1
    if side in ("short", "sell"):
        return -1
    raise ValueError(f"Invalid side: {side}")


def compute_liq_price(side: str, entry: float, leverage: float, mmr: float = MMR, maintenance_amount: float = 0, quantity: float = 1) -> float:
    if not all(math.isfinite(x) for x in (entry, leverage, mmr)) or entry <= 0 or leverage <= 0 or not 0 <= mmr < 1:
        raise ValueError("Invalid liquidation inputs")
    if not all(math.isfinite(v) for v in (maintenance_amount, quantity)) or maintenance_amount < 0 or quantity <= 0: raise ValueError('Invalid maintenance bracket')
    sm = direction(side)
    # Isolated equity = maintenance: Q*(E/L + sm*(P-E)) = Q*P*MMR - cum.
    return (entry * (1 - sm / leverage) - sm * maintenance_amount / quantity) / (1 - sm * mmr)


def max_safe_stop_pct(leverage: float, mmr: float = MMR,
                      safety_margin: float = _CFG["liq_safety_margin"]) -> float:
    if not all(math.isfinite(x) for x in (leverage, mmr, safety_margin)) or leverage <= 0 or min(mmr, safety_margin) < 0:
        raise ValueError("Invalid liquidation inputs")
    # Do not impose a minimum which could itself be beyond liquidation.
    distance = 1 / leverage - mmr - safety_margin
    if distance <= 0:
        raise ValueError("No positive liquidation-safe stop for these settings")
    return distance


def liquidation_stop_cap(side, entry, leverage, mmr, safety_margin, maintenance_amount=0, quantity=1):
    """Distance to the multiplicatively buffered liquidation boundary, per tier."""
    if not math.isfinite(safety_margin) or not 0 <= safety_margin < 1:
        raise ValueError('Invalid liquidation safety margin')
    sm = direction(side)
    liq = compute_liq_price(side, entry, leverage, mmr, maintenance_amount, quantity)
    boundary = liq * (1 + sm * safety_margin)
    distance = sm * (entry - boundary) / entry
    if not math.isfinite(distance) or distance <= 0:
        raise ValueError('No positive liquidation-safe stop for these settings')
    return distance


def validate_stop_against_liq(side: str, entry: float, stop: float,
                             leverage: float, mmr: float = MMR,
                             safety_margin: float = _CFG['liq_safety_margin'], maintenance_amount=None, quantity: float = 1) -> bool:
    sm = direction(side)
    liq = compute_liq_price(side, entry, leverage, mmr, maintenance_amount or 0, quantity)
    pct = sm * (entry - stop) / entry
    cap = (max_safe_stop_pct(leverage, mmr, safety_margin) if maintenance_amount is None else
           liquidation_stop_cap(side, entry, leverage, mmr, safety_margin, maintenance_amount, quantity))
    return (math.isfinite(stop) and 0 < pct <= cap + math.ulp(cap)
            and sm * (stop - liq * (1 + sm * safety_margin)) > 0)
