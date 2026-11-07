"""Bind an entry risk decision to a single concrete futures order."""
from dataclasses import asdict, dataclass
from math import isfinite

from .qualification import fingerprint


@dataclass(frozen=True)
class EntryOrder:
    symbol: str
    side: str
    quantity: str
    order_type: str
    limit: str
    stop: float
    target: float
    risk_budget: float
    leverage: int
    config: str
    strategy: str
    strategy_version: int
    account_version: str

    @property
    def digest(self):
        return fingerprint(asdict(self))


def check_entry(order, book, now, asof, max_age, max_spread, equity, free, fee):
    reasons = []
    if (not 0 <= now-asof <= max_age or book is None or book.symbol != order.symbol
            or not book.fresh(now, max_age)):
        reasons.append("STALE_OR_UNSYNCED_MARKET")
    quantity, price = float(order.quantity), float(order.limit)
    if (order.side not in ("buy", "sell") or order.order_type != "LIMIT_IOC"
            or not all(isfinite(v) and v > 0 for v in (quantity, price, order.stop, order.target,
                    order.risk_budget, equity, free)) or not 1 <= order.leverage <= 10):
        return (*reasons, "INVALID_ORDER_OR_ACCOUNT")
    sign = 1 if order.side == "buy" else -1
    if sign*(price-order.stop) <= 0 or sign*(order.target-price) <= 0:
        reasons.append("INVALID_PROTECTION_ENVELOPE")
    if order.risk_budget > equity*.0025:
        reasons.append("RISK_BUDGET_EXCEEDED")
    if quantity*price*(1/order.leverage+fee) > free:
        reasons.append("INSUFFICIENT_MARGIN")
    if not reasons:
        if book.quote.spread >= max_spread:
            reasons.append("SPREAD_LIMIT")
        levels = book.asks if sign > 0 else book.bids
        if sum(q for p, q in levels.items() if sign*(p-price) <= 0) < quantity:
            reasons.append("INSUFFICIENT_EXECUTABLE_DEPTH")
    return tuple(reasons)
