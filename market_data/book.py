"""USD-M local book. Quantities in updates replace the previous quantities."""
from dataclasses import dataclass
from math import isfinite, inf


class BookGap(ValueError):
    pass


def levels(rows):
    parsed = {}
    for price, qty, *_ in rows:
        price, qty = float(price), float(qty)
        if not isfinite(price) or not isfinite(qty) or price <= 0 or qty < 0:
            raise ValueError("Invalid depth level")
        parsed[price] = qty
    return parsed


@dataclass(frozen=True)
class Quote:
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float

    @property
    def mid(self):
        return (self.bid + self.ask) / 2

    @property
    def spread(self):
        return (self.ask - self.bid) / self.mid


def imbalance_change(old, new):
    bid = (new.bid_qty if new.bid >= old.bid else 0) - (old.bid_qty if new.bid <= old.bid else 0)
    ask = (new.ask_qty if new.ask <= old.ask else 0) - (old.ask_qty if new.ask >= old.ask else 0)
    return (bid - ask) / (old.bid_qty + old.ask_qty + new.bid_qty + new.ask_qty)


class OrderBook:
    def __init__(self, symbol):
        self.symbol = symbol
        self.invalidate()

    def invalidate(self):
        self.bids, self.asks = {}, {}
        self.last = self.event_ms = self.received_ms = None
        self.synced = False

    def snapshot(self, data):
        self.invalidate()
        bids, asks = levels(data["bids"]), levels(data["asks"])
        self.bids = {p: q for p, q in bids.items() if q}
        self.asks = {p: q for p, q in asks.items() if q}
        self.last = int(data["lastUpdateId"])

    @property
    def quote(self):
        bid, ask = max(self.bids), min(self.asks)
        if bid >= ask:
            raise ValueError("Crossed book")
        return Quote(bid, self.bids[bid], ask, self.asks[ask])

    def update(self, data, received_ms):
        try:
            return self._update(data, received_ms)
        except (ValueError, TypeError, KeyError, OverflowError):
            self.invalidate()
            raise

    def _update(self, data, received_ms):
        if self.last is None or data["s"] != self.symbol:
            raise BookGap("Snapshot required")
        first, last, prev = (int(data[k]) for k in ("U", "u", "pu"))
        event = int(data["E"])
        if last < self.last or (self.synced and last == self.last):
            return None
        if (first > last or prev >= last or event > received_ms
                or (self.event_ms is not None and event < self.event_ms)
                or (self.synced and prev != self.last)
                or (not self.synced and not first <= self.last <= last)):
            raise BookGap("Invalid sequence or timestamp; rebuild book")
        old = self.quote if self.synced else None
        removed = 0.
        for side, rows in ((self.bids, data["b"]), (self.asks, data["a"])):
            for price, qty in levels(rows).items():
                removed += max(0., side.get(price, 0.) - qty) * price
                if qty:
                    side[price] = qty
                else:
                    side.pop(price, None)
        new = self.quote
        self.last, self.event_ms, self.received_ms, self.synced = last, event, received_ms, True
        # Depletion is observable; public depth alone cannot distinguish its cause.
        return (imbalance_change(old, new) if old else 0.), removed

    def fresh(self, now_ms, age_ms):
        return (self.synced and 0 <= now_ms - self.received_ms <= age_ms
                and 0 <= now_ms - self.event_ms <= age_ms)

    def depth(self, count=10):
        return sum(p*q for p, q in sorted(self.bids.items(), reverse=True)[:count]
                   + sorted(self.asks.items())[:count])

    def impact(self, side, qty):
        if not self.synced or not isfinite(qty) or qty <= 0 or side not in ("buy", "sell"):
            return inf
        remaining, cost = qty, 0.
        for p, q in sorted((self.asks if side == "buy" else self.bids).items(), reverse=side == "sell"):
            filled = min(remaining, q)
            cost += filled*p
            remaining -= filled
            if remaining <= 0:
                direction = 1 if side == "buy" else -1
                return max(0., direction*(cost/qty/self.quote.mid - 1))
        return inf
