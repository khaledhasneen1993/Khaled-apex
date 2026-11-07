"""Displayed-depth execution model for replay, independent of exchange clients."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from math import fsum


def floor_quantity(quantity, step):
    quantum = Decimal(str(step))
    units = (Decimal(str(quantity))/quantum).to_integral_value(rounding=ROUND_FLOOR)
    return float(units*quantum)


@dataclass(frozen=True)
class Fill:
    quantity: float
    price: float
    levels: tuple


class DisplayedLiquidity:
    def __init__(self):
        self.consumed = {}

    def observe(self, symbol, book):
        # A removed price loses its reservation. Unchanged displayed liquidity
        # never refills an earlier simulated order.
        for side, depth in (("buy", book.asks), ("sell", book.bids)):
            used = self.consumed.get((symbol, side), {})
            for price in tuple(used):
                if price not in depth:
                    del used[price]
                else:
                    used[price] = min(used[price], depth[price])

    def quote(self, symbol, book, side, quantity, step, stress, *, partial, limit=None):
        sign = 1 if side == "buy" else -1
        depth = book.asks if side == "buy" else book.bids
        used = self.consumed.get((symbol, side), {})
        levels = []
        for price in sorted(depth, reverse=side == "sell"):
            adjusted = price*(1+sign*stress)
            if limit is not None and sign*(adjusted-limit) > 0:
                break
            remaining = max(0., depth[price]-used.get(price, 0.))
            if remaining:
                levels.append((price, remaining))
        capacity = fsum(qty for _, qty in levels)
        filled = quantity if capacity >= quantity else floor_quantity(capacity, step)
        if filled <= 0 or (filled < quantity and not partial):
            return None
        left = filled
        takes = []
        for price, available in levels:
            amount = min(left, available)
            takes.append((price, amount))
            left -= amount
            if left <= 0:
                break
        average = fsum(p*q for p, q in takes)/filled
        return Fill(filled, average*(1+sign*stress), tuple(takes))

    def commit(self, symbol, side, fill):
        used = self.consumed.setdefault((symbol, side), {})
        for price, quantity in fill.levels:
            used[price] = used.get(price, 0.)+quantity
