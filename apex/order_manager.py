"""Deterministic reservations shared by Apex Paper and replay."""
from dataclasses import dataclass


@dataclass(frozen=True)
class PendingEntry:
    id: str
    decision: object
    due: int
    expires: int
    margin: float


class OrderManager:
    def __init__(self):
        self.pending = {}
        self.sequence = 0
        self.events = []

    @property
    def margin(self):
        return sum(order.margin for order in self.pending.values())

    def exposures(self, excluding=None):
        return [o.decision.exposure for o in self.pending.values() if o is not excluding]

    def reserve(self, decision, now, latency, cfg):
        symbol = decision.signal.symbol
        if symbol in self.pending:
            raise ValueError("Symbol already reserved")
        self.sequence += 1
        p = decision.plan
        adverse_notional = p.notional*(1+cfg.max_slippage)
        order = PendingEntry(f"apex-{self.sequence:010d}", decision, now+latency,
                             decision.signal.asof_ms+cfg.max_age_ms,
                             max(p.margin, adverse_notional/cfg.leverage)+adverse_notional*cfg.fee)
        self.pending[symbol] = order
        self.log(order, now, "INTENT")
        return order

    def finish(self, order, now, status, reason=None, quantity=0., price=None, fee=0.):
        symbol = order.decision.signal.symbol
        if self.pending.get(symbol) is not order:
            raise ValueError("Order already resolved")
        del self.pending[symbol]
        self.log(order, now, status, reason, quantity, price, fee)

    def log(self, order, now, status, reason=None, quantity=0., price=None, fee=0.):
        signal = order.decision.signal
        requested = order.decision.plan.quantity
        self.events.append(dict(time=now, id=order.id, symbol=signal.symbol, side=signal.side,
                                strategy=signal.strategy, strategy_version=1, status=status,
                                reason=reason, requested=requested, filled=quantity, price=price, fee=fee,
                                unfilled=max(0., requested-quantity),
                                reserved_margin=order.margin if status == "INTENT" else 0.,
                                reserved_risk=order.decision.exposure.stop_risk if status == "INTENT" else 0.))
