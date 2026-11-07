"""Exchange quantity constraints, independent of strategy and leverage policy."""
from decimal import Decimal
from math import lcm


class OrderValidationError(ValueError):
    """Local validation prevented any POST; not an execution-unknown event."""


def quantity_rules(filters):
    """A zero MARKET step does not discard its min/max quantity bounds."""
    rules = [filters[k] for k in ('LOT_SIZE', 'MARKET_LOT_SIZE') if k in filters]
    if not rules: raise OrderValidationError('Missing exchange quantity rules')
    steps = [Decimal(r['stepSize']) for r in rules if Decimal(r['stepSize']) > 0]
    if not steps: raise OrderValidationError('No positive exchange quantity step')
    scale = 10 ** max(max(0, -s.as_tuple().exponent) for s in steps)
    common = lcm(*(int(s * scale) for s in steps))
    return {'min': max(Decimal(r['minQty']) for r in rules),
            'max': min(Decimal(r['maxQty']) for r in rules),
            'step': Decimal(common) / scale}


def validate_quantity(market, amount, price=None):
    qty = Decimal(str(amount))
    if not qty.is_finite() or qty <= 0:
        raise OrderValidationError('quantity_nonpositive_or_invalid')
    rules = [market.get('filters', {})[k] for k in ('LOT_SIZE', 'MARKET_LOT_SIZE') if k in market.get('filters', {})]
    if not rules:
        limits = market['limits']['amount']
        rules = [{'minQty': str(limits.get('min', 0)), 'maxQty': str(limits.get('max', 'Infinity')), 'stepSize': market['step']}]
    for rule in rules:
        minimum, maximum, step = (Decimal(str(rule[k])) for k in ('minQty', 'maxQty', 'stepSize'))
        if qty < minimum: raise OrderValidationError('quantity_below_min')
        if qty > maximum: raise OrderValidationError('quantity_above_max')
        if step > 0 and (qty - minimum) % step:
            raise OrderValidationError('quantity_step_mismatch')
    if price is not None:
        value = Decimal(str(price))
        if not value.is_finite() or value <= 0: raise OrderValidationError('invalid_order_price')
        if qty * value < Decimal(str(market['limits']['cost'].get('min') or 0)):
            raise OrderValidationError('notional_below_min')
    return True
