"""Actual cumulative order VWAP; never substitutes a signal or ticker price."""
import math
from decimal import Decimal, InvalidOperation


def positive_decimal(value):
    try:
        value = Decimal(str(value))
        if value.is_finite() and value > 0 and math.isfinite(float(value)):
            return value
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        pass
    return None


def execution_price(order):
    average = positive_decimal(order.get('avgPrice'))
    if average is not None: return average, 'avgPrice'
    quantity = positive_decimal(order.get('executedQty'))
    quote = positive_decimal(order.get('cumQuote'))
    if quantity is not None and quote is not None:
        average = quote / quantity
        if math.isfinite(float(average)) and average > 0:
            return average, 'cumQuote/executedQty'
    return None, None
