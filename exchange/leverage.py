"""Solve the circular margin × leverage / notional bracket constraint."""
import math


def bracket_for(brackets, notional):
    for b in sorted(brackets, key=lambda b: float(b['notionalFloor'])):
        if float(b['notionalFloor']) <= notional < float(b['notionalCap']):
            return b
    raise ValueError('Notional outside account leverage brackets')


def choose_leverage(brackets, margin, price, round_amount, ceiling=None):
    if not all(math.isfinite(v) and v > 0 for v in (margin, price)):
        raise ValueError('Invalid current equity margin or price')
    maximum = max(int(b['initialLeverage']) for b in brackets)
    for leverage in range(min(maximum, ceiling or maximum), 0, -1):
        amount = round_amount(margin * leverage / price)
        if amount <= 0: continue
        try: bracket = bracket_for(brackets, amount * price)
        except ValueError: continue
        if leverage <= int(bracket['initialLeverage']):
            return {'leverage': leverage, 'amount': amount, 'margin': amount * price / leverage,
                    'notional': amount * price, 'bracket': bracket, 'confirmed': False}
    raise ValueError('No valid leverage for current equity and quantity precision')
