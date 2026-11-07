import math
from risk.liq_math import direction
from risk.config import load_config

CORRELATED_GROUPS = [set(group) for group in load_config()['exposure']['correlated_groups']]


def raw_symbol(symbol):
    return symbol.split(':')[0].replace('/', '')


def directional_exposure(state):
    longs = shorts = 0.
    for pos in state.open_positions.values():
        notional = pos['entry'] * pos['amount'] if isinstance(pos, dict) else pos.notional
        side = pos['side'] if isinstance(pos, dict) else pos.side
        if direction(side) > 0:
            longs += notional
        else:
            shorts += notional
    return {'long': longs, 'short': shorts, 'net': longs - shorts, 'gross': longs + shorts}


def can_open(state, symbol, side, new_notional, cfg):
    if not math.isfinite(state.equity) or state.equity <= 0 or not math.isfinite(new_notional) or new_notional <= 0:
        return False, 'invalid_equity_or_notional'
    if len(state.open_positions) >= cfg['max_concurrent']:
        return False, 'max_positions'
    exp = directional_exposure(state)
    # Config explicitly specifies the basis. Notional is the conservative default
    # for callers supplying the example dictionary without a basis field.
    divisor = cfg['leverage'] if cfg.get('basis', 'notional') == 'margin' else 1
    if cfg.get('basis') == 'margin':
        exp = {'long': 0., 'short': 0., 'gross': 0.}
        for pos in state.open_positions.values():
            margin = pos['entry'] * pos['amount'] / pos.get('leverage', cfg['leverage'])
            exp['long' if direction(pos['side']) > 0 else 'short'] += margin
            exp['gross'] += margin
        # New exposure is explicitly a margin amount when using variable leverage.
        new_notional /= cfg.get('candidate_leverage', cfg['leverage'])
        divisor = 1
    name = 'long' if direction(side) > 0 else 'short'
    tolerance = math.ulp(state.equity)
    if (exp[name] + new_notional) / divisor > cfg[f'max_{name}_exposure_pct'] * state.equity + tolerance:
        return False, f'max_{name}_exposure'
    if (exp['gross'] + new_notional) / divisor > cfg['max_gross_exposure_pct'] * state.equity + tolerance:
        return False, 'max_gross_exposure'
    for group in cfg.get('correlated_groups', CORRELATED_GROUPS):
        if raw_symbol(symbol) not in group:
            continue
        same_dir = sum(raw_symbol(s) in group and direction(p['side'] if isinstance(p, dict) else p.side) == direction(side)
                       for s, p in state.open_positions.items())
        if same_dir >= cfg['max_correlated_same_direction']:
            return False, 'correlated_limit'
    return True, 'ok'
