"""Round trips from actual Paper fills; fees include entry and all exit fills."""
import math

def round_trips(fills):
    opened, closed = {}, []
    for fill in fills:
        key = (fill['symbol'], fill.get('signal_id'))
        if not fill.get('reduceOnly'):
            if key not in opened:
                opened[key] = dict(symbol=fill['symbol'], signal_id=fill.get('signal_id'),
                    side=fill['side'], entry_time=fill.get('timestamp'), fills=[], amount=0.,
                    cost=0., fees_usdt=0., exit_amount=0., exit_cost=0., gross_pnl_usdt=0.,
                    entry_reason=fill.get('entry_reason'), entry_signal=fill.get('entry_signal'))
            p = opened[key]
            p['fills'].append(fill)
            p['amount'] += fill['amount']
            p['cost'] += fill['amount'] * fill['price']
            p['fees_usdt'] += fill.get('fee_usdt', 0)
            p['average_entry'] = p['cost'] / p['amount']
            p['adds'] = sum(not f.get('reduceOnly') for f in p['fills']) - 1
        else:
            p = opened.get(key)
            if p is None:
                continue  # Do not invent a missing entry.
            p['fills'].append(fill)
            p['fees_usdt'] += fill.get('fee_usdt', 0)
            p['exit_amount'] += fill['amount']
            p['exit_cost'] += fill['amount'] * fill['price']
            p['gross_pnl_usdt'] += fill.get('pnl_usdt', 0)
            if not math.isclose(p['exit_amount'], p['amount'], rel_tol=1e-8, abs_tol=1e-10):
                continue
            p.update(exit_time=fill.get('timestamp'), exit_price=p['exit_cost']/p['exit_amount'],
                     exit_reason=fill.get('exit_reason'), trigger_price=fill.get('trigger_price'),
                     exit_detail=fill.get('exit_detail'),
                     net_pnl_usdt=p['gross_pnl_usdt']-p['fees_usdt'])
            closed.append(p)
            del opened[key]
    return list(opened.values()), closed
