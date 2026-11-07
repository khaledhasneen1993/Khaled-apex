"""All-account reconciliation; orphan positions outside the scanner are included."""
from risk.liq_math import direction


async def reconcile(exchange, state, journal):
    report = {'orphan_positions': [], 'phantom_positions': [], 'orphan_orders': [], 'size_mismatches': [], 'missing_protection': []}
    # Never limit this query to the scanner universe or known local symbols.
    remote = {exchange.symbol(p['symbol']) if hasattr(exchange, 'symbol') else p['symbol']: p
              for p in await exchange.get_position(None) if float(p['positionAmt']) != 0}
    local = state.open_positions
    cfg = state.cfg['recon']
    for symbol in set(remote) - set(local):
        report['orphan_positions'].append(symbol)
        if cfg['auto_close_orphans']:
            position = remote[symbol]
            await exchange.create_order(symbol, 'market', 'sell' if float(position['positionAmt']) > 0 else 'buy',
                                        abs(float(position['positionAmt'])), params={'reduceOnly': True})
        journal.log('orphan_close', symbol=symbol)
    for symbol in set(local) - set(remote):
        if hasattr(state, 'adopt_native_close') and await state.adopt_native_close(symbol):
            continue
        report['phantom_positions'].append(symbol)
        # Preserve strategy metadata until a remote exit can be attributed.
        journal.log('phantom_retained_for_review', symbol=symbol)
    for symbol in set(local) & set(remote):
        pos, actual = local[symbol], remote[symbol]
        qty, entry = abs(float(actual['positionAmt'])), float(actual['entryPrice'])
        expected_side = 'buy' if float(actual['positionAmt']) > 0 else 'sell'
        qty_diff = abs(pos['amount'] - qty) > cfg['size_tolerance_pct'] * max(pos['amount'], qty)
        entry_diff = abs(pos['entry'] - entry) > cfg['entry_tolerance_pct'] * entry
        if qty_diff or entry_diff or direction(pos['side']) != direction(expected_side):
            report['size_mismatches'].append(symbol)
            # Execution facts are immutable here. The live account snapshot
            # already contains the remaining quantity used for emergency closes.
            # Mutating the entry quantity would corrupt final accounting.
            journal.log('position_mismatch', symbol=symbol)
    owned = {o['clientOrderId'] for pos in local.values() for o in pos.get('protection', {}).values()}
    owned |= {o['clientOrderId'] for o in state.open_orders.values()}
    for symbol, fault in getattr(exchange, 'management_faults', {}).items():
        if fault.get('status') != 'resolved' and fault.get('position'):
            owned |= {ref['clientOrderId'] for _, ref in exchange.protection_references(symbol, fault['position'])}
    remote_orders = await exchange.get_open_orders()
    remote_ids = {o['clientOrderId'] for o in remote_orders}
    for symbol, pos in list(local.items()):
        if hasattr(exchange, 'protect') and symbol in remote:
            for name in (('sl',) if pos.get('tp') is None else ('sl', 'tp')):
                if name not in pos.get('protection', {}): report['missing_protection'].append(symbol + ':' + name)
        for protection in pos.get('protection', {}).values():
            if protection['clientOrderId'] in remote_ids: continue
            # A native fill can race the account/open-order snapshots. Confirm
            # whether it really closed before classifying it as missing protection.
            latest = [p for p in await exchange.get_position(symbol) if float(p['positionAmt']) != 0]
            if not latest and hasattr(state, 'adopt_native_close') and await state.adopt_native_close(symbol):
                break
            report['missing_protection'].append(protection['clientOrderId'])
    for order in remote_orders:
        if order['clientOrderId'] not in owned:
            report['orphan_orders'].append(order['clientOrderId'])
            order_symbol = exchange.symbol(order['symbol']) if hasattr(exchange, 'symbol') else order['symbol']
            protective = order.get('reduceOnly') is True or order.get('reduceOnly') == 'true' or order.get('closePosition') is True
            # Keep an unregistered protective order in place until the caller's
            # latched flatten succeeds; never create a naked-position window.
            if protective and order_symbol in remote:
                journal.log('orphan_protection_retained', symbol=order_symbol)
                continue
            if order.get('is_algo'):
                await exchange.cancel_algo(order)
            else:
                await exchange.cancel_order(order['symbol'], order_id=order['orderId'])
            journal.log('orphan_order_cancelled', symbol=order['symbol'], client_id=order['clientOrderId'])
    return report
