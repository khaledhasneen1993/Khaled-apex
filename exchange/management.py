"""Durable symbol-scoped management faults and bounded protection recovery."""
import asyncio
import copy
import time
from datetime import datetime, timezone

from infra.errors import diagnostic_fields
from .orders import ExchangeError, ExecutionUnknown, OrderRejected, safe_create_order
from .protection_state import ProtectionGuardError

CONDITIONAL = ('STOP_MARKET', 'TAKE_PROFIT_MARKET')


class ProtectionRecovery:
    def management_fault(self, symbol, reason, pos=None, **fields):
        previous = self.management_faults.get(symbol, {})
        record = {**previous, 'reason': reason, 'status': 'pending',
                  'updated_at': datetime.now(timezone.utc).isoformat(), **fields}
        if previous.get('status') == 'resolved':
            record.update(flat_confirmed=False, accounting_pending=False, retirement_pending=False, close_pending=False, obsolete=[])
            record.update(fields)
        if pos is not None: record['position'] = copy.deepcopy(pos)
        if previous.get('status') == 'resolved':
            record['history'] = previous.get('history', []) + [{
                k: previous[k] for k in ('reason', 'status', 'updated_at') if k in previous}]
        self.management_faults[symbol] = record
        self.write_state(self.management_path, self.management_faults)
        return record

    def resolve_management_fault(self, symbol):
        record = self.management_faults.get(symbol)
        if record is None: return
        record.update(status='pending' if record.get('accounting_pending') else 'resolved', close_pending=False, retirement_pending=False, obsolete=[],
                      updated_at=datetime.now(timezone.utc).isoformat())
        self.write_state(self.management_path, self.management_faults)

    def management_blocked(self, symbol):
        fault = self.management_faults.get(symbol, {})
        return fault.get('status') not in (None, 'resolved') or fault.get('entry_block_until', 0) > time.time()

    def management_closed(self, symbol):
        fault = self.management_faults.get(symbol, {})
        return fault.get('status') != 'resolved' and fault.get('flat_confirmed', False)

    def protection_references(self, symbol, pos):
        refs = {r['clientOrderId']: (name, r) for name, r in pos.get('protection', {}).items()}
        fault = self.management_faults.get(symbol, {})
        for name, ref in fault.get('obsolete', []):
            refs.setdefault(ref['clientOrderId'], (name, ref))
        for cid, intent in self.intents.items():
            options = intent.get('params', {})
            if (intent['symbol'] != symbol or intent['type'] not in CONDITIONAL
                    or intent['status'] == 'rejected' or options.get('signal_id') != pos.get('signal_id')):
                continue
            if intent.get('protection_committed') and cid not in refs: continue
            response = intent.get('response', {})
            ref = {'clientOrderId': cid, 'orderId': intent.get('order_id'),
                'is_algo': options.get('protection_transport') != 'regular', 'amount': intent['amount'],
                'trigger': str(response.get('triggerPrice', response.get('stopPrice', options.get('stopPrice', 0))))}
            refs.setdefault(cid, (options.get('protection_role', 'sl'), ref))
        return list(refs.values())

    async def query_protection(self, symbol, ref):
        try:
            if ref.get('is_algo', True):
                return await self._request('GET', '/fapi/v1/algoOrder', {'clientAlgoId': ref['clientOrderId']}, retry=True)
            return await self._request('GET', '/fapi/v1/order', {
                'symbol': self.raw(symbol), 'origClientOrderId': ref['clientOrderId']}, retry=True)
        except ExchangeError as error:
            if error.code == -2013: return None
            raise

    async def cancel_protection(self, symbol, ref):
        if ref.get('is_algo', True):
            if ref.get('orderId') is not None: return await self.cancel_algo(ref)
            return await self._request('DELETE', '/fapi/v1/algoOrder', {'clientAlgoId': ref['clientOrderId']})
        return await self.cancel_order(symbol, client_id=ref['clientOrderId'])

    async def protect_role(self, symbol, pos, name, kind, target, transport='algo'):
        self.execution_stage = 'protection_' + name
        old = pos.get('protection', {}).get(name)
        round_up = pos['side'] == 'buy'
        if name == 'sl' and old:
            sign = 1 if pos['side'] == 'buy' else -1
            if sign * (target - float(old['trigger'])) < 0:
                target = pos['sl'] = float(old['trigger'])
        trigger = self._price(symbol, target, round_up)
        if old and old['trigger'] == trigger and old.get('amount') == pos['amount']: return
        pending = next(((cid, intent) for cid, intent in self.intents.items()
            if intent['symbol'] == symbol and intent['type'] == kind
            and intent['status'] != 'rejected' and not intent.get('protection_committed')
            and intent.get('params', {}).get('signal_id') == pos['signal_id']), None)
        if pending:
            cid, intent = pending
            transport = intent.get('params', {}).get('protection_transport', 'algo')
            options = intent.get('params', {})
            trigger = self._price(symbol, options['stopPrice'], options['round_up'])
            old = options.get('replaces', old)
            result = await self.query_order_by_client_id(symbol, cid)
            if not result: raise ExecutionUnknown()
            self.resolve_intent(cid, result)
        else:
            result = await safe_create_order(self, {'symbol': symbol, 'type': kind,
                'side': 'sell' if pos['side'] == 'buy' else 'buy', 'amount': pos['amount'],
                'params': {'reduceOnly': True, 'stopPrice': float(trigger), 'round_up': round_up,
                    'signal_id': pos['signal_id'], 'protection_role': name,
                    'replaces': copy.deepcopy(old), 'protection_transport': transport}}, self.journal)
            cid = result['clientAlgoId'] if transport == 'algo' else result['clientOrderId']
        confirmed = await self.query_order_by_client_id(symbol, cid)
        oid = result['algoId'] if transport == 'algo' else result['orderId']
        self.verify_protection(symbol, pos, name, confirmed, cid, oid, trigger, transport)
        await self.commit_protection(symbol, pos, name, confirmed, trigger, old, transport)

    async def protect_with_recovery(self, symbol, pos):
        for name, kind, target in (('sl', 'STOP_MARKET', pos['sl']), ('tp', 'TAKE_PROFIT_MARKET', pos['tp'])):
            if target is None: continue
            error = None
            transport = 'regular' if pos.get('protection', {}).get(name, {}).get('is_algo') is False else 'algo'
            for attempt in range(self.cfg['exchange']['protection_attempts']):
                try:
                    await self.protect_role(symbol, pos, name, kind, target, transport)
                    error = None
                    break
                except Exception as caught:
                    if self.storage_fault or self.journal.failed: raise
                    error = caught
                    self.journal.log('protection_retry', symbol=symbol, stage=self.execution_stage,
                        attempt=attempt + 1, recovery_action='query_same_intent_before_new_post', **diagnostic_fields(caught))
                    if attempt + 1 < self.cfg['exchange']['protection_attempts']:
                        await asyncio.sleep(self.cfg['exchange']['protection_backoff_sec'] * (attempt + 1))
            if error is None: continue
            # Ordinary conditional placement is attempted only when advertised,
            # never while an Algo POST might already have been accepted.
            pending = any(i['symbol'] == symbol and i['type'] in CONDITIONAL and i['status'] != 'rejected'
                and not i.get('protection_committed') for i in self.intents.values())
            supported = (self.cfg['exchange']['regular_stop_fallback'] and name == 'sl'
                and kind in self.market(symbol).get('order_types', [])
                and not self.management_faults.get(symbol, {}).get('regular_unavailable'))
            if supported and not pending and transport == 'algo':
                try:
                    await self.protect_role(symbol, pos, name, kind, target, 'regular')
                    continue
                except OrderRejected as caught:
                    if caught.code in self.cfg['exchange']['regular_stop_unsupported_codes']:
                        self.management_fault(symbol, 'regular_stop_unavailable', pos, regular_unavailable=True)
                    error = caught
                except Exception as caught:
                    if self.storage_fault or self.journal.failed: raise
                    error = caught
            raise ProtectionGuardError('protection_install_failed', {'symbol': symbol,
                'attempts': self.cfg['exchange']['protection_attempts'], **diagnostic_fields(error)}) from None

    async def try_retire_protection(self, symbol, pos):
        """Flat execution is retained even if reduce-only cleanup stays pending."""
        self.execution_stage = 'native_protection_retirement'
        error = None
        flat_seen = symbol not in self.positions
        previous_fault = self.execution_fault
        for attempt in range(self.cfg['exchange']['retirement_attempts']):
            try:
                rows = await self.get_position(symbol)
                if any(float(r['positionAmt']) != 0 for r in rows):
                    raise ProtectionGuardError('position_not_flat_for_retirement', {'symbol': symbol})
                flat_seen = True
                known = {ref['clientOrderId'] for _, ref in self.protection_references(symbol, pos)}
                for order in await self.get_open_orders(symbol):
                    if order['clientOrderId'] in known: continue
                    if order.get('symbol') != self.raw(symbol):
                        raise ProtectionGuardError('retirement_identity_mismatch', {'symbol': symbol})
                    if order.get('reduceOnly') not in (True, 'true'):
                        raise ProtectionGuardError('unexpected_nonreducing_order', {'symbol': symbol})
                    fault = self.management_fault(symbol, 'protection_retirement_pending', pos,
                        close_pending=False, retirement_pending=True, flat_confirmed=True)
                    ref = {'clientOrderId': order['clientOrderId'], 'orderId': order['orderId'],
                        'is_algo': bool(order.get('is_algo')), 'amount': pos['amount'],
                        'trigger': str(order.get('triggerPrice', order.get('stopPrice', 0)))}
                    fault.setdefault('obsolete', []).append(('sl', ref))
                    self.write_state(self.management_path, self.management_faults)
                    known.add(ref['clientOrderId'])
                for name, ref in self.protection_references(symbol, pos):
                    parent = await self.query_protection(symbol, ref)
                    if not parent: raise ProtectionGuardError('retirement_order_unknown', {'symbol': symbol})
                    cid = parent.get('clientAlgoId', parent.get('clientOrderId'))
                    if (parent.get('symbol') != self.raw(symbol) or cid != ref['clientOrderId']
                            or parent.get('side') != ('SELL' if pos['side'] == 'buy' else 'BUY')
                            or parent.get('reduceOnly') not in (True, 'true')
                            or ref.get('orderId') is not None and str(parent.get('algoId', parent.get('orderId'))) != str(ref['orderId'])):
                        raise ProtectionGuardError('retirement_identity_mismatch', {'symbol': symbol})
                    if cid in self.intents: self.resolve_intent(cid, parent)
                    status = parent.get('algoStatus', parent.get('status'))
                    if status in ('NEW', 'TRIGGERING', 'PARTIALLY_FILLED'):
                        try: await self.cancel_protection(symbol, ref)
                        except ExchangeError: pass
                        parent = await self.query_protection(symbol, ref)
                        if not parent or parent.get('algoStatus', parent.get('status')) in ('NEW', 'TRIGGERING', 'PARTIALLY_FILLED'):
                            raise ProtectionGuardError('native_cancel_unresolved', {'symbol': symbol})
                await self.inspect_native_exits(symbol, pos, strict=False)
                if any(float(r['positionAmt']) != 0 for r in await self.get_position(symbol)):
                    raise ProtectionGuardError('position_not_flat_for_retirement', {'symbol': symbol})
                for _, ref in self.protection_references(symbol, pos):
                    if ref['clientOrderId'] in self.intents:
                        self.intents[ref['clientOrderId']]['protection_committed'] = True
                self.write_state(self.intent_path, self.intents)
                pending_prices = any(r['symbol'] == symbol and r.get('native_exit') and r.get('fill_price_pending')
                    for r in self.executions.values())
                if pending_prices:
                    self.execution_fault = previous_fault
                    self.management_fault(symbol, 'closed_symbol_accounting_pending', pos,
                        close_pending=False, retirement_pending=False, accounting_pending=True,
                        flat_confirmed=True, last_attempt=time.time())
                    return False
                self.resolve_management_fault(symbol)
                return True
            except Exception as caught:
                if self.storage_fault or self.journal.failed: raise
                error = caught
                if attempt + 1 < self.cfg['exchange']['retirement_attempts']:
                    await asyncio.sleep(self.cfg['exchange']['retirement_backoff_sec'] * (attempt + 1))
        self.execution_fault = previous_fault
        refs = self.protection_references(symbol, pos)
        fault = self.management_fault(symbol, 'protection_retirement_pending', pos,
            retirement_pending=True, close_pending=False, flat_confirmed=flat_seen, last_attempt=time.time())
        # Preserve even uncommitted/unknown parents independently of intent
        # recovery flags; a later flat-account observation can commit the intent
        # before its child and cancellation have actually been settled.
        obsolete = fault.setdefault('obsolete', [])
        known = {ref['clientOrderId'] for _, ref in obsolete}
        obsolete.extend((name, copy.deepcopy(ref)) for name, ref in refs if ref['clientOrderId'] not in known)
        self.write_state(self.management_path, self.management_faults)
        self.journal.log('protection_retirement_pending', symbol=symbol, stage='native_protection_retirement',
            recovery_action='block_symbol_and_retry_cleanup', **diagnostic_fields(error))
        return False
