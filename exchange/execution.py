"""Durable execution facts and independently retryable exact USDT accounting.

No network exactly-once claim: an unresolved intent blocks another POST. The
execution ledger is keyed by Binance order ID, not a locally generated trade ID.
"""
import asyncio
import copy
import math
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from paper_account import make_position
from risk.slippage import entry_slippage_bps, exit_slippage_bps
from .orders import ExchangeError, ExecutionUnknown, FillPriceUnavailable, OrderRejected, safe_create_order
from .fill_price import execution_price, positive_decimal
from .quantity import validate_quantity, OrderValidationError
from .leverage import bracket_for
from .orders import TERMINAL_ORDER_STATUSES as TERMINAL
from .protection_state import price_exit_reason, ProtectionGuardError



class DurableExecution:
    def execution_key(self, symbol, order_id):
        return symbol + ':' + str(order_id)

    def _flush_executions(self):
        self.write_state(self.execution_path, self.executions)

    def _upsert_trade(self, trade):
        old = next((t for t in self.trades if str(t['id']) == str(trade['id']) and t['symbol'] == trade['symbol']), None)
        if old is None: self.trades.append(copy.deepcopy(trade))
        else: old.update(copy.deepcopy(trade))

    def merge_strategy_position(self, canonical, local, protection=False):
        """Only the strategy owns moving stops/flags, never fill facts or fees."""
        if canonical.get('signal_id') != local.get('signal_id'): return
        sm = 1 if canonical['side'] == 'buy' else -1
        if sm * (local['sl'] - canonical['sl']) > 0: canonical['sl'] = local['sl']
        for key in ('breakeven_armed', 'trail_moved', 'risk_capped'):
            canonical[key] = canonical.get(key, False) or local.get(key, False)
        for key in ('trailing_armed', 'trailing_extreme'):
            if key in local: canonical[key] = local[key]
        if protection and 'protection' in local: canonical['protection'] = copy.deepcopy(local['protection'])

    def _remember_order(self, symbol, order, options, side):
        key = self.execution_key(symbol, order['orderId'])
        previous = self.executions.get(key, {})
        # Never overwrite a larger confirmed cumulative fill with a stale query.
        if float(order.get('executedQty', 0)) < float(previous.get('order', {}).get('executedQty', 0)):
            return previous
        if previous.get('order', {}).get('status') in TERMINAL and order.get('status') not in TERMINAL:
            return previous
        options = copy.deepcopy(options)
        if previous.get('native_exit'):
            # Generic history discovery must not erase a native child's origin,
            # SL/TP reason or snapshot while its price/accounting is pending.
            for field in ('native_parent_id', 'native_parent_client_id', 'signal_id',
                          'exit_reason', 'expected_price', 'exit_position'):
                if field in previous.get('options', {}):
                    options[field] = copy.deepcopy(previous['options'][field])
        record = {**previous, 'symbol': symbol, 'order': order,
                  'options': options, 'side': side}
        self.executions[key] = record
        self._flush_executions()
        return record

    async def settle_fill(self, symbol, order):
        order_id, client_id = str(order['orderId']), order.get('clientOrderId')
        for attempt in range(self.cfg['exchange']['order_poll_attempts'] + 1):
            qty = positive_decimal(order.get('executedQty'))
            if order.get('status') in TERMINAL:
                if qty is None: raise ExchangeError(400)
                if self.known_execution_price(symbol, order)[0] is not None: return order
                record = self.executions.get(self.execution_key(symbol, order_id))
                if record: self.remember_unpriced_fill(symbol, order, record['options'], record['side'])
            if attempt == self.cfg['exchange']['order_poll_attempts']: break
            # A FILLED acknowledgement missing price gets an immediate query;
            # further reads use the existing bounded settlement policy.
            if attempt or order.get('status') not in TERMINAL:
                await asyncio.sleep(self.cfg['exchange']['order_poll_sec'])
            order = await self._request('GET', '/fapi/v1/order', {'symbol': self.raw(symbol), 'orderId': order['orderId']}, retry=True)
            if str(order.get('orderId')) != order_id or client_id and order.get('clientOrderId') != client_id:
                raise ExecutionUnknown()
            client = order.get('clientOrderId')
            if client in self.intents and self.intents[client]['type'] == 'market': self.resolve_intent(client, order)
            record = self.executions.get(self.execution_key(symbol, order_id))
            if record: order = self._remember_order(symbol, order, record['options'], record['side'])['order']
        if positive_decimal(order.get('executedQty')) is not None and self.known_execution_price(symbol, order)[0] is None:
            raise FillPriceUnavailable()
        raise ExecutionUnknown()

    def known_execution_price(self, symbol, order):
        price, source = execution_price(order)
        record = self.executions.get(self.execution_key(symbol, order.get('orderId')), {})
        trade = record.get('trade') or {}
        if price is None and positive_decimal(order.get('executedQty')) == positive_decimal(trade.get('amount')):
            price = positive_decimal(trade.get('price'))
            if price is not None: source = record.get('price_source', 'persisted_execution')
        return price, source

    def remember_unpriced_fill(self, symbol, order, options, side):
        record = self._remember_order(symbol, order, options, side)
        quantity = positive_decimal(record['order'].get('executedQty'))
        if quantity is None: raise ExecutionUnknown()
        record['fill_price_pending'] = True
        if not options.get('reduceOnly'):
            prior = record.get('entry_facts') or {}
            stamp = order.get('updateTime', order.get('time'))
            opened_at = prior.get('opened_at') or (stamp / 1000 if stamp else datetime.now(timezone.utc).timestamp())
            record['entry_facts'] = dict(signal_id=order['clientOrderId'], side=side,
                entry=None, amount=float(quantity), leverage=options.get('leverage'), margin=None,
                opened_at=opened_at, entry_fee=None, accounting_status='pending',
                entry_order_id=str(order['orderId']), atr_at_entry=options.get('exit_atr'),
                atr_source=options.get('atr_source', 'unknown'), expected_entry=options.get('expected_price'),
                actual_entry=None, entry_slippage_bps=None)
        self._flush_executions()
        return record

    async def order_fills(self, symbol, order_id):
        ec = self.cfg['exchange']
        params = {'symbol': symbol, 'orderId': order_id, 'limit': ec['trade_page_limit']}
        result, seen = [], set()
        for _ in range(ec['trade_max_pages']):
            page = await self._request('GET', '/fapi/v1/userTrades', params, retry=True)
            if not isinstance(page, list) or any(not isinstance(f, dict) for f in page):
                raise ValueError('Invalid execution-details schema')
            for i, fill in enumerate(page):
                identity = str(fill.get('id', i))
                if identity not in seen and str(fill.get('orderId', order_id)) == str(order_id):
                    result.append(fill); seen.add(identity)
            if len(page) < ec['trade_page_limit'] or not page or 'id' not in page[-1]: break
            next_id = int(page[-1]['id']) + 1
            if next_id <= params.get('fromId', -1): break
            params['fromId'] = next_id
        return result

    async def sync_accounting(self):
        """Retry missing execution data; never erase a confirmed position."""
        for record in self.executions.values():
            trade = record.get('trade')
            if not trade: continue
            self._upsert_trade(trade)
            if trade.get('accounting_status') == 'complete': continue
            try:
                fills = await self.order_fills(self.raw(record['symbol']), record['order']['orderId'])
                # Missing, malformed and partial accounting are not evidence
                # against an order that Binance already confirmed FILLED.
                unique = {str(f.get('id', i)): f for i, f in enumerate(fills)}
                fills = list(unique.values())
                if not fills or any(f['commissionAsset'] != 'USDT' for f in fills): continue
                quantities = [Decimal(str(f['qty'])) for f in fills]
                commissions = [Decimal(str(f['commission'])) for f in fills]
                realized = [Decimal(str(f['realizedPnl'])) for f in fills]
                if not all(v.is_finite() for v in quantities + commissions + realized) or any(v <= 0 for v in quantities):
                    raise ValueError('Invalid numeric execution details')
                if not math.isclose(float(sum(quantities)), trade['amount'], rel_tol=self.cfg['exchange']['fill_qty_tolerance_pct']): continue
                fee, pnl = sum(commissions, Decimal(0)), sum(realized, Decimal(0))
                if not math.isfinite(float(fee)) or not math.isfinite(float(pnl)): raise ValueError('Invalid accounting totals')
            except (ExchangeError, KeyError, ValueError, TypeError, InvalidOperation) as error:
                if not record.get('accounting_pending_reason'):
                    record['accounting_pending_reason'] = 'execution_details_unavailable'
                    self.journal.log('accounting_pending', symbol=record['symbol'], stage='execution_details',
                        order_id=record['order']['orderId'], error_type=type_name(error),
                        recovery_action='retain_fill_and_protection_retry_accounting')
                    self._flush_executions()
                continue
            record.pop('accounting_pending_reason', None)
            trade.update(fee_usdt=float(fee), pnl_usdt=float(pnl), fee_exact=str(fee),
                         realized_pnl_exact=str(pnl), accounting_status='complete',
                         execution_ids=[str(f.get('id', i)) for i, f in enumerate(fills)])
            if record.get('entry_facts'):
                record['entry_facts'].update(entry_fee=float(fee), accounting_status='complete')
            if record.get('position'):
                record['position']['entry_fee'] = float(fee)
                record['position']['accounting_status'] = 'complete'
                # Actual entry fees can exceed the configured estimate. Tighten
                # risk distance only; never loosen a dynamic stop.
                pos = record['position']
                entry_qty = record.get('entry_facts', {}).get('amount', pos['amount'])
                closed_qty = sum(t['amount'] for t in self.trades if t.get('reduceOnly') and t.get('signal_id') == pos['signal_id'])
                if closed_qty >= entry_qty or math.isclose(closed_qty, entry_qty, rel_tol=self.cfg['exchange']['fill_qty_tolerance_pct']):
                    # Historical exact fees still matter for PnL. Rebuilding
                    # protection risk for an already closed entry does not.
                    self._flush_executions()
                    self._upsert_trade(trade)
                    continue
                options = record['options']
                cfg = copy.deepcopy(options.get('risk_cfg', self.cfg))
                cfg['tp_sl'].update(mmr=pos['mmr'], maintenance_amount=pos['maintenance_amount'], quantity=pos['amount'])
                effective_fee = max(self.fee_rate, float(fee) / (pos['amount'] * pos['entry']))
                try:
                    levels = make_position(pos['side'], pos['entry'], pos['amount'], pos['leverage'], pos['atr_at_entry'],
                    cfg, pos['equity_at_entry'], effective_fee)
                except (ValueError, RuntimeError):
                    record['risk_error'] = 'fee_budget_unavailable'
                    self.execution_fault = 'fee_budget_unavailable'
                    self._flush_executions()
                    if not self.emergency_mode: raise ExecutionUnknown() from None
                    self._upsert_trade(trade)
                    continue
                sm = 1 if pos['side'] == 'buy' else -1
                if sm * (levels['sl'] - pos['sl']) > 0:
                    pos['sl'] = levels['sl']
                    pos['risk_capped'] = True
                symbol = record['symbol']
                active = self.pending_protection.get(symbol)
                if active and active.get('signal_id') == pos['signal_id']:
                    active['entry_fee'] = float(fee)
                    active['accounting_status'] = 'complete'
                    if sm * (pos['sl'] - active['sl']) > 0: active['sl'] = pos['sl']
            self._flush_executions()
            self._upsert_trade(trade)

    async def _accept_fill(self, symbol, order, options, side, recovered=False, facts_only=False):
        self.execution_stage = 'fill_persistence'
        record = self._remember_order(symbol, order, options, side)
        order = record['order']
        options = record['options']
        price, source = self.known_execution_price(symbol, order)
        quantity = positive_decimal(order.get('executedQty'))
        if quantity is None: raise ExecutionUnknown()
        if price is None:
            self.execution_stage = 'fill_price_recovery'
            self.remember_unpriced_fill(symbol, order, options, side)
            order = await self.settle_fill(symbol, order)
            record = self._remember_order(symbol, order, options, side)
            price, source = self.known_execution_price(symbol, order)
            if price is None: raise FillPriceUnavailable()
        actual, filled = float(price), float(positive_decimal(order.get('executedQty')))
        record.pop('fill_price_pending', None)
        record.update(price_source=source, average_exact=str(price))
        reducing = bool(options.get('reduceOnly'))
        expected = float(options.get('expected_price') or actual)
        timestamp = order.get('updateTime', order.get('time'))
        stamp = (datetime.fromtimestamp(timestamp / 1000, timezone.utc) if timestamp else datetime.now(timezone.utc)).isoformat()
        trade = record.get('trade') or {'id': str(order['orderId']), 'client_id': order['clientOrderId'],
            'timestamp': stamp, 'symbol': symbol, 'side': side, 'amount': filled, 'price': actual,
            'reduceOnly': reducing, 'fee_usdt': 0., 'pnl_usdt': 0., 'accounting_status': 'pending',
            'balance_usdt': self.balance, 'mode': self.mode, 'recovered': recovered,
            'protection_status': 'unconfirmed' if not reducing else 'closed',
            'emergency': bool(options.get('emergency') or self.execution_fault or record.get('emergency')), 'signal_id': options.get('signal_id') if reducing else order['clientOrderId']}
        # A later cumulative fill invalidates provisional/previous accounting.
        if trade['amount'] != filled:
            trade.update(amount=filled, price=actual, fee_usdt=0., pnl_usdt=0., accounting_status='pending')
        if not reducing:
            leverage = options.get('leverage')
            record['entry_facts'] = {
                'signal_id': trade['signal_id'], 'side': side, 'entry': actual, 'amount': filled,
                'leverage': leverage, 'margin': actual * filled / leverage if leverage else None,
                'opened_at': (record.get('entry_facts') or {}).get('opened_at', datetime.fromisoformat(stamp).timestamp()), 'entry_fee': trade['fee_usdt'],
                'accounting_status': trade['accounting_status'], 'entry_order_id': str(order['orderId']),
                'atr_at_entry': options.get('exit_atr'), 'atr_source': options.get('atr_source', 'unknown'),
                'expected_entry': expected, 'actual_entry': actual,
                'entry_slippage_bps': entry_slippage_bps(expected, actual, side)}
            trade.update(entry_time=stamp, leverage=leverage, margin=record['entry_facts']['margin'],
                expected_entry=expected, actual_entry=actual,
                entry_slippage_bps=record['entry_facts']['entry_slippage_bps'])
        # The fill is a fact even if building risk levels fails next.
        record['trade'] = trade
        self._flush_executions()
        self._upsert_trade(trade)
        pos = record.get('position')
        if recovered and not reducing:
            closed_qty = sum(t['amount'] for t in self.trades if t.get('reduceOnly') and t.get('signal_id') == trade['signal_id'])
            if math.isclose(closed_qty, filled, rel_tol=self.cfg['exchange']['fill_qty_tolerance_pct']): facts_only = True
        if facts_only and not reducing:
            await self.sync_accounting()
            return dict(record['trade'])
        if not reducing:
            if recovered and any(k not in options for k in ('leverage', 'risk_cfg', 'exit_atr')):
                raise ExecutionUnknown()  # Legacy history needs explicit recovery; never invent ATR/leverage.
            leverage = options.get('leverage', self.cfg['leverage'])
            risk_cfg = copy.deepcopy(options.get('risk_cfg', self.cfg))
            if symbol in self.brackets:
                bracket = bracket_for(self.brackets[symbol], actual * filled)
                risk_cfg['tp_sl'].update(mmr=float(bracket['maintMarginRatio']), maintenance_amount=float(bracket.get('cum', 0)), quantity=filled)
                if leverage > int(bracket['initialLeverage']):
                    record['risk_error'] = 'fill_exceeds_leverage_bracket'
                    self._flush_executions()
                    raise ExecutionUnknown()
            if pos is None or pos['amount'] != filled:
                previous_pos = pos
                try:
                    pos = make_position(side, actual, filled, leverage,
                    options.get('exit_atr', actual * self.cfg['atr']['fallback_atr_pct']), risk_cfg,
                    options.get('equity', self.equity), self.fee_rate,
                    opened_at=previous_pos['opened_at'] if previous_pos else datetime.fromisoformat(trade['timestamp']).timestamp(),
                    signal_id=pos.get('signal_id') if pos else order['clientOrderId'],
                    atr_source=options.get('atr_source', 'fallback'))
                except (ValueError, RuntimeError):
                    record['risk_error'] = 'risk_levels_unavailable'
                    self._flush_executions()
                    raise ExecutionUnknown() from None
                if previous_pos:
                    pos['protection'] = copy.deepcopy(previous_pos.get('protection', {}))
                    sm = 1 if side == 'buy' else -1
                    if sm * (previous_pos['sl'] - pos['sl']) > 0: pos['sl'] = previous_pos['sl']
                    for field in ('breakeven_armed', 'trail_moved'):
                        pos[field] = previous_pos.get(field, False)
                pos.update(margin=filled * actual / leverage, entry_order_id=str(order['orderId']),
                    client_order_id=order['clientOrderId'], accounting_status=trade['accounting_status'],
                    expected_entry=expected, actual_entry=actual,
                    entry_slippage_bps=entry_slippage_bps(expected, actual, side),
                    mmr=risk_cfg['tp_sl']['mmr'], maintenance_amount=risk_cfg['tp_sl'].get('maintenance_amount', 0))
            if trade['accounting_status'] == 'complete': pos['entry_fee'] = trade['fee_usdt']
            trade.update(leverage=pos['leverage'], margin=pos['margin'], signal_id=pos['signal_id'],
                planned_sl=pos['stop_initial'], planned_tp=pos['tp'], entry_time=stamp,
                expected_entry=expected, actual_entry=actual, entry_slippage_bps=pos['entry_slippage_bps'])
            record['position'] = pos
            self.pending_protection[symbol] = pos
        else:
            opening = None
            for prior in reversed(list(self.executions.values())):
                if prior['symbol'] != symbol: continue
                candidate = {**(prior.get('position') or {}), **(prior.get('entry_facts') or {})}
                if not candidate: continue
                sid = candidate['signal_id']
                if options.get('signal_id') and options['signal_id'] != sid: continue
                closed = sum(t['amount'] for t in self.trades if t.get('reduceOnly') and t.get('signal_id') == sid and str(t['id']) != str(order['orderId']))
                if closed >= candidate['amount']: continue
                opening = candidate
                trade['signal_id'] = sid
                break
            trade.update(exit_time=stamp, expected_exit=expected, actual_exit=actual,
                exit_reason=options.get('exit_reason', trade.get('exit_reason', 'recovered_close')),
                exit_position=copy.deepcopy(options.get('exit_position') or opening),
                exit_slippage_bps=exit_slippage_bps(expected, actual, 'sell' if side == 'buy' else 'buy'))
            if opening:
                trade.update(leverage=opening['leverage'], margin=opening['margin'],
                    entry_order_id=opening['entry_order_id'], entry_time=datetime.fromtimestamp(opening['opened_at'], timezone.utc).isoformat(),
                    planned_sl=opening.get('stop_initial'), planned_tp=opening.get('tp'),
                    actual_protection=copy.deepcopy(opening.get('protection', {})))
        record['trade'] = trade
        # Commit execution truth before any userTrades request or protection HTTP.
        self._flush_executions()
        self._upsert_trade(trade)
        if not reducing:
            self.execution_stage = 'position_visibility'
            remote = []
            for attempt in range(self.cfg['exchange']['order_poll_attempts'] + 1):
                remote = [p for p in await self.get_position(symbol) if float(p['positionAmt']) != 0]
                if remote and float(remote[0].get('liquidationPrice', 0)) > 0: break
                if attempt < self.cfg['exchange']['order_poll_attempts']:
                    await asyncio.sleep(self.cfg['exchange']['order_poll_sec'])
            if remote:
                self.execution_stage = 'liquidation_validation'
                ticker = await self.fetch_ticker(symbol)
                actual_risk = {**remote[0], '_trigger_price': ticker['last']}
                self.constrain_actual_liquidation(symbol, pos, actual_risk)
                trigger_price = float(remote[0]['markPrice']) if self.cfg['exchange']['working_type'] == 'MARK_PRICE' else ticker['last']
                if price_exit_reason(pos, trigger_price):
                    self.execution_stage = 'initial_exit_level_reached'
                    raise ProtectionGuardError('initial_exit_level_reached', {'symbol': symbol, 'trigger_price': trigger_price})
                self._flush_executions()
                await self.protect(symbol, pos)
                trade['actual_protection'] = copy.deepcopy(pos['protection'])
                trade['protection_status'] = 'confirmed'
                record['position'] = pos
                self._flush_executions()
                self._upsert_trade(trade)
            elif not recovered:
                raise ExecutionUnknown()
        self.execution_stage = 'accounting'
        await self.sync_accounting()
        if not reducing and pos and any(float(p['positionAmt']) != 0 for p in await self.get_position(symbol)):
            await self.protect(symbol, pos)
        return {**record['trade'], 'average': actual, 'filled': filled, 'status': 'closed',
                **({'risk_position': pos} if pos else {})}

    async def recover_fill(self, symbol, order, options, side, **kwargs):
        """Apply the same confirmed-entry scope during restart recovery."""
        try:
            return await self._accept_fill(symbol, order, options, side, **kwargs)
        except FillPriceUnavailable:
            raise
        except Exception as error:
            record = self.executions.get(self.execution_key(symbol, order['orderId']), {})
            if (not options.get('reduceOnly') and record.get('trade') and record['order'].get('status') in TERMINAL
                    and not self.storage_fault and not self.journal.failed):
                self.management_fault(symbol, 'entry_confirmed_management_failed', record.get('position'),
                    close_pending=True, flat_confirmed=False, stage=self.execution_stage,
                    entry_block_until=time.time() + self.cfg['exchange']['management_entry_cooldown_sec'],
                    order_id=order['orderId'])
                self.journal.log('entry_management_recovery_pending', symbol=symbol, stage=self.execution_stage,
                    order_id=order['orderId'], error_type=type_name(error), recovery_action='close_affected_symbol_only')
                return None
            raise

    async def recover_execution(self):
        """Called under runtime lock before reconciliation, on startup and periodically."""
        unresolved = False
        for client_id, intent in list(self.intents.items()):
            if intent['status'] == 'rejected' or intent['type'] != 'market': continue
            options = intent.get('params', {})
            if options.get('brackets'): self.brackets[intent['symbol']] = options['brackets']
            record = self.executions.get(self.execution_key(intent['symbol'], intent.get('order_id')))
            if record and not record.get('fill_price_pending') and record.get('order', {}).get('status') in TERMINAL and (record.get('trade') or float(record['order'].get('executedQty', 0)) == 0):
                if record.get('trade'):
                    self._upsert_trade(record['trade'])
                    if not record['trade']['reduceOnly'] and not record.get('position'):
                        live = any(float(p['positionAmt']) != 0 for p in await self.get_position(intent['symbol']))
                        if record.get('risk_error'):
                            unresolved |= live
                        elif live and not self.emergency_mode:
                            await self.recover_fill(intent['symbol'], record['order'], options, intent['side'], recovered=True)
                continue
            order = await self.query_order_by_client_id(intent['symbol'], client_id)
            if not order:
                if options.get('reduceOnly') and self.management_closed(intent['symbol']): continue
                unresolved = True
                continue
            self.resolve_intent(client_id, order)
            order = self._remember_order(intent['symbol'], order, options, intent['side'])['order']
            if float(order.get('executedQty', 0)) > 0:
                try:
                    await self.recover_fill(intent['symbol'], order, options, intent['side'], recovered=True, facts_only=self.emergency_mode)
                except FillPriceUnavailable:
                    if not (options.get('reduceOnly') and self.management_closed(intent['symbol'])): unresolved = True
            if order.get('status') not in TERMINAL: unresolved = True
        if not await self.recover_native_exits(): unresolved = True
        await self.sync_accounting()
        if not self.emergency_mode and not await self.recover_protection_intents(): unresolved = True
        return not unresolved

    async def settle_before_close(self, symbol):
        """Cancel/confirm a live remainder before sending a new reducing order."""
        pos = self.pending_protection.get(symbol)
        if pos: await self.inspect_native_exits(symbol, pos, strict=False)
        for client_id, intent in list(self.intents.items()):
            if intent['symbol'] != symbol or intent['type'] != 'market' or intent['status'] == 'rejected': continue
            record = self.executions.get(self.execution_key(symbol, intent.get('order_id')))
            if record and record['order'].get('status') in TERMINAL: continue
            order = await self.query_order_by_client_id(symbol, client_id)
            if not order: raise ExecutionUnknown()
            if order.get('status') not in TERMINAL:
                try: await self.cancel_order(symbol, order_id=order['orderId'])
                except ExchangeError: pass  # Confirm the same order, even if DELETE response was lost.
                order = await self.query_order_by_client_id(symbol, client_id)
                if not order or order.get('status') not in TERMINAL: raise ExecutionUnknown()
            self.resolve_intent(client_id, order)
            self._remember_order(symbol, order, intent.get('params', {}), intent['side'])
            if float(order.get('executedQty', 0)) > 0:
                try:
                    await self._accept_fill(symbol, order, intent.get('params', {}), intent['side'], recovered=True, facts_only=True)
                except FillPriceUnavailable:
                    # The remainder is terminal/cancelled. Missing entry price
                    # cannot prevent reducing the independently proven position.
                    pass

    async def create_order(self, symbol, type, side, amount, params=None):
        self.execution_stage = 'before_post_validation'
        previous_ids = set(self.intents)
        previous_fault = self.execution_fault
        try:
            return await self._create_order(symbol, type, side, amount, params)
        except Exception as error:
            current_ids = set(self.intents) - previous_ids
            symbol_scoped = False
            reducing = bool((params or {}).get('reduceOnly'))
            if isinstance(error, OrderValidationError) and not current_ids and not reducing:
                self.journal.log('entry_validation_blocked', symbol=symbol, stage='before_post',
                    error_type=type_name(error), reason=str(error), recovery_action='no_order_sent')
                raise
            latest = next((i for cid, i in reversed(list(self.intents.items())) if cid in current_ids), {})
            if self.storage_fault or self.journal.failed:
                self.execution_fault = 'storage_failure'
            elif isinstance(error, FillPriceUnavailable):
                self.execution_fault = 'fill_price_unavailable'
            elif isinstance(error, OrderRejected):
                self.execution_fault = ('close_rejected' if reducing else
                    'protection_rejected' if latest.get('type') in ('STOP_MARKET', 'TAKE_PROFIT_MARKET') else 'order_rejected')
            elif isinstance(error, OrderValidationError):
                self.execution_fault = 'close_validation_failed' if reducing else 'protection_validation_failed'
            else:
                self.execution_fault = 'order_execution_uncertain'
            for record in self.executions.values():
                if record['symbol'] == symbol and record['order'].get('clientOrderId') in current_ids:
                    record['emergency'] = True
            affected = next((r for r in reversed(list(self.executions.values())) if r['symbol'] == symbol
                             and r.get('trade') and r['order'].get('clientOrderId') in current_ids), None)
            if affected:
                affected['trade']['emergency'] = True
                if not affected['trade']['reduceOnly']:
                    if self.execution_stage.startswith(('protection_', 'liquidation_', 'position_')):
                        affected['trade']['protection_status'] = 'failed'
                    if self.execution_fault == 'order_execution_uncertain' and affected['order'].get('status') in TERMINAL:
                        self.execution_fault = 'entry_confirmed_management_failed'
                elif self.execution_fault == 'order_execution_uncertain' and affected['order'].get('status') in TERMINAL:
                    self.execution_fault = 'close_confirmed_reconciliation_failed'
                self._upsert_trade(affected['trade'])
            if (affected and not affected['trade']['reduceOnly'] and affected['order'].get('status') in TERMINAL
                    and not self.storage_fault and not self.journal.failed):
                symbol_scoped = True
                self.management_fault(symbol, 'entry_confirmed_management_failed', affected.get('position'),
                    close_pending=True, flat_confirmed=False, stage=self.execution_stage,
                    entry_block_until=time.time() + self.cfg['exchange']['management_entry_cooldown_sec'],
                    order_id=affected['order']['orderId'])
                self.execution_fault = previous_fault
            latest = next((i for i in reversed(list(self.intents.values())) if i['symbol'] == symbol), {})
            self.journal.log('order_execution_failed', symbol=symbol, stage=self.execution_stage,
                order_id=latest.get('order_id'), client_order_id=latest.get('params', {}).get('newClientOrderId'),
                error_type=type_name(error), http_status=getattr(error, 'status', 0),
                exchange_error_code=getattr(error, 'code', 0),
                recovery_action='close_affected_symbol_only' if symbol_scoped else 'review_definitive_rejection' if isinstance(error, OrderRejected) else 'reconcile_before_resend')
            raise

    async def _create_order(self, symbol, type, side, amount, params=None):
        options = copy.deepcopy(params or {})
        if self.emergency_mode: options['emergency'] = True
        if options.get('reduceOnly'):
            await self.settle_before_close(symbol)
        # Resolve previous same-symbol ambiguity before considering another send.
        for client_id, intent in list(self.intents.items()):
            if options.get('reduceOnly'): break
            if intent['symbol'] != symbol or intent['type'] != 'market' or intent['status'] == 'rejected': continue
            record = self.executions.get(self.execution_key(intent['symbol'], intent.get('order_id')))
            if not record or record['order'].get('status') not in TERMINAL or not record.get('trade'):
                await self.recover_execution()
                record = self.executions.get(self.execution_key(symbol, intent.get('order_id')))
                if not record or record['order'].get('status') not in TERMINAL:
                    raise ExecutionUnknown()
        if not options.get('reduceOnly'):
            if self.execution_fault or self.storage_fault or self.journal.failed: raise ExecutionUnknown()
            # Runtime selected/confirmed with current equity before computing size.
            if any(float(p['positionAmt']) != 0 for p in await self.get_position(symbol)):
                raise ExecutionUnknown()
            choice = self.selected_leverage.get(symbol)
            if self.cfg['exchange'].get('leverage_policy') in ('maximum_by_bracket', 'fixed'):
                if choice and choice.get('entry_order_valid') is False:
                    raise OrderValidationError(choice['validation_reason'])
                if not choice or not choice['confirmed']: raise ValueError('Leverage must be selected and confirmed before sizing')
                if not math.isclose(amount, choice['amount'], rel_tol=self.cfg['exchange']['fill_qty_tolerance_pct']):
                    raise OrderValidationError('quantity_differs_from_confirmed_sizing')
                leverage = choice['leverage']
                risk_cfg = copy.deepcopy(self.cfg)
                bracket = bracket_for(self.brackets[symbol], amount * float(options['expected_price']))
                risk_cfg['tp_sl'].update(mmr=float(bracket['maintMarginRatio']),
                    maintenance_amount=float(bracket.get('cum', 0)), quantity=amount)
            else:
                leverage, risk_cfg = self.leverage, self.cfg
                await self.set_leverage(leverage, symbol)
            options.update(leverage=leverage, risk_cfg=risk_cfg, equity=self.equity, brackets=self.brackets.get(symbol))
            expected = float(options.get('expected_price') or (await self.fetch_ticker(symbol))['last'])
            options['expected_price'] = expected
            try:
                planned = make_position(side, expected, amount, leverage,
                    options.get('exit_atr', expected * self.cfg['atr']['fallback_atr_pct']), risk_cfg, self.equity, self.fee_rate)
            except (ValueError, RuntimeError):
                raise OrderValidationError('risk_levels_unavailable_before_entry') from None
            self.validate_initial_protection(symbol, planned)
            market = self.market(symbol)
            validate_quantity(market, self.amount_to_precision(symbol, amount), expected)
            if amount * expected / leverage + amount * expected * self.fee_rate > self.free:
                raise OrderValidationError('insufficient_available_margin_including_fee')
        else:
            remote = [p for p in await self.get_position(symbol) if float(p['positionAmt']) != 0]
            if not remote: raise ExecutionUnknown()
            residual = float(remote[0]['positionAmt'])
            if self.cfg['exit'].get('policy') == 'hard_stop_trailing' and not math.isclose(amount, abs(residual), rel_tol=self.cfg['exchange']['fill_qty_tolerance_pct']):
                raise OrderValidationError('partial_close_disabled')
            if (residual > 0) != (side == 'sell'): raise ValueError('Close side does not reduce actual position')
            maximum = self.market(symbol)['limits']['amount'].get('max', float('inf'))
            amount = min(amount, abs(residual), maximum)
            validate_quantity(self.market(symbol), self.amount_to_precision(symbol, amount))
        self.execution_stage = 'close_post' if options.get('reduceOnly') else 'entry_post'
        result = await safe_create_order(self, {'symbol': symbol, 'type': type, 'side': side, 'amount': amount, 'params': options}, self.journal)
        self._remember_order(symbol, result, options, side)
        self.execution_stage = 'fill_settlement'
        try:
            result = await self.settle_fill(symbol, result)
        except ExecutionUnknown as error:
            latest = self.intents[result['clientOrderId']].get('response', result)
            stored = self.executions.get(self.execution_key(symbol, result['orderId']))
            if stored: latest = stored['order']
            if float(latest.get('executedQty', 0)) > 0:
                if isinstance(error, FillPriceUnavailable):
                    self.execution_stage = 'fill_price_recovery'
                    self.remember_unpriced_fill(symbol, latest, options, side)
                else:
                    await self._accept_fill(symbol, latest, options, side)
            raise
        trade = await self._accept_fill(symbol, result, options, side)
        self.execution_stage = 'account_refresh_after_fill'
        await self.refresh_account()
        self.execution_stage = 'close_remaining_confirmation' if options.get('reduceOnly') else 'entry_complete'
        if options.get('reduceOnly') and symbol in self.positions: raise ExecutionUnknown()
        return trade


def type_name(error):
    # Type only: exception messages can include signed URLs and credentials.
    return error.__class__.__name__
