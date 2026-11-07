"""Safety orchestration around existing signal and exit calculations."""
import asyncio
import json
import math
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace

from infra.alerts import TelegramAlerter
from infra.errors import diagnostic_fields
from infra.journal import OperationalJournal, atomic_json
from risk.kill_switch import KillSwitch
from risk.liq_math import direction
from risk.exposure_limits import can_open
from risk.funding import check_funding_before_settlement
from risk.slippage import check_slippage_health
from exchange.protection_state import ProtectionGuardError, price_exit_reason


class SafetySupervisor:
    def __init__(self, bot):
        self.bot, self.cfg = bot, bot.cfg
        self.live = bot.execution_mode != 'paper'
        root = Path(self.cfg['exchange']['state_dir']) / bot.execution_mode if self.live else bot.state_path.parent
        self.journal = OperationalJournal(root / 'journal')
        self.alerter = TelegramAlerter(self.cfg['alerts'])
        self.kill = KillSwitch(self.cfg['kill_switch'], 'demo' if bot.execution_mode == 'testnet' and self.cfg['exchange']['base_url_testnet'] == 'https://demo-fapi.binance.com' else bot.execution_mode)
        self.spreads = {}
        self.latest = {}
        self.active = self.live
        self.ready = not self.live  # Existing offline paper callers remain usable.
        self.last_recon = self.last_funding = self.last_evidence = self.last_spread = self.last_account = 0.
        self.last_report_day = None
        self.warned = set()
        self.last_announced_reason = None
        self.open_orders = {}
        self.accounted_closes = set()
        self.storage_failed = False
        self.loss_cooldowns = {}
        self.last_slippage_alert = 0.
        self.kill_observation_count = 0
        self.evidence = None
        if bot.execution_mode in ('testnet', 'mainnet'):
            from exchange.rollout import TestnetEvidence
            self.evidence = TestnetEvidence(self.cfg, bot.execution_mode)

    def dump(self):
        return {'kill_observation_count': self.kill_observation_count, 'loss_cooldowns': self.loss_cooldowns, 'last_slippage_alert': self.last_slippage_alert, 'accounted_closes': sorted(self.accounted_closes), 'storage_failed': self.storage_failed, 'kill': self.kill.dump(), 'last_report_day': self.last_report_day,
                'announced_kill': 'kill_announced' in self.warned, 'announced_kill_reason': self.last_announced_reason}

    def restore(self, value):
        self.kill_observation_count = value.get('kill_observation_count', 0)
        self.accounted_closes = set(value.get('accounted_closes', []))
        self.loss_cooldowns = value.get('loss_cooldowns', {})
        self.last_slippage_alert = value.get('last_slippage_alert', 0.)
        self.storage_failed = value.get('storage_failed', False)
        self.kill.restore(value['kill'])
        self.last_report_day = value.get('last_report_day')
        if value.get('announced_kill'):
            self.warned.add('kill_announced')
            self.last_announced_reason = value.get('announced_kill_reason') or self.kill.triggered
        if self.kill.blocks_entries:
            self.ready = False

    async def event(self, name, level='info', **fields):
        if self.journal:
            self.journal.log(name, **fields)
        self.alerter.send_nowait(name + ' ' + json.dumps(fields, ensure_ascii=False), level)

    def heartbeat(self):
        self.kill.on_ws_heartbeat()

    def observe_book(self, symbol, book):
        if not book['bids'] or not book['asks']: return
        bid, ask = float(book['bids'][0][0]), float(book['asks'][0][0])
        if bid <= 0 or ask < bid: return
        spread = (ask - bid) / ((ask + bid) / 2)
        history = self.spreads.setdefault(symbol, deque(maxlen=self.cfg['kill_switch']['spread_window']))
        # Never include the candidate spike in its own reference mean.
        if len(history) >= self.cfg['kill_switch']['spread_warmup']:
            self.latest[symbol] = SimpleNamespace(avg_spread=sum(history) / len(history), spread=lambda: spread, samples=len(history))
        history.append(spread)

    async def allow_entry(self, symbol, side, price):
        if not self.ready or self.kill.blocks_entries or self.storage_failed or self.journal.failed:
            return False
        if self.live and self.bot.exchange.management_blocked(symbol):
            await self.event('entry_blocked', 'warning', symbol=symbol, reason='symbol_management_pending')
            return False
        market = self.latest.get(symbol)
        if reason := self.kill.check(self.bot.exchange, market):
            await self.trip(reason)
            return False
        if self.kill.mode == 'observe' and self.kill.last_observed:
            await self.trip(self.kill.last_observed)
        block = self.kill.entry_block_reason()
        if self.loss_cooldowns.get(symbol, 0) > time.time(): block = 'same_symbol_loss_cooldown'
        if block:
            if block not in self.warned:
                self.warned.add(block)
                await self.event('entry_blocked', 'warning', symbol=symbol, reason=block)
                self.save_safely()
            return False
        for name in ('spread_spike', 'consecutive_losses', 'same_symbol_loss_cooldown'): self.warned.discard(name)
        if self.live and self.cfg['funding']['block_new_entries']:
            data = await self.bot.exchange.get_funding_rate(symbol)
            minutes = (int(data['nextFundingTime']) / 1000 - time.time()) / 60
            if (0 <= minutes <= self.cfg['funding']['warning_minutes']
                    and direction(side) * float(data['lastFundingRate']) > self.cfg['funding']['close_threshold']):
                await self.event('entry_blocked', symbol=symbol, reason='funding_window')
                return False
        from risk.trading_policy import entry_amount, fixed_paper_notional
        notional = fixed_paper_notional(self.cfg)
        if notional is None:
            notional = entry_amount(self.bot.exchange.equity, price, self.cfg['leverage'],
                                    self.cfg, self.bot.exchange.fee_rate) * price
        if self.bot.execution_mode == 'mainnet':
            notional *= self.bot.exchange.allocation_fraction
        allowed, reason = can_open(self.bot, symbol, side, notional, self.cfg['exposure'])
        if not allowed:
            await self.event('exposure_block', 'warning', symbol=symbol, reason=reason)
        return allowed

    @staticmethod
    def close_key(trade):
        return ':'.join((trade.get('mode', 'paper'), trade['symbol'], trade.get('signal_id') or str(trade['id'])))

    async def closed(self, trade):
        key = self.close_key(trade)
        if key in self.accounted_closes: return
        self.accounted_closes.add(key)
        detail = trade['exit_detail']
        self.kill.roll_day(self.bot.exchange.equity)
        self.kill.on_trade_closed(detail['net_pnl_usdt'], trade.get('exit_time', trade.get('timestamp')))
        self.save_safely()
        self.note_closed(trade['symbol'], detail['net_pnl_usdt'], trade.get('exit_time', trade.get('timestamp')))
        self.save_safely()
        await self.event('trade_closed', symbol=trade['symbol'], side=detail['side'], reason=detail['exit_reason'],
                         pnl=detail['net_pnl_usdt'], equity=self.bot.exchange.equity)
        if self.kill.check(self.bot.exchange, None):
            # Caller holds the trade lock: monitor will flatten on its next tick.
            self.ready = False
        if warning := check_slippage_health(self, self.cfg['slippage']):
            if time.time() - self.last_slippage_alert >= self.cfg['slippage']['alert_cooldown_sec']:
                self.last_slippage_alert = time.time()
                self.save_safely()
                await self.event('slippage_high', 'warning', reason=warning)

    def note_closed(self, symbol, pnl, closed_at=None):
        if pnl < 0:
            stamp = datetime.fromisoformat(closed_at).timestamp() if closed_at else time.time()
            self.loss_cooldowns[symbol] = max(self.loss_cooldowns.get(symbol, 0), stamp + self.cfg['same_symbol_loss_cooldown'])

    def last_n(self, n):
        return [t for t in self.bot.exchange.trades if not t['reduceOnly']][-n:]

    def save_safely(self):
        try:
            self.bot.save()
        except (OSError, ValueError, TypeError) as error:
            if self.journal: self.journal.log('storage_snapshot_failed', stage='state_save', **diagnostic_fields(error))
            self.storage_failed = True
            self.ready = False
            if not self.kill.triggered or not self.kill.should_flatten(self.kill.triggered):
                self.kill.triggered = 'storage_failure'
            return False
        return True

    async def trip(self, reason):
        if self.kill.mode in ('observe', 'soft'):
            now = time.time()
            self.kill.triggered = reason
            if self.kill.triggered_at is None: self.kill.triggered_at = now
            if self.kill.mode == 'soft': self.ready = False
            last = getattr(self, '_mode_announced', {})
            if now - last.get(reason, 0) >= 60:
                self.kill_observation_count += 1
                last[reason] = now
                self._mode_announced = last
                await self.event('kill_' + self.kill.mode, 'warning', reason=reason, mode=self.kill.mode)
            self.save_safely()
            return
        if reason in ('spread_spike', 'consecutive_losses') and self.kill.environment != 'mainnet':
            duration = self.cfg['kill_switch']['spread_block_sec' if reason == 'spread_spike' else 'consecutive_loss_pause_sec']
            self.kill.entry_blocks[reason] = time.time() + duration
            self.save_safely()
            await self.event('entry_blocked', 'warning', reason=reason)
            return
        exchange = self.bot.exchange
        if self.live: exchange.emergency_mode = True
        try:
            await self._trip(reason)
        finally:
            if self.live: exchange.emergency_mode = False

    async def _trip(self, reason):
        first = self.kill.triggered is None
        if self.kill.triggered and not self.kill.should_flatten(self.kill.triggered) and self.kill.should_flatten(reason):
            self.kill.triggered = reason
            self.warned.discard('kill_announced')
        self.kill.triggered = self.kill.triggered or reason
        self.ready = False
        self.save_safely()  # Latch is durable before any market request.
        if first or 'kill_announced' not in self.warned or self.last_announced_reason != self.kill.triggered:
            self.warned.add('kill_announced')
            self.last_announced_reason = self.kill.triggered
            if self.evidence and reason not in ('daily_drawdown', 'consecutive_losses', 'spread_spike', 'process_shutdown'):
                self.evidence.data['technical_kills'] += 1
                try: atomic_json(self.evidence.path, self.evidence.data)
                except OSError: self.storage_failed = True
            self.save_safely()
            await self.event('kill_triggered', 'critical', reason=self.kill.triggered)
        if not self.kill.should_flatten(self.kill.triggered):
            return  # Keep managing native stops and recovery; only entries stop.
        # Repeat while latched. Failed flattening never removes remaining stops.
        remaining_orders = []
        for _ in range(self.cfg['kill_switch']['flatten_attempts']):
            flatten_stage = 'kill_account_refresh'
            try:
                if self.live:
                    await self.bot.exchange.refresh_account()
                for symbol in list(self.bot.exchange.positions):
                    try:
                        await self.bot.close_position(symbol, 'kill_switch')
                    except Exception as error:
                        await self.event('kill_close_failed', 'critical', symbol=symbol,
                            stage=getattr(self.bot.exchange, 'execution_stage', 'close'),
                            recovery_action='query_orders_and_remaining_position', **diagnostic_fields(error))
                if self.live:
                    flatten_stage = 'kill_order_cleanup'
                    for order in await self.bot.exchange.get_open_orders():
                        # Retain protective reduce-only orders if flatten failed.
                        symbol = self.bot.exchange.symbol(order['symbol'])
                        protective = bool(order.get('reduceOnly')) or order.get('closePosition') is True
                        if symbol in self.bot.exchange.positions and protective:
                            continue
                        try:
                            if order.get('is_algo'): await self.bot.exchange.cancel_algo(order)
                            else: await self.bot.exchange.cancel_order(order['symbol'], order_id=order['orderId'])
                        except Exception as error:
                            await self.event('kill_cancel_failed', 'critical', symbol=symbol,
                                stage='cancel_protection' if order.get('is_algo') else 'cancel_order',
                                recovery_action='recheck_open_orders', **diagnostic_fields(error))
                    flatten_stage = 'kill_account_verify'
                    await self.bot.exchange.refresh_account()
                    remaining_orders = await self.bot.exchange.get_open_orders()
                if not self.bot.exchange.positions and not remaining_orders:
                    if self.live:
                        try:
                            await self.bot.exchange.recover_execution()
                            self.bot.merge_execution_state()
                            self.bot.persist_execution_metadata()
                            await self.bot._drain_notifications()
                        except Exception as error:
                            await self.event('kill_accounting_pending', 'warning', stage='kill_accounting_recovery',
                                recovery_action='retain_ledger_for_recovery', **diagnostic_fields(error))
                    flatten_stage = 'kill_local_close_adoption'
                    for symbol in list(self.bot.positions):
                        if not await self.bot.adopt_native_close(symbol):
                            await self.event('kill_phantom_retained', 'warning', symbol=symbol)
                    break
            except Exception as error:
                await self.event('kill_exchange_unreachable', 'critical', reason=reason, stage=flatten_stage,
                    recovery_action='retain_latch_and_retry_remaining_only', **diagnostic_fields(error))
            await asyncio.sleep(self.cfg['kill_switch']['flatten_retry_sec'])
        if self.bot.exchange.positions or remaining_orders:
            await self.event('kill_flatten_incomplete', 'critical', reason=self.kill.triggered)
        self.save_safely()

    async def adopt_closed_symbol(self, symbol):
        """Caller has just proven flatness; attribution can remain pending."""
        try:
            return await self.bot.adopt_native_close(symbol)
        except Exception as error:
            if self.bot.exchange.storage_fault or self.storage_failed or self.bot.exchange.journal.failed: raise
            await self.event('symbol_accounting_pending', 'warning', symbol=symbol,
                stage=getattr(self.bot.exchange, 'execution_stage', 'closed_symbol_recovery'),
                recovery_action='retain_closed_symbol_metadata', **diagnostic_fields(error))
            return False

    async def handle_symbol_failure(self, symbol, reason, error=None):
        """Close a known affected symbol; escalate only an unsafe/unclosed state."""
        exchange = self.bot.exchange
        if exchange.storage_fault or self.storage_failed or exchange.journal.failed:
            await self.trip('storage_failure')
            return False
        pos = self.bot.positions.get(symbol) or exchange.pending_protection.get(symbol)
        if pos is None:
            pos = next((r.get('position') or r.get('entry_facts') for r in reversed(list(exchange.executions.values()))
                        if r['symbol'] == symbol and not r.get('trade', {}).get('reduceOnly')), None)
        exchange.management_fault(symbol, reason, pos, close_pending=True,
                                  stage=getattr(exchange, 'execution_stage', 'symbol_management'))
        await self.event('symbol_management_failed', 'critical', symbol=symbol, reason=reason,
            stage=getattr(exchange, 'execution_stage', 'symbol_management'),
            recovery_action='close_affected_symbol_only', **(diagnostic_fields(error) if error else {}))
        previous_fault = exchange.execution_fault
        previous_emergency = exchange.emergency_mode
        exchange.emergency_mode = True
        try:
            for attempt in range(self.cfg['exchange']['symbol_close_attempts']):
                try:
                    await exchange.refresh_account()
                    if symbol in exchange.positions:
                        if pos: self.bot.positions.setdefault(symbol, pos)
                        await self.bot.close_position(symbol, 'protection_failure')
                    else:
                        if symbol in self.bot.positions:
                            if not await self.adopt_closed_symbol(symbol):
                                exchange.management_fault(symbol, 'closed_symbol_accounting_pending', pos,
                                    close_pending=False, retirement_pending=True, accounting_pending=True,
                                    flat_confirmed=True)
                                self.bot.positions.pop(symbol, None)
                        if pos and exchange.management_faults.get(symbol, {}).get('retirement_pending'):
                            await exchange.retire_protection(symbol, pos)
                    await exchange.refresh_account()
                    if symbol not in exchange.positions:
                        if not exchange.management_faults[symbol].get('retirement_pending'):
                            exchange.resolve_management_fault(symbol)
                        else:
                            exchange.management_faults[symbol]['close_pending'] = False
                            exchange.write_state(exchange.management_path, exchange.management_faults)
                        if not exchange.storage_fault: exchange.execution_fault = previous_fault
                        self.save_safely()
                        return True
                except Exception as caught:
                    await self.event('symbol_close_pending', 'critical', symbol=symbol, attempt=attempt + 1,
                        stage=getattr(exchange, 'execution_stage', 'symbol_close'),
                        recovery_action='query_previous_close_and_actual_remaining', **diagnostic_fields(caught))
                if attempt + 1 < self.cfg['exchange']['symbol_close_attempts']:
                    await asyncio.sleep(self.cfg['exchange']['symbol_close_backoff_sec'] * (attempt + 1))
            # Account-wide policy is explicit and only reached if bounded,
            # reducing symbol recovery could not establish a safe closure.
            await self.trip('symbol_emergency_close_failed')
            return False
        finally:
            exchange.emergency_mode = previous_emergency

    async def recover_symbol_management(self):
        exchange = self.bot.exchange
        for symbol, fault in list(exchange.management_faults.items()):
            if fault.get('status') == 'resolved': continue
            if fault.get('close_pending'):
                if not await self.handle_symbol_failure(symbol, fault['reason']): return False
            elif fault.get('retirement_pending') or fault.get('accounting_pending'):
                if time.time() - fault.get('last_attempt', 0) < self.cfg['exchange']['management_recovery_interval_sec']:
                    continue
                pos = fault.get('position')
                if not pos: continue
                previous_fault = exchange.execution_fault
                if fault.get('accounting_pending'):
                    await exchange.refresh_account()
                    if symbol in exchange.positions:
                        if not await self.handle_symbol_failure(symbol, 'symbol_position_reappeared'): return False
                        continue
                    self.bot.positions.setdefault(symbol, pos)
                    adopted = await self.adopt_closed_symbol(symbol)
                    fault = exchange.management_faults[symbol]
                    if adopted:
                        fault['accounting_pending'] = False
                    else:
                        self.bot.positions.pop(symbol, None)
                    if not exchange.storage_fault: exchange.execution_fault = previous_fault
                if await exchange.retire_protection(symbol, pos):
                    exchange.clear_active_protection(symbol, pos.get('signal_id'))
                exchange.write_state(exchange.management_path, exchange.management_faults)
        return True

    async def reconcile_once(self):
        from exchange.reconciler import reconcile
        if self.live:
            if not await self.recover_symbol_management(): return False
            recovery_ok = await self.bot.exchange.recover_execution()
            if not await self.recover_symbol_management(): return False
            if not recovery_ok and not await self.bot.exchange.recover_execution():
                await self.trip('pending_order_review')
                return False
            await self.bot.exchange.refresh_account()
            self.bot.merge_execution_state()
            local, remote = self.bot.positions, self.bot.exchange.positions
            bad_identity = [s for s in set(local) & set(remote) if local[s]['side'] != remote[s]['side']
                or not math.isclose(local[s]['entry'], remote[s]['entry'], rel_tol=self.cfg['recon']['entry_tolerance_pct'])]
            if bad_identity:
                await self.event('reconcile_mismatch', 'critical', report={'position_identity': sorted(bad_identity)})
                await self.trip('reconcile_mismatch')
                return False
            try:
                for symbol in set(self.bot.positions) - set(self.bot.exchange.positions):
                    await self.bot.adopt_native_close(symbol)
                for symbol in list(self.bot.positions):
                    await self.bot.reconcile_native_exit(symbol)
            except Exception as error:
                await self.event('protection_recovery_exception', 'critical',
                    stage=getattr(self.bot.exchange, 'execution_stage', 'native_exit_resolution'),
                    report=error.details if isinstance(error, ProtectionGuardError) else None,
                    recovery_action='account_emergency' if isinstance(error, ProtectionGuardError) and error.reason == 'mark_liquidation_buffer_breach' else 'close_affected_symbol_only', **diagnostic_fields(error))
                if isinstance(error, ProtectionGuardError) and error.reason == 'mark_liquidation_buffer_breach':
                    await self.trip('protection_recovery_failed')
                    return False
                if not await self.handle_symbol_failure(symbol, 'protection_recovery_failed', error): return False
                return await self.reconcile_once()
            # Check identity and size before replacing any native protection.
            local, remote = self.bot.positions, self.bot.exchange.positions
            mismatch = set(local) != set(remote) or any(
                local[s]['side'] != remote[s]['side']
                or not math.isclose(local[s]['amount'], remote[s]['amount'], rel_tol=self.cfg['recon']['size_tolerance_pct'])
                or not math.isclose(local[s]['entry'], remote[s]['entry'], rel_tol=self.cfg['recon']['entry_tolerance_pct'])
                for s in set(local) & set(remote))
            if mismatch:
                await self.event('reconcile_mismatch', 'critical', report={'position_identity_or_quantity': sorted(set(local) | set(remote))})
                await self.trip('reconcile_mismatch')
                return False
            try:
                for symbol, pos in list(self.bot.positions.items()):
                    if symbol in self.bot.exchange.positions:
                        actual = self.bot.exchange.positions[symbol]['remote']
                        self.bot.exchange.execution_stage = 'protection_price_snapshot'
                        ticker = await self.bot.exchange.fetch_ticker(symbol)
                        self.bot.exchange.constrain_actual_liquidation(symbol, pos, {**actual, '_trigger_price': ticker['last']})
                        price = float(actual['markPrice']) if self.cfg['exchange']['working_type'] == 'MARK_PRICE' else ticker['last']
                        if reason := price_exit_reason(pos, price):
                            await self.bot.close_position(symbol, reason, price)
                            continue
                        await self.bot.exchange.protect(symbol, pos)
            except Exception as error:
                await self.event('protection_recovery_exception', 'critical',
                    stage=getattr(self.bot.exchange, 'execution_stage', 'protection_recovery'),
                    report=error.details if isinstance(error, ProtectionGuardError) else None,
                    recovery_action='account_emergency' if isinstance(error, ProtectionGuardError) and error.reason == 'mark_liquidation_buffer_breach' else 'close_affected_symbol_only', **diagnostic_fields(error))
                if isinstance(error, ProtectionGuardError) and error.reason == 'mark_liquidation_buffer_breach':
                    await self.trip('protection_recovery_failed')
                    return False
                if not await self.handle_symbol_failure(symbol, 'protection_recovery_failed', error): return False
                return await self.reconcile_once()
        report = await reconcile(self.bot.exchange, self.bot, self.journal)
        if any(report.values()):
            if self.evidence:
                self.evidence.data['reconcile_mismatches'] += 1
                atomic_json(self.evidence.path, self.evidence.data)
            await self.event('reconcile_mismatch', 'critical', report=report)
            await self.trip('reconcile_mismatch')
            return False
        return True

    async def startup(self):
        self.active = True
        if not self.live:
            self.ready = True
            return
        from exchange.rollout import require_operator_policy
        require_operator_policy(self.cfg, self.bot.execution_mode)
        if not self.alerter.enabled:
            raise ValueError('Live modes require TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID')
        # A successful preflight notification must be drained before entries.
        worker = asyncio.create_task(self.alerter._worker())
        try:
            await self.alerter.send('Khaled startup preflight: ' + self.bot.execution_mode)
            await self.alerter.queue.join()
            if self.alerter.failed:
                raise ValueError('Telegram preflight delivery failed')
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        # Durable active-session marker survives a full disk or process crash
        # that prevents the later kill snapshot from being committed.
        guard_path = self.bot.state_path.parent / self.cfg['exchange']['session_guard_filename']
        guard = json.loads(guard_path.read_text()) if guard_path.exists() else {}
        if guard.get('active'):
            if self.kill.mode == 'hard': self.kill.triggered = self.kill.triggered or 'unclean_session_review'
            await self.event('unclean_session_recovery', 'warning')
        atomic_json(guard_path, {'active': True, 'mode': self.bot.execution_mode,
            'started_at': datetime.now(timezone.utc).isoformat()})
        self.bot._management_authorized = True
        if not await self.reconcile_once():
            raise ValueError('Reconciliation changed state; review and reset the latch before restart')
        for symbol, pos in self.bot.positions.items():
            await self.bot.exchange.protect(symbol, pos)
        if self.kill.blocks_entries:
            await self.trip(self.kill.triggered)
            raise ValueError('Kill switch remains latched; manual review required')
        self.kill.on_ws_heartbeat()  # bounded connection warmup grace
        self.ready = True
        if self.evidence: self.evidence.start_session()

    async def watchdog(self):
        # Independent of a slow REST operation holding the trading lock. Entry
        # gating latches immediately; the serialized monitor performs flattening.
        while True:
            if self.kill.mode == 'soft' and self.ready is False and not self.storage_failed and not getattr(self.bot.exchange, 'execution_fault', None):
                if not self.kill.check(self.bot.exchange, None):
                    async with self.bot.lock:
                        try:
                            if await self.reconcile_once(): self.ready = True
                        except Exception as error:
                            await self.event('soft_recovery_pending', 'warning', **diagnostic_fields(error))
            if self.kill.check(self.bot.exchange, None):
                self.ready = False
                if not getattr(self, '_watchdog_latched', False):
                    self.save_safely()
                    self._watchdog_latched = True
            await asyncio.sleep(self.cfg['kill_switch']['monitor_interval_sec'])

    async def monitor(self):
        while True:
            async with self.bot.lock:
                now = time.time()
                stage, diagnostic_symbol = 'account_refresh', None
                try:
                    if self.live and now - self.last_account >= self.cfg['exchange']['account_refresh_sec']:
                        await self.bot.exchange.refresh_account()
                        self.last_account = now
                    stage = 'storage_health'
                    if self.live and self.kill.mode != 'hard' and (self.storage_failed or self.bot.exchange.storage_fault):
                        # A durable probe must succeed before reconciliation can authorize entries.
                        from journal.wal import WriteAheadLog
                        WriteAheadLog(self.bot.state_path.parent / 'journal.wal').snapshot(
                            self.bot.state_path.parent / 'storage_probe.json', {'checked_at': now})
                        self.journal.log('storage_recovery_probe')
                        self.bot.exchange.journal.log('storage_recovery_probe')
                        if self.journal.failed or self.bot.exchange.journal.failed: raise OSError('Storage probe failed')
                        self.storage_failed = self.bot.exchange.storage_fault = False
                        if self.bot.exchange.execution_fault == 'storage_failure': self.bot.exchange.execution_fault = None
                        if await self.reconcile_once():
                            self.ready = True
                            await self.event('storage_recovered', 'warning')
                    if self.live and (self.storage_failed or self.journal.failed or self.bot.exchange.journal.failed):
                        await self.trip('storage_failure')
                    stage = 'alert_health'
                    if self.alerter.failed and self.live:
                        await self.trip('alerts_unavailable')
                    stage = 'execution_fault_handling'
                    if getattr(self.bot.exchange, 'execution_fault', None):
                        await self.trip(self.bot.exchange.execution_fault)
                    if self.live:
                        await self.recover_symbol_management()
                    stage = 'spread_check'
                    if self.live and now - self.last_spread >= self.cfg['kill_switch']['spread_poll_sec']:
                        for symbol in list(self.bot.positions):
                            diagnostic_symbol = symbol
                            book = await self.bot.exchange.fetch_order_book(symbol, self.cfg['kill_switch']['book_depth'])
                            self.observe_book(symbol, book)
                            self.kill.check(self.bot.exchange, self.latest.get(symbol))
                            if self.kill.mode == 'observe' and self.kill.last_observed:
                                await self.trip(self.kill.last_observed)
                        self.last_spread = now
                    stage, diagnostic_symbol = 'kill_evaluation', None
                    reason = self.kill.check(self.bot.exchange, None)
                    if self.kill.mode == 'observe' and self.kill.last_observed:
                        await self.trip(self.kill.last_observed)
                    if reason:
                        stage = 'kill_actions'
                        await self.trip(reason)
                    if self.live and (not reason or not self.kill.should_flatten(reason)) and now - self.last_recon >= self.cfg['recon']['interval_sec']:
                        stage = 'reconciliation_account_refresh'
                        await self.bot.exchange.refresh_account()
                        stage = 'reconciliation'
                        await self.reconcile_once()
                        self.last_recon = now
                    stage = 'funding_check'
                    if not self.kill.blocks_entries and self.live and now - self.last_funding >= self.cfg['funding']['poll_sec']:
                        await check_funding_before_settlement(self.bot, self.bot.exchange, self.cfg['funding'], self.journal)
                        self.last_funding = now
                    stage = 'health_warnings'
                    if self.kill.state.api_errors_1m >= self.cfg['kill_switch']['api_warning_count'] and 'api' not in self.warned:
                        self.warned.add('api')
                        await self.event('api_errors_warning', 'warning', count=self.kill.state.api_errors_1m)
                    if self.kill.state.api_errors_1m < self.cfg['kill_switch']['api_warning_count']:
                        self.warned.discard('api')
                    age = now - self.kill.state.last_ws_heartbeat
                    if age > self.cfg['kill_switch']['ws_warning_sec'] and 'ws' not in self.warned:
                        self.warned.add('ws')
                        await self.event('ws_disconnected', 'warning')
                    if age <= self.cfg['kill_switch']['ws_warning_sec']: self.warned.discard('ws')
                    stage = 'daily_boundary'
                    utc = datetime.now(timezone.utc)
                    day = utc.date().isoformat()
                    if utc.hour == self.cfg['alerts']['daily_report_hour_utc'] and self.last_report_day != day:
                        stage = 'daily_report'
                        await self.daily_report()
                        self.last_report_day = day
                    if self.evidence and not self.kill.blocks_entries and now - self.last_evidence >= self.cfg['rollout']['equity_sample_sec']:
                        stage = 'evidence_sample'
                        self.evidence.sample(self.bot.exchange.equity, self.bot.exchange.trades, self.bot.positions)
                        self.last_evidence = now
                    stage = 'state_save'
                    if hasattr(self.bot, 'process_watchdog'): self.bot.process_watchdog.heartbeat('reconciler')
                    if now - getattr(self, '_last_snapshot', 0) >= self.cfg['persistence']['state_save_interval_sec']:
                        self.save_safely()
                        self._last_snapshot = now
                except Exception as error:
                    from infra.resilience import transient_error
                    if transient_error(error):
                        await self.event('monitor_retry', 'warning', stage=stage, **diagnostic_fields(error))
                        await asyncio.sleep(1)
                        continue
                    self.ready = False
                    await self.event('monitor_exception', 'critical', stage=stage,
                        symbol=diagnostic_symbol, recovery_action='latch_and_reconcile', **diagnostic_fields(error))
                    # The adapter counts failed HTTP attempts; local failures
                    # must not be counted as an additional Binance API error.
                    await self.trip('monitor_failure')
            await asyncio.sleep(self.cfg['kill_switch']['monitor_interval_sec'])

    async def daily_report(self):
        report_day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        trades = [t for t in self.bot.exchange.trades if t.get('exit_detail')
                  and datetime.fromisoformat(t['timestamp']).date() == report_day]
        wins = sum(t['exit_detail']['net_pnl_usdt'] > 0 for t in trades)
        metrics = self.evidence.metrics() if self.evidence else {}
        report = {'equity': self.bot.exchange.equity, 'daily_pnl': sum(t['exit_detail']['net_pnl_usdt'] for t in trades), 'day_utc': report_day.isoformat(),
                  'trade_count': len(trades), 'win_rate': wins / len(trades) if trades else None, **metrics}
        await self.event('daily_report', report=report)
        if self.live:
            atomic_json(Path(self.cfg['exchange']['state_dir']) / self.bot.execution_mode / 'daily_report.json', report)
