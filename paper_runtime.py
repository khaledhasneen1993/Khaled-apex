"""Shared Paper/Demo runtime with durable execution and current exit policy."""

import asyncio
import copy
from pathlib import Path

import ccxt.async_support as ccxt

import binance_cascade_hunter_dynamic_v2 as dynamic
from paper_account import PaperState, PaperExchange
from continuous_scanner import ContinuousScanner
from risk.config import load_config
from journal.trade_journal import exit_record
from datetime import datetime, timezone
import fcntl
import time
import math
import sys
from infra.supervisor import SafetySupervisor
from risk.liq_math import direction


class DynamicPaperExchange(PaperExchange):
    async def fetch_tickers(self):
        return await self.market_data.fetch_tickers()


class DynamicPaperHunter(dynamic.CascadeHunter, PaperState):
    def __init__(self, state_path="paper_state_dynamic_v2.json", fee_rate=0.0005,
                 reset=False, market_data=None, strategy_config=dynamic):
        self.strategy = strategy_config
        self.cfg = load_config()
        self.execution_mode = self.cfg["exchange"]["mode"]
        if self.execution_mode not in ("paper", "testnet", "mainnet"):
            raise ValueError("Invalid exchange mode")
        if self.execution_mode != "paper" and reset:
            raise ValueError("Live state cannot be reset; reconcile and review it")
        # A single config mode selects both data and execution environments.
        if self.execution_mode == "paper":
            public = market_data if market_data is not None else ccxt.binanceusdm({
                "enableRateLimit": True, "options": {"defaultType": "future"},
            })
            self.exchange = DynamicPaperExchange(public, fee_rate, leverage=self.strategy.LEVERAGE)
        else:
            if market_data is not None:
                raise ValueError("Injected public paper clients cannot be used in live modes")
            from exchange.live_exchange import LiveExchange
            from infra.rate_limit import TokenBucket
            ec = self.cfg["exchange"]
            self.exchange = LiveExchange(self.cfg, TokenBucket(ec["rate_per_sec"], ec["rate_burst"]))
        self.exchange.cfg = self.cfg
        self.initial_balance = self.cfg["initial_balance_usdt"] if self.execution_mode == "paper" else None
        self.positions = {}
        self.last_trade = {}
        self.lock = asyncio.Lock()
        self.state_path = Path(state_path)
        if self.execution_mode != "paper":
            self.state_path = Path(self.cfg["exchange"]["state_dir"]) / self.execution_mode / self.state_path.name
        self._notifications = []
        self._state_lock = None
        self._owns_state = self.execution_mode == "paper"
        self._management_authorized = False
        from infra.watchdog import Watchdog
        self.process_watchdog = Watchdog(self.cfg['process'])
        self.safety = SafetySupervisor(self)
        if self.execution_mode != "paper":
            self.exchange.on_api_error = self.safety.kill.on_api_error
        from journal.wal import WriteAheadLog
        if (self.state_path.exists() or self.state_path.name in WriteAheadLog(self.state_path.parent / 'journal.wal').latest()) and not reset:
            self.restore()
        self.scanner = ContinuousScanner(self, config=self.strategy)

    def settings(self):
        result = {"mode": "dynamic_scan", "execution_mode": self.execution_mode, "active_exit": {key: self.cfg[key] for key in ("tp_sl", "atr", "exit")},
                **{name: getattr(self.strategy, name) for name in (
            "LEVERAGE", "MAX_POSITION_PCT", "CASCADE_THRESHOLD", "VOLUME_SPIKE",
            "COOLDOWN_SEC", "SCAN_INTERVAL",
            "MAX_SYMBOLS_TO_SCAN", "MAX_CONCURRENT_POSITIONS", "MIN_QUOTE_VOLUME",
        )}}
        from risk.trading_policy import fixed_paper_notional
        notional = fixed_paper_notional(self.cfg)
        if notional is not None:
            result["paper_position_notional_usdt"] = notional
        if self.execution_mode != 'paper' and self.cfg['exchange']['leverage_policy'] != 'fixed':
            result['LEVERAGE'] = self.cfg['exchange']['leverage_policy']
        return result

    async def enter(self, symbol, side, price):
        if symbol in self.positions: return False
        # ContinuousScanner owns the entry lock; do not acquire it again here.
        if self.execution_mode != "paper":
            if not self.safety.ready:
                return False
            await self.exchange.refresh_account()
            remote = self.exchange.positions
            mismatch = set(remote) != set(self.positions)
            if not mismatch:
                for current_symbol, actual in remote.items():
                    local = self.positions[current_symbol]
                    if (actual["side"] != local["side"]
                            or not math.isclose(actual["amount"], local["amount"], rel_tol=self.cfg["recon"]["size_tolerance_pct"])
                            or not math.isclose(actual["entry"], local["entry"], rel_tol=self.cfg["recon"]["entry_tolerance_pct"])):
                        mismatch = True
                        break
            if mismatch and not await self.safety.reconcile_once():
                return False
        if not await self.safety.allow_entry(symbol, side, price):
            return False
        result = await self.strategy.CascadeHunter.enter(self, symbol, side, float(price))
        if getattr(self.exchange, "execution_fault", None):
            # CascadeHunter preserves its public False-on-failure contract. The
            # live runtime still latches and handles actual possible fills.
            for pending_symbol, pending in self.exchange.pending_protection.items():
                if pending_symbol not in self.positions:
                    self.positions[pending_symbol] = pending
            await self.safety.trip(self.exchange.execution_fault)
        elif self.execution_mode != 'paper' and self.exchange.management_faults.get(symbol, {}).get('close_pending'):
            for pending_symbol, pending in self.exchange.pending_protection.items():
                self.positions.setdefault(pending_symbol, pending)
            await self.safety.handle_symbol_failure(symbol, 'entry_confirmed_management_failed')
            result = False
        elif self.safety.kill.blocks_entries and self.safety.active:
            await self.safety.trip(self.safety.kill.triggered)
            result = False
        elif result and self.safety.active:
            await self.safety.event("trade_opened", symbol=symbol, reason=side)
        self.safety.save_safely()
        return result

    def merge_execution_state(self):
        """Merge exchange facts while retaining local strategy/dynamic stop metadata."""
        from paper_account import Position
        live = self.exchange
        seen = set()
        for record in reversed(list(live.executions.values())):
            pos = record.get('position')
            symbol = record['symbol']
            if pos and symbol in live.positions and symbol not in seen:
                seen.add(symbol)
                if symbol not in self.positions:
                    self.positions[symbol] = Position(pos)
                else:
                    if self.positions[symbol].get('signal_id') != pos.get('signal_id'):
                        # A different locally retained position requires review;
                        # never apply another position's fees or moving stop.
                        continue
                    sm = 1 if pos['side'] == 'buy' else -1
                    if sm * (pos['sl'] - self.positions[symbol]['sl']) > 0:
                        self.positions[symbol]['sl'] = pos['sl']
                    for field in ('trailing_armed', 'trailing_extreme'):
                        if field in pos and field not in self.positions[symbol]: self.positions[symbol][field] = pos[field]
                    self.positions[symbol]['entry_fee'] = pos['entry_fee']
                    self.positions[symbol]['accounting_status'] = pos.get('accounting_status', 'pending')
                    self.positions[symbol]['protection'] = copy.deepcopy(pos.get('protection', {}))
        self.finalize_closed_trades()

    def finalize_closed_trades(self):
        """One position result, only after exact exit quantity and accounting."""
        from paper_account import Position
        exchange = self.exchange
        groups = {}
        for trade in exchange.trades:
            if trade.get('reduceOnly') and trade.get('signal_id'):
                groups.setdefault(trade['signal_id'], []).append(trade)
        for signal_id, closes in groups.items():
            # An older partial fill may acquire its price after the final exit.
            # Final reason/time belong to execution chronology, not arrival order.
            closes.sort(key=lambda t: (datetime.fromisoformat(t.get('exit_time', t['timestamp'])).timestamp(), int(t['id'])))
            final = closes[-1]
            stored = final.get('exit_position')
            if not stored: continue
            entry = next((r.get('entry_facts') or r.get('position') for r in exchange.executions.values()
                          if (r.get('position') or r.get('entry_facts') or {}).get('signal_id') == signal_id), None)
            entry = entry or stored
            quantity = sum(t['amount'] for t in closes)
            if not math.isclose(quantity, entry['amount'], rel_tol=self.cfg['exchange']['fill_qty_tolerance_pct'], abs_tol=0):
                continue
            active = self.positions.get(final['symbol'])
            if active and active.get('signal_id') == signal_id and final['symbol'] in exchange.positions: continue
            if entry.get('accounting_status') != 'complete' or any(t.get('accounting_status') != 'complete' for t in closes): continue
            for close in closes:
                facts = dict(leverage=entry.get('leverage'), margin=entry.get('margin'),
                    entry_order_id=entry.get('entry_order_id'),
                    entry_time=datetime.fromtimestamp(entry['opened_at'], timezone.utc).isoformat())
                close.update(facts)
                record = exchange.executions.get(exchange.execution_key(close['symbol'], close['id']))
                if record and record.get('trade'): record['trade'].update(facts)
            if not final.get('exit_detail'):
                stored = dict(stored, entry_fee=entry['entry_fee'], amount=entry['amount'],
                              entry=entry['entry'], leverage=entry['leverage'], margin=entry['margin'])
                for field in ('expected_entry', 'actual_entry', 'entry_slippage_bps'):
                    if field in entry: stored[field] = entry[field]
                fees = sum(t['fee_usdt'] for t in closes)
                price = sum(t['amount'] * t['price'] for t in closes) / quantity
                reason = final.get('exit_reason', 'recovered_close')
                reason = {'SL': 'stop_loss', 'TP': 'take_profit'}.get(reason, reason)
                final['exit_detail'] = exit_record(Position(stored), reason, price,
                    datetime.fromisoformat(final['timestamp']).timestamp(), fees)
                from decimal import Decimal
                fee_total = sum((Decimal(t.get('fee_exact', str(t['fee_usdt']))) for t in closes), Decimal(0))
                realized = sum((Decimal(t.get('realized_pnl_exact', str(t['pnl_usdt']))) for t in closes), Decimal(0))
                entry_trade = next((t for t in exchange.trades if not t.get('reduceOnly') and t.get('signal_id') == signal_id), {})
                entry_fee = Decimal(entry_trade.get('fee_exact', str(entry['entry_fee'])))
                net = realized - fee_total - entry_fee
                final['exit_detail'].update(net_pnl_usdt=float(net), net_pnl_exact=str(net),
                    entry_fee_exact=str(entry_fee), exit_fee_exact=str(fee_total), realized_pnl_exact=str(realized))
                for key in ('expected_entry', 'actual_entry', 'entry_slippage_bps'):
                    final['exit_detail'][key] = final[key] = stored.get(key, stored['entry'] if key != 'entry_slippage_bps' else 0.)
                for key in ('expected_exit', 'actual_exit', 'exit_slippage_bps'):
                    if key in final: final['exit_detail'][key] = final[key]
                final['exit_detail']['actual_exit'] = price
            key = self.safety.close_key(final)
            if key not in self.safety.accounted_closes and not any(self.safety.close_key(t) == key for t in self._notifications):
                self._notifications.append(final)

    def record_entry_signal(self, symbol, signal):
        for trade in reversed(self.exchange.trades):
            if trade["symbol"] == symbol and not trade["reduceOnly"]:
                trade["entry_signal"] = dict(signal)
                trade["entry_reason"] = "CASCADE_MOVE_VOLUME_ORDERBOOK"
                position = self.positions[symbol]
                trade["take_profit_price"] = position["tp"]
                trade["stop_loss_price"] = position["sl"]
                self.save()
                break

    def record_exit_reason(self, symbol, reason, trigger_price):
        if self.execution_mode != 'paper':
            position = self.positions[symbol]
            for trade in reversed(self.exchange.trades):
                if trade.get('reduceOnly') and trade['symbol'] == symbol and trade.get('signal_id') == position['signal_id']:
                    trade.update(exit_reason=reason, trigger_price=float(trigger_price), exit_position=dict(position))
                    break
            self.finalize_closed_trades()
            return
        for trade in reversed(self.exchange.trades):
            if trade["symbol"] == symbol and trade["reduceOnly"]:
                trade["exit_reason"] = reason
                trade["trigger_price"] = float(trigger_price)
                position = self.positions[symbol]
                closes = [t for t in self.exchange.trades if t.get("reduceOnly")
                          and t.get("signal_id") == position["signal_id"]]
                if not closes: closes = [trade]
                quantity = sum(t["amount"] for t in closes)
                weighted_exit = sum(t["price"] * t["amount"] for t in closes) / quantity
                if (position.get('accounting_status') == 'pending'
                        or any(t.get('accounting_status') == 'pending' for t in closes)):
                    trade['accounting_status'] = 'pending'
                    trade['exit_position'] = dict(position)
                    return
                total_fees = sum(t["fee_usdt"] for t in closes)
                trade["exit_detail"] = exit_record(
                    position, {"SL": "stop_loss", "TP": "take_profit"}.get(reason, reason),
                    weighted_exit, datetime.fromisoformat(trade["timestamp"]).timestamp(), total_fees)
                trade["exit_detail"]["net_pnl_usdt"] = sum(t["pnl_usdt"] for t in closes) - position["entry_fee"] - total_fees
                for key in ("expected_entry", "actual_entry", "entry_slippage_bps"):
                    trade[key] = position.get(key, position["entry"] if key != "entry_slippage_bps" else 0.)
                    trade["exit_detail"][key] = trade[key]
                for key in ("expected_exit", "actual_exit", "exit_slippage_bps"):
                    if key in trade: trade["exit_detail"][key] = trade[key]
                trade["exit_detail"]["actual_exit"] = weighted_exit
                if not self.safety.active:
                    self.safety.kill.on_trade_closed(trade['exit_detail']['net_pnl_usdt'])
                    self.safety.note_closed(symbol, trade['exit_detail']['net_pnl_usdt'], trade['timestamp'])
                if self.safety.active: self._notifications.append(trade)
                break

    @property
    def open_positions(self): return self.positions

    @property
    def equity(self): return self.exchange.equity

    @property
    def open_orders(self): return self.safety.open_orders

    def symbols(self): return tuple(self.positions)

    def remove_position(self, symbol):
        self.positions.pop(symbol, None)

    def on_market_heartbeat(self):
        self.safety.heartbeat()
        self.scanner.ws_policy.connected()

    def on_order_book(self, symbol, book): self.safety.observe_book(symbol, book)

    async def close_position(self, symbol, reason, trigger_price=None):
        if self.execution_mode != "paper":
            await self.exchange.settle_before_close(symbol)
            await self.exchange.refresh_account()
        account = self.exchange.positions.get(symbol)
        if account is None:
            if symbol in self.positions and await self.adopt_native_close(symbol): return
            if symbol in self.positions: raise RuntimeError("Remote close requires reconciliation")
            return
        price = trigger_price if trigger_price is not None else (await self.exchange.fetch_ticker(symbol))["last"]
        previous_fault = getattr(self.exchange, "execution_fault", None)
        try:
            await self.exchange.create_order(symbol, "market", "sell" if account["side"] == "buy" else "buy",
                account["amount"], params={"reduceOnly": True, "expected_price": price,
                                          "signal_id": self.positions.get(symbol, {}).get("signal_id"),
                                          "exit_reason": reason, "exit_position": self.positions.get(symbol)})
        except Exception:
            if self.execution_mode != "paper":
                await self.exchange.refresh_account()
                if symbol not in self.exchange.positions and symbol in self.positions and await self.adopt_native_close(symbol):
                    self.exchange.execution_fault = previous_fault
                    return
            raise
        retired = True
        if symbol in self.positions:
            if self.execution_mode != 'paper':
                retired = await self.exchange.retire_protection(symbol, self.positions[symbol])
            self.record_exit_reason(symbol, reason, price)
            self.positions.pop(symbol, None)
        if self.execution_mode != 'paper' and not retired:
            await self.safety.event('symbol_cleanup_pending', 'warning', symbol=symbol,
                stage='native_protection_retirement', recovery_action='block_symbol_and_retry_cleanup')
        if self.execution_mode != "paper" and retired:
            self.exchange.clear_active_protection(symbol)
        if self.execution_mode != 'paper': self.finalize_closed_trades()
        self.persist_execution_metadata()
        await self._drain_notifications()
        self.save()

    def persist_execution_metadata(self):
        if self.execution_mode == 'paper': return
        for trade in self.exchange.trades:
            record = self.exchange.executions.get(self.exchange.execution_key(trade['symbol'], trade['id']))
            if record:
                for key in ('entry_signal', 'entry_reason', 'take_profit_price', 'stop_loss_price',
                            'exit_reason', 'trigger_price', 'exit_position', 'exit_detail', 'legacy_exit_detail'):
                    if key in trade: record['trade'][key] = copy.deepcopy(trade[key])
                pos = self.positions.get(trade['symbol'])
                if record.get('position') and pos and record['position']['signal_id'] == pos['signal_id']:
                    self.exchange.merge_strategy_position(record['position'], pos)
        self.exchange._flush_executions()

    async def _drain_notifications(self):
        while self._notifications:
            await self.safety.closed(self._notifications.pop(0))

    async def reconcile_native_exit(self, symbol):
        """Native child execution precedes any replacement or fallback close."""
        from exchange.protection_state import ProtectionGuardError
        pos = self.positions.get(symbol)
        if not pos: return False
        events = await self.exchange.inspect_native_exits(symbol, pos, strict=True)
        if not events: return False
        await self.exchange.refresh_account()
        if symbol not in self.exchange.positions:
            if await self.adopt_native_close(symbol): return True
            raise ProtectionGuardError('native_exit_unresolved', {'symbol': symbol})
        if any(e.get('missing') for e in events):
            raise ProtectionGuardError('native_protection_missing', {'symbol': symbol})
        filled = [e for e in events if e['filled'] > 0]
        if not filled: raise ProtectionGuardError('native_exit_unfilled', {'symbol': symbol})
        # Terminal partial native exits may leave a proven remainder. The close
        # path refreshes it again and rechecks child settlement before its POST.
        event = filled[-1]
        await self.close_position(symbol, event['reason'], event['trigger'])
        return True

    async def exit_reference(self, symbol):
        if self.execution_mode != 'paper' and self.cfg['exchange']['working_type'] == 'MARK_PRICE':
            from exchange.fill_price import positive_decimal
            from exchange.protection_state import ProtectionGuardError
            self.exchange.execution_stage = 'exit_price_snapshot'
            rows = [r for r in await self.exchange.get_position(symbol) if float(r['positionAmt']) != 0]
            if not rows:
                await self.adopt_native_close(symbol)
                return None
            price = positive_decimal(rows[0].get('markPrice'))
            if price is None: raise ProtectionGuardError('invalid_mark_price', {'symbol': symbol})
            self.exchange.prices[symbol] = float(price)
            return float(price)
        return (await self.exchange.fetch_ticker(symbol))['last']

    async def adopt_native_close(self, symbol):
        if self.execution_mode == 'paper': return False
        pos = self.positions[symbol]
        # An explicit close can have succeeded before its local state write.
        await self.exchange.recover_execution()
        closes = [t for t in self.exchange.trades if t.get('reduceOnly') and t['symbol'] == symbol
                  and t.get('signal_id') == pos['signal_id']]
        entry = next((r.get('entry_facts') for r in self.exchange.executions.values()
                      if r.get('entry_facts', {}).get('signal_id') == pos['signal_id']), None)
        original_quantity = entry['amount'] if entry else pos['amount']
        def incomplete():
            quantity = sum(t['amount'] for t in closes)
            return not math.isclose(quantity, original_quantity, rel_tol=self.cfg['exchange']['fill_qty_tolerance_pct'])
        reason = closes[-1].get('exit_reason', 'recovered_close') if closes else 'recovered_close'
        if incomplete():
            events = await self.exchange.inspect_native_exits(symbol, pos, strict=False)
            filled = [e for e in events if e['filled'] > 0]
            if filled: reason = filled[-1]['reason']
            closes = [t for t in self.exchange.trades if t.get('reduceOnly') and t.get('signal_id') == pos['signal_id']]
        if incomplete():
            # A remote reducing execution may be absent from both local intents
            # and native protection metadata. Discover it without sending orders.
            params = {'symbol': self.exchange.raw(symbol), 'startTime': int(pos['opened_at'] * 1000),
                      'limit': self.cfg['exchange']['trade_page_limit']}
            fills = []
            for _ in range(self.cfg['exchange']['trade_max_pages']):
                page = await self.exchange._request('GET', '/fapi/v1/userTrades', params, retry=True)
                fills.extend(page)
                if len(page) < params['limit'] or not page or 'id' not in page[-1]: break
                next_id = int(page[-1]['id']) + 1
                if next_id <= params.get('fromId', -1): break
                params.pop('startTime', None)  # Binance forbids time filters with fromId.
                params['fromId'] = next_id
            for order_id in dict.fromkeys(str(f['orderId']) for f in fills if f.get('orderId')):
                if any(str(t['id']) == order_id for t in closes): continue
                order = await self.exchange._request('GET', '/fapi/v1/order', {
                    'symbol': self.exchange.raw(symbol), 'orderId': order_id}, retry=True)
                if order.get('reduceOnly') not in (True, 'true') or order.get('status') not in ('FILLED', 'EXPIRED', 'CANCELED'): continue
                if order.get('side') != ('SELL' if pos['side'] == 'buy' else 'BUY'): continue
                if float(order.get('executedQty', 0)) <= 0: continue
                await self.exchange._accept_fill(symbol, order, {'reduceOnly': True,
                    'signal_id': pos['signal_id'], 'exit_position': dict(pos)}, order['side'].lower(), recovered=True)
            closes = [t for t in self.exchange.trades if t.get('reduceOnly') and t.get('signal_id') == pos['signal_id']]
        if not closes: return False
        await self.exchange.refresh_account()
        if symbol in self.exchange.positions: return False
        quantity = sum(t['amount'] for t in self.exchange.trades if t.get('reduceOnly')
                       and t.get('signal_id') == pos['signal_id'])
        if not math.isclose(quantity, original_quantity, rel_tol=self.cfg['exchange']['fill_qty_tolerance_pct']):
            return False
        retired = await self.exchange.retire_protection(symbol, pos)
        self.record_exit_reason(symbol, reason, closes[-1]['price'])
        self.persist_execution_metadata()
        self.positions.pop(symbol)
        if retired:
            self.exchange.clear_active_protection(symbol, pos['signal_id'])
        await self._drain_notifications()
        self.save()
        return True

    async def manage_exits(self):
        if self.execution_mode != "paper":
            await self.exchange.refresh_account()
            if not await self.safety.reconcile_once(): return
            for symbol, pos in self.positions.items():
                remote = self.exchange.positions.get(symbol, {}).get("remote", {})
                liquidation = float(remote.get("liquidationPrice", 0))
                buffer = self.cfg["tp_sl"]["liq_safety_margin"]
                safe = (pos["sl"] > liquidation * (1 + buffer) if pos["side"] == "buy"
                        else pos["sl"] < liquidation * (1 - buffer))
                if liquidation <= 0 or not safe:
                    await self.safety.trip("liquidation_guard_breach")
                    return
        await self.strategy.CascadeHunter.manage_exits(self)
        # P2 follows existing TP/SL checks without altering their calculations.
        for symbol, pos in list(self.positions.items()):
            price = self.exchange.prices.get(symbol, pos["entry"])
            pnl_pct = direction(pos["side"]) * (price / pos["entry"] - 1)
            age = time.time() - pos["opened_at"]
            if self.cfg["time_stop"]["enabled"] and age >= self.cfg["time_stop"]["minutes"] * 60 and pnl_pct < self.cfg["time_stop"]["min_profit_pct"]:
                await self.close_position(symbol, "time_stop", price)
            elif self.execution_mode != "paper":
                try: await self.exchange.protect(symbol, pos)
                except Exception as error:
                    from infra.errors import diagnostic_fields
                    await self.safety.event('protection_update_exception', 'critical', symbol=symbol,
                        stage=getattr(self.exchange, 'execution_stage', 'protection_update'),
                        recovery_action='close_affected_symbol_only', **diagnostic_fields(error))
                    await self.safety.handle_symbol_failure(symbol, 'protection_update_failed', error)
        await self._drain_notifications()
        self.save()

    async def scan_loop(self):
        await self.scanner.run()

    async def start(self):
        tasks = []
        try:
            if self.execution_mode != "paper":
                self.state_path.parent.mkdir(parents=True, exist_ok=True)
                self._state_lock = (self.state_path.parent / "account.lock").open("a")
                try: fcntl.flock(self._state_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError: raise ValueError("Another process owns this live account") from None
                self._owns_state = True
            from infra.health_server import HealthServer
            self.health_server = HealthServer(self)
            if self.cfg['health']['enabled']: await self.health_server.start()
            await self.exchange.load_markets()
            if self.initial_balance is None: self.initial_balance = self.exchange.equity
            await self.safety.startup()
            self.save()
            dynamic.log.info("CONTINUOUS %s | equity=%.4f | state=%s", self.execution_mode,
                             self.exchange.equity, self.state_path)
            for component in self.cfg['process']['component_timeout_sec']:
                self.process_watchdog.heartbeat(component)
            tasks = [asyncio.create_task(self.scan_loop()),
                     asyncio.create_task(self.exit_watcher()),
                     asyncio.create_task(self.report()), asyncio.create_task(self.safety.monitor()),
                     asyncio.create_task(self.safety.watchdog()),
                     asyncio.create_task(self.process_watchdog.run()),
                     asyncio.create_task(self.safety.alerter._worker())]
            await asyncio.gather(*tasks)
        finally:
            cause = sys.exc_info()[0]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                if self._owns_state and self._management_authorized and self.execution_mode != "paper" and getattr(self.exchange, "markets", None):
                    shutdown_worker = asyncio.create_task(self.safety.alerter._worker())
                    try:
                        reason = "process_shutdown" if cause in (None, asyncio.CancelledError, KeyboardInterrupt) else "runtime_failure"
                        self.safety.ready = False
                        self.save()
                        await self.safety.event('graceful_shutdown', 'warning', reason=reason)
                        from infra.journal import atomic_json
                        atomic_json(self.state_path.parent / self.cfg['exchange']['session_guard_filename'], {'active': False})
                        if self.safety.alerter.enabled:
                            await asyncio.wait_for(self.safety.alerter.queue.join(), min(3, self.cfg["alerts"]["shutdown_timeout_sec"]))
                    except Exception:
                        if self.safety.journal: self.safety.journal.log("shutdown_incomplete")
                    finally:
                        shutdown_worker.cancel()
                        await asyncio.gather(shutdown_worker, return_exceptions=True)
                if self._owns_state: self.save()
            finally:
                if hasattr(self, 'health_server'): await self.health_server.close()
                await self.exchange.close()
                if self._state_lock:
                    self._state_lock.close()

