"""Gated Apex strategy runner on the inherited account/recovery lifecycle."""
from copy import deepcopy
from dataclasses import asdict
from math import isfinite
import time

from apex.demo_execution import ApexDemoExchange, require_demo_release
from apex.engine import Engine
from apex.qualification import DAILY_LOSS, preflight
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from exchange.quantity import OrderValidationError
from journal.wal import WriteAheadLog, load_durable
from market_data.capture import SYMBOL, stream
from market_data.events import apply_market_row
from portfolio.limits import Exposure, correlations
from regime.classifier import classify
from risk.apex_exit import Exit


class ApexDemoRuntime(V2PaperHunter):
    live_exchange = ApexDemoExchange

    def __init__(self, config, instruments, *args, approval=None, **kwargs):
        if approval is None:
            require_demo_release()
        self.approval = approval
        if not 1 <= len(instruments) <= 8 or any(not SYMBOL.fullmatch(s) for s in instruments):
            raise ValueError("Apex Demo requires filters for one to eight USD-M symbols")
        self.apex_config = config
        self.engine = Engine(config, instruments)
        self.market_states = {}
        self.next_selection = None
        self.high_water = 0.
        super().__init__(*args, **kwargs)
        if self.execution_mode != "testnet":
            raise ValueError("ApexDemoRuntime requires Demo")
        if approval is not None:
            self.safety.kill.cfg["daily_dd_limit_pct"] = DAILY_LOSS
            self.safety.kill.cfg["daily_drawdown_action"] = "block_entries"
        self.high_path = self.state_path.parent/"apex-high-water.json"
        stored = load_durable(self.high_path)
        if stored:
            if stored.get("fingerprint") != self.apex_config.fingerprint:
                raise ValueError("Apex Demo high-water configuration mismatch")
            self.high_water = float(stored["value"])
            if not isfinite(self.high_water) or self.high_water <= 0:
                raise ValueError("Invalid Apex Demo high water")

    def settings(self):
        return {**super().settings(), "mode": "apex_demo", "apex_fingerprint": self.apex_config.fingerprint,
                "apex_instruments": {s: asdict(rules) for s, rules in self.engine.instruments.items()}}

    async def start(self):
        try:
            if self.approval is None:
                require_demo_release()
            self.approval.check_entry()
            if self.exchange.fence is None:
                raise ValueError("ACCOUNT_FENCE_NOT_CONFIGURED")
            await preflight(self.exchange, self.engine.instruments, expected_alias=self.approval.account_alias)
            if self.positions or self.safety.kill.blocks_entries or self.safety.storage_failed:
                raise ValueError("LOCAL_ACCOUNT_STATE_REQUIRES_REVIEW")
            await self.exchange.fence.acquire()
            self.exchange.qualification_symbols = tuple(self.engine.instruments)
            await preflight(self.exchange, self.engine.instruments, expected_alias=self.approval.account_alias)
        except BaseException:
            await self.exchange.close()
            raise
        await super().start()

    async def scan_loop(self):
        await stream(tuple(self.engine.instruments), self.consume_market)

    async def consume_market(self, rows):
        cfg = self.apex_config
        for row in rows:
            try:
                state = apply_market_row(self.market_states, row, cfg.max_age_ms)
            except (ValueError, KeyError, TypeError, OverflowError):
                self.market_states[row["symbol"]].disconnect()
                continue
            now = time.time_ns()//1_000_000
            if state.book.fresh(now, cfg.max_age_ms):
                self.on_market_heartbeat()
        self.process_watchdog.heartbeat("scanner")
        self.process_watchdog.heartbeat("ws")
        if not rows:
            return
        now = time.time_ns()//1_000_000
        if self.next_selection is None:
            self.next_selection = now+cfg.rank_window_ms
            return
        if now < self.next_selection:
            return
        self.next_selection = now+cfg.rank_window_ms
        async with self.lock:
            if not self.safety.ready or self.safety.kill.blocks_entries:
                return
            current = self.apex_exposures()
            if current is None:
                return
            returns = {}
            for symbol, state in self.market_states.items():
                candles = [c for c in state.candles if c[0]+60000 <= now]
                returns[symbol] = {b[0]: b[4]/a[4]-1 for a, b in zip(candles, candles[1:])}
            linked = correlations(returns)
            decisions, rejected = self.engine.decide(self.market_states, now, self.exchange.equity,
                                                      self.exchange.free, max(self.high_water, self.exchange.equity),
                                                      current, linked)
            for symbol, reason in rejected:
                self.safety.journal.log("apex_signal_rejected", symbol=symbol, reason=reason)
            if not decisions or not await self.safety.reconcile_once():
                return
            current = self.apex_exposures()
            if current is None:
                return
            if not self.update_high_water():
                return
            for decision in decisions:
                try:
                    symbol = self.exchange.symbol(decision.signal.symbol)
                except (KeyError, StopIteration):
                    self.safety.journal.log("apex_entry_rejected", symbol=decision.signal.symbol,
                                            reason="SYMBOL_NOT_IN_DEMO_MARKETS")
                    continue
                try:
                    trade = await self.exchange.enter_apex(
                        decision, cfg, self.safety, market_state=self.market_states[decision.signal.symbol],
                        exposures=current, high_water=self.high_water, correlated=linked)
                except OrderValidationError as error:
                    self.safety.journal.log("apex_entry_rejected", symbol=symbol, reason=str(error))
                    continue
                except Exception:
                    # The inherited ledger and reconciler own ambiguous POSTs.
                    self.safety.ready = False
                    await self.safety.reconcile_once()
                    return
                if trade is not None:
                    self.merge_execution_state()
                    self.record_entry_signal(symbol, asdict(decision.signal), reason=decision.signal.strategy)
                    current = self.apex_exposures()
                    if current is None:
                        return

    def update_high_water(self):
        equity = self.exchange.equity
        if not isfinite(equity) or equity <= 0:
            return False
        if equity <= self.high_water:
            return True
        wal = WriteAheadLog(self.high_path.parent/"journal.wal")
        try:
            wal.snapshot(self.high_path, {"value": equity, "fingerprint": self.apex_config.fingerprint})
        except OSError:
            self.safety.storage_failed = True
            self.safety.ready = False
            return False
        self.high_water = equity
        return True

    def apex_exposures(self):
        result = []
        for symbol, pos in self.positions.items():
            if not pos.get("apex") or symbol not in self.exchange.positions:
                return None
            side, entry, amount = pos["side"], pos["entry"], pos["amount"]
            sign = 1 if side == "buy" else -1
            worst = pos["sl"]*(1-sign*self.apex_config.max_slippage)
            risk = amount*max(0., sign*(entry-worst)+self.exchange.fee_rate*(entry+worst))
            raw = self.exchange.raw(symbol)
            state = self.market_states.get(raw)
            f = state.features(time.time_ns()//1_000_000, self.apex_config) if state else None
            if f is None:
                return None
            result.append(Exposure(raw, side, amount*f.price, risk, f.long_vol))
        return result

    async def manage_exits(self):
        if not await self.safety.reconcile_once():
            return
        self.update_high_water()
        cfg = self.apex_config
        now = time.time_ns()//1_000_000
        for symbol, pos in list(self.positions.items()):
            state = self.market_states.get(self.exchange.raw(symbol))
            f = state.features(now, cfg) if state else None
            if not f or not pos.get("apex"):
                continue
            order = Exit(pos["side"], pos["entry"], pos["sl"], pos["tp"],
                         int(pos["opened_at"]*1000), pos.get("trailing_extreme"))
            thesis_lost = classify(f, cfg) != pos["apex"]["signal"]["regime"]
            reason = order.update(f.price, f.atr, f.trend, f.spread, cfg, now_ms=now,
                                  timeout_ms=cfg.thesis_timeout_ms or None, thesis_lost=thesis_lost)
            if reason == "TIME_STOP":
                await self.close_position(symbol, reason, f.price)
            elif reason is None and order.stop != pos["sl"]:
                replacement = deepcopy(pos)
                replacement["sl"] = order.stop
                replacement["trailing_extreme"] = order.extreme
                try:
                    await self.exchange.protect(symbol, replacement)
                except Exception:
                    self.safety.ready = False
                    self.exchange.management_fault(symbol, "apex_trailing_update_unresolved", pos)
                    return
                pos.update(replacement)
                self.save()
            elif reason is None:
                pos["trailing_extreme"] = order.extreme
        self.save()
