"""Adapter for the durable Binance executor. Operator entry remains release-gated."""
import asyncio
from dataclasses import asdict
from math import isfinite
import time

from exchange.demo import validate_demo
from exchange.leverage import bracket_for
from exchange.live_exchange import LiveExchange
from exchange.quantity import OrderValidationError, validate_quantity
from paper_account import Position
from portfolio.limits import check, risk_multiplier
from risk.liq_math import validate_stop_against_liq

from .qualification import DAILY_LOSS, RELEASE_BLOCKERS, fingerprint, preflight
from .order_authorization import EntryOrder, check_entry

DEMO_BLOCKERS = ("DEMO_PREFLIGHT_PENDING", *RELEASE_BLOCKERS)


def require_demo_release():
    raise ValueError("Apex Demo operational release blocked: "+", ".join(DEMO_BLOCKERS))


class ApexDemoExchange(LiveExchange):
    """Use the same intent ledger, recovery, account reads and native protection."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.apex_lock = asyncio.Lock()
        self.apex_book = None
        self.apex_max_age = 0
        self.approval = self.fence = None
        self.qualification_symbols = ()
        self.approved_order = None

    def authorize_apex_entry(self):
        if self.approval is None or self.fence is None:
            require_demo_release()
        self.approval.check_entry()

    async def before_mutation(self, method, path, params):
        if self.fence is None:
            # Unconfigured real clients cannot reach any mutation. The normal
            # operational release is unavailable; tests inject their own fence.
            raise OrderValidationError("ACCOUNT_FENCE_NOT_CONFIGURED")
        try:
            await self.fence.check()
        except ValueError as exc:
            raise OrderValidationError(str(exc)) from None
        if (self.approval is not None and path == "/fapi/v1/order"
                and method == "POST" and params.get("type") == "LIMIT"):
            order = self.approved_order
            if (order is None or params.get("symbol") != order.symbol
                    or params.get("side") != order.side.upper() or params.get("price") != order.limit
                    or params.get("quantity") != order.quantity or params.get("timeInForce") != "IOC"):
                raise OrderValidationError("ORDER_PAYLOAD_CHANGED_AFTER_APPROVAL")
            self.authorize_apex_entry()
            await self.refresh_account()
            if (self.positions or await self.get_open_orders()
                    or order.account_version != self.account_version()):
                raise OrderValidationError("ACCOUNT_CHANGED_AT_SEND")
            if not self.apex_book or not self.apex_book.fresh(time.time_ns()//1_000_000, self.apex_max_age):
                raise OrderValidationError("MARKET_EXPIRED_AT_SEND")
            # Recheck ownership after the account and market checks.
            try:
                await self.fence.check()
            except ValueError as exc:
                raise OrderValidationError(str(exc)) from None
            if not self.apex_book.fresh(time.time_ns()//1_000_000, self.apex_max_age):
                raise OrderValidationError("MARKET_EXPIRED_AT_SEND")

    async def close(self):
        try:
            if self.fence is not None and self.qualification_symbols:
                try:
                    await preflight(self, self.qualification_symbols, expected_alias=self.approval.account_alias)
                    await self.fence.release()
                except Exception:
                    self.journal.log("qualification_fence_retained", reason="flat_reconciled_state_unproven")
        finally:
            await super().close()

    def account_version(self):
        return fingerprint(dict(equity=self.equity, free=self.free, used=self.used, positions=self.positions))

    def validate_entry_submission(self, symbol, options):
        self.authorize_apex_entry()
        plan = options.get("apex_plan")
        if not plan or self.storage_fault or self.journal.failed:
            raise OrderValidationError("Apex plan or durable storage unavailable")
        now = time.time_ns()//1_000_000
        if (not 0 <= now-plan["asof_ms"] <= plan["max_age_ms"] or self.apex_book is None
                or self.apex_book.symbol != self.raw(symbol) or not self.apex_book.fresh(now, plan["max_age_ms"])):
            raise OrderValidationError("Apex signal or book expired before submission")

    async def enter_apex(self, decision, cfg, supervisor, *, market_state, exposures, high_water, correlated):
        async with self.apex_lock:
            self.apex_book = market_state.book
            self.apex_max_age = cfg.max_age_ms
            try:
                return await self._enter_apex(decision, cfg, supervisor, exposures, high_water, correlated)
            finally:
                self.apex_book = None

    async def _enter_apex(self, decision, cfg, supervisor, exposures, high_water, correlated):
        self.authorize_apex_entry()
        validate_demo(self.cfg)
        if decision.model is not None or cfg.leverage != self.cfg["leverage"] or self.cfg["exchange"]["leverage_policy"] != "fixed":
            raise OrderValidationError("Apex Demo requires rules-only and the configured fixed leverage")
        signal = decision.signal
        symbol = self.symbol(signal.symbol)
        if not await self.recover_execution():
            raise OrderValidationError("Prior execution remains unresolved")
        await self.refresh_account()
        remote_symbols = {self.raw(s) for s in self.positions}
        if (remote_symbols != {p.symbol for p in exposures} or len(exposures) >= cfg.max_positions
                or any(s not in self.pending_protection or not self.pending_protection[s].get("protection", {}).get("sl")
                       for s in self.positions)):
            raise OrderValidationError("Exposure or protection not reconciled")
        if not await supervisor.allow_entry(symbol, signal.side, signal.reference):
            raise OrderValidationError("Safety supervisor blocked entry")
        if reason := check(exposures, decision.exposure, self.equity, high_water, correlated, cfg):
            raise OrderValidationError(reason)
        adverse = signal.reference*(1+cfg.max_slippage)
        if decision.plan.quantity*adverse*(1/cfg.leverage+self.fee_rate) > self.free:
            raise OrderValidationError("Insufficient available margin")
        if self.approval is not None and exposures:
            raise OrderValidationError("QUALIFICATION_SINGLE_POSITION_LIMIT")
        brackets = await self.leverage_brackets(symbol)
        sign = 1 if signal.side == "buy" else -1
        limit = self._price(symbol, signal.reference*(1+sign*cfg.max_slippage), round_up=sign < 0)
        amount = decision.plan.quantity
        validate_quantity(self.market(symbol), amount, limit)
        bracket = bracket_for(brackets, amount*adverse)
        if cfg.leverage > int(bracket["initialLeverage"]):
            raise OrderValidationError("Apex fixed leverage unavailable")
        risk = min(decision.plan.stop_risk, self.equity*cfg.risk_fraction*risk_multiplier(self.equity, high_water, cfg))
        planned = dict(stop=decision.plan.stop, target=decision.plan.target, budget=risk,
                       limit=limit, max_slippage=cfg.max_slippage, asof_ms=signal.asof_ms,
                       max_age_ms=cfg.max_age_ms, signal=asdict(signal), config=cfg.fingerprint)
        options = dict(apex_plan=planned, expected_price=float(limit),
                       exit_atr=abs(signal.reference-decision.plan.stop)/cfg.atr_stop)
        # Validate the protection envelope before changing leverage or posting.
        risk_cfg = {"tp_sl": {**self.cfg["tp_sl"], "mmr": float(bracket["maintMarginRatio"]),
                              "maintenance_amount": float(bracket.get("cum", 0))}}
        self.build_entry_position(signal.side, float(limit), amount, cfg.leverage, options["exit_atr"],
                                  risk_cfg, self.equity, options)
        self.validate_entry_submission(symbol, options)
        maximum = await self.set_leverage(cfg.leverage, symbol)
        if amount*adverse > maximum:
            raise OrderValidationError("Apex order exceeds confirmed leverage bracket")
        if self.approval is not None:
            await self.refresh_account()
            if self.positions or await self.get_open_orders():
                raise OrderValidationError("ACCOUNT_CHANGED_BEFORE_SUBMISSION")
            if not await supervisor.allow_entry(symbol, signal.side, signal.reference):
                raise OrderValidationError("SAFETY_CHANGED_BEFORE_SUBMISSION")
            kill = supervisor.kill
            start = kill.day_start_equity
            if start is None or not isfinite(start) or start <= 0:
                raise OrderValidationError("DAILY_START_EQUITY_UNAVAILABLE")
            headroom = start*DAILY_LOSS + min(0., self.equity-start, kill.state.daily_pnl)
            planned["budget"] = min(risk, headroom, self.equity*cfg.risk_fraction*
                                    risk_multiplier(self.equity, high_water, cfg))
            if planned["budget"] <= 0:
                raise OrderValidationError("DAILY_RISK_LIMIT")
            order = EntryOrder(signal.symbol, signal.side, self.amount_to_precision(symbol, amount),
                               "LIMIT_IOC", limit, planned["stop"], planned["target"], planned["budget"],
                               cfg.leverage, self.approval.digest, signal.strategy, 1, self.account_version())
            reasons = check_entry(order, self.apex_book, time.time_ns()//1_000_000, signal.asof_ms,
                                  cfg.max_age_ms, cfg.max_spread, self.equity, self.free, self.fee_rate)
            if reasons:
                self.journal.log("apex_risk_block", symbol=symbol, report={"reasons": reasons})
                raise OrderValidationError(reasons[0])
            if not self.journal.log("apex_risk_pass", symbol=symbol, report={"fingerprint": order.digest,
                                    "strategy": signal.strategy, "budget": order.risk_budget}):
                raise OrderValidationError("RISK_APPROVAL_JOURNAL_UNAVAILABLE")
            planned["order_fingerprint"] = order.digest
            self.approved_order = order
        self.selected_leverage[symbol] = dict(leverage=cfg.leverage, amount=amount, confirmed=True,
                                              entry_order_valid=True)
        try:
            return await self.create_order(symbol, "market", signal.side, amount, options)
        finally:
            self.selected_leverage.pop(symbol, None)
            self.approved_order = None

    def build_entry_position(self, side, price, quantity, leverage, atr, risk_cfg,
                             equity, options, **metadata):
        plan = options.get("apex_plan")
        if not plan:
            return super().build_entry_position(side, price, quantity, leverage, atr,
                                                risk_cfg, equity, options, **metadata)
        sign = 1 if side == "buy" else -1
        sl, tp = plan["stop"], plan["target"]
        worst = sl*(1-sign*plan["max_slippage"])
        loss = quantity*(sign*(price-worst)+self.fee_rate*(price+worst))
        cfg = risk_cfg["tp_sl"]
        if (not all(isfinite(v) for v in (price, sl, tp, loss, plan["budget"]))
                or sign*(tp-price) <= 0 or sign*(price-sl) <= 0
                or sign*(price-float(plan["limit"])) > 0 or loss > plan["budget"]
                or not validate_stop_against_liq(side, price, worst, leverage, cfg["mmr"],
                                                 cfg["liq_safety_margin"], cfg["maintenance_amount"], quantity)):
            raise OrderValidationError("Apex position breaches protection envelope")
        stop_pct, tp_pct = sign*(price-sl)/price, sign*(tp-price)/price
        return Position(side=side, amount=quantity, entry=price, leverage=leverage,
                        sl=sl, tp=tp, stop_initial=sl, stop_pct=stop_pct, stop_pct_initial=stop_pct,
                        tp_pct=tp_pct, rr=tp_pct/stop_pct, exit_policy="apex",
                        atr_at_entry=atr, entry_fee=quantity*price*self.fee_rate,
                        opened_at=metadata.get("opened_at") or time.time(),
                        signal_id=metadata.get("signal_id"), atr_source="apex",
                        breakeven_armed=False, trail_moved=False, risk_capped=False,
                        stop_capped=False, equity_at_entry=equity, apex=plan)

    async def submit_order(self, symbol, type, side, amount, params=None):
        options = params or {}
        if type != "market" or options.get("reduceOnly") or not options.get("apex_plan"):
            return await super().submit_order(symbol, type, side, amount, params)
        self.validate_entry_submission(symbol, options)
        order = self.approved_order
        if self.approval is not None and (order is None or options["apex_plan"].get("order_fingerprint") != order.digest
                or order.account_version != self.account_version()
                or options["apex_plan"]["stop"] != order.stop or options["apex_plan"]["target"] != order.target
                or options["apex_plan"]["budget"] != order.risk_budget):
            raise OrderValidationError("ORDER_APPROVAL_STALE_OR_CHANGED")
        quantity = self.amount_to_precision(symbol, amount)
        limit = options["apex_plan"]["limit"]
        validate_quantity(self.market(symbol), quantity, limit)
        return await self._request("POST", "/fapi/v1/order", {
            "symbol": self.raw(symbol), "side": side.upper(), "type": "LIMIT", "timeInForce": "IOC",
            "quantity": quantity, "price": limit, "newClientOrderId": options["newClientOrderId"],
            "newOrderRespType": "RESULT",
        })
