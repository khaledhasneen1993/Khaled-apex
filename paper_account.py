"""Virtual account and persistent state for the Khaled paper simulation."""

import asyncio
import math
from uuid import uuid4
from datetime import datetime, timezone

import binance_cascade_hunter_dynamic_v2 as strategy
from risk.config import load_config
from risk.atr_levels import compute_tp_sl
from risk.slippage import entry_slippage_bps, exit_slippage_bps
from infra.journal import atomic_json


class Position(dict):
    """JSON-compatible existing dict, with additive dynamic-stop attributes."""
    @property
    def side(self):
        return "long" if self["side"] == "buy" else "short"

    @property
    def entry(self):
        return self["entry"]

    @property
    def stop(self):
        return self["sl"]

    @stop.setter
    def stop(self, value):
        self["sl"] = value

    @property
    def breakeven_armed(self):
        return self.get("breakeven_armed", False)

    @breakeven_armed.setter
    def breakeven_armed(self, value):
        self["breakeven_armed"] = value


def make_position(side, entry, amount, leverage, atr, cfg, equity, fee_rate,
                  opened_at=None, signal_id=None, atr_source="candles", *, enforce_equity_risk=True):
    sm = 1 if side == "buy" else -1
    from risk.trading_policy import fixed_exits
    if fixed_exits(cfg):
        from risk.liq_math import validate_stop_against_liq
        pct = cfg['exit']['hard_stop_pct']
        stop = entry * (1 - sm * pct)
        loss = amount * (entry * pct + fee_rate * (entry + stop))
        if enforce_equity_risk and loss > equity * cfg['tp_sl']['max_equity_risk_pct'] + math.ulp(equity) * 8:
            raise ValueError('Hard stop exceeds equity risk budget')
        if not validate_stop_against_liq(side, entry, stop, leverage, cfg['tp_sl']['mmr'],
                cfg['tp_sl']['liq_safety_margin'], cfg['tp_sl'].get('maintenance_amount', 0), amount):
            raise ValueError('Hard stop is beyond liquidation safety boundary')
        return Position(side=side, entry=entry, amount=amount, leverage=leverage,
            tp=None, sl=stop, stop_initial=stop, stop_pct=pct, stop_pct_initial=pct,
            tp_pct=None, rr=None, stop_capped=False, risk_capped=False,
            exit_policy='hard_stop_trailing', trailing_armed=False, trailing_extreme=entry,
            trail_arm_pct=cfg['exit']['trail_arm_pct'], trail_distance_pct=cfg['exit']['trail_distance_pct'],
            breakeven_armed=False, trail_moved=False, atr_at_entry=atr, atr_source=atr_source,
            opened_at=datetime.now(timezone.utc).timestamp() if opened_at is None else opened_at,
            signal_id=signal_id or uuid4().hex, entry_fee=entry * amount * fee_rate, equity_at_entry=equity)
    # Loss at stop including entry + exit fees:
    # amount * (distance + fee * (2*entry - sm*distance)).
    budget = equity * cfg["tp_sl"]["max_equity_risk_pct"]
    distance_cap = (budget / amount - 2 * entry * fee_rate) / (1 - sm * fee_rate)
    levels = compute_tp_sl(side, entry, atr, leverage,
                           {**cfg["tp_sl"], "max_stop_distance": distance_cap})
    return Position(exit_policy="atr", side=side, entry=entry, amount=amount, tp=levels["tp"], sl=levels["stop"],
                    stop_initial=levels["stop"], stop_pct=levels["stop_pct"],
                    stop_pct_initial=levels["stop_pct"], tp_pct=levels["tp_pct"], rr=levels["rr"],
                    stop_capped=levels["stop_capped"], risk_capped=levels["risk_capped"],
                    breakeven_armed=False, trail_moved=False, atr_at_entry=atr, atr_source=atr_source,
                    leverage=leverage, opened_at=datetime.now(timezone.utc).timestamp() if opened_at is None else opened_at,
                    signal_id=signal_id or uuid4().hex, entry_fee=entry * amount * fee_rate,
                    equity_at_entry=equity)


class PaperExchange:
    """Explicit public-data allowlist. Orders and balances exist only in memory."""

    def __init__(self, market_data, fee_rate=0.0005, leverage=None):
        self.market_data = market_data
        self.fee_rate = fee_rate
        self.leverage = strategy.LEVERAGE if leverage is None else leverage
        self.balance = 100.0
        self.positions = {}
        self.prices = {}
        self.trades = []
        self.cfg = load_config()

    @property
    def used(self):
        return sum(p["margin"] for p in self.positions.values())

    @property
    def free(self):
        return max(0.0, self.balance - self.used)

    @property
    def equity(self):
        return self.balance + sum(
            (self.prices.get(s, p["entry"]) - p["entry"]) * p["amount"]
            * (1 if p["side"] == "buy" else -1)
            for s, p in self.positions.items()
        )

    async def load_markets(self):
        return await self.market_data.load_markets()

    async def fetch_ohlcv(self, symbol, timeframe, limit):
        return await self.market_data.fetch_ohlcv(symbol, timeframe, limit=limit)

    async def fetch_order_book(self, symbol, limit):
        return await self.market_data.fetch_order_book(symbol, limit=limit)

    async def fetch_ticker(self, symbol):
        ticker = await self.market_data.fetch_ticker(symbol)
        price = float(ticker["last"])
        if not math.isfinite(price) or price <= 0:
            raise ValueError("Invalid market price")
        self.prices[symbol] = price
        return ticker

    def amount_to_precision(self, symbol, amount):
        return self.market_data.amount_to_precision(symbol, amount)

    async def fetch_balance(self):
        return {"USDT": {"free": self.free, "used": self.used, "total": self.balance, "equity": self.equity}}

    async def set_leverage(self, leverage, symbol):
        # Never call a private Binance endpoint.
        if leverage != self.leverage:
            raise ValueError("Leverage must match the original strategy")

    async def create_order(self, symbol, type, side, amount, params=None):
        if type != "market" or side not in {"buy", "sell"}:
            raise ValueError("Paper account supports buy/sell market orders only")
        amount = float(amount)
        if not math.isfinite(amount) or amount <= 0:
            raise ValueError("Invalid amount")
        closing = bool((params or {}).get("reduceOnly", False))
        position = self.positions.get(symbol)
        if closing and (not position or position["side"] == side
                        or not math.isclose(amount, position["amount"])):
            raise ValueError("Invalid reduce-only close")
        if not closing and position:
            raise ValueError("Position already exists")
        price = float((await self.fetch_ticker(symbol))["last"])
        target_notional = None
        if not closing and (params or {}).get("use_paper_notional"):
            from risk.trading_policy import fixed_paper_notional
            target_notional = fixed_paper_notional(self.cfg)
            if target_notional is not None:
                amount = float(self.amount_to_precision(symbol, target_notional / price))
                if not math.isfinite(amount) or amount <= 0:
                    raise ValueError("Fixed Paper notional is below market quantity precision")
        notional = amount * price
        fee = notional * self.fee_rate
        pnl = 0.0
        if closing:
            pnl = (price - position["entry"]) * amount * (
                1 if position["side"] == "buy" else -1
            )
            self.balance += pnl - fee
            del self.positions[symbol]
        else:
            limits = self.market_data.market(symbol).get("limits", {})
            for key, value in (("amount", amount), ("cost", notional)):
                bounds = limits.get(key) or {}
                if bounds.get("min") is not None and value < bounds["min"]:
                    raise ValueError(f"Below market minimum {key}")
                if bounds.get("max") is not None and value > bounds["max"]:
                    raise ValueError(f"Above market maximum {key}")
            margin = notional / self.leverage
            if margin + fee > self.free:
                raise ValueError("Insufficient virtual margin including entry fee")
            # Validate at the actual fill price BEFORE mutating account balances.
            atr = (params or {}).get("exit_atr", price * self.cfg["atr"]["fallback_atr_pct"])
            risk_position = make_position(side, price, amount, self.leverage, atr,
                                          self.cfg, self.equity, self.fee_rate,
                                          atr_source=(params or {}).get("atr_source", "fallback"),
                                          enforce_equity_risk=target_notional is None)
            self.balance -= fee
            self.positions[symbol] = {
                "side": side, "entry": price, "amount": amount, "margin": margin,
            }
        expected = float((params or {}).get("expected_price", price))
        if not closing:
            risk_position.update(expected_entry=expected, actual_entry=price,
                                 entry_slippage_bps=entry_slippage_bps(expected, price, side))
        trade = {
            "id": f"paper-{len(self.trades) + 1}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol, "side": side, "amount": amount, "price": price,
            "reduceOnly": closing, "fee_usdt": fee, "pnl_usdt": pnl,
            "balance_usdt": self.balance,
        }
        if closing:
            trade["signal_id"] = (params or {}).get("signal_id")
            original_side = "sell" if side == "buy" else "buy"
            trade.update(expected_exit=expected, actual_exit=price,
                         exit_slippage_bps=exit_slippage_bps(expected, price, original_side))
        if not closing:
            trade.update(expected_entry=expected, actual_entry=price,
                         entry_slippage_bps=risk_position["entry_slippage_bps"])
            trade["signal_id"] = risk_position["signal_id"]
            trade["take_profit_price"] = risk_position["tp"]
            trade["stop_loss_price"] = risk_position["sl"]
        self.trades.append(trade)
        strategy.log.info("PAPER FILL %s | %s %s | wallet=%.4f free=%.4f equity=%.4f",
                          trade["id"], side, symbol, self.balance, self.free, self.equity)
        return {**trade, "average": price, "filled": amount, "status": "closed",
                **({"risk_position": risk_position} if not closing else {})}

    async def cancel_order(self, symbol, order_id=None, client_id=None):
        raise ValueError("PaperExchange has no pending exchange orders")

    async def get_position(self, symbol):
        return [{"symbol": s, "positionAmt": p["amount"] * (1 if p["side"] == "buy" else -1),
                 "entryPrice": p["entry"]} for s, p in self.positions.items()
                if symbol is None or symbol == s]

    async def get_open_orders(self, symbol=None):
        return []

    async def fetch_market(self, symbol):
        return self.market_data.market(symbol)

    async def close(self):
        await self.market_data.close()


class PaperState:
    def restore(self):
        from journal.wal import load_durable
        state = load_durable(self.state_path)
        saved_settings = dict(state["settings"])
        saved_settings.setdefault("execution_mode", "paper")
        if saved_settings.get("position_basis") == "wallet_total":
            saved_settings["position_basis"] = "current_equity"
        # Discard superseded fixed-exit metadata only; retain strict validation
        # of entry, leverage, sizing, fee and active-exit settings.
        saved_settings.pop("STOP_LOSS_PCT", None)
        saved_settings.pop("TAKE_PROFIT_PCT", None)
        if saved_settings.get("stop_basis") == "signal_price":
            saved_settings["stop_basis"] = "fill_price"
        # Pre-feature states have no exit-config snapshot. Existing settings
        # must still match, while positions are upgraded on their next check.
        saved_settings.setdefault("active_exit", self.settings()["active_exit"])
        current_settings = self.settings()
        # This size applies to new entries only. Existing positions retain their fills.
        if current_settings.get("execution_mode") == "paper":
            saved_settings.pop("paper_position_notional_usdt", None)
            if "paper_position_notional_usdt" in current_settings:
                saved_settings["paper_position_notional_usdt"] = current_settings["paper_position_notional_usdt"]
        if (state["version"] != 1 or saved_settings != current_settings
                or state["fee_rate"] != self.exchange.fee_rate):
            raise ValueError("State settings differ; use a new --state path or --reset")
        wallet = float(state["balance_usdt"])
        if not math.isfinite(wallet):
            raise ValueError("Invalid saved wallet")
        self.initial_balance = state.get("initial_balance_usdt")
        self.exchange.balance = wallet
        self.exchange.positions = state["account_positions"]
        self.exchange.prices = state["prices"]
        self.exchange.trades = state["trades"]
        self.positions = {symbol: Position(pos) for symbol, pos in state["strategy_positions"].items()}
        self.last_trade = state["last_trade"]
        if state.get("operations") and hasattr(self, "safety"):
            self.safety.restore(state['operations'])
            if getattr(self, 'execution_mode', 'paper') != 'paper' and 'accounted_closes' not in state['operations']:
                # Upgrade only facts already committed in the legacy state. Do
                # not apply the same closed-trade PnL to KillSwitch a second time.
                groups = {}
                for trade in self.exchange.trades:
                    if trade.get('reduceOnly') and trade.get('signal_id'):
                        groups.setdefault(trade['signal_id'], []).append(trade)
                for closes in groups.values():
                    reported = [t for t in closes if t.get('exit_detail')]
                    if not reported: continue
                    self.safety.accounted_closes.add(self.safety.close_key(reported[-1]))
                    snapshot = reported[-1].get('exit_position')
                    quantity = sum(t['amount'] for t in closes)
                    if len(reported) > 1 or snapshot and not math.isclose(quantity, snapshot['amount'],
                            rel_tol=self.cfg['exchange']['fill_qty_tolerance_pct'], abs_tol=0):
                        self.safety.kill.triggered = self.safety.kill.triggered or 'legacy_accounting_review'
                        self.safety.ready = False
                        # Keep the original evidence without treating a known
                        # premature/duplicated result as a fresh final result.
                        for trade in reported:
                            trade['legacy_exit_detail'] = trade.pop('exit_detail')
        if getattr(self, 'execution_mode', 'paper') == 'paper' and set(self.positions) != set(self.exchange.positions):
            raise ValueError("Inconsistent saved positions")

    def save(self):
        if hasattr(self, 'persist_execution_metadata'): self.persist_execution_metadata()
        state = {
            "version": 1, "initial_balance_usdt": getattr(self, "initial_balance", 100.0),
            **({"operations": self.safety.dump()} if hasattr(self, "safety") else {}),
            "settings": self.settings(), "fee_rate": self.exchange.fee_rate,
            "balance_usdt": self.exchange.balance, "equity_usdt": self.exchange.equity,
            "free_usdt": self.exchange.free, "used_usdt": self.exchange.used,
            "account_positions": self.exchange.positions,
            "strategy_positions": self.positions, "prices": self.exchange.prices,
            "last_trade": self.last_trade, "trades": self.exchange.trades,
        }
        from journal.wal import WriteAheadLog
        WriteAheadLog(self.state_path.parent / 'journal.wal').snapshot(self.state_path, state, writer=atomic_json)

    async def report(self):
        while True:
            strategy.log.info("%s | wallet=%.4f equity=%.4f free=%.4f used=%.4f positions=%d",
                              getattr(self, "execution_mode", "paper"), self.exchange.balance, self.exchange.equity,
                              self.exchange.free, self.exchange.used, len(self.positions))
            await asyncio.sleep(30)
