"""Event-based shadow simulation with delayed entries and observable order-book prices."""
import argparse
from dataclasses import dataclass, replace
import csv
import json
from math import isclose, isfinite
from pathlib import Path

from apex.config import Config
from apex.engine import Engine, Instrument
from market_data.features import MarketState
from models.meta import Model
from portfolio.limits import check, correlations, risk_multiplier
from risk.apex_exit import Exit
from risk.liq_math import validate_stop_against_liq
from regime.classifier import classify
from .liquidity import DisplayedLiquidity, floor_quantity
from .statistics import block_bootstrap, summary


@dataclass
class OpenPosition:
    decision: object
    exit: Exit
    amount: float
    entry_fee: float
    slippage: float
    funding: float = 0.
    exit_reason: str | None = None
    scaled_quantity: float = 0.
    initial_amount: float = 0.
    closed_pnl: float = 0.
    exit_value: float = 0.
    total_fees: float = 0.
    total_funding: float = 0.


@dataclass(frozen=True)
class Experiment:
    partial_fills: bool = False
    scale_out_fraction: float = 0.
    scale_out_r: float = 1.
    thesis_timeout_ms: int | None = None

    def __post_init__(self):
        if (type(self.partial_fills) is not bool or not 0 <= self.scale_out_fraction < 1
                or not isfinite(self.scale_out_r) or self.scale_out_r <= 0
                or self.thesis_timeout_ms is not None
                and (type(self.thesis_timeout_ms) is not int or self.thesis_timeout_ms <= 0)):
            raise ValueError("Invalid research execution experiment")


class Replay:
    def __init__(self, engine, *, equity=100., latency_ms=100, stress_slippage=0., experiment=None, exit_type=Exit):
        if not isfinite(equity) or equity <= 0 or latency_ms < 0 or not 0 <= stress_slippage < .1:
            raise ValueError("Invalid simulation assumptions")
        self.engine = engine
        self.states, self.open, self.funding_keys = {}, {}, set()
        self.balance = self.initial = self.high_water = equity
        self.latency = latency_ms
        self.stress = stress_slippage
        self.trades, self.signals, self.equity = [], [], [equity]
        self.exit_fills = []
        self.experiment = experiment if experiment is not None else Experiment()
        self.liquidity = DisplayedLiquidity()
        self.exit_type = exit_type
        self.depth_versions = {}
        self.exit_fill_count = self.partial_entries = 0
        self.pending = None
        self.started = self.clock = self.next_batch = None
        self.last_prices, self.last_rejections = {}, {}
        self.config_hash = engine.cfg.fingerprint
        self.exposure_ms = self.missing = self.funding_events = 0

    def apply(self, row):
        now, symbol, kind, data = (row[k] for k in ("received_ms", "symbol", "kind", "data"))
        if type(now) is not int or now < 0 or (self.clock is not None and now < self.clock):
            raise ValueError("Replay timestamps must be monotonically ordered")
        if self.started is None:
            self.started, self.next_batch = now, now+self.engine.cfg.rank_window_ms
        if self.open:
            self.exposure_ms += now-self.clock
        self.clock = now
        state = self.states.setdefault(symbol, MarketState(symbol))
        cfg = self.engine.cfg
        try:
            if kind == "event":
                state.ingest(data, now, cfg.max_age_ms)
            elif kind == "snapshot":
                state.book.snapshot(data)
            elif kind == "reset_depth":
                state.book.invalidate()
                state.flows.clear()
            elif kind == "reset_market":
                book, flows = state.book, state.flows
                state.disconnect()
                state.book, state.flows = book, flows
            elif kind == "candles":
                for candle in data:
                    if int(candle[0])+60000 <= now:
                        state.add_candle(int(candle[0]), *(float(v) for v in candle[1:6]))
            elif kind == "open_interest":
                state.open_interest(data, now)
            elif kind == "funding_settlement":
                self.funding(symbol, now, data)
            else:
                raise ValueError("Unknown replay event")
        except (ValueError, KeyError, TypeError, OverflowError):
            self.missing += 1
            state.disconnect()
            return
        if state.book.fresh(now, cfg.max_age_ms):
            self.last_prices[symbol] = state.book.quote.mid
            if self.depth_versions.get(symbol) != (state.book.last, state.book.event_ms):
                self.depth_versions[symbol] = state.book.last, state.book.event_ms
                self.liquidity.observe(symbol, state.book)
                self.manage(symbol, now)
        if self.pending and now >= self.pending[0]:
            self.fill(now)
        if now >= self.next_batch:
            self.next_batch = now+cfg.rank_window_ms
            if self.pending is None:
                equity = self.mark_equity()
                margin = sum(p.amount*p.exit.entry/cfg.leverage for p in self.open.values())
                decisions, rejected = self.engine.decide(
                    self.states, now, equity, max(0., self.balance-margin), self.high_water,
                    [p.decision.exposure for p in self.open.values()], self.correlations(now))
                current_rejections = set(rejected)
                self.signals.extend(dict(time=now, symbol=s, reason=reason, config=self.config_hash)
                                    for s, reason in sorted(current_rejections - self.last_rejections.keys()))
                self.last_rejections = dict.fromkeys(current_rejections)
                if decisions:
                    self.pending = now+self.latency, decisions[0]
                    decision = decisions[0]
                    signal = decision.signal
                    self.signals.append(dict(time=now, symbol=signal.symbol, strategy=signal.strategy,
                                             regime=signal.regime, reason="SHADOW_CANDIDATE",
                                             confidence=signal.confidence, edge_proxy=decision.net_edge_proxy,
                                             slippage=decision.slippage, risk=decision.plan.equity_risk,
                                             stop=decision.plan.stop, model=decision.model, config=self.config_hash))
            self.sample_equity()

    def correlations(self, now):
        closed_returns = {}
        for symbol, state in self.states.items():
            rows = [r for r in state.candles if r[0]+60000 <= now]
            closed_returns[symbol] = {b[0]: b[4]/a[4]-1 for a, b in zip(rows, rows[1:])}
        return correlations(closed_returns)

    def mark_equity(self):
        return self.balance+sum((self.last_prices.get(s, p.exit.entry)-p.exit.entry)*p.amount
                                * (1 if p.exit.side == "buy" else -1) for s, p in self.open.items())

    def sample_equity(self):
        equity = self.mark_equity()
        self.high_water = max(self.high_water, equity)
        self.equity.append(equity)

    def fill(self, now):
        due, decision = self.pending
        cfg, signal = self.engine.cfg, decision.signal
        state = self.states[signal.symbol]
        book = state.book
        if now-signal.asof_ms > cfg.max_age_ms:
            self.pending = None
            self.rejected_fill(now, signal, "EXPIRED_BEFORE_FILL")
            return
        if not book.fresh(now, cfg.max_age_ms) or book.received_ms < due:
            return
        self.pending = None
        if signal.symbol in self.open or state.features(now, cfg) is None:
            self.rejected_fill(now, signal, "STALE_MARKET_DATA_AT_FILL")
            return
        sign = 1 if signal.side == "buy" else -1
        instrument = self.engine.instruments[signal.symbol]
        limit = (min(book.quote.mid, signal.reference) if sign > 0 else
                 max(book.quote.mid, signal.reference))*(1+sign*cfg.max_slippage)
        fill = self.liquidity.quote(signal.symbol, book, signal.side, decision.plan.quantity,
                                    instrument.step, self.stress, partial=self.experiment.partial_fills,
                                    limit=limit)
        if fill is None:
            self.rejected_fill(now, signal, "INSUFFICIENT_DEPTH_WITHIN_SLIPPAGE_LIMIT")
            return
        qty, price = fill.quantity, fill.price
        realized_slippage = sign*(price/signal.reference-1)
        fee = price*qty*cfg.fee
        worst_exit = decision.plan.stop*(1-sign*cfg.max_slippage)
        loss = qty*(sign*(price-worst_exit)+cfg.fee*(price+worst_exit))
        if (sign*(price-decision.plan.stop) <= 0 or sign*(decision.plan.target-price) <= 0
                or loss > decision.plan.stop_risk * (qty/decision.plan.quantity)
                or price*qty < instrument.min_notional):
            self.rejected_fill(now, signal, "FILL_RISK_OR_MINIMUM_NOTIONAL")
            return
        equity = self.mark_equity()
        multiplier = risk_multiplier(equity, self.high_water, cfg)
        if loss > equity*cfg.risk_fraction*multiplier:
            self.rejected_fill(now, signal, "RISK_BUDGET_CHANGED_AT_FILL")
            return
        if not validate_stop_against_liq(signal.side, price, worst_exit, cfg.leverage,
                                        cfg.mmr, cfg.liquidation_buffer, 0., qty):
            self.rejected_fill(now, signal, "LIQUIDATION_BUFFER_AT_FILL")
            return
        exposure = replace(decision.exposure, notional=price*qty, stop_risk=loss)
        current = [p.decision.exposure for p in self.open.values()]
        if reason := check(current, exposure, equity, self.high_water, self.correlations(now), cfg):
            self.rejected_fill(now, signal, reason)
            return
        used = sum(p.amount*p.exit.entry/cfg.leverage for p in self.open.values())
        if used+price*qty/cfg.leverage+fee > self.balance:
            self.rejected_fill(now, signal, "INSUFFICIENT_MARGIN_AT_FILL")
            return
        decision = replace(decision, exposure=exposure)
        self.balance -= fee
        self.liquidity.commit(signal.symbol, signal.side, fill)
        self.partial_entries += qty < decision.plan.quantity
        self.open[signal.symbol] = OpenPosition(decision, self.exit_type(signal.side, price, decision.plan.stop,
                                                              decision.plan.target, now),
                                                qty, fee, realized_slippage, initial_amount=qty)

    def rejected_fill(self, now, signal, reason):
        self.signals.append(dict(time=now, symbol=signal.symbol, strategy=signal.strategy,
                                 reason=reason, config=self.config_hash))

    def funding(self, symbol, now, data):
        ts, rate, mark = int(data["settled_ms"]), float(data["rate"]), float(data["mark"])
        if ts != now or not isfinite(rate) or not isfinite(mark) or mark <= 0:
            raise ValueError("Invalid funding settlement")
        key = symbol, ts
        if key in self.funding_keys:
            return
        self.funding_keys.add(key)
        self.funding_events += 1
        pos = self.open.get(symbol)
        if pos and ts > pos.exit.opened_ms:
            charge = (1 if pos.exit.side == "buy" else -1)*pos.amount*mark*rate
            self.balance -= charge
            pos.funding += charge

    def manage(self, symbol, now):
        if symbol not in self.open:
            return
        p, state, cfg = self.open[symbol], self.states[symbol], self.engine.cfg
        f = state.features(now, cfg)
        book = state.book
        side = "sell" if p.exit.side == "buy" else "buy"
        sign = 1 if side == "buy" else -1
        best = (book.quote.ask if sign > 0 else book.quote.bid)*(1+sign*self.stress)
        step = self.engine.instruments[symbol].step
        experiment = self.experiment
        reason = p.exit_reason or p.exit.update(
            best, f.atr if f else p.decision.signal.reference*cfg.min_stop,
            f.trend if f else 0., book.quote.spread, cfg, now_ms=now,
            timeout_ms=experiment.thesis_timeout_ms,
            thesis_lost=f is not None and classify(f, cfg) != p.decision.signal.regime,
        )
        if reason:
            p.exit_reason = reason
            requested, limit = p.amount, None
        else:
            scale_price = p.exit.entry-sign*abs(p.exit.entry-p.decision.plan.stop)*experiment.scale_out_r
            scale_quantity = floor_quantity(p.initial_amount*experiment.scale_out_fraction, step)
            requested = min(p.amount, max(0., scale_quantity-p.scaled_quantity))
            if requested <= step*1e-8 or sign*(best-scale_price) > 0:
                return
            reason, limit = "SCALE_OUT", scale_price
        fill = self.liquidity.quote(symbol, book, side, requested, step, self.stress,
                                    partial=experiment.partial_fills, limit=limit)
        if fill is None:
            return
        self.liquidity.commit(symbol, side, fill)
        quantity, price = fill.quantity, fill.price
        fraction = quantity/p.amount
        entry_fee, funding = fraction*p.entry_fee, fraction*p.funding
        raw = -sign*(price-p.exit.entry)*quantity
        fee = price*quantity*cfg.fee
        net = raw-entry_fee-fee-funding
        self.balance += raw-fee
        p.closed_pnl += net
        p.total_fees += entry_fee+fee
        p.total_funding += funding
        p.exit_value += quantity*price
        if reason == "SCALE_OUT":
            p.scaled_quantity += quantity
        self.exit_fill_count += 1
        self.exit_fills.append(dict(symbol=symbol, time=now, amount=quantity, price=price,
                                    reason=reason, fees=entry_fee+fee, funding=funding, net_pnl=net))
        if not isclose(quantity, p.amount, rel_tol=1e-12, abs_tol=step*1e-8):
            p.amount -= quantity
            p.entry_fee -= entry_fee
            p.funding -= funding
            exposure = p.decision.exposure
            p.decision = replace(p.decision, exposure=replace(
                exposure, notional=p.amount*p.exit.entry, stop_risk=exposure.stop_risk*(1-fraction)))
            return
        self.trades.append(dict(symbol=symbol, side=p.exit.side, strategy=p.decision.signal.strategy,
                                regime=p.decision.signal.regime, opened_ms=p.exit.opened_ms, closed_ms=now,
                                amount=p.initial_amount, entry=p.exit.entry, exit=p.exit_value/p.initial_amount,
                                reason=reason, slippage=p.slippage, stop=p.exit.stop,
                                fees=p.total_fees, funding=p.total_funding, net_pnl=p.closed_pnl))
        del self.open[symbol]

    def report(self):
        cfg = self.engine.cfg
        elapsed = 0 if self.clock is None else self.clock-self.started
        feeds = {symbol: dict(
            depth_age_ms=self.clock-state.book.event_ms if state.book.event_ms is not None else None,
            trade_age_ms=self.clock-state.trades[-1][0] if state.trades else None,
            book_synced=state.book.synced,
            data_ready=state.features(self.clock, cfg) is not None,
        ) for symbol, state in self.states.items()}
        return dict(fidelity="public_event_shadow", config=self.config_hash, missing_events=self.missing,
                    safety_state="ready" if feeds and all(x["data_ready"] for x in feeds.values()) else "degraded",
                    feed_health=feeds, model_version=self.engine.model.version if self.engine.model else None,
                    funding_settlements=self.funding_events, elapsed_ms=elapsed,
                    latency_ms=self.latency, stress_slippage=self.stress,
                    execution_experiment=vars(self.experiment), partial_entries=self.partial_entries,
                    exit_fill_count=self.exit_fill_count,
                    initial_equity=self.initial, final_equity=self.mark_equity(), wallet=self.balance,
                    open_positions=[dict(symbol=s, side=p.exit.side, amount=p.amount, entry=p.exit.entry,
                                         stop=p.exit.stop, target=p.exit.target, opened_ms=p.exit.opened_ms,
                                         risk=p.decision.exposure.stop_risk, exit_pending=p.exit_reason, strategy=p.decision.signal.strategy,
                                         regime=p.decision.signal.regime) for s, p in self.open.items()],
                    exposure_fraction=self.exposure_ms/elapsed if elapsed else None,
                    trades_per_day=len(self.trades)*86400000/elapsed if elapsed else None,
                    aggregate_stop_risk=sum(p.decision.exposure.stop_risk for p in self.open.values()),
                    gross_exposure=sum(p.decision.exposure.notional for p in self.open.values()),
                    unrealized_pnl=self.mark_equity()-self.balance,
                    realized_pnl_including_partial_exits=sum(fill["net_pnl"] for fill in self.exit_fills),
                    metrics=summary(self.trades, self.equity),
                    by_strategy=self.grouped("strategy"), by_regime=self.grouped("regime"),
                    expectancy_ci=(block_bootstrap([t["net_pnl"] for t in self.trades])
                                   if len(self.trades) >= 30 else None))

    def grouped(self, key):
        groups = {}
        for trade in self.trades:
            group = groups.setdefault(trade[key], dict(count=0, net_pnl=0.))
            group["count"] += 1
            group["net_pnl"] += trade["net_pnl"]
        return groups


def export(replay, destination):
    destination.mkdir(parents=True, exist_ok=False)
    (destination/"report.json").write_text(json.dumps(replay.report(), indent=2, allow_nan=False)+"\n")
    for name in ("trades", "signals", "exit_fills"):
        rows = getattr(replay, name)
        (destination/f"{name}.json").write_text(json.dumps(rows, indent=2, allow_nan=False)+"\n")
        with (destination/f"{name}.csv").open("w", newline="") as stream:
            columns = sorted({key for row in rows for key in row})
            if columns:
                writer = csv.DictWriter(stream, columns)
                writer.writeheader()
                writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("instruments", type=Path, help="Point-in-time symbol to step/min_notional JSON map")
    parser.add_argument("destination", type=Path)
    parser.add_argument("--config", type=Path, default=Path("apex.json"))
    parser.add_argument("--latency-ms", type=int, default=100)
    parser.add_argument("--stress-slippage", type=float, default=0.)
    parser.add_argument("--partial-fills", action="store_true")
    parser.add_argument("--scale-out-fraction", type=float, default=0.)
    parser.add_argument("--scale-out-r", type=float, default=1.)
    parser.add_argument("--thesis-timeout-ms", type=int)
    parser.add_argument("--model", type=Path)
    parser.add_argument("--approval", type=Path)
    args = parser.parse_args()
    if bool(args.model) != bool(args.approval):
        parser.error("Model and approval are required together")
    cfg = Config.load(args.config)
    instruments = {s: Instrument(**rules) for s, rules in json.loads(args.instruments.read_text()).items()}
    model = Model.load_approved(args.model, args.approval, cfg.fingerprint) if args.model else None
    simulation = Replay(Engine(cfg, instruments, model), latency_ms=args.latency_ms,
                        stress_slippage=args.stress_slippage,
                        experiment=Experiment(args.partial_fills, args.scale_out_fraction,
                                              args.scale_out_r, args.thesis_timeout_ms))
    with args.capture.open() as source:
        for line in source:
            if line.strip():
                simulation.apply(json.loads(line))
    export(simulation, args.destination)


if __name__ == "__main__":
    main()
