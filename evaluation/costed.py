"""Compare a Champion signal/exit proxy and Apex under shared research constraints."""
import argparse
from dataclasses import replace
import json
from math import isfinite
from pathlib import Path

from apex.config import Config
from apex.engine import Decision, Engine, Instrument
from models.meta import Model
from portfolio.limits import Exposure, check, risk_multiplier
from regime.classifier import classify
from research.replay import Replay
from risk.apex_exit import Exit
from risk.apex_sizing import plan
from strategies.signals import Signal
from .paired import CandleObservations

CHAMPION_STOP = .08
CHAMPION_ARM = .015
CHAMPION_TRAIL = .006


class ChampionExit(Exit):
    def __init__(self, side, entry, stop, target, opened_ms):
        super().__init__(side, entry, stop, None, opened_ms, entry)

    def update(self, price, atr, trend, spread, cfg, **context):
        sign = 1 if self.side == "buy" else -1
        if sign*(price-self.stop) <= 0:
            return "CHAMPION_STOP"
        self.extreme = max(self.extreme, price) if sign > 0 else min(self.extreme, price)
        if sign*(self.extreme/self.entry-1) >= CHAMPION_ARM:
            candidate = self.extreme*(1-sign*CHAMPION_TRAIL)
            if sign*(candidate-self.stop) > 0:
                self.stop = candidate
        return None


class ChampionProxy(Engine):
    def __init__(self, cfg, instruments):
        super().__init__(cfg, instruments)
        self.observations = CandleObservations(cfg)
        self.sizing = replace(cfg, min_stop=CHAMPION_STOP)

    def decide(self, states, now, equity, free, high_water, positions, correlated):
        cfg = self.cfg
        decisions, rejected = [], []
        multiplier = risk_multiplier(equity, high_water, cfg)
        for symbol, ts in sorted(self.observations.events.items(), key=lambda item: (item[1], item[0])):
            state = states.get(symbol)
            instrument = self.instruments.get(symbol)
            if state is None or instrument is None:
                continue
            f = state.features(now, cfg)
            if f is None or not (side := self.observations.side(symbol, state.book, now)):
                continue
            signal = Signal(symbol, side, "champion_proxy", ts, f.price, classify(f, cfg),
                            1., 0., f.price)
            try:
                order = plan(signal, replace(f, atr=0), self.sizing, equity, free, instrument.step,
                             instrument.min_notional, cfg.max_slippage, multiplier)
                slippage = state.book.impact(side, order.quantity)
                if not isfinite(slippage) or slippage > cfg.max_slippage:
                    raise ValueError("EXCESSIVE_SLIPPAGE")
                exposure = Exposure(symbol, side, order.notional, order.stop_risk, f.long_vol)
                if reason := check(positions, exposure, equity, high_water, correlated, cfg):
                    raise ValueError(reason)
                decisions.append(Decision(signal, order, exposure, None, slippage, None))
            except ValueError as error:
                rejected.append((symbol, str(error)))
        return decisions, rejected


def compare(rows, cfg, instruments, *, latency_ms=100, stress_slippage=0., equity=100., model=None):
    champion = ChampionProxy(cfg, instruments)
    a = Replay(champion, exit_type=ChampionExit, equity=equity, latency_ms=latency_ms, stress_slippage=stress_slippage)
    b = Replay(Engine(cfg, instruments), equity=equity, latency_ms=latency_ms, stress_slippage=stress_slippage)
    ai = Replay(Engine(cfg, instruments, model), equity=equity, latency_ms=latency_ms,
                stress_slippage=stress_slippage) if model is not None else None
    count = 0
    for row in rows:
        champion.observations.observe(row)
        a.apply(row)
        b.apply(row)
        if ai is not None:
            ai.apply(row)
        count += 1
    baseline, challenger = a.report(), b.report()
    return dict(
        fidelity="costed_champion_proxy_vs_apex", source_events=count,
        champion_stop_fraction=CHAMPION_STOP, champion_trailing_arm=CHAMPION_ARM,
        champion_trailing_distance=CHAMPION_TRAIL,
        comparison_scope="Shared data, sizing budget, latency, depth and cost model; Champion is a proxy",
        closed_net_difference=challenger["metrics"]["net_pnl"]-baseline["metrics"]["net_pnl"],
        marked_equity_difference=b.mark_equity()-a.mark_equity(),
        promotion_eligible=False, champion=baseline, challenger=challenger,
        apex_ai_candidate=ai.report() if ai is not None else None,
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("capture", type=Path)
    p.add_argument("instruments", type=Path)
    p.add_argument("output", type=Path)
    p.add_argument("--config", type=Path, default=Path("apex.json"))
    p.add_argument("--initial-equity", type=float, default=100.)
    p.add_argument("--latency-ms", type=int, default=100)
    p.add_argument("--stress-slippage", type=float, default=0.)
    p.add_argument("--model", type=Path)
    p.add_argument("--approval", type=Path)
    args = p.parse_args()
    if bool(args.model) != bool(args.approval):
        p.error("--model and --approval must be supplied together")
    cfg = Config.load(args.config)
    instruments = {s: Instrument(**v) for s, v in json.loads(args.instruments.read_text()).items()}
    model = Model.load_approved(args.model, args.approval, cfg.fingerprint) if args.model else None
    with args.capture.open() as tape:
        result = compare((json.loads(line) for line in tape if line.strip()), cfg, instruments,
                         latency_ms=args.latency_ms, stress_slippage=args.stress_slippage,
                         equity=args.initial_equity, model=model)
    with args.output.open("x") as f:
        json.dump(result, f, indent=2, allow_nan=False)


if __name__ == "__main__":
    main()
