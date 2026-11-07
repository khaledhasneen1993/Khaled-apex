"""Deterministic research/shadow decisions. Never holds an exchange client."""
from dataclasses import dataclass
from math import isfinite

from models.meta import vector
from portfolio.limits import Exposure, check, risk_multiplier
from regime.classifier import classify
from risk.apex_sizing import Plan, plan
from strategies.ranking import Candidate, rank
from strategies.signals import generate


@dataclass(frozen=True)
class Instrument:
    step: float
    min_notional: float

    def __post_init__(self):
        if not isfinite(self.step) or not isfinite(self.min_notional) or self.step <= 0 or self.min_notional < 0:
            raise ValueError("Invalid instrument filters")


@dataclass(frozen=True)
class Decision:
    signal: object
    plan: Plan
    exposure: Exposure
    net_edge_proxy: float
    slippage: float
    model: str | None
    model_inputs: tuple | None = None


class Engine:
    def __init__(self, cfg, instruments, model=None):
        self.cfg, self.instruments, self.model = cfg, instruments, model

    def decide(self, states, now, equity, free, high_water, positions, correlated):
        cfg = self.cfg
        proposals, rejected = [], []
        btc = states["BTCUSDT"].features(now, cfg) if "BTCUSDT" in states else None
        multiplier = risk_multiplier(equity, high_water, cfg)
        for symbol, state in states.items():
            f = state.features(now, cfg, btc.momentum if btc else None)
            regime = classify(f, cfg)
            if f is None:
                rejected.append((symbol, "STALE_OR_INCOMPLETE_MARKET_DATA"))
                continue
            if regime == "liquidity_stress":
                rejected.append((symbol, "LIQUIDITY_STRESS"))
                continue
            if symbol not in self.instruments:
                rejected.append((symbol, "MISSING_INSTRUMENT_FILTERS"))
                continue
            for signal in generate(f, regime, cfg):
                try:
                    instrument = self.instruments[symbol]
                    order = plan(signal, f, cfg, equity, free, instrument.step,
                                 instrument.min_notional, cfg.max_slippage, multiplier)
                    slippage = state.book.impact(signal.side, order.quantity)
                    if not isfinite(slippage) or slippage > cfg.max_slippage:
                        raise ValueError("EXCESSIVE_SLIPPAGE")
                    exit_cost = cfg.max_slippage
                    cost = 2*cfg.fee+slippage+exit_cost+abs(f.funding or 0.)
                    edge = signal.gross_edge_proxy-cost
                    inputs = vector(f, regime, now, cost)
                    if self.model:
                        try:
                            target, stopped, _ = self.model.probabilities(inputs)
                        except (ArithmeticError, TypeError, ValueError):
                            raise ValueError("MODEL_INFERENCE_FAILED") from None
                        edge = (target*abs(order.target-f.price)-stopped*abs(order.stop-f.price))/f.price-cost
                    if edge <= 0:
                        raise ValueError("NEGATIVE_EXPECTED_EDGE")
                    exposure = Exposure(symbol, signal.side, order.notional, order.stop_risk, f.long_vol)
                    if reason := check(positions, exposure, equity, high_water, correlated, cfg):
                        raise ValueError(reason)
                    corr = max((max(0., v if p.side == signal.side else -v)
                                for p in positions
                                if (v := correlated.get(tuple(sorted((symbol, p.symbol))))) is not None), default=0.)
                    candidate = Candidate(signal, edge, slippage, abs(order.stop-f.price)/f.price, corr, f.depth_usdt)
                    proposals.append((candidate, Decision(signal, order, exposure, edge, slippage,
                                                          self.model.version if self.model else None, inputs)))
                except ValueError as error:
                    rejected.append((symbol, str(error)))
        indexed = {candidate: decision for candidate, decision in proposals}
        return [indexed[c] for c in rank(indexed, now, cfg)], rejected
