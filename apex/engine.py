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
    min_qty: float = 0.
    max_qty: float | None = None

    def __post_init__(self):
        if (not all(isfinite(v) for v in (self.step, self.min_notional, self.min_qty))
                or self.step <= 0 or min(self.min_notional, self.min_qty) < 0
                or self.max_qty is not None and (not isfinite(self.max_qty) or self.max_qty < max(self.step, self.min_qty))):
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
    score_kind: str = "HEURISTIC_SCORE"
    data_provenance: dict | None = None


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
                if signal.strategy not in cfg.enabled_strategies:
                    rejected.append((symbol, "STRATEGY_DISABLED:"+signal.strategy))
                    continue
                try:
                    instrument = self.instruments[symbol]
                    order = plan(signal, f, cfg, equity, free, instrument.step,
                                 instrument.min_notional, cfg.max_slippage, multiplier,
                                 min_qty=instrument.min_qty, max_qty=instrument.max_qty)
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
                        raise ValueError("NONPOSITIVE_STRATEGY_SCORE")
                    exposure = Exposure(symbol, signal.side, order.notional, order.stop_risk, f.long_vol)
                    if reason := check(positions, exposure, equity, high_water, correlated, cfg):
                        raise ValueError(reason)
                    corr = max((max(0., v if p.side == signal.side else -v)
                                for p in positions
                                if (v := correlated.get(tuple(sorted((symbol, p.symbol))))) is not None), default=0.)
                    candidate = Candidate(signal, edge, slippage, abs(order.stop-f.price)/f.price, corr, f.depth_usdt)
                    proposals.append((candidate, Decision(signal, order, exposure, edge, slippage,
                                                          self.model.version if self.model else None, inputs,
                                                          ("CALIBRATED_MODEL_SCORE" if getattr(self.model, "calibrated", False)
                                                           else "MODEL_RESEARCH_SCORE") if self.model else "HEURISTIC_SCORE",
                                                          dict(event_ms=f.observed_ms, book_received_ms=state.book.received_ms,
                                                               available_ms=now, funding_missing=f.funding is None,
                                                               oi_missing=f.oi_change is None, btc_missing=f.btc_momentum is None))))
                except ValueError as error:
                    rejected.append((symbol, str(error)))
        indexed = {candidate: decision for candidate, decision in proposals}
        return [indexed[c] for c in rank(indexed, now, cfg)], rejected
