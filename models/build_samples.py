"""Build chronological meta-label samples from executable prices on a captured tape."""
import argparse
from dataclasses import asdict, dataclass
import json
from math import isfinite
from pathlib import Path

from apex.config import Config
from apex.engine import Engine, Instrument
from research.replay import Replay
from .train import Sample


@dataclass
class Observation:
    decision: object
    filled_ms: int
    quantity: float
    last_price_ms: int


class SampleBuilder:
    def __init__(self, replay, horizon_ms):
        if type(horizon_ms) is not int or horizon_ms <= 0:
            raise ValueError("Invalid label horizon")
        self.replay = replay
        self.horizon_ms = horizon_ms
        self.active = {}
        self.censored = 0

    def apply(self, row):
        symbol, now = row["symbol"], row["received_ms"]
        before = self.replay.orders.pending.get(symbol)
        self.replay.apply(row)
        state = self.replay.states.get(symbol)
        if state is None:
            return None
        obs = self.active.get(symbol)
        sample = None
        if obs is not None:
            if (row["kind"] in ("reset_market", "reset_depth", "snapshot")
                    or now-obs.last_price_ms > self.replay.engine.cfg.max_age_ms):
                del self.active[symbol]
                self.censored += 1
            elif state.book.synced and state.book.received_ms == now and now > obs.last_price_ms:
                sample = self.classify(obs, state.book, now)
                if sample is not None:
                    del self.active[symbol]
        if before is not None:
            decision = before.decision
            selected = decision.signal.symbol
            position = self.replay.open.get(selected)
            if (selected not in self.active and position is not None
                    and position.exit.opened_ms == now and position.decision.model_inputs is not None
                    and position.order_id == before.id):
                self.active[selected] = Observation(decision, now, position.initial_amount, now)
        return sample

    def classify(self, obs, book, now):
        d = obs.decision
        sign = 1 if d.signal.side == "buy" else -1
        impact = book.impact("sell" if sign > 0 else "buy", obs.quantity)+self.replay.stress
        price = book.quote.mid*(1-sign*impact)
        if not isfinite(price) or price <= 0:
            return None
        obs.last_price_ms = now
        end = obs.filled_ms+self.horizon_ms
        if now > end:
            result = "timeout"
        elif sign*(price-d.plan.stop) <= 0:
            result = "stop"
        elif sign*(price-d.plan.target) >= 0:
            result = "target"
        elif now == end:
            result = "timeout"
        else:
            return None
        asof = d.signal.asof_ms
        return Sample(asof, asof, now, list(d.model_inputs), result)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("capture", type=Path)
    p.add_argument("instruments", type=Path)
    p.add_argument("output", type=Path)
    p.add_argument("--config", type=Path, default=Path("apex.json"))
    p.add_argument("--horizon-ms", type=int, required=True)
    p.add_argument("--latency-ms", type=int, default=100)
    p.add_argument("--initial-equity", type=float, default=100.)
    args = p.parse_args()
    cfg = Config.load(args.config)
    instruments = {s: Instrument(**v) for s, v in json.loads(args.instruments.read_text()).items()}
    builder = SampleBuilder(Replay(Engine(cfg, instruments), equity=args.initial_equity,
                                   latency_ms=args.latency_ms), args.horizon_ms)
    counts = dict(target=0, stop=0, timeout=0)
    with args.capture.open() as source, args.output.open("x") as destination:
        for line in source:
            if line.strip() and (sample := builder.apply(json.loads(line))) is not None:
                counts[sample.label] += 1
                destination.write(json.dumps(asdict(sample), allow_nan=False)+"\n")
    print(json.dumps(dict(counts=counts, censored=builder.censored+len(builder.active),
                          config=cfg.fingerprint, horizon_ms=args.horizon_ms, latency_ms=args.latency_ms)))


if __name__ == "__main__":
    main()
