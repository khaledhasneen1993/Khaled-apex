"""Measure offline feature/decision latency on a ready, fixed recorded-data fixture."""
import argparse
import json
from pathlib import Path
from statistics import median
from time import perf_counter_ns

from apex.config import Config
from apex.engine import Engine, Instrument
from research.replay import Replay


def run(capture, instruments, cfg, repetitions):
    if repetitions < 10:
        raise ValueError("At least 10 observations required")
    engine = Engine(cfg, instruments)
    replay = Replay(engine)
    with capture.open() as events:
        for line in events:
            replay.apply(json.loads(line))
    ready = sum(state.features(replay.clock, cfg) is not None for state in replay.states.values())
    if not ready:
        raise ValueError("Fixture must end with at least one ready symbol")
    correlations = replay.correlations(replay.clock)
    elapsed = []
    for _ in range(repetitions):
        before = perf_counter_ns()
        engine.decide(replay.states, replay.clock, 100, 100, 100, [], correlations)
        elapsed.append((perf_counter_ns()-before)/1_000_000)
    elapsed.sort()
    return dict(ready_symbols=ready, repetitions=repetitions, median_ms=median(elapsed),
                p95_ms=elapsed[int(.95*(repetitions-1))], max_ms=elapsed[-1], config=cfg.fingerprint)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("capture", type=Path)
    p.add_argument("instruments", type=Path)
    p.add_argument("--repetitions", type=int, default=500)
    args = p.parse_args()
    instruments = {k: Instrument(**v) for k, v in json.loads(args.instruments.read_text()).items()}
    print(json.dumps(run(args.capture, instruments, Config.load("apex.json"), args.repetitions), indent=2))


if __name__ == "__main__":
    main()
