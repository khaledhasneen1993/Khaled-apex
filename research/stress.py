"""Replay identical inputs across explicit delay, slippage and sizing assumptions."""
import argparse
from dataclasses import replace
import json
from pathlib import Path

from apex.config import Config
from apex.engine import Engine, Instrument
from .replay import Replay


def sweep(capture, instruments, cfg, latencies, slips, risk_fractions):
    for risk in risk_fractions:
        for latency in latencies:
            for slip in slips:
                scenario = Replay(Engine(replace(cfg, risk_fraction=risk), instruments),
                                  latency_ms=latency, stress_slippage=slip)
                with capture.open() as rows:
                    for row in rows:
                        if row.strip():
                            scenario.apply(json.loads(row))
                yield {"risk_fraction": risk, **scenario.report()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("instruments", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    instruments = {s: Instrument(**v) for s, v in json.loads(args.instruments.read_text()).items()}
    results = sweep(args.capture, instruments, Config.load("apex.json"), (50, 100, 300),
                    (0., .0005), (.0025, .005))
    with args.output.open("x") as destination:
        json.dump(list(results), destination, indent=2, allow_nan=False)


if __name__ == "__main__":
    main()
