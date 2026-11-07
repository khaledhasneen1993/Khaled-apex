"""Paired signal diagnostic on one event tape, without inferring Champion fills."""
import argparse
from collections import Counter, deque
from math import isfinite
import json
from pathlib import Path

from apex.config import Config
from apex.engine import Engine
from regime.classifier import classify
from research.replay import Replay
from strategies.signals import generate
from strategies.legacy import cascade_signal, imbalance_matches


def champion_prefilter(rows, book, now, age, *, movement=.012, volume=3.5, imbalance=.35):
    if len(rows) < 15 or not book.fresh(now, age):
        return None
    signal = cascade_signal(rows, movement, volume)
    if signal is None:
        return None
    bids = sum(q for _, q in sorted(book.bids.items(), reverse=True)[:20])
    asks = sum(q for _, q in sorted(book.asks.items())[:20])
    flow = (bids-asks)/(bids+asks)
    return signal["side"] if imbalance_matches(signal["side"], flow, imbalance) else None


class CandleObservations:
    def __init__(self, cfg):
        self.cfg = cfg
        self.candles, self.events = {}, {}

    def observe(self, row):
        sym, now, kind, data = (row[k] for k in ("symbol", "received_ms", "kind", "data"))
        rows = self.candles.setdefault(sym, deque(maxlen=30))
        try:
            if kind == "reset_market":
                rows.clear()
                self.events.pop(sym, None)
            elif kind == "candles":
                rows.clear()
                for candle in data[-30:]:
                    if int(candle[0])+60000 <= now:
                        self.add(sym, rows, int(candle[0]), list(map(float, candle[1:6])))
            elif kind == "event" and data.get("e") == "kline":
                k = data["k"]
                ts, event = int(k["t"]), int(data["E"])
                if (data["s"] == sym and k["i"] == "1m" and ts <= event
                        and 0 <= now-event <= self.cfg.max_age_ms
                        and event > self.events.get(sym, -1)):
                    if self.add(sym, rows, ts, [float(k[key]) for key in ("o", "h", "l", "c", "v")]):
                        self.events[sym] = event
        except (KeyError, ValueError, TypeError, OverflowError):
            rows.clear()
            self.events.pop(sym, None)

    def add(self, symbol, rows, timestamp, values):
        opening, high, low, close, volume = values
        if (timestamp % 60000 or not all(isfinite(v) for v in values)
                or not 0 < low <= min(opening, close)
                or high < max(opening, close) or volume < 0):
            raise ValueError("Invalid Champion proxy candle")
        if rows and timestamp < rows[-1][0]:
            return False
        current = [timestamp, *values]
        if rows and timestamp == rows[-1][0]:
            rows[-1] = current
        else:
            if rows and timestamp != rows[-1][0]+60000:
                rows.clear()
                self.events.pop(symbol, None)
            rows.append(current)
        return True

    def side(self, symbol, book, now):
        event = self.events.get(symbol)
        if event is None or not 0 <= now-event <= self.cfg.max_age_ms:
            return None
        return champion_prefilter(list(self.candles[symbol]), book, now, self.cfg.max_age_ms)


class PairedDiagnostic:
    def __init__(self, cfg):
        self.cfg = cfg
        self.replay = Replay(Engine(cfg, {}))
        self.observations = CandleObservations(cfg)
        self.counts = Counter()
        self.next_window = None

    def apply(self, row):
        self.replay.apply(row)
        self.observations.observe(row)
        now = row["received_ms"]
        if self.next_window is None:
            self.next_window = now+self.cfg.rank_window_ms
        if now < self.next_window:
            return
        self.next_window = now+self.cfg.rank_window_ms
        cfg = self.cfg
        for symbol, state in self.replay.states.items():
            f = state.features(now, cfg)
            event = self.observations.events.get(symbol)
            if f is None or event is None or not 0 <= now-event <= cfg.max_age_ms:
                continue
            old = self.observations.side(symbol, state.book, now)
            new = {s.side for s in generate(f, classify(f, cfg), cfg)}
            self.counts["paired_symbol_windows"] += 1
            self.counts["champion_proxy_windows"] += old is not None
            self.counts["challenger_proposal_windows"] += bool(new)
            self.counts["same_direction_windows"] += old in new

    def report(self):
        return dict(fidelity="paired_signal_diagnostic", config=self.cfg.fingerprint,
                    counts=dict(self.counts), excluded="fills, PnL, execution latency and account constraints")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("--config", type=Path, default=Path("apex.json"))
    args = parser.parse_args()
    diagnostic = PairedDiagnostic(Config.load(args.config))
    with args.capture.open() as rows:
        for row in rows:
            if row.strip():
                diagnostic.apply(json.loads(row))
    print(json.dumps(diagnostic.report(), indent=2))


if __name__ == "__main__":
    main()
