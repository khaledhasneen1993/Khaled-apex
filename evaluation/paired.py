"""Paired signal diagnostic on one event tape, without inferring Champion fills."""
import argparse
from collections import Counter, deque
import json
from pathlib import Path

from apex.config import Config
from apex.engine import Engine
from regime.classifier import classify
from research.replay import Replay
from strategies.signals import generate


def champion_prefilter(rows, book, now, age, *, movement=.012, volume=3.5, imbalance=.35):
    if len(rows) < 15 or not book.fresh(now, age):
        return None
    change = rows[-1][4]/rows[-3][4]-1
    reference = sum(r[5] for r in rows[-15:-3])/12
    if reference <= 0 or abs(change) <= movement or rows[-1][5]/reference <= volume:
        return None
    bids = sum(q for _, q in sorted(book.bids.items(), reverse=True)[:20])
    asks = sum(q for _, q in sorted(book.asks.items())[:20])
    flow = (bids-asks)/(bids+asks)
    if change > 0 and flow > imbalance:
        return "buy"
    if change < 0 and flow < -imbalance:
        return "sell"
    return None


class PairedDiagnostic:
    def __init__(self, cfg):
        self.cfg = cfg
        self.replay = Replay(Engine(cfg, {}))
        self.candles, self.events = {}, {}
        self.counts = Counter()
        self.next_window = None

    def apply(self, row):
        self.replay.apply(row)
        sym, now, kind, data = (row[k] for k in ("symbol", "received_ms", "kind", "data"))
        rows = self.candles.setdefault(sym, deque(maxlen=30))
        if kind == "reset_market":
            rows.clear()
            self.events.pop(sym, None)
        elif kind == "candles":
            rows.clear()
            rows.extend(row[:6] for row in data[-30:] if int(row[0])+60000 <= now)
        elif kind == "event" and data.get("e") == "kline":
            k = data["k"]
            ts, event = int(k["t"]), int(data["E"])
            if k["i"] == "1m" and 0 <= now-event <= self.cfg.max_age_ms:
                current = [ts, *(float(k[key]) for key in ("o", "h", "l", "c", "v"))]
                if not rows or ts == int(rows[-1][0])+60000:
                    rows.append(current)
                    self.events[sym] = event
                elif ts == int(rows[-1][0]):
                    rows[-1] = current
                    self.events[sym] = event
                elif ts > int(rows[-1][0])+60000:
                    rows.clear()
                    self.events.pop(sym, None)
        if self.next_window is None:
            self.next_window = now+self.cfg.rank_window_ms
        if now < self.next_window:
            return
        self.next_window = now+self.cfg.rank_window_ms
        cfg = self.cfg
        for symbol, state in self.replay.states.items():
            f = state.features(now, cfg)
            if f is None or not 0 <= now-self.events.get(symbol, -10**18) <= cfg.max_age_ms:
                continue
            history = [list(map(float, r)) for r in self.candles[symbol]]
            old = champion_prefilter(history, state.book, now, cfg.max_age_ms)
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
