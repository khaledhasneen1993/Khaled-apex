"""OHLCV-only exit sensitivity on a Cascade prefilter proxy.

Orders are priced with an explicit spread/slippage assumption. No book, CVD,
observed spread or funding data can be inferred from these candles.
"""
import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from math import isfinite
from pathlib import Path

from .statistics import summary


@dataclass(frozen=True)
class Bar:
    start: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def bars(path):
    values = []
    with path.open() as source:
        for row in csv.reader(source):
            if not row or row[0] == "open_time":
                continue
            bar = Bar(int(row[0]), *(float(x) for x in row[1:6]))
            if (bar.start % 60000 or not all(isfinite(x) for x in
                    (bar.open, bar.high, bar.low, bar.close, bar.volume))
                    or not 0 < bar.low <= min(bar.open, bar.close)
                    or not bar.high >= max(bar.open, bar.close) or bar.volume < 0):
                raise ValueError("Invalid OHLCV bar")
            if values and bar.start <= values[-1].start:
                raise ValueError("OHLCV timestamps are not strictly increasing")
            values.append(bar)
    if len(values) < 16:
        raise ValueError("Insufficient OHLCV history")
    return values


def segments(rows):
    start = 0
    for i in range(1, len(rows)):
        if rows[i].start-rows[i-1].start != 60000:
            yield rows[start:i]
            start = i
    yield rows[start:]


def prefilter(rows, i, momentum=.012, spike=3.5):
    bar = rows[i]
    reference = rows[i-2].close
    average = sum(r.volume for r in rows[i-14:i-2])/12
    change = bar.close/reference-1
    if average > 0 and bar.volume/average > spike and abs(change) > momentum:
        return 1 if change > 0 else -1
    return 0


def simulate(rows, *, stop_pct, from_ms, fee=.0005, slippage=.0005, notional=100.):
    if not 0 < stop_pct < 1 or not 0 <= slippage < .1 or not 0 <= fee < .1 or notional <= 0:
        raise ValueError("Invalid proxy assumptions")
    wallet = initial = 100.
    history, trades = [wallet], []
    current, next_side = None, 0
    candidates = 0
    for i, bar in enumerate(rows):
        if i < 14 or bar.start < from_ms:
            continue
        if current is None and next_side:
            side = next_side
            entry = bar.open*(1+side*slippage)
            size = notional/entry
            current = (side, entry, size, entry*(1-side*stop_pct), entry, bar.start)
            wallet -= notional*fee
        next_side = 0
        if current:
            side, entry, size, stop, extreme, opened = current
            adverse = bar.low if side > 0 else bar.high
            if side*(adverse-stop) <= 0:
                trigger = min(bar.open, stop) if side > 0 else max(bar.open, stop)
                exit_price = trigger*(1-side*slippage)
                profit = side*(exit_price-entry)*size
                exit_fee = exit_price*size*fee
                wallet += profit-exit_fee
                trades.append(dict(net_pnl=profit-exit_fee-notional*fee, opened_ms=opened,
                                   closed_bar_ms=bar.start, slippage=slippage))
                current = None
            else:
                extreme = max(extreme, bar.high) if side > 0 else min(extreme, bar.low)
                if side*(extreme/entry-1) >= .015:
                    candidate = extreme*(1-side*.006)
                    stop = max(stop, candidate) if side > 0 else min(stop, candidate)
                current = (side, entry, size, stop, extreme, opened)
        proposed = prefilter(rows, i)
        candidates += bool(proposed)
        if current is None:
            next_side = proposed
        marked = wallet if not current else wallet + side*(bar.close-entry)*size
        history.append(marked)
    return dict(candidate_bars=candidates, end_open=current is not None,
                modeled_wallet=wallet, marked_equity=history[-1], metrics=summary(trades, history),
                assumptions=dict(initial_equity=initial, stop_pct=stop_pct, trail_arm=.015,
                                 trail_distance=.006, fee=fee, slippage_per_fill=slippage,
                                 notional=notional, funding="unavailable", fill_timing="next_bar_open"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    start = int(datetime.fromisoformat(args.from_date).replace(tzinfo=timezone.utc).timestamp()*1000)
    result = {"fidelity": "ohlcv_prefilter_proxy", "from_utc": args.from_date, "markets": {}}
    for path in args.inputs:
        rows = bars(path)
        def iso(ts):
            return datetime.fromtimestamp(ts/1000, timezone.utc).isoformat()
        parts = list(segments(rows))
        gaps = [{"after": iso(a[-1].start), "before": iso(b[0].start),
                 "missing_bars": (b[0].start-a[-1].start)//60000-1}
                for a, b in zip(parts, parts[1:])]
        result["markets"][path.stem] = dict(candles=len(rows), gaps=gaps, segments=[
            dict(start=iso(max(start, part[0].start)), end=iso(part[-1].start), variants=[
                simulate(part, stop_pct=distance, from_ms=start) for distance in (.08, .04, .02)])
            for part in parts if part[-1].start >= start and len(part) >= 16])
    with args.output.open("x") as target:
        json.dump(result, target, indent=2, allow_nan=False)
        target.write("\n")


if __name__ == "__main__":
    main()
