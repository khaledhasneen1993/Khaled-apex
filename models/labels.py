"""Label an executable-price barrier path after the decision; gaps are censored."""
from math import isfinite

from .train import Sample


def label(decision_ms, features_ms, row, path, side, target, stop, horizon_ms, max_gap_ms):
    if (side not in ("buy", "sell") or horizon_ms <= 0 or max_gap_ms <= 0
            or (side == "buy" and not stop < target) or (side == "sell" and not target < stop)):
        raise ValueError("Invalid label parameters")
    last = decision_ms
    sign = 1 if side == "buy" else -1
    end = decision_ms + horizon_ms
    for ts, executable_price in path:
        if not isfinite(executable_price) or executable_price <= 0:
            raise ValueError("Invalid executable price")
        if ts <= last:
            raise ValueError("Label path must follow the decision in strict order")
        if ts-last > max_gap_ms:
            return None
        if ts > end:
            return Sample(decision_ms, features_ms, ts, row, "timeout")
        if sign*(executable_price-stop) <= 0:
            return Sample(decision_ms, features_ms, ts, row, "stop")
        if sign*(executable_price-target) >= 0:
            return Sample(decision_ms, features_ms, ts, row, "target")
        if ts == end:
            return Sample(decision_ms, features_ms, ts, row, "timeout")
        last = ts
    return None
