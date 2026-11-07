"""Portable linear meta-label inference with explicit artifact approval."""
from dataclasses import dataclass
from hashlib import sha256
import json
from math import exp, isfinite
from pathlib import Path

LABELS = ("target", "stop", "timeout")
FEATURES = ("momentum", "trend", "short_vol", "long_vol", "spike", "flow", "ofi",
            "absorption", "spread", "depth_usdt", "funding", "oi_change", "btc_momentum",
            "age", "cost", "up", "down", "range", "funding_missing", "oi_missing", "btc_missing")


def vector(f, regime, now_ms, cost):
    values = tuple(getattr(f, key) for key in FEATURES[:13])
    age = (now_ms-f.observed_ms)/1000
    if age < 0 or f.asof_ms > now_ms:
        raise ValueError("Future feature observation")
    return (*[0. if v is None else v for v in values], age, cost,
            float(regime == "trending_up"), float(regime == "trending_down"),
            float(regime == "ranging"), *[float(v is None) for v in values[-3:]])


def softmax(scores, temperature=1.):
    maximum = max(scores)
    values = [exp((x-maximum)/temperature) for x in scores]
    return tuple(x/sum(values) for x in values)


@dataclass(frozen=True)
class Model:
    version: str
    means: list
    scales: list
    weights: list
    temperature: float

    def __post_init__(self):
        n = len(FEATURES)
        if (not self.version or len(self.means) != n or len(self.scales) != n
                or len(self.weights) != len(LABELS) or any(len(w) != n+1 for w in self.weights)
                or not all(isfinite(v) for v in [*self.means, *self.scales, self.temperature,
                                                 *(v for row in self.weights for v in row)])
                or min(self.scales) <= 0 or not .1 <= self.temperature <= 10.):
            raise ValueError("Invalid model schema")

    def probabilities(self, row):
        if len(row) != len(FEATURES) or not all(isfinite(v) for v in row):
            raise ValueError("Invalid model inputs")
        x = [(v-mean)/scale for v, mean, scale in zip(row, self.means, self.scales)]
        scores = [w[-1]+sum(a*b for a, b in zip(x, w)) for w in self.weights]
        if not all(isfinite(v) for v in scores):
            raise ValueError("Invalid model scores")
        return softmax(scores, self.temperature)

    @classmethod
    def load_approved(cls, path, approval_path, config_hash):
        content = Path(path).read_bytes()
        approval = json.loads(Path(approval_path).read_text())
        if (approval["artifact_sha256"] != sha256(content).hexdigest()
                or approval["config_sha256"] != config_hash
                or approval["scope"] != "shadow" or not approval["approved_by"]):
            raise ValueError("Model approval does not match artifact or configuration")
        envelope = json.loads(content)
        if envelope["features"] != list(FEATURES) or envelope["labels"] != list(LABELS):
            raise ValueError("Model schema mismatch")
        return cls(**envelope["model"])
