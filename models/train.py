"""Offline three-way fit/calibration/evaluation. Never creates approval records."""
import argparse
from dataclasses import asdict, dataclass, replace
import json
from hashlib import sha256
from collections import Counter
from math import isfinite, log
from pathlib import Path
from statistics import fmean, pstdev

from .meta import FEATURES, LABELS, Model


@dataclass(frozen=True)
class Sample:
    decision_ms: int
    features_ms: int
    label_end_ms: int
    row: list
    label: str
    provenance: dict | None = None

    def __post_init__(self):
        if (not 0 <= self.features_ms <= self.decision_ms < self.label_end_ms
                or self.label not in LABELS or len(self.row) != len(FEATURES)
                or not all(isfinite(v) for v in self.row)):
            raise ValueError("Invalid point-in-time sample")
        if self.provenance is not None:
            p = self.provenance
            if (not 0 <= p["event_ms"] <= p["available_ms"] <= self.decision_ms
                    or not 0 <= p["book_received_ms"] <= p["available_ms"]):
                raise ValueError("Sample provenance exceeds decision time")


def split(samples, calibration_ms, test_ms, embargo_ms):
    if embargo_ms < 0 or calibration_ms >= test_ms:
        raise ValueError("Invalid split")
    train = [s for s in samples if s.label_end_ms < calibration_ms-embargo_ms]
    calibration = [s for s in samples if s.decision_ms >= calibration_ms
                   and s.label_end_ms < test_ms-embargo_ms]
    test = [s for s in samples if s.decision_ms >= test_ms]
    return train, calibration, test


def walk_forward(samples, boundaries, embargo_ms):
    if len(boundaries) < 3 or any(a >= b for a, b in zip(boundaries, boundaries[1:])):
        raise ValueError("Walk-forward boundaries must be increasing")
    for calibration, test, end in zip(boundaries, boundaries[1:], boundaries[2:]):
        yield split([s for s in samples if s.label_end_ms < end], calibration, test, embargo_ms)


def fit(train, version, iterations=200):
    if set(s.label for s in train) != set(LABELS):
        raise ValueError("Training requires examples of all outcomes")
    cols = list(zip(*(s.row for s in train)))
    means, scales = [fmean(col) for col in cols], [max(pstdev(col), 1e-9) for col in cols]
    width = len(FEATURES)+1
    weights = [[0.]*width for _ in LABELS]
    model = Model(version, means, scales, weights, 1.)
    rows = [([(v-m)/s for v, m, s in zip(sample.row, means, scales)]+[1.], sample)
            for sample in train]
    counts = Counter(s.label for s in train)
    class_weights = {label: len(train)/(len(LABELS)*count) for label, count in counts.items()}
    for _ in range(iterations):
        gradients = [[0.]*width for _ in LABELS]
        for row, sample in rows:
            for i, probability in enumerate(model.probabilities(sample.row)):
                error = (probability-(LABELS[i] == sample.label))*class_weights[sample.label]
                for j, value in enumerate(row):
                    gradients[i][j] += error*value
        for i, w in enumerate(weights):
            for j in range(width):
                w[j] -= .05*(gradients[i][j]/len(rows)+.01*w[j])
    return model


def calibrate(model, samples):
    if set(s.label for s in samples) != set(LABELS):
        raise ValueError("Calibration requires examples of all outcomes")
    options = [replace(model, temperature=t, calibrated=True) for t in (.25, .5, .75, 1., 1.5, 2., 3., 4.)]
    return min(options, key=lambda m: -fmean(
        log(max(1e-15, m.probabilities(s.row)[LABELS.index(s.label)])) for s in samples))


def metrics(model, test):
    if not test:
        raise ValueError("Held-out evaluation requires samples")
    probabilities = [(model.probabilities(s.row), LABELS.index(s.label)) for s in test]
    bins = {}
    for p, actual in probabilities:
        for i, value in enumerate(p):
            bins.setdefault((LABELS[i], min(9, int(value*10))), []).append((value, i == actual))
    return {
        "samples": len(test),
        "brier_multiclass": fmean(sum((v-(i == actual))**2 for i, v in enumerate(p)) for p, actual in probabilities),
        "reliability": [dict(label=label, bin=b, n=len(rows), predicted=fmean(p for p, _ in rows),
                             observed=fmean(hit for _, hit in rows)) for (label, b), rows in sorted(bins.items())],
    }


def diagnostics(model, samples):
    if not samples:
        raise ValueError("Diagnostics require holdout samples")
    return {
        "class_counts": dict(Counter(s.label for s in samples)),
        "standardized_mean_shift": {key: (fmean(s.row[i] for s in samples)-model.means[i])/model.scales[i]
                                     for i, key in enumerate(FEATURES)},
        "linear_coefficient_importance": {key: max(abs(w[i]) for w in model.weights)
                                          for i, key in enumerate(FEATURES)},
    }


def window(samples):
    return dict(count=len(samples), first_decision_ms=min(s.decision_ms for s in samples),
                last_label_ms=max(s.label_end_ms for s in samples))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("samples", type=Path)
    p.add_argument("artifact", type=Path)
    p.add_argument("--version", required=True)
    p.add_argument("--calibration-ms", required=True, type=int)
    p.add_argument("--test-ms", required=True, type=int)
    p.add_argument("--embargo-ms", required=True, type=int)
    p.add_argument("--walk-boundaries", type=int, nargs="+")
    args = p.parse_args()
    dataset = args.samples.read_bytes()
    samples = [Sample(**json.loads(line)) for line in dataset.splitlines() if line.strip()]
    train, cal, test = split(samples, args.calibration_ms, args.test_ms, args.embargo_ms)
    model = calibrate(fit(train, args.version), cal)
    report = {**metrics(model, test), **diagnostics(model, test)}
    if args.walk_boundaries is not None:
        # Keep the final held-out partition outside all walk-forward development.
        if args.walk_boundaries[-1] > args.test_ms-args.embargo_ms:
            p.error("Walk-forward development overlaps final held-out data")
        report["walk_forward"] = []
        for train_fold, cal_fold, test_fold in walk_forward(samples, args.walk_boundaries, args.embargo_ms):
            candidate = calibrate(fit(train_fold, args.version), cal_fold)
            report["walk_forward"].append(dict(train=window(train_fold), calibration=window(cal_fold),
                                               test=window(test_fold), metrics=metrics(candidate, test_fold)))
    payload = dict(features=FEATURES, labels=LABELS, model=asdict(model), evaluation=report,
                   dataset_sha256=sha256(dataset).hexdigest(), embargo_ms=args.embargo_ms,
                   training=window(train), calibration=window(cal), held_out=window(test))
    with args.artifact.open("x") as stream:
        json.dump(payload, stream, allow_nan=False, indent=2)
    print(json.dumps(report))


if __name__ == "__main__":
    main()
