"""Export an existing Paper ledger without loading market clients or mutating state."""
import argparse
import csv
import json
from pathlib import Path

from journal.wal import load_durable
from paper_dashboard.ledger import round_trips


def export(state_path, target):
    state = load_durable(state_path)
    if state["settings"].get("execution_mode", "paper") != "paper":
        raise ValueError("Expected a Paper state file")
    fills = state["trades"]
    opened, closed = round_trips(fills)
    target.mkdir(parents=True, exist_ok=False)
    (target/"paper.json").write_text(json.dumps(
        dict(fills=fills, opened=opened, closed=closed), indent=2, allow_nan=False)+"\n")
    for name, rows in (("fills", fills), ("closed", closed)):
        keys = sorted({key for row in rows for key in row})
        with (target/f"{name}.csv").open("w", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=keys)
            if keys:
                writer.writeheader()
            for row in rows:
                clean = {k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                         for k, v in row.items()}
                writer.writerow({k: "'"+v if isinstance(v, str) and v.startswith(("=", "+", "-", "@"))
                                 else v for k, v in clean.items()})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("state", type=Path)
    p.add_argument("destination", type=Path)
    args = p.parse_args()
    export(args.state, args.destination)


if __name__ == "__main__":
    main()
