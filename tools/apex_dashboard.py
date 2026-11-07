"""Loopback-only viewer for Apex research/shadow exports. No trading controls."""
import argparse
from html import escape
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ARTIFACTS = ("report.json", "signals.json", "signals.csv", "trades.json", "trades.csv")


def page(report, signals):
    cards = (
        ("Equity", report["final_equity"]), ("Realized PnL", report["metrics"]["net_pnl"]),
        ("Unrealized PnL", report["unrealized_pnl"]), ("Aggregate stop risk", report["aggregate_stop_risk"]),
        ("Gross exposure", report["gross_exposure"]), ("Safety state", report["safety_state"]),
        ("Model", report["model_version"] or "rules-only"),
    )
    facts = "".join(f"<dt>{escape(label)}</dt><dd>{escape(str(value))}</dd>" for label, value in cards)
    details = "".join(f"<h2>{label}</h2><pre>{escape(json.dumps(data, indent=2))}</pre>"
                      for label, data in (("Feed health", report["feed_health"]),
                                          ("Positions", report["open_positions"]),
                                          ("Recent signal decisions", signals[-50:])))
    links = " · ".join(f'<a href="/{name}">{name}</a>' for name in ARTIFACTS)
    return f"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
    <title>Khaled Apex · Shadow</title><style>
    body{{max-width:75rem;margin:2rem auto;padding:0 1.25rem;font:1rem system-ui;color:#e8eef9;background:#101827}}
    h1,h2{{font-weight:600}} dl{{display:grid;grid-template-columns:repeat(auto-fit,minmax(10rem,1fr));gap:1rem}}
    dt{{color:#a4bad6}} dd{{margin:0}} pre{{padding:1rem;background:#192539;overflow:auto}} a{{color:#84ddcb}}
    </style><h1>Khaled Apex · Shadow</h1><p>Recorded research output. Prices and balances are simulated.</p>
    <dl>{facts}</dl><p>{links}</p>{details}</html>"""


def serve(directory, port):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.headers.get("Host") not in (f"127.0.0.1:{port}", f"localhost:{port}"):
                self.send_error(403)
                return
            name = self.path.removeprefix("/")
            try:
                if name == "":
                    report = json.loads((directory/"report.json").read_text())
                    signals = json.loads((directory/"signals.json").read_text())
                    body = page(report, signals).encode()
                    content_type = "text/html; charset=utf-8"
                elif name in ARTIFACTS:
                    body = (directory/name).read_bytes()
                    content_type = "application/json" if name.endswith(".json") else "text/csv; charset=utf-8"
                else:
                    self.send_error(404)
                    return
            except (OSError, ValueError, KeyError):
                self.send_error(503, "Research report unavailable")
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.end_headers()
            self.wfile.write(body)
    with ThreadingHTTPServer(("127.0.0.1", port), Handler) as httpd:
        httpd.serve_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--port", type=int, default=8788)
    args = parser.parse_args()
    try:
        serve(args.directory, args.port)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
