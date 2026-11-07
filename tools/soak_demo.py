"""Record a real running Demo's local health for 72 hours; never starts trading.

A passing report requires uninterrupted coverage, no process restart, healthy
resource limits and real elapsed time. Offline unit tests cannot replace it.
"""
import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
from infra.journal import atomic_json
from risk.config import load_config

async def collect(hours):
    cfg = load_config()
    root = Path(cfg['exchange']['state_dir']) / 'testnet'
    root.mkdir(parents=True, exist_ok=True)
    report = {'status':'running', 'started_at':datetime.now(timezone.utc).isoformat(),
              'required_hours':hours, 'samples':0, 'unavailable_samples':0, 'restarts':0,
              'max_memory_bytes':0, 'max_open_fds':0, 'max_health_latency_ms':0}
    started = time.monotonic()
    previous_uptime = None
    try:
        async with httpx.AsyncClient(timeout=2, trust_env=False) as client:
            while time.monotonic() - started < hours * 3600:
                tick = time.monotonic()
                try:
                    response = await client.get('http://127.0.0.1:%d/health' % cfg['health']['port'])
                    response.raise_for_status()
                    sample = response.json()
                    if sample.get('mode') != 'testnet' or sample['kill_switch']['mode'] != 'observe':
                        raise ValueError('Soak requires the Demo observe process')
                    latency = (time.monotonic() - tick) * 1000
                    if previous_uptime is not None and sample['uptime_sec'] < previous_uptime: report['restarts'] += 1
                    previous_uptime = sample['uptime_sec']
                    if report['samples'] == 0: report['initial_equity'] = sample['equity']
                    report.update(last_health=sample, samples=report['samples']+1,
                        max_memory_bytes=max(report['max_memory_bytes'], sample.get('memory_bytes', 0)),
                        max_open_fds=max(report['max_open_fds'], sample.get('open_fds', 0)),
                        max_health_latency_ms=max(report['max_health_latency_ms'], latency))
                    with (root / 'soak_samples.jsonl').open('a') as stream:
                        stream.write(json.dumps({'timestamp':datetime.now(timezone.utc).isoformat(), **sample})+'\n')
                except (httpx.HTTPError, ValueError, KeyError): report['unavailable_samples'] += 1
                report['elapsed_hours'] = (time.monotonic() - started) / 3600
                atomic_json(root / 'soak_report.json', report)
                await asyncio.sleep(10)
    finally:
        report['elapsed_hours'] = (time.monotonic() - started) / 3600
        complete = report['elapsed_hours'] >= hours and hours >= 72
        report['status'] = 'passed' if (complete and report['samples'] > 0 and not report['unavailable_samples']
            and not report['restarts'] and report['max_memory_bytes'] <= cfg['process']['max_memory_gb'] * 1e9
            and report['max_open_fds'] <= cfg['process']['max_fds'] and report['max_health_latency_ms'] < 100) else 'pending' if not complete else 'failed'
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        atomic_json(root / 'soak_report.json', report)
        print(json.dumps(report, indent=2))

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--hours', type=float, default=72)
    args = p.parse_args()
    if args.hours <= 0: p.error('--hours must be positive')
    try: asyncio.run(collect(args.hours))
    except KeyboardInterrupt: pass

if __name__ == '__main__': main()
