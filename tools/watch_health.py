"""Independent local heartbeat monitor. Run separately from the trading process.

Uses only the localhost health endpoint and the configured Telegram account.
It cannot place trades. A device/network outage can prevent delivery entirely.
"""
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
from infra.alerts import TelegramAlerter
from risk.config import load_config

async def main():
    cfg = load_config()
    alerter = TelegramAlerter(cfg['alerts'])
    if not alerter.enabled: raise SystemExit('Configure Telegram locally before starting health watcher')
    worker = asyncio.create_task(alerter._worker())
    previous = None
    try:
        async with httpx.AsyncClient(timeout=2, trust_env=False) as client:
            while True:
                try:
                    response = await client.get('http://127.0.0.1:%s/health' % cfg['health']['port'])
                    response.raise_for_status()
                    status = response.json()['status']
                except (httpx.HTTPError, ValueError, KeyError): status = 'offline'
                if status != previous:
                    alerter.send_nowait('Khaled external health: ' + status, 'info' if status == 'healthy' else 'critical')
                    previous = status
                await asyncio.sleep(10)
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
