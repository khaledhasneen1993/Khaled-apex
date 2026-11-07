"""Explicit human-only Demo emergency closure. Stop the bot first."""
import argparse
import asyncio
import sys
import fcntl
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from risk.config import execution_mode, load_config
from infra.rate_limit import TokenBucket
from exchange.live_exchange import LiveExchange

async def close_all(cfg):
    root = Path(cfg['exchange']['state_dir']) / 'testnet'
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'account.lock').open('a') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError: raise SystemExit('Stop the running bot before emergency closure')
        ex = LiveExchange(cfg, TokenBucket(cfg['exchange']['rate_per_sec'], cfg['exchange']['rate_burst']))
        ex.emergency_mode = True
        try:
            await ex.load_markets()
            if not await ex.recover_execution(): raise RuntimeError('Resolve uncertain orders before manual closure')
            await ex.refresh_account()
            for symbol, pos in list(ex.positions.items()):
                await ex.create_order(symbol, 'market', 'sell' if pos['side'] == 'buy' else 'buy',
                    pos['amount'], params={'reduceOnly':True, 'emergency':True})
            await ex.refresh_account()
            if ex.positions: raise RuntimeError('Positions remain; protection retained')
            for order in await ex.get_open_orders():
                if order.get('is_algo'): await ex.cancel_algo(order)
                else: await ex.cancel_order(order['symbol'], order_id=order['orderId'])
        finally: await ex.close()

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--confirm', required=True, choices=['YES_I_KNOW'])
    p.parse_args()
    with execution_mode('testnet'): asyncio.run(close_all(load_config()))

if __name__ == '__main__': main()
