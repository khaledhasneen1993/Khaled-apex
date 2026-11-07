"""Read local runtime state and the health endpoint; never connects to Binance."""
import argparse
import json
import sys
from pathlib import Path
from urllib.request import urlopen
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from risk.config import load_config
from journal.wal import load_durable, WriteAheadLog

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['testnet','paper','mainnet'], default='testnet')
    args = parser.parse_args()
    cfg = load_config()
    root = Path(cfg['exchange']['state_dir']) / args.mode
    state = load_durable(root / cfg['exchange']['state_filename'])
    try:
        with urlopen('http://127.0.0.1:%d/health' % cfg['health']['port'], timeout=1) as response:
            health = json.load(response)
    except OSError: health = {'status':'offline', 'uptime_sec':None}
    events = WriteAheadLog(root / 'journal.wal').replay()[-5:]
    print(json.dumps({'health':health, 'last_trade':state.get('trades', [])[-1:],
        'open_positions':len(state.get('strategy_positions', {})), 'equity':state.get('equity_usdt'),
        'daily_pnl':state.get('operations', {}).get('kill', {}).get('state', {}).get('daily_pnl'),
        'kill_switch':state.get('operations', {}).get('kill'),
        'wal_events':[{k:v for k,v in e.items() if k != 'value'} for e in events]}, indent=2))

if __name__ == '__main__': main()
