"""Watch structured local events without API credentials."""
import argparse
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from risk.config import load_config

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--follow', action='store_true')
    p.add_argument('--mode', default='testnet', choices=['paper','testnet','mainnet'])
    args = p.parse_args()
    root = Path(load_config()['exchange']['state_dir']) / args.mode / 'journal'
    offsets = {}
    try:
        while True:
            for name in ('events.log','kill_events.log'):
                path = root / name
                if not path.exists(): continue
                stat = path.stat()
                inode, offset = offsets.get(name, (None, 0))
                if inode != stat.st_ino or stat.st_size < offset: offset = 0
                with path.open() as stream:
                    stream.seek(offset)
                    for line in stream: print(line.rstrip(), flush=True)
                    offsets[name] = (stat.st_ino, stream.tell())
            if not args.follow: break
            time.sleep(.5)
    except KeyboardInterrupt: pass

if __name__ == '__main__': main()
