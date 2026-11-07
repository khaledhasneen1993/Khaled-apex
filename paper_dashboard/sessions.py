"""Choose a separate Paper comparison session, preserving all older sessions."""
import argparse
import fcntl
import json
import secrets
from datetime import datetime, timezone
from pathlib import Path
from paths import BASE


def select_session(fresh=False):
    BASE.mkdir(parents=True, exist_ok=True)
    with (BASE/'server.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('The Paper dashboard is running. Stop it with Ctrl+C first.') from None
        pointer = BASE/'current'
        if not fresh and pointer.exists():
            target = Path(pointer.read_text().strip()).resolve()
            if target.parent != (BASE/'sessions').resolve() or not target.is_dir():
                raise ValueError('Invalid saved Paper session; use --fresh to create a separate one')
            return target
        tag = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+secrets.token_hex(4)
        target = BASE/'sessions'/tag
        target.mkdir(parents=True, exist_ok=False)
        (target/'session.json').write_text(json.dumps(dict(mode='paper', initial_balance_usdt=100,
            bots=['final','khaled'], session=tag), indent=2))
        temp = pointer.with_suffix('.tmp')
        temp.write_text(str(target.resolve())+'\n')
        temp.replace(pointer)
        return target.resolve()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fresh', action='store_true')
    args = parser.parse_args()
    try:
        print(select_session(args.fresh))
    except ValueError as error:
        parser.exit(1, str(error)+'\n')
