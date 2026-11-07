import json
import os
from datetime import datetime, timezone
from pathlib import Path


class OperationalJournal:
    """Allowlisted structured records; never serialize exceptions or credentials."""
    fields = {'symbol', 'side', 'client_id', 'order_id', 'reason', 'status', 'code', 'mode',
              'report', 'equity', 'daily_pnl', 'count', 'pnl', 'expected', 'actual',
              'entry_slippage_bps', 'exit_slippage_bps', 'rate', 'average', 'attempt',
              'stage', 'client_order_id', 'error_type', 'http_status', 'exchange_error_code', 'recovery_action', 'amount', 'leverage', 'min_qty', 'max_qty', 'transport_error_type', 'source', 'line', 'function'}

    def __init__(self, root):
        self.failed = False
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def log(self, event, **values):
        record = {'timestamp': datetime.now(timezone.utc).isoformat(), 'event': event,
                  **{k: v for k, v in values.items() if k in self.fields}}
        name = 'kill_events.log' if event.startswith('kill') else 'events.log'
        try:
            with (self.root / name).open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(record, allow_nan=False) + '\n')
                stream.flush()
                os.fsync(stream.fileno())
        except OSError:
            self.failed = True
            return False
        self.failed = False
        return True


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(temp, 0o600)
    temp.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
