"""Durable snapshot WAL. Replay restores facts, never resubmits an order.

Only a torn final line is ignored. Interior corruption fails closed. Compaction
retains the latest snapshot of every file and uses fsync + atomic replacement.
"""
import json
import os
from pathlib import Path
from infra.journal import atomic_json

class WriteAheadLog:
    def __init__(self, path, max_bytes=16 * 1024 * 1024):
        self.path = Path(path)
        self.max_bytes = max_bytes

    def replay(self):
        if not self.path.exists(): return []
        records = []
        with self.path.open('rb') as stream:
            for line in stream:
                if not line.endswith(b'\n'): break
                records.append(json.loads(line))
        return records

    def latest(self):
        return {e['file']: e['value'] for e in self.replay() if e.get('event') == 'snapshot'}

    def log(self, event):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Remove an uncommitted partial tail before appending a new record.
        if self.path.exists():
            with self.path.open('rb+') as stream:
                end = stream.seek(0, 2)
                if end:
                    stream.seek(end - 1)
                    if stream.read(1) != b'\n':
                        while end:
                            end -= 1
                            stream.seek(end)
                            if stream.read(1) == b'\n':
                                end += 1
                                break
                        stream.truncate(end)
        encoded = (json.dumps(event, allow_nan=False, separators=(',', ':')) + '\n').encode()
        existed = self.path.exists()
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            with os.fdopen(fd, 'ab') as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            if not existed:
                directory = os.open(self.path.parent, os.O_RDONLY)
                try: os.fsync(directory)
                finally: os.close(directory)
        except BaseException:
            raise
        if self.path.stat().st_size > self.max_bytes: self.compact()

    def compact(self):
        latest = self.latest()
        temp = self.path.with_suffix('.compact')
        with temp.open('w') as stream:
            os.chmod(temp, 0o600)
            for name, value in latest.items():
                stream.write(json.dumps({'event': 'snapshot', 'file': name, 'value': value}, allow_nan=False) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        temp.replace(self.path)
        fd = os.open(self.path.parent, os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)

    def snapshot(self, path, value, writer=None):
        self.log({'event': 'snapshot', 'file': Path(path).name, 'value': value})
        (writer or atomic_json)(path, value)


def load_durable(path, default=None):
    path = Path(path)
    # WAL is authoritative even if a crash left a valid but older JSON file.
    latest = WriteAheadLog(path.parent / 'journal.wal').latest()
    if path.name in latest: return latest[path.name]
    if not path.exists(): return {} if default is None else default
    return json.loads(path.read_text())
