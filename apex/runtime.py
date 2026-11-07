"""Single-writer Apex Paper session. A durable public tape is its recovery ledger."""
import argparse
import asyncio
from dataclasses import asdict
import fcntl
import json
import os
from pathlib import Path
import signal
import time

from apex.config import Config
from apex.engine import Engine, Instrument
from apex.safety import state
from infra.journal import atomic_json
from market_data.capture import SYMBOL, stream
from research.replay import Experiment, Replay, export


class PaperSession:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.replay = None
        self.last_status = 0.
        self.storage_failed = False

    def __enter__(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = (self.directory/"session.lock").open("a")
        try:
            fcntl.flock(self.lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise ValueError("Apex Paper session already has a writer") from None
        return self

    def __exit__(self, *_):
        self.lock.close()

    def load(self, *, create=None):
        metadata = self.directory/"session.json"
        self.tape = self.directory/"events.jsonl"
        if metadata.exists():
            if create is not None:
                raise ValueError("Session already exists; resume it or choose a new directory")
            settings = json.loads(metadata.read_text())
        elif create is None or self.tape.exists():
            raise ValueError("Session metadata missing; specify --new-session for a new directory")
        else:
            settings = create
            atomic_json(metadata, settings)
        if settings["format"] != 1 or settings["mode"] != "apex-paper":
            raise ValueError("Unexpected session type")
        cfg = Config(**settings["config"])
        if cfg.fingerprint != settings["fingerprint"]:
            raise ValueError("Session configuration fingerprint mismatch")
        self.instruments = {s: Instrument(**v) for s, v in settings["instruments"].items()}
        self.replay = Replay(Engine(cfg, self.instruments), equity=settings["initial_equity"],
                             experiment=Experiment(**settings["experiment"]))
        if self.tape.exists():
            with self.tape.open("rb+") as tape:
                while line := tape.readline():
                    if not line.endswith(b"\n"):
                        tape.truncate(tape.tell()-len(line))
                        tape.flush()
                        os.fsync(tape.fileno())
                        break
                    self.replay.apply(json.loads(line))

    def commit(self, rows):
        if self.storage_failed:
            raise ValueError("Session storage failed; restart and review the tape")
        if not rows:
            return
        if self.replay.clock is not None and rows[0]["received_ms"] < self.replay.clock:
            raise ValueError("Clock precedes durable session; correct system time")
        encoded = b"".join((json.dumps(row, allow_nan=False, separators=(",", ":"))+"\n").encode()
                           for row in rows)
        try:
            existed = self.tape.exists()
            with self.tape.open("ab") as tape:
                tape.write(encoded)
                tape.flush()
                os.fsync(tape.fileno())
            if not existed:
                fd = os.open(self.directory, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        except OSError:
            self.storage_failed = True
            raise
        for row in rows:
            self.replay.apply(row)

    def boundary(self):
        now = max(time.time_ns()//1_000_000, self.replay.clock or 0)
        self.commit([dict(kind="session_boundary", symbol="", received_ms=now, data={})])

    def publish_status(self, running=True):
        now = time.time_ns()//1_000_000
        if running and now-self.last_status < 2000:
            return
        self.last_status = now
        report = self.replay.report()
        report.update(mode="apex-paper", running=running, updated_ms=now,
                      recent_signals=self.replay.signals[-50:],
                      recent_exits=self.replay.exit_fills[-25:])
        feed_ok = (running and self.replay.clock is not None
                   and 0 <= now-self.replay.clock <= self.replay.engine.cfg.max_age_ms
                   and report["safety_state"] == "ready")
        report["safety_state"] = state(data_ok=feed_ok, storage_ok=not self.storage_failed,
                                       unresolved=any(p.exit_reason for p in self.replay.open.values()))
        # Status is disposable. The account ledger remains authoritative.
        try:
            atomic_json(self.directory/"status.json", report)
        except OSError:
            pass


def settings(config, instruments, equity, timeout):
    if not 1 <= len(instruments) <= 8 or any(not SYMBOL.fullmatch(s) for s in instruments):
        raise ValueError("Provide filters for one to eight USD-M symbols")
    for rules in instruments.values():
        Instrument(**rules)
    # Validate initial balance before writing the immutable session metadata.
    Replay(Engine(config, {}), equity=equity)
    return dict(format=1, mode="apex-paper", config=asdict(config), fingerprint=config.fingerprint,
                initial_equity=equity, instruments=instruments,
                experiment=asdict(Experiment(thesis_timeout_ms=timeout or config.thesis_timeout_ms or None)))


async def run_session(session, *, feed=stream):
    session.boundary()
    loop = asyncio.get_running_loop()
    stopped = asyncio.Event()
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        loop.add_signal_handler(sig, stopped.set)
    # Shield the worker and await it after cancellation so the tape never has
    # two writers, including when SIGTERM arrives during fsync.
    async def consume(rows):
        def apply():
            session.commit(rows)
            session.publish_status()
        worker = asyncio.create_task(asyncio.to_thread(apply))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            await worker
            raise

    source = asyncio.create_task(feed(tuple(session.instruments), consume))
    stop = asyncio.create_task(stopped.wait())
    try:
        await asyncio.wait((source, stop), return_when=asyncio.FIRST_COMPLETED)
    finally:
        source.cancel()
        stop.cancel()
        try:
            await source
        except asyncio.CancelledError:
            pass
        finally:
            for sig in signals:
                loop.remove_signal_handler(sig)
            if not session.storage_failed:
                session.boundary()
            session.publish_status(running=False)


def add_arguments(parser):
    parser.add_argument("--apex-session", type=Path, default=Path("runtime/apex-paper/default"))
    parser.add_argument("--new-session", action="store_true")
    parser.add_argument("--instruments", type=Path)
    parser.add_argument("--apex-config", type=Path, default=Path("apex.json"))
    parser.add_argument("--equity", type=float)
    parser.add_argument("--thesis-timeout-ms", type=int)
    parser.add_argument("--export", type=Path)


def paper_main(args, parser):
    if args.new_session and (args.instruments is None or args.export is not None):
        parser.error("--new-session requires --instruments and cannot be used with --export")
    if not args.new_session and any(x is not None for x in (args.instruments, args.equity, args.thesis_timeout_ms)):
        parser.error("Resume uses the saved filters, balance and exit policy")
    create = None
    if args.new_session:
        cfg = Config.load(args.apex_config)
        create = settings(cfg, json.loads(args.instruments.read_text()),
                          100. if args.equity is None else args.equity, args.thesis_timeout_ms)
    with PaperSession(args.apex_session) as session:
        session.load(create=create)
        if args.export is not None:
            export(session.replay, args.export)
        else:
            asyncio.run(run_session(session))
