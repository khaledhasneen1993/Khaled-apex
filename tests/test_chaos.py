"""Offline fault injection. Real systemd and 72h Demo soak are separate gates."""
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from risk.config import load_config
from risk.kill_switch import KillSwitch
from exchange.ws_manager import ResilientWebSocket
from infra.circuit_breaker import CircuitBreaker, BreakerState, CircuitBreakerOpen
from infra.watchdog import Watchdog
from infra.shutdown import GracefulShutdown
from journal.wal import WriteAheadLog, load_durable
from futures_simulator import FuturesSimulator, production_bot


def test_ws_disconnects_30s_reconnects():
    cfg = load_config()
    now = [0.]
    ws = ResilientWebSocket(cfg['ws'], clock=lambda: now[0])
    assert 1 <= ws.disconnected() <= 2
    now[0] = 30
    assert not ws.critical()
    ws.connected()
    assert ws.connection_lost_at is None and ws.reconnect_attempts == 0
    kill = KillSwitch(cfg['kill_switch'], 'demo')
    kill.state.last_ws_heartbeat = time.time() - 30
    assert kill.check(SimpleNamespace(equity=100), None) is None
    assert not kill.blocks_entries


def test_ws_disconnects_10min_kill():
    cfg = load_config()
    kill = KillSwitch(cfg['kill_switch'], 'demo')
    kill.state.last_ws_heartbeat = time.time() - 600
    assert kill.check(SimpleNamespace(equity=100), None) is None
    assert kill.triggered == 'ws_timeout' and not kill.should_flatten(kill.triggered)
    hard = KillSwitch(cfg['kill_switch'], 'mainnet')
    hard.state.last_ws_heartbeat = time.time() - 600
    assert hard.check(SimpleNamespace(equity=100), None) == 'ws_timeout'
    assert hard.blocks_entries and hard.should_flatten('ws_timeout')


def test_api_errors_20_no_halt():
    async def check():
        cfg = load_config()
        breaker = CircuitBreaker(cfg['circuit_breaker'])
        kill = KillSwitch(cfg['kill_switch'], 'demo')
        async def failure():
            kill.on_api_error()
            raise TimeoutError()
        for _ in range(20):
            with pytest.raises(TimeoutError): await breaker.call(failure)
        assert breaker.state == BreakerState.CLOSED
        assert kill.check(SimpleNamespace(equity=100), None) is None
        assert not kill.blocks_entries
    asyncio.run(check())


def test_api_errors_100_halt():
    cfg = load_config()
    kill = KillSwitch(cfg['kill_switch'], 'mainnet')
    for _ in range(100): kill.on_api_error()
    assert kill.check(SimpleNamespace(equity=100), None) == 'api_errors'
    assert kill.blocks_entries and kill.should_flatten('api_errors')


def test_process_kill_recovery(tmp_path):
    # Actually SIGKILL a writer after WAL fsync and before snapshot replacement.
    path = tmp_path / 'state.json'
    code = """
import time
from pathlib import Path
from journal.wal import WriteAheadLog
path = Path(__import__('sys').argv[1])
WriteAheadLog(path.parent / 'journal.wal').log({'event':'snapshot','file':path.name,'value':{'order_id':42}})
print('durable', flush=True)
time.sleep(60)
"""
    process = subprocess.Popen([sys.executable, '-c', code, str(path)], stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == 'durable'
        process.kill()
        process.wait(timeout=5)
        assert process.returncode == -signal.SIGKILL
        assert load_durable(path) == {'order_id':42}
        unit = Path('deploy/khaled-bot.service').read_text()
        assert 'Restart=always' in unit and 'StartLimitIntervalSec=0' in unit
    finally:
        if process.poll() is None: process.kill(); process.wait(timeout=5)
        process.stdout.close()


def test_state_corruption_recovery(tmp_path):
    path = tmp_path / 'state.json'
    wal = WriteAheadLog(tmp_path / 'journal.wal')
    wal.snapshot(path, {'position':{'amount':3}, 'order_id':42})
    path.write_text('{broken')
    with wal.path.open('ab') as stream: stream.write(b'{partial')
    assert load_durable(path)['order_id'] == 42
    wal.snapshot(path, {'position':{'amount':4}, 'order_id':43})
    wal.compact()
    assert load_durable(path)['order_id'] == 43
    assert len(wal.replay()) == 1


def test_disk_full_recovery(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        # No new financial mutation can pass a failed WAL write, even in observe.
        with patch.object(bot.exchange.wal, 'log', side_effect=OSError(28, 'disk full')):
            assert not await bot.enter(bot.exchange.symbol('AIOUSDT'), 'buy', .05)
        assert not server.orders and not server.position_rows()
        assert bot.exchange.storage_fault
        assert not bot.safety.ready
        # Read-only health and the event loop remain responsive.
        from infra.health_server import HealthServer
        assert HealthServer(bot).snapshot()['status'] == 'degraded'
        await bot.exchange.close()
    asyncio.run(check())


def test_memory_leak_restart():
    cfg = load_config()['process']
    exits = []
    process = SimpleNamespace(memory_info=lambda:SimpleNamespace(rss=2_100_000_000), num_fds=lambda:3)
    watchdog = Watchdog(cfg, process=process, exit_fn=exits.append)
    assert watchdog.check() == 3 and exits == [3]
    process.memory_info = lambda: SimpleNamespace(rss=1000)
    process.num_fds = lambda: 10001
    assert watchdog.check() == 4 and exits[-1] == 4


def test_network_partition():
    async def check():
        cfg = load_config()
        clock = [0.]
        ws = ResilientWebSocket(cfg['ws'], clock=lambda:clock[0])
        breaker = CircuitBreaker(cfg['circuit_breaker'], clock=lambda:clock[0])
        async def failure(): raise TimeoutError()
        for _ in range(30):
            with pytest.raises(TimeoutError): await breaker.call(failure)
        assert breaker.state == BreakerState.OPEN
        with pytest.raises(CircuitBreakerOpen): await breaker.call(AsyncMock())
        ws.disconnected()
        clock[0] = 300
        assert ws.critical()
        assert await breaker.call(AsyncMock(return_value='recovered')) == 'recovered'
        ws.connected()
        assert not ws.critical() and breaker.state == BreakerState.CLOSED
    asyncio.run(check())


def test_concurrent_restart(tmp_path, monkeypatch):
    async def check():
        import fcntl
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        symbol = bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        bot.save()
        with (bot.state_path.parent / 'account.lock').open('a') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            other = await production_bot(tmp_path, monkeypatch, server, startup=False)
            with pytest.raises(ValueError, match='Another process'): await other.start()
        assert len(server.orders) == 1 and len(server.position_rows()) == 1
        await bot.exchange.close()
    asyncio.run(check())


def test_reconcile_after_crash(tmp_path, monkeypatch):
    async def check():
        import httpx
        class Crash(BaseException): pass
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        symbol = bot.exchange.symbol('AIOUSDT')
        bot.save()
        async def transport(request):
            response = await server(request)
            if request.method == 'POST' and request.url.path == '/fapi/v1/order': raise Crash()
            return response
        await bot.exchange.client.aclose()
        bot.exchange.client = httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(transport))
        with pytest.raises(Crash): await bot.enter(symbol, 'buy', .05)
        await bot.exchange.close()
        restored = await production_bot(tmp_path, monkeypatch, server)
        assert restored.safety.ready and len(server.orders) == 1
        assert symbol in restored.positions and restored.positions[symbol]['protection']
        assert not await restored.enter(symbol, 'buy', .05)
        assert len(server.orders) == 1
        await restored.exchange.close()
    asyncio.run(check())


def test_graceful_shutdown_during_trade(tmp_path, monkeypatch):
    async def check():
        server = FuturesSimulator()
        bot = await production_bot(tmp_path, monkeypatch, server)
        symbol = bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol, 'buy', .05)
        # Exercise actual start/finally cleanup with preloaded simulated account.
        bot.exchange.load_markets = AsyncMock()
        bot.cfg['health']['enabled'] = False
        async def waiting(): await asyncio.Event().wait()
        bot.scan_loop = bot.exit_watcher = bot.report = waiting
        bot.safety.monitor = bot.safety.watchdog = bot.process_watchdog.run = waiting
        task = asyncio.create_task(bot.start())
        for _ in range(20):
            await asyncio.sleep(0)
            if bot._management_authorized and bot._state_lock: break
        shutdown = GracefulShutdown(bot, task)
        start = time.monotonic()
        shutdown.request(); shutdown.request()
        with pytest.raises(asyncio.CancelledError): await task
        assert time.monotonic() - start < 5
        assert len(server.orders) == 1 and server.position_rows()
        assert all(o['algoStatus'] == 'NEW' for o in server.algos.values())
        assert not load_durable(bot.state_path.parent / 'session_guard.json').get('active')
        assert load_durable(bot.state_path)['strategy_positions'][symbol]['protection']
    asyncio.run(check())
