import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from infra.circuit_breaker import CircuitBreaker, CircuitBreakerOpen, BreakerState
from infra.watchdog import Watchdog
from infra.health_server import HealthServer
from journal.wal import WriteAheadLog
from risk.config import load_config
from risk.kill_switch import KillSwitch


def test_environment_modes_cannot_weaken_mainnet():
    cfg = load_config()['kill_switch']
    cfg['mode'] = 'observe'
    cfg['mode_per_environment']['mainnet'] = 'observe'
    assert KillSwitch(cfg, 'mainnet').mode == 'hard'
    assert KillSwitch(cfg, 'demo').mode == 'observe'
    assert KillSwitch(cfg, 'testnet').mode == 'soft'


def test_soft_cooldown_requires_healthy_conditions():
    cfg = load_config()['kill_switch']
    with patch('risk.kill_switch.time.time', return_value=1000):
        kill = KillSwitch(cfg, 'testnet')
        kill.state.last_ws_heartbeat = 1
        assert kill.check(SimpleNamespace(equity=100), None) == 'ws_timeout'
        assert not kill.should_flatten('ws_timeout')
    with patch('risk.kill_switch.time.time', return_value=1700):
        assert kill.check(SimpleNamespace(equity=100), None) == 'ws_timeout'
        kill.on_ws_heartbeat()
        assert kill.check(SimpleNamespace(equity=100), None) is None
        assert not kill.blocks_entries


def test_only_one_half_open_probe_and_failed_probe_reopens():
    async def check():
        clock = [0.]
        breaker = CircuitBreaker({'failure_threshold':1, 'cooldown_sec':30}, clock=lambda:clock[0])
        async def fail(): raise TimeoutError()
        with pytest.raises(TimeoutError): await breaker.call(fail)
        clock[0] = 31
        started = asyncio.Event()
        release = asyncio.Event()
        async def probe():
            started.set()
            await release.wait()
            raise TimeoutError()
        task = asyncio.create_task(breaker.call(probe))
        await started.wait()
        with pytest.raises(CircuitBreakerOpen): await breaker.call(AsyncMock())
        release.set()
        with pytest.raises(TimeoutError): await task
        assert breaker.state == BreakerState.OPEN and breaker.failed_probes == 1
        clock[0] = 62
        assert await breaker.call(AsyncMock(return_value=42)) == 42
    asyncio.run(check())


def test_wal_interior_corruption_is_not_silently_skipped(tmp_path):
    wal = WriteAheadLog(tmp_path / 'journal.wal')
    wal.path.write_bytes(b'{broken}\n{"event":"snapshot","file":"state.json","value":{}}\n')
    with pytest.raises(ValueError): wal.replay()


def test_heartbeat_hang_is_distinct_from_network_outage():
    clock = [0.]
    exits = []
    process = SimpleNamespace(memory_info=lambda: SimpleNamespace(rss=1), num_fds=lambda:1)
    watchdog = Watchdog(load_config()['process'], clock=lambda:clock[0], exit_fn=exits.append, process=process)
    watchdog.heartbeat('order_manager')
    clock[0] = 121
    assert watchdog.check() == 2 and exits == [2]


def test_health_snapshot_is_fast_and_read_only():
    kill = KillSwitch(load_config()['kill_switch'], 'demo')
    bot = SimpleNamespace(safety=SimpleNamespace(ready=True, storage_failed=False, kill=kill),
                          positions={}, exchange=SimpleNamespace(equity=100))
    health = HealthServer(bot)
    start = time.perf_counter()
    for _ in range(100): assert health.snapshot()['status'] == 'healthy'
    assert (time.perf_counter() - start) / 100 < .1


def test_mainnet_spread_and_losses_are_hard():
    from types import SimpleNamespace
    cfg = load_config()['kill_switch']
    kill = KillSwitch(cfg, 'mainnet')
    market = SimpleNamespace(samples=50, avg_spread=.001, spread=lambda:.02)
    assert kill.check(SimpleNamespace(equity=100), market) == 'spread_spike'
    assert kill.should_flatten('spread_spike')
    kill = KillSwitch(cfg, 'mainnet')
    for _ in range(4): kill.on_trade_closed(-.1)
    assert kill.check(SimpleNamespace(equity=100), None) == 'consecutive_losses'
    assert kill.should_flatten('consecutive_losses')
