"""Local, cached health reads: no exchange requests in HTTP handlers."""
import time
from aiohttp import web
from infra.circuit_breaker import BreakerState

class HealthServer:
    def __init__(self, bot):
        self.bot = bot
        self.started = time.monotonic()
        self.runner = None

    def snapshot(self):
        bot, safety = self.bot, self.bot.safety
        age = max(0., time.time() - safety.kill.state.last_ws_heartbeat)
        process = getattr(getattr(bot, 'process_watchdog', None), 'process', None)
        resources = {'memory_bytes': process.memory_info().rss, 'open_fds': process.num_fds()} if process else {}
        return {**resources, 'mode': getattr(bot, 'execution_mode', 'unknown'), 'closed_trades': len([t for t in bot.exchange.trades if t.get('exit_detail')]) if hasattr(bot.exchange, 'trades') else 0,
                'observe_events': getattr(safety, 'kill_observation_count', 0), 'status': 'healthy' if safety.ready and not safety.storage_failed and age < safety.kill.cfg['ws_timeout_sec'] and not safety.kill.blocks_entries and not getattr(bot.exchange, 'storage_fault', False) and getattr(getattr(bot.exchange, 'breaker', None), 'state', None) in (None, BreakerState.CLOSED) else 'degraded',
                'uptime_sec': time.monotonic() - self.started,
                'last_ws_message_age_sec': age, 'open_positions': len(bot.positions),
                'kill_switch': {'mode': safety.kill.mode, 'triggered': safety.kill.triggered},
                'equity': bot.exchange.equity, 'daily_pnl': safety.kill.state.daily_pnl,
                'circuit_breaker': getattr(getattr(bot.exchange, 'breaker', None), 'state', None).value if hasattr(bot.exchange, 'breaker') else None}

    async def start(self):
        async def handler(request): return web.json_response(self.snapshot())
        app = web.Application()
        app.router.add_get('/health', handler)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        cfg = self.bot.cfg['health']
        await web.TCPSite(self.runner, cfg['host'], cfg['port']).start()

    async def close(self):
        if self.runner: await self.runner.cleanup()
