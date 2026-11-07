"""Idempotent signal cancellation lets the runtime persist and close clients."""
import asyncio
import signal

class GracefulShutdown:
    def __init__(self, bot, task):
        self.bot, self.task = bot, task
        self.stop_event = asyncio.Event()
        self.started = False
        self.installed = []
        self.drain_task = None

    def request(self):
        if self.started: return
        self.started = True
        self.bot.safety.ready = False
        self.stop_event.set()
        self.drain_task = asyncio.create_task(self._drain())

    async def _drain(self):
        # Entry/exit workers serialize financial actions with bot.lock. Give an
        # in-flight response/protection commit a bounded chance to finish.
        try:
            if hasattr(self.bot, 'lock'):
                async with asyncio.timeout(2):
                    async with self.bot.lock: pass
        except TimeoutError: pass
        finally:
            self.task.cancel()

    def install(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.request)
                self.installed.append(sig)
            except NotImplementedError: pass

    def close(self):
        if self.drain_task and not self.drain_task.done(): self.drain_task.cancel()
        for sig in self.installed: asyncio.get_running_loop().remove_signal_handler(sig)
