"""Process liveness is separate from market connectivity.

A responsive component may report degraded network health without being hung.
The injected exit callback permits deterministic offline resource tests.
"""
import asyncio
import os
import time
import logging
import psutil
from pathlib import Path

class Watchdog:
    def __init__(self, cfg, *, clock=time.monotonic, exit_fn=os._exit, process=None):
        self.cfg, self.clock, self.exit_fn = cfg, clock, exit_fn
        self.process = process or psutil.Process(int(Path('/proc/self/stat').read_text().split()[0]))
        self.last_heartbeats = {}

    def heartbeat(self, component): self.last_heartbeats[component] = self.clock()

    def check(self):
        now = self.clock()
        for component, ts in self.last_heartbeats.items():
            if now - ts > self.cfg['component_timeout_sec'][component]:
                logging.getLogger('CASCADE').critical('watchdog_component_timeout component=%s age=%.3f', component, now - ts)
                self.exit_fn(2)
                return 2
        if self.process.memory_info().rss > self.cfg['max_memory_gb'] * 1e9:
            logging.getLogger('CASCADE').critical('watchdog_memory_limit')
            self.exit_fn(3)
            return 3
        if hasattr(self.process, 'num_fds') and self.process.num_fds() > self.cfg['max_fds']:
            logging.getLogger('CASCADE').critical('watchdog_fd_limit')
            self.exit_fn(4)
            return 4
        return 0

    async def run(self):
        while True:
            await asyncio.sleep(self.cfg['watchdog_check_interval_sec'])
            self.check()
