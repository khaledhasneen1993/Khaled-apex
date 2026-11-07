"""Reconnect policy shared by the existing candle scanner."""
import random
import time

class ResilientWebSocket:
    def __init__(self, cfg, *, clock=time.monotonic):
        self.cfg, self.clock = cfg, clock
        self.connection_lost_at = None
        self.reconnect_attempts = 0

    def connected(self):
        self.connection_lost_at = None
        self.reconnect_attempts = 0

    def disconnected(self):
        if self.connection_lost_at is None: self.connection_lost_at = self.clock()
        self.reconnect_attempts += 1
        cap = min(self.cfg['reconnect_max_delay_sec'], 2 ** min(self.reconnect_attempts, 16))
        return random.uniform(cap / 2, cap)

    def critical(self):
        return self.connection_lost_at is not None and self.clock() - self.connection_lost_at >= self.cfg['hard_offline_sec']
