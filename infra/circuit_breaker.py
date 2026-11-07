"""Single-probe API breaker. Never retries a financial mutation."""
import asyncio
import time
from enum import Enum

class BreakerState(Enum):
    CLOSED = 'closed'
    OPEN = 'open'
    HALF_OPEN = 'half_open'

class CircuitBreakerOpen(RuntimeError): pass

class CircuitBreaker:
    def __init__(self, cfg, *, clock=time.monotonic, failure_predicate=None):
        self.cfg, self.clock = cfg, clock
        self.failure_predicate = failure_predicate or (lambda error: True)
        self.state = BreakerState.CLOSED
        self.failures = self.failed_probes = 0
        self.opened_at = 0.
        self._probe = False

    async def call(self, coro_fn):
        if self.state == BreakerState.OPEN:
            if self.clock() - self.opened_at < self.cfg['cooldown_sec']:
                raise CircuitBreakerOpen()
            self.state = BreakerState.HALF_OPEN
        probe = self.state == BreakerState.HALF_OPEN
        if probe and self._probe: raise CircuitBreakerOpen()
        if probe: self._probe = True
        try:
            result = await coro_fn()
        except asyncio.CancelledError:
            if probe:
                self.state = BreakerState.OPEN
                self.opened_at = self.clock()
            raise
        except Exception as error:
            if not self.failure_predicate(error):
                self.state = BreakerState.CLOSED
                self.failures = self.failed_probes = 0
                raise
            self.failures += 1
            if probe: self.failed_probes += 1
            if probe or self.failures >= self.cfg['failure_threshold']:
                self.state = BreakerState.OPEN
                self.opened_at = self.clock()
            raise
        else:
            self.failures = self.failed_probes = 0
            self.state = BreakerState.CLOSED
            return result
        finally:
            if probe: self._probe = False
