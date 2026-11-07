import asyncio
import random
import time
from risk.config import load_config


class TokenBucket:
    """Cold-start empty; burst capacity accumulates during idle time."""
    def __init__(self, rate_per_sec: float, burst: int):
        if rate_per_sec <= 0 or burst < 1:
            raise ValueError('Invalid rate limits')
        self.rate, self.capacity = rate_per_sec, burst
        self.tokens = 0.
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self, weight=1):
        if not 0 < weight <= self.capacity:
            raise ValueError('Request weight exceeds bucket capacity')
        while True:
            async with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= weight:
                    self.tokens -= weight
                    return
                wait = (weight - self.tokens) / self.rate
            await asyncio.sleep(wait)


async def with_backoff(coro_fn, max_retries=None, base=None):
    """For idempotent reads only. Order submission has its own adoption flow."""
    cfg = load_config()['exchange']
    retries = cfg['max_retries'] if max_retries is None else max_retries
    base = cfg['backoff_base'] if base is None else base
    for attempt in range(retries):
        try:
            return await coro_fn()
        except Exception as error:
            status = getattr(error, 'status', getattr(getattr(error, 'response', None), 'status_code', 0))
            transient = bool(getattr(error, 'transport_error_type', None) or getattr(error, 'protocol_error', False))
            if not transient and status not in (429, 418) and not 500 <= status < 600:
                raise
            if attempt == retries - 1:
                raise
            retry_after = getattr(error, 'retry_after', 0)
            delay = min(cfg['max_backoff_sec'], base ** attempt + random.uniform(0, cfg['backoff_jitter_sec']))
            await asyncio.sleep(max(delay, retry_after))
    raise RuntimeError('Invalid retry configuration')
