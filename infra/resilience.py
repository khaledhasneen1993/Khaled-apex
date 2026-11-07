"""Retry classification never includes ambiguous executions or invalid state."""
import asyncio
import httpx
from infra.circuit_breaker import CircuitBreakerOpen
from exchange.orders import ExchangeError, ExecutionUnknown

def transient_error(error):
    return isinstance(error, (CircuitBreakerOpen, httpx.TransportError, asyncio.TimeoutError, ConnectionError)) or (
        isinstance(error, ExchangeError) and not isinstance(error, ExecutionUnknown) and
        (error.status in (0, 418, 429) or error.status >= 500))
