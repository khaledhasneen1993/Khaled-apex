"""At-most-once intent handling. An uncertain POST is never blindly retried."""
import asyncio
import uuid
from .quantity import OrderValidationError

TERMINAL_ORDER_STATUSES = {'FILLED', 'EXPIRED', 'CANCELED', 'REJECTED', 'EXPIRED_IN_MATCH'}


class ExchangeError(RuntimeError):
    def __init__(self, status=0, code=0, retry_after=0):
        self.status, self.code, self.retry_after = status, code, retry_after
        super().__init__(f'Exchange request failed (HTTP {status}, code {code})')


class OrderRejected(ExchangeError):
    """POST definitively rejected and the same client ID not found by query."""


class ExecutionUnknown(ExchangeError):
    pass


class FillPriceUnavailable(ExecutionUnknown):
    """Filled quantity is known; its actual average price is still unavailable."""


async def safe_create_order(exchange, order_params, journal):
    params = dict(order_params)
    # A pending or observed-but-unsettled POST is never permission to send again.
    for intent in getattr(exchange, 'intents', {}).values():
        if intent['symbol'] == params['symbol'] and intent['type'] == params['type'] and intent['status'] == 'pending':
            raise ExecutionUnknown()
    options = dict(params.get('params') or {})
    # ProtectionRecovery owns the bounded conditional retry budget. Each
    # definitive rejection here returns to that loop; market retry rules stay.
    attempts = 1 if params['type'] in ('STOP_MARKET', 'TAKE_PROFIT_MARKET') else exchange.cfg['exchange']['order_attempts']
    for attempt in range(attempts):
        client_id = exchange.cfg['exchange']['client_id_prefix'] + uuid.uuid4().hex
        options['newClientOrderId'] = client_id
        params['params'] = options
        exchange.persist_intent(client_id, params)
        try:
            result = await exchange.submit_order(**params)
            exchange.resolve_intent(client_id, result)
            return result
        except OrderValidationError:
            exchange.reject_intent(client_id)
            raise
        except (ExchangeError, OSError, asyncio.TimeoutError) as error:
            existing = None
            definitive_absence = False
            try:
                for _ in range(exchange.cfg['exchange']['order_poll_attempts']):
                    existing = await exchange.query_order_by_client_id(params['symbol'], client_id)
                    if existing:
                        break
                    definitive_absence = True
                    await asyncio.sleep(exchange.cfg['exchange']['order_poll_sec'])
            except Exception:
                # Query failure cannot prove the POST did not execute.
                raise ExecutionUnknown() from None
            if existing:
                journal.log('order_adopted', symbol=params['symbol'], client_id=client_id)
                exchange.resolve_intent(client_id, existing)
                return existing
            # Only definite exchange rejection can justify a fresh order ID.
            status = getattr(error, 'status', 0)
            code = getattr(error, 'code', 0)
            rejected = status in (418, 429) or (400 <= status < 500 and status != 408 and code not in (-1006, -1007, -4111))
            if not definitive_absence or not rejected:
                raise ExecutionUnknown() from None
            exchange.reject_intent(client_id)
            if attempt + 1 == attempts or status not in (418, 429):
                journal.log('order_failed', symbol=params['symbol'], code=code, status=status)
                raise OrderRejected(status, code) from None
            await asyncio.sleep(max(getattr(error, 'retry_after', 0), exchange.cfg['exchange']['backoff_base'] ** attempt))
    raise ExecutionUnknown()
