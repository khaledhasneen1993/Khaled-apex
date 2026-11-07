"""Binance USD-M execution. Existing PaperExchange call signatures are retained.

All signed requests use environment credentials; all HTTP errors are sanitized.
Native conditional protection uses the Futures Algo API. Requires one-way mode
and a dedicated single-asset USDT-M account. Mainnet is gated before construction.
"""
import asyncio
import base64
import copy
import hashlib
import hmac
import json
import math
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_FLOOR, ROUND_CEILING
from pathlib import Path
from urllib.parse import urlencode

import httpx

from paper_account import PaperExchange
from infra.journal import OperationalJournal, atomic_json
from infra.http_logging import secure_http_logging
from infra.rate_limit import TokenBucket, with_backoff
from journal.wal import WriteAheadLog, load_durable
from infra.circuit_breaker import CircuitBreaker
from .orders import ExchangeError, ExecutionUnknown
from .execution import DurableExecution
from .management import ProtectionRecovery
from .protection_state import NativeExitState, ProtectionGuardError
from .quantity import quantity_rules, validate_quantity, OrderValidationError


class LiveExchange(DurableExecution, NativeExitState, ProtectionRecovery, PaperExchange):
    def __init__(self, cfg: dict, rate_limiter):
        self.cfg = cfg
        self.mode = cfg['exchange']['mode']
        if self.mode not in ('testnet', 'mainnet'):
            raise ValueError('LiveExchange requires testnet or mainnet mode')
        if self.mode == 'mainnet':
            from .rollout import require_mainnet_readiness
            require_mainnet_readiness(cfg)
        if self.mode == 'testnet':
            from .demo import validate_demo
            validate_demo(cfg)
        key, secret = os.getenv('BINANCE_API_KEY'), os.getenv('BINANCE_API_SECRET')
        rsa_path = os.getenv('BINANCE_RSA_PRIVATE_KEY_PATH')
        if not key or bool(secret) == bool(rsa_path):
            raise ValueError('Set API key and exactly one HMAC secret or RSA private key path locally')
        if any(not value.isprintable() or any(char.isspace() for char in value) for value in (key, secret or 'unused')):
            raise ValueError('API credentials contain invalid whitespace or control characters')
        self._rsa = None
        if rsa_path:
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
            try:
                path = Path(rsa_path)
                if path.stat().st_mode & 0o077:
                    raise ValueError()
                self._rsa = load_pem_private_key(path.read_bytes(), password=None)
                if not isinstance(self._rsa, RSAPrivateKey): raise ValueError()
            except Exception:
                raise ValueError('RSA private key must be a readable RSA PEM with mode 0600') from None
        self.allocation_fraction = 1.
        if self.mode == 'mainnet':
            from .rollout import mainnet_allocation_fraction
            self.allocation_fraction = mainnet_allocation_fraction(cfg)
        self._secret = secret
        ec = cfg['exchange']
        secure_http_logging()
        self.client = httpx.AsyncClient(base_url=ec['base_url_' + self.mode],
            headers={'X-MBX-APIKEY': key}, timeout=ec['http_timeout_sec'], follow_redirects=False)
        self.rl = rate_limiter
        self.weights = TokenBucket(ec['weight_per_minute'] / 60, ec['weight_per_minute'])
        self.orders_short = TokenBucket(ec['orders_per_10sec'] / 10, ec['orders_per_10sec'])
        self.orders_long = TokenBucket(ec['orders_per_minute'] / 60, ec['orders_per_minute'])
        self.market_data = self
        self.fee_rate = cfg['fee_rate']
        self.leverage = cfg['leverage']
        self.balance = self._equity = self._free = self._used = 0.
        self.positions, self.prices, self.markets = {}, {}, {}
        self.trades = []
        self.clock_offset_ms = 0
        self._last_sync = 0.
        self._blocked_until = 0.
        self._leverage_set = set()
        self._leverage_lock = asyncio.Lock()
        self.journal = OperationalJournal(Path(ec['state_dir']) / self.mode / 'journal')
        self.intent_path = Path(ec['state_dir']) / self.mode / 'order_intents.json'
        self.intents = load_durable(self.intent_path)
        self.breaker = CircuitBreaker(cfg['circuit_breaker'], failure_predicate=lambda error:
            isinstance(error, (httpx.HTTPError, asyncio.TimeoutError)) or
            isinstance(error, ExchangeError) and (error.status in (0, 418, 429) or error.status >= 500))
        self.wal = WriteAheadLog(self.intent_path.parent / 'journal.wal')
        self.on_api_error = None
        self.pending_protection = {}
        self.selected_leverage = {}
        self.brackets = {}
        self.execution_path = self.intent_path.parent / 'executions.json'
        self.executions = load_durable(self.execution_path)
        protection_path = self.intent_path.parent / 'protection.json'
        self.pending_protection = load_durable(protection_path)
        self.management_path = self.intent_path.parent / ec['management_filename']
        self.management_faults = load_durable(self.management_path)
        self.execution_fault = None
        self.emergency_mode = False
        self.storage_fault = False

    def write_state(self, path, value):
        path = Path(path)
        digest = hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).digest()
        if not hasattr(self, '_state_digests'): self._state_digests = {}
        if self._state_digests.get(path) == digest and path.exists(): return
        try:
            self.wal.snapshot(path, value, writer=atomic_json)
        except OSError:
            self.storage_fault = True
            self.execution_fault = 'storage_failure'
            if not self.emergency_mode: raise
            # Stable IDs remain in memory during emergency reducing operations;
            # disk durability is impossible until storage recovers.
        else:
            self._state_digests[path] = digest

    def clear_active_protection(self, symbol, signal_id=None):
        pos = self.pending_protection.get(symbol)
        if pos and (signal_id is None or pos.get('signal_id') == signal_id):
            self.pending_protection.pop(symbol)
            self.write_state(self.intent_path.parent / 'protection.json', self.pending_protection)

    @property
    def equity(self): return self._equity
    @property
    def free(self): return self._free
    @property
    def used(self): return self._used

    def persist_intent(self, client_id, params):
        self.intents[client_id] = {'symbol': params['symbol'], 'type': params['type'],
            'side': params['side'], 'amount': params['amount'],
            'params': {k: v for k, v in params['params'].items() if k in
                       ('reduceOnly', 'expected_price', 'exit_atr', 'atr_source', 'stopPrice', 'round_up', 'newClientOrderId', 'signal_id', 'leverage', 'risk_cfg', 'equity', 'brackets', 'exit_reason', 'exit_position', 'protection_role', 'replaces', 'protection_transport', 'emergency')},
            'status': 'pending'}
        self.write_state(self.intent_path, self.intents)
        logged = self.journal.log('order_intent_saved', stage='before_post', symbol=params['symbol'], client_order_id=client_id, recovery_action='query_same_id')
        if logged is False and not self.emergency_mode:
            self.storage_fault = True
            self.execution_fault = 'storage_failure'
            raise OSError('Operational journal unavailable before submission')

    def resolve_intent(self, client_id, result):
        intent = self.intents[client_id]
        algo = intent['type'] in ('STOP_MARKET', 'TAKE_PROFIT_MARKET') and intent.get('params', {}).get('protection_transport') != 'regular'
        identity, client_key = ('algoId', 'clientAlgoId') if algo else ('orderId', 'clientOrderId')
        if (not isinstance(result, dict) or not str(result.get(identity, '')).isdigit()
                or int(result[identity]) <= 0 or result.get(client_key) != client_id):
            raise ExecutionUnknown()
        # Preserve safe order facts at each change. A later successful query must
        # not erase the first response needed to investigate an incident.
        keys = ('orderId', 'clientOrderId', 'algoId', 'clientAlgoId', 'symbol', 'side',
                'status', 'algoStatus', 'type', 'orderType', 'origQty', 'executedQty',
                'cumQuote', 'avgPrice', 'reduceOnly', 'quantity', 'triggerPrice',
                'actualOrderId', 'workingType', 'stopPrice', 'time', 'updateTime', 'createTime')
        def snapshot(value): return {k: value[k] for k in keys if k in value}
        observations = intent.setdefault('observations', [])
        if not observations and intent.get('response'):
            observations.append({'observed_at': None, 'stage': 'legacy_last_snapshot',
                                 'order': snapshot(intent['response'])})
        current = snapshot(result)
        if not observations or observations[-1]['order'] != current:
            observations.append({'observed_at': datetime.now(timezone.utc).isoformat(),
                'stage': getattr(self, 'execution_stage', 'order_query_or_response'), 'order': current})
        self.intents[client_id]['status'] = 'observed'
        self.intents[client_id]['order_id'] = result.get('orderId', result.get('algoId'))
        self.intents[client_id]['response'] = result
        self.write_state(self.intent_path, self.intents)
        self.journal.log('order_observed', stage='order_query_or_response', symbol=self.intents[client_id]['symbol'], client_order_id=client_id, order_id=self.intents[client_id]['order_id'])

    def reject_intent(self, client_id):
        self.intents[client_id]['status'] = 'rejected'
        self.write_state(self.intent_path, self.intents)

    def _sign(self, params):
        value = {**params, 'timestamp': int(time.time() * 1000 + self.clock_offset_ms),
                 'recvWindow': self.cfg['exchange']['recv_window_ms']}
        query = urlencode(value)
        if self._rsa:
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import padding
            signature = base64.b64encode(self._rsa.sign(query.encode(), padding.PKCS1v15(), hashes.SHA256())).decode()
        else:
            signature = hmac.new(self._secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return {**value, 'signature': signature}

    async def _request(self, method, path, params=None, signed=True, retry=False):
        if method != 'GET' and hasattr(self, 'wal'):
            try:
                self.wal.log({'event': 'financial_request', 'method': method, 'path': path, 'params': params or {}})
            except OSError:
                self.storage_fault = True
                self.execution_fault = 'storage_failure'
                raise
        clock_recovered = False
        async def once():
            nonlocal clock_recovered
            delay = self._blocked_until - time.monotonic()
            if delay > 0: await asyncio.sleep(delay)
            await self.rl.acquire()
            weight = self.cfg['exchange']['http_weights'].get(path, self.cfg['exchange']['http_weights']['default'])
            await self.weights.acquire(weight)
            if method == 'POST' and path in ('/fapi/v1/order', '/fapi/v1/algoOrder'):
                await self.orders_short.acquire()
                await self.orders_long.acquire()
            values = self._sign(params or {}) if signed else (params or {})
            try:
                response = await self.client.request(method, path, params=values)
            except httpx.TransportError as error:
                if self.on_api_error: self.on_api_error()
                sanitized = ExchangeError()
                sanitized.transport_error_type = type(error).__name__
                raise sanitized from None
            try:
                data = response.json()
            except ValueError:
                sanitized = ExchangeError(response.status_code)
                sanitized.protocol_error = True
                if self.on_api_error: self.on_api_error()
                raise sanitized from None
            if response.status_code >= 400 or isinstance(data, dict) and int(data.get('code', 0)) < 0:
                code = data.get('code', 0) if isinstance(data, dict) else 0
                # Clock recovery is a GET-only operation. A mutating request is
                # always recovered by its durable intent, never retried here.
                if code == -1021 and signed and method == 'GET' and not clock_recovered:
                    clock_recovered = True
                    await self.sync_time()
                    return await once()
                expected_limit = path == '/fapi/v1/leverage' and code in self.cfg['exchange']['leverage_rejection_codes']
                local_protection_rejection = (method == 'POST' and path in ('/fapi/v1/algoOrder', '/fapi/v1/order')
                    and (params or {}).get('type') in ('STOP_MARKET', 'TAKE_PROFIT_MARKET')
                    and response.status_code == 400 and code in self.cfg['exchange']['protection_rejection_codes'])
                if self.on_api_error and not expected_limit and not local_protection_rejection and code not in self.cfg['exchange']['non_incident_error_codes']:
                    self.on_api_error()
                try: after = float(response.headers.get('Retry-After', 0))
                except ValueError: after = self.cfg['exchange']['max_backoff_sec']
                if response.status_code in (418, 429):
                    after = max(after, self.cfg['exchange']['max_backoff_sec'] if response.status_code == 418 else self.cfg['exchange']['backoff_base'])
                    self._blocked_until = max(self._blocked_until, time.monotonic() + after)
                raise ExchangeError(response.status_code, data.get('code', 0) if isinstance(data, dict) else 0, after)
            return data
        async def guarded_once():
            if method == 'GET' and not self.emergency_mode and hasattr(self, 'breaker'):
                return await self.breaker.call(once)
            return await once()
        # Bound a whole GET including rate-limit waits/backoff, so network
        # outages do not masquerade as a hung order/reconciliation component.
        if method == 'GET':
            async with asyncio.timeout(self.cfg['circuit_breaker']['request_budget_sec']):
                return await with_backoff(guarded_once, max_retries=self.cfg['exchange']['max_retries'],
                    base=self.cfg['exchange']['backoff_base']) if retry else await guarded_once()
        return await guarded_once()

    async def sync_time(self):
        start = time.time() * 1000
        server = await self._request('GET', '/fapi/v1/time', signed=False, retry=True)
        end = time.time() * 1000
        self.clock_offset_ms = server['serverTime'] - (start + end) / 2
        self._last_sync = time.monotonic()

    async def load_markets(self):
        await self.sync_time()
        data = await self._request('GET', '/fapi/v1/exchangeInfo', signed=False, retry=True)
        for raw in data['symbols']:
            if raw.get('contractType') != 'PERPETUAL' or raw.get('quoteAsset') != 'USDT':
                continue
            symbol = raw['baseAsset'] + '/USDT:USDT'
            filters = {f['filterType']: f for f in raw['filters']}
            bounds = quantity_rules(filters)
            self.markets[symbol] = {'id': raw['symbol'], 'symbol': symbol, 'linear': True, 'swap': True,
                'settle': raw['marginAsset'], 'order_types': raw.get('orderTypes', []), 'active': raw['status'] == 'TRADING', 'filters': filters,
                'step': str(bounds['step']), 'tick': filters['PRICE_FILTER']['tickSize'],
                'limits': {'amount': {'min': float(bounds['min']), 'max': float(bounds['max'])},
                           'cost': {'min': float(filters.get('MIN_NOTIONAL', {}).get('notional', 0))}}}
        # Do not silently change global account position/multi-asset modes.
        hedge = await self._request('GET', '/fapi/v1/positionSide/dual', retry=True)
        multi = await self._request('GET', '/fapi/v1/multiAssetsMargin', retry=True)
        if hedge['dualSidePosition'] or multi['multiAssetsMargin']:
            raise ValueError('Dedicated one-way, single-asset Futures account required')
        await self.refresh_account()
        return self.markets

    def market(self, symbol):
        if symbol in self.markets:
            return self.markets[symbol]
        return next(m for m in self.markets.values() if m['id'] == symbol)

    async def fetch_market(self, symbol): return self.market(symbol)
    def symbol(self, raw): return self.market(raw)['symbol']
    def raw(self, symbol): return self.market(symbol)['id']

    def amount_to_precision(self, symbol, amount):
        step = Decimal(self.market(symbol)['step'])
        return format((Decimal(str(amount)) / step).to_integral_value(rounding=ROUND_DOWN) * step, 'f')

    def _price(self, symbol, price, round_up=False):
        tick = Decimal(self.market(symbol)['tick'])
        return format((Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_UP if round_up else ROUND_DOWN) * tick, 'f')

    async def fetch_ohlcv(self, symbol, timeframe, limit):
        rows = await self._request('GET', '/fapi/v1/klines', {'symbol': self.raw(symbol), 'interval': timeframe, 'limit': limit}, signed=False, retry=True)
        return [[int(r[0]), *map(float, r[1:6])] for r in rows]

    async def fetch_order_book(self, symbol, limit):
        data = await self._request('GET', '/fapi/v1/depth', {'symbol': self.raw(symbol), 'limit': limit}, signed=False, retry=True)
        return {'bids': [[float(p), float(q)] for p, q in data['bids']], 'asks': [[float(p), float(q)] for p, q in data['asks']]}

    async def fetch_ticker(self, symbol):
        result = await self._request('GET', '/fapi/v1/ticker/price', {'symbol': self.raw(symbol)}, signed=False, retry=True)
        price = float(result['price'])
        if not math.isfinite(price) or price <= 0: raise ValueError('Invalid ticker')
        self.prices[symbol] = price
        return {'last': price}

    async def fetch_tickers(self):
        values = await self._request('GET', '/fapi/v1/ticker/24hr', signed=False, retry=True)
        supported = {m['id']: s for s, m in self.markets.items()}
        return {supported[t['symbol']]: {'quoteVolume': float(t['quoteVolume'])} for t in values if t['symbol'] in supported}

    async def refresh_account(self):
        if time.monotonic() - self._last_sync > self.cfg['exchange']['time_sync_interval_sec']:
            await self.sync_time()
        data = await self._request('GET', '/fapi/v3/account', retry=True)
        # Restrict accounting to the USDT collateral asset.
        asset = next(a for a in data['assets'] if a['asset'] == 'USDT')
        values = [float(asset[k]) for k in ('walletBalance', 'marginBalance', 'availableBalance', 'initialMargin')]
        if not all(math.isfinite(v) for v in values): raise ValueError('Invalid Binance balance snapshot')
        remote = await self.get_position(None)
        positions = {self.symbol(p['symbol']): {'side': 'buy' if float(p['positionAmt']) > 0 else 'sell',
            'entry': float(p['entryPrice']), 'amount': abs(float(p['positionAmt'])),
            'margin': float(p.get('positionInitialMargin', 0)), 'remote': p}
            for p in remote if float(p['positionAmt']) != 0}
        if any(not all(math.isfinite(p[k]) and p[k] > 0 for k in ('entry', 'amount')) for p in positions.values()):
            raise ValueError('Invalid Binance position snapshot')
        # Commit a complete pair of reads; a failed position read must not leave
        # new balances beside positions from the previous observation.
        self.balance, self._equity, self._free, self._used = values
        self.positions = positions
        return data

    async def fetch_balance(self):
        await self.refresh_account()
        return {'USDT': {'free': self.free, 'used': self.used, 'total': self.balance, 'equity': self.equity}}

    async def leverage_brackets(self, symbol):
        data = await self._request('GET', '/fapi/v1/leverageBracket', {'symbol': self.raw(symbol)}, retry=True)
        row = data[0] if isinstance(data, list) else data
        self.brackets[symbol] = row['brackets']
        return row['brackets']

    async def select_leverage(self, symbol, margin, price, confirm=True):
        from .leverage import choose_leverage, bracket_for
        from risk.trading_policy import entry_amount
        brackets = await self.leverage_brackets(symbol)
        fixed = self.cfg['exchange'].get('leverage_policy') == 'fixed'
        equity = margin / self.cfg['margin_fraction_per_position']
        round_amount = lambda q: float(self.amount_to_precision(symbol, q))
        ceiling = None
        while True:
            choice = {'leverage': self.cfg['leverage'], 'confirmed': False} if fixed else choose_leverage(
                brackets, margin, price, round_amount, ceiling)
            choice['amount'] = entry_amount(equity, price, choice['leverage'], self.cfg, self.fee_rate, round_amount)
            choice.update(notional=choice['amount'] * price, margin=choice['amount'] * price / choice['leverage'])
            choice['bracket'] = bracket_for(brackets, choice['notional'])
            if choice['leverage'] > int(choice['bracket']['initialLeverage']):
                raise OrderValidationError('configured_leverage_unavailable')
            market = self.market(symbol)
            choice['quantity_limits'] = dict(market['limits']['amount'])
            try:
                validate_quantity(market, choice['amount'], price)
            except OrderValidationError as error:
                choice.update(entry_order_valid=False, validation_reason=str(error))
                if confirm:
                    self.journal.log('entry_validation_blocked', symbol=symbol, stage='before_leverage_and_post',
                        reason=str(error), amount=choice['amount'], leverage=choice['leverage'],
                        min_qty=market['limits']['amount'].get('min'), max_qty=market['limits']['amount'].get('max'),
                        recovery_action='skip_entry_preserve_margin_and_leverage_policy')
                break
            choice['entry_order_valid'] = True
            if not confirm: break
            try:
                maximum = await self.set_leverage(choice['leverage'], symbol)
            except ExchangeError as error:
                # Definitive leverage/account limits only. Transport uncertainty
                # never authorizes an order or a silent guessed leverage.
                if fixed or error.code not in self.cfg['exchange']['leverage_rejection_codes'] or choice['leverage'] <= 1: raise
                ceiling = choice['leverage'] - 1
                continue
            if choice['notional'] > maximum:
                if fixed or choice['leverage'] <= 1: raise ValueError('No accepted leverage/notional combination')
                ceiling = choice['leverage'] - 1
                continue
            choice['confirmed'] = True
            break
        choice.update(leverage_policy=self.cfg['exchange']['leverage_policy'], target_margin=margin,
                      max_equity_risk_pct=self.cfg['tp_sl']['max_equity_risk_pct'])
        if confirm and choice.get('confirmed'):
            self.journal.log('entry_sizing', symbol=symbol, stage='confirmed_sizing',
                amount=choice['amount'], leverage=choice['leverage'], report={
                    'equity': equity, 'target_margin': margin, 'actual_margin': choice['margin'],
                    'notional': choice['notional'], 'risk_budget': equity * self.cfg['tp_sl']['max_equity_risk_pct']})
        self.selected_leverage[symbol] = choice
        return choice

    async def set_leverage(self, leverage, symbol):
        async with self._leverage_lock:
            try:
                await self._request('POST', '/fapi/v1/marginType', {'symbol': self.raw(symbol), 'marginType': 'ISOLATED'})
            except ExchangeError as error:
                if error.code != -4046: raise
            result = await self._request('POST', '/fapi/v1/leverage', {'symbol': self.raw(symbol), 'leverage': leverage})
            if int(result['leverage']) != leverage: raise ValueError('Leverage was not confirmed')
            maximum = float(result.get('maxNotionalValue', 'inf'))
            self._leverage_set.add(symbol)
            return maximum

    async def get_position(self, symbol):
        return await self._request('GET', '/fapi/v3/positionRisk', {'symbol': self.raw(symbol)} if symbol else {}, retry=True)

    async def get_open_orders(self, symbol=None):
        params = {'symbol': self.raw(symbol)} if symbol else {}
        normal = await self._request('GET', '/fapi/v1/openOrders', params, retry=True)
        algo = await self._request('GET', '/fapi/v1/openAlgoOrders', params, retry=True)
        return normal + [{**o, 'is_algo': True, 'clientOrderId': o['clientAlgoId'], 'orderId': o['algoId']} for o in algo]

    async def cancel_order(self, symbol, order_id=None, client_id=None):
        return await self._request('DELETE', '/fapi/v1/order', {'symbol': self.raw(symbol),
            **({'orderId': order_id} if order_id is not None else {'origClientOrderId': client_id})})

    async def cancel_algo(self, order):
        return await self._request('DELETE', '/fapi/v1/algoOrder', {'algoId': order['orderId']})

    async def query_order_by_client_id(self, symbol, client_id):
        try:
            if (self.intents.get(client_id, {}).get('type') in ('STOP_MARKET', 'TAKE_PROFIT_MARKET')
                    and self.intents[client_id].get('params', {}).get('protection_transport') != 'regular'):
                return await self._request('GET', '/fapi/v1/algoOrder', {'clientAlgoId': client_id}, retry=True)
            return await self._request('GET', '/fapi/v1/order', {'symbol': self.raw(symbol), 'origClientOrderId': client_id}, retry=True)
        except ExchangeError as error:
            if error.code == -2013: return None
            raise

    async def submit_order(self, symbol, type, side, amount, params=None):
        options = params or {}
        client_id = options['newClientOrderId']
        quantity = self.amount_to_precision(symbol, amount)
        validate_quantity(self.market(symbol), quantity)
        if type in ('STOP_MARKET', 'TAKE_PROFIT_MARKET') and options.get('protection_transport') == 'regular':
            return await self._request('POST', '/fapi/v1/order', {'symbol': self.raw(symbol), 'side': side.upper(),
                'type': type, 'quantity': quantity, 'reduceOnly': 'true', 'newClientOrderId': client_id,
                'stopPrice': self._price(symbol, options['stopPrice'], options['round_up']),
                'workingType': self.cfg['exchange']['working_type']})
        if type in ('STOP_MARKET', 'TAKE_PROFIT_MARKET'):
            values = {'algoType': 'CONDITIONAL', 'symbol': self.raw(symbol), 'side': side.upper(), 'type': type,
                'quantity': quantity, 'reduceOnly': 'true', 'clientAlgoId': client_id,
                'triggerPrice': self._price(symbol, options['stopPrice'], options['round_up']),
                'workingType': self.cfg['exchange']['working_type']}
            return await self._request('POST', '/fapi/v1/algoOrder', values)
        if type != 'market': raise ValueError('Runtime only uses market and protective conditional orders')
        return await self._request('POST', '/fapi/v1/order', {'symbol': self.raw(symbol), 'side': side.upper(),
            'type': 'MARKET', 'quantity': quantity, 'newClientOrderId': client_id, 'newOrderRespType': 'RESULT',
            **({'reduceOnly': 'true'} if options.get('reduceOnly') else {})})

    async def protect(self, symbol, pos):
        await self.protect_with_recovery(symbol, pos)

    def verify_protection(self, symbol, pos, name, confirmed, client_id, order_id, trigger, transport="algo"):
        kind = 'STOP_MARKET' if name == 'sl' else 'TAKE_PROFIT_MARKET'
        side = 'SELL' if pos['side'] == 'buy' else 'BUY'
        if transport == 'regular':
            confirmed = ({**confirmed, 'algoStatus': confirmed.get('status'), 'clientAlgoId': confirmed.get('clientOrderId'),
                'algoId': confirmed.get('orderId'), 'orderType': confirmed.get('type'),
                'quantity': confirmed.get('origQty'), 'triggerPrice': confirmed.get('stopPrice')} if confirmed else None)
        if (not confirmed or confirmed.get('algoStatus') != 'NEW'
                or confirmed.get('clientAlgoId') != client_id
                or str(confirmed.get('algoId')) != str(order_id)
                or confirmed.get('symbol') != self.raw(symbol)
                or confirmed.get('side') != side or confirmed.get('orderType') != kind
                or confirmed.get('reduceOnly') not in (True, 'true')
                or Decimal(str(confirmed.get('quantity', 0))) != Decimal(self.amount_to_precision(symbol, pos['amount']))
                or confirmed.get('workingType', self.cfg['exchange']['working_type']) != self.cfg['exchange']['working_type']
                or Decimal(str(confirmed.get('triggerPrice', 0))) != Decimal(trigger)):
            raise ExecutionUnknown()

    async def commit_protection(self, symbol, pos, name, confirmed, trigger, old, transport="algo"):
        client_id = confirmed['clientAlgoId'] if transport == 'algo' else confirmed['clientOrderId']
        pos.setdefault('protection', {})[name] = {'orderId': confirmed['algoId'] if transport == 'algo' else confirmed['orderId'],
            'clientOrderId': client_id, 'trigger': trigger, 'is_algo': transport == 'algo', 'amount': pos['amount']}
        self.pending_protection[symbol] = pos
        self.write_state(self.intent_path.parent / 'protection.json', self.pending_protection)
        for record in self.executions.values():
            if record['symbol'] == symbol and record.get('position', {}).get('signal_id') == pos['signal_id']:
                self.merge_strategy_position(record['position'], pos, protection=True)
        self._flush_executions()
        self.intents[client_id]['protection_committed'] = True
        self.write_state(self.intent_path, self.intents)
        # Every durable replacement reference is committed before DELETE.
        if old and old['clientOrderId'] != client_id:
            try:
                await self.cancel_protection(symbol, old)
                previous = await self.query_protection(symbol, old)
                if previous and previous.get('algoStatus', previous.get('status')) in ('NEW', 'TRIGGERING', 'PARTIALLY_FILLED'):
                    raise ExecutionUnknown()
            except ExchangeError:
                fault = self.management_fault(symbol, 'superseded_protection_cleanup_pending', pos)
                obsolete = fault.setdefault('obsolete', [])
                if not any(ref['clientOrderId'] == old['clientOrderId'] for _, ref in obsolete):
                    obsolete.append((name, copy.deepcopy(old)))
                self.write_state(self.management_path, self.management_faults)

    async def recover_protection_intents(self):
        unresolved = False
        for client_id, intent in list(self.intents.items()):
            if intent['type'] == 'market' or intent['status'] == 'rejected' or intent.get('protection_committed'):
                continue
            symbol, options = intent['symbol'], intent.get('params', {})
            pos = next((r['position'] for r in reversed(list(self.executions.values()))
                        if r['symbol'] == symbol and r.get('position') and
                        (not options.get('signal_id') or options['signal_id'] == r['position']['signal_id'])), None)
            known = pos and any(o['clientOrderId'] == client_id for o in pos.get('protection', {}).values())
            if known and intent['status'] == 'observed':
                intent['protection_committed'] = True
                self.write_state(self.intent_path, self.intents)
                continue
            confirmed = await self.query_order_by_client_id(symbol, client_id)
            if not confirmed:
                if self.management_blocked(symbol): continue
                unresolved = True
                continue
            self.resolve_intent(client_id, confirmed)
            active = any(float(p['positionAmt']) != 0 for p in await self.get_position(symbol))
            if not active or confirmed.get('algoStatus', confirmed.get('status')) in ('FINISHED', 'FILLED', 'CANCELED', 'EXPIRED', 'REJECTED'):
                # Native fills and flat-account orphan cleanup are reconciled by
                # their actual child order/remaining position, never a new POST.
                intent['protection_committed'] = True
                self.write_state(self.intent_path, self.intents)
                continue
            if not pos:
                unresolved = True
                continue
            name = options.get('protection_role') or ('sl' if intent['type'] == 'STOP_MARKET' else 'tp')
            trigger = self._price(symbol, options['stopPrice'], options['round_up'])
            self.verify_protection(symbol, pos, name, confirmed, client_id, confirmed.get('algoId', confirmed.get('orderId')), trigger, options.get('protection_transport', 'algo'))
            old = options.get('replaces') or pos.get('protection', {}).get(name)
            if name == 'sl' and old:
                sm = 1 if pos['side'] == 'buy' else -1
                if sm * (float(trigger) - float(old['trigger'])) < 0:
                    unresolved = True
                    continue
            await self.commit_protection(symbol, pos, name, confirmed, trigger, old, options.get('protection_transport', 'algo'))
        return not unresolved

    def validate_initial_protection(self, symbol, pos):
        entry = Decimal(str(pos['entry']))
        stop = Decimal(self._price(symbol, pos['sl'], pos['side'] == 'buy'))
        target = Decimal(self._price(symbol, pos['tp'], pos['side'] == 'buy')) if pos.get('tp') is not None else None
        sm = 1 if pos['side'] == 'buy' else -1
        if stop <= 0 or sm * (entry - stop) <= 0 or target is not None and (target <= 0 or sm * (target - entry) <= 0):
            raise OrderValidationError('protection_unavailable_at_price_tick')

    def constrain_actual_liquidation(self, symbol, pos, actual):
        """Tighten to Binance's actual boundary; never expand a loss budget."""
        self.execution_stage = 'liquidation_validation'
        from .fill_price import positive_decimal
        def fail(reason, **values):
            details = {'symbol': symbol, 'working_type': self.cfg['exchange']['working_type'],
                'stop_price': pos['sl'], 'leverage': pos['leverage'], **values}
            # Only selected numeric facts and fixed enums, never exception text.
            for record in self.executions.values():
                if record.get('position', {}).get('signal_id') == pos['signal_id']:
                    record.setdefault('guard_failures', []).append({'timestamp': datetime.now(timezone.utc).isoformat(),
                        'reason': reason, **details})
            self._flush_executions()
            self.journal.log('liquidation_guard_failed', symbol=symbol, stage=self.execution_stage,
                reason=reason, report=details, recovery_action='latch_and_close_remaining')
            raise ProtectionGuardError(reason, details)
        liquidation = positive_decimal(actual.get('liquidationPrice'))
        if liquidation is None: fail('invalid_liquidation_price')
        sm = 1 if pos['side'] == 'buy' else -1
        buffer = Decimal(str(self.cfg['tp_sl']['liq_safety_margin']))
        boundary = liquidation * (1 + sm * buffer)
        if positive_decimal(boundary) is None: fail('invalid_liquidation_price')
        mark = positive_decimal(actual.get('markPrice'))
        trigger = mark if self.cfg['exchange']['working_type'] == 'MARK_PRICE' else positive_decimal(actual.get('_trigger_price'))
        facts = dict(liquidation_price=float(liquidation), buffered_liquidation=float(boundary),
                     mark_price=float(mark) if mark is not None else None,
                     trigger_price=float(trigger) if trigger is not None else None)
        if mark is None: fail('invalid_mark_price', **facts)
        if sm * (mark - boundary) <= 0: fail('mark_liquidation_buffer_breach', **facts)
        tick = Decimal(self.market(symbol)['tick'])
        stop = Decimal(self._price(symbol, pos['sl'], sm == 1))
        if sm * (stop - boundary) <= 0:
            if pos.get('exit_policy') == 'hard_stop_trailing': fail('hard_stop_liquidation_conflict', **facts)
            rounding = ROUND_FLOOR if sm == 1 else ROUND_CEILING
            stop = ((boundary / tick).to_integral_value(rounding=rounding) + sm) * tick
            if trigger is None: fail('trigger_price_unavailable', **facts)
            if sm * (trigger - stop) <= 0: fail('no_valid_stop_before_trigger', candidate_stop=float(stop), **facts)
        if sm * (stop - Decimal(str(pos['sl']))) > 0:
            pos['sl'] = float(stop)
            pos['risk_capped'] = True
        pos['liquidation_price'] = float(liquidation)

    async def get_funding_rate(self, symbol):
        return await self._request('GET', '/fapi/v1/premiumIndex', {'symbol': self.raw(symbol)}, signed=False, retry=True)

    async def close(self):
        await self.client.aclose()
