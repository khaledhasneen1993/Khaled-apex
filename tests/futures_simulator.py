"""Strict, multi-symbol HTTP model for offline production-constructor tests.

Prices, fills, wallet and commissions are Decimal. This is a fault-injection
model, not a claim to reproduce Binance's matching or liquidation engine.
"""
import asyncio
import copy
import time
from datetime import datetime, timezone
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from infra.journal import atomic_json
from risk.config import load_config


class FuturesSimulator:
    def __init__(self, equity='4912.19949871'):
        self.wallet = D(equity)
        self.start_wallet = self.wallet
        self.specs = {
            'BTCUSDT': ('76000', '.001', '.10', 125, 50000, '.004'),
            'AIOUSDT': ('.05', '1', '.00001', 50, 50000, '.01'),
            'SOLUSDT': ('120', '.01', '.01', 75, 10000, '.0065'),
        }
        self.prices = {s: D(v[0]) for s, v in self.specs.items()}
        self.mark_prices = {}
        self.leverage, self.margin_type, self.positions = {}, {}, {}
        self.orders, self.algos, self.fills, self.requests = {}, {}, [], []
        self.fee = D('.0005')
        self.lose_entry_response = self.lose_close_response = False
        self.delay_accounting = False
        self.reject_role = None
        self.advertise_regular_stop = self.regular_stops = False
        self.partial_close = False

    def brackets(self, symbol):
        _, _, _, lev, cap, mmr = self.specs[symbol]
        return [dict(bracket=1, initialLeverage=lev, notionalFloor=0, notionalCap=cap,
                     maintMarginRatio=mmr, cum=0),
                dict(bracket=2, initialLeverage=20, notionalFloor=cap, notionalCap=1000000,
                     maintMarginRatio='.025', cum=str(D(cap) * (D('.025') - D(mmr))))]

    def position_rows(self, symbol=None):
        rows = []
        for s, pos in self.positions.items():
            if symbol and s != symbol: continue
            qty, entry = pos['qty'], pos['entry']
            mark = self.mark_prices.get(s, self.prices[s])
            if not qty: continue
            sign = 1 if qty > 0 else -1
            size = abs(qty)
            tier = next(b for b in self.brackets(s) if D(b['notionalFloor']) <= entry * size < D(b['notionalCap']))
            # Solve Q*P*MMR-cum = isolatedWallet + side*Q*(P-entry).
            isolated = entry * size / self.leverage[s] - entry * size * self.fee
            liq = (entry * size - sign * (isolated + D(tier['cum']))) / (size * (1 - sign * D(tier['maintMarginRatio'])))
            rows.append(dict(symbol=s, positionAmt=str(qty), entryPrice=str(entry),
                markPrice=str(mark), liquidationPrice=str(liq),
                positionInitialMargin=str(mark * size / self.leverage[s])))
        return rows

    def fill(self, p):
        s, side, qty = p['symbol'], p['side'], D(p['quantity'])
        reducing = p.get('reduceOnly') == 'true'
        price = self.prices[s]
        pos = self.positions.get(s)
        if reducing:
            assert pos and pos['qty'] and (pos['qty'] > 0) == (side == 'SELL')
            qty = min(qty, abs(pos['qty']))
            if self.partial_close:
                qty = (qty / 2 / D(self.specs[s][1])).to_integral_value(rounding='ROUND_DOWN') * D(self.specs[s][1])
                self.partial_close = False
                status = 'EXPIRED'
            else: status = 'FILLED'
            pnl = (price - pos['entry']) * qty * (1 if pos['qty'] > 0 else -1)
            pos['qty'] += qty if side == 'BUY' else -qty
        else:
            assert not pos or not pos['qty'], 'Duplicate entry POST'
            self.positions[s] = {'qty': qty if side == 'BUY' else -qty, 'entry': price}
            pnl, status = D(0), 'FILLED'
        fee = price * qty * self.fee
        self.wallet += pnl - fee
        oid, stamp = len(self.orders) + 1000, int(time.time() * 1000)
        order = dict(orderId=oid, clientOrderId=p['newClientOrderId'], symbol=s, side=side,
            status=status, avgPrice=str(price), executedQty=str(qty), origQty=p['quantity'],
            reduceOnly=reducing, updateTime=stamp, time=stamp)
        self.orders[oid] = order
        self.fills.append(dict(id=len(self.fills) + 10000, orderId=oid, symbol=s,
            qty=str(qty), price=str(price), commission=str(fee), commissionAsset='USDT',
            realizedPnl=str(pnl), time=stamp))
        return order

    def trigger_algo(self, algo_id, partial=False):
        algo = self.algos[algo_id]
        self.partial_close = partial
        order = self.fill(dict(symbol=algo['symbol'], side=algo['side'], quantity=algo['quantity'],
            newClientOrderId=algo['clientAlgoId'], reduceOnly='true'))
        if partial:
            order.update(status='PARTIALLY_FILLED', origQty=algo['quantity'])
        algo.update(algoStatus='FINISHED', actualOrderId=str(order['orderId']))
        return order

    def complete_native_order(self, order_id):
        order = self.orders[order_id]
        symbol, price = order['symbol'], self.prices[order['symbol']]
        pos = self.positions[symbol]
        amount = min(D(order['origQty']) - D(order['executedQty']), abs(pos['qty']))
        sign = 1 if pos['qty'] > 0 else -1
        pnl, fee = (price - pos['entry']) * amount * sign, price * amount * self.fee
        quote = D(order['avgPrice']) * D(order['executedQty']) + price * amount
        total = D(order['executedQty']) + amount
        self.wallet += pnl - fee
        pos['qty'] -= sign * amount
        order.update(status='FILLED', executedQty=str(total), avgPrice=str(quote / total),
                     cumQuote=str(quote), updateTime=int(time.time() * 1000))
        self.fills.append(dict(id=len(self.fills) + 10000, orderId=order_id, symbol=symbol,
            qty=str(amount), price=str(price), commission=str(fee), commissionAsset='USDT',
            realizedPnl=str(pnl), time=order['updateTime']))

    async def __call__(self, request):
        p, method, path = request.url.params, request.method, request.url.path
        # Record contract facts without ever storing a signed URL or credentials.
        self.requests.append((method, path, {k: v for k, v in p.items() if k not in ('signature', 'timestamp')}))
        s = p.get('symbol')
        if path.endswith('/time'): data = dict(serverTime=int(time.time() * 1000))
        elif path.endswith('/exchangeInfo'):
            data = {'symbols': [dict(symbol=s, baseAsset=s[:-4], quoteAsset='USDT', marginAsset='USDT',
                contractType='PERPETUAL', status='TRADING', orderTypes=['MARKET', 'LIMIT'] + (['STOP_MARKET'] if self.advertise_regular_stop else []), filters=[
                    dict(filterType='LOT_SIZE', minQty=v[1], maxQty='10000000', stepSize=v[1]),
                    dict(filterType='MARKET_LOT_SIZE', minQty=v[1], maxQty='10000000', stepSize='0'),
                    dict(filterType='PRICE_FILTER', minPrice=v[2], maxPrice='10000000', tickSize=v[2]),
                    dict(filterType='MIN_NOTIONAL', notional='5')]) for s, v in self.specs.items()]}
        elif path.endswith('/positionSide/dual'): data = {'dualSidePosition': False}
        elif path.endswith('/multiAssetsMargin'): data = {'multiAssetsMargin': False}
        elif path.endswith('/accountConfig'): data = {'canTrade': True}
        elif path.endswith('/symbolConfig'):
            data = [dict(symbol=s, marginType=self.margin_type.get(s, 'CROSSED'),
                         leverage=self.leverage.get(s, self.specs[s][3]), maxNotionalValue='1000000')]
        elif path.endswith('/marginType'):
            self.margin_type[s] = p['marginType']; data = {'code': 200}
        elif path.endswith('/leverageBracket'): data = [dict(symbol=s, brackets=self.brackets(s))]
        elif path.endswith('/leverage'):
            self.leverage[s] = int(p['leverage'])
            data = dict(leverage=self.leverage[s], maxNotionalValue=str(max(
                b['notionalCap'] for b in self.brackets(s) if b['initialLeverage'] >= self.leverage[s])))
        elif path.endswith('/ticker/price'): data = dict(symbol=s, price=str(self.prices[s]))
        elif path.endswith('/positionRisk'): data = self.position_rows(s)
        elif path.endswith('/account'):
            rows = self.position_rows()
            used = sum((D(r['positionInitialMargin']) for r in rows), D(0))
            unrealized = sum(((self.mark_prices.get(s, self.prices[s]) - v['entry']) * v['qty'] for s, v in self.positions.items()), D(0))
            data = {'assets': [dict(asset='USDT', walletBalance=str(self.wallet),
                marginBalance=str(self.wallet + unrealized), availableBalance=str(self.wallet + unrealized - used), initialMargin=str(used))]}
        elif path.endswith('/klines'):
            price = str(self.prices[s]); minute = int(time.time() // 60) * 60000
            data = [[minute - i * 60000, price, price, price, price, '10'] for i in reversed(range(int(p['limit'])))]
        elif path.endswith('/depth'):
            data = dict(bids=[[str(self.prices[s] * D('.99999')), '1000000000']], asks=[[str(self.prices[s] * D('1.00001')), '1000000000']])
        elif path.endswith('/premiumIndex'):
            data = dict(lastFundingRate='0', nextFundingTime=int((time.time() + 3600) * 1000))
        elif path == '/fapi/v1/order':
            if method == 'POST':
                if p.get('type') == 'STOP_MARKET':
                    if not self.regular_stops: return httpx.Response(400, json={'code': -4120})
                    oid = len(self.orders) + 1000
                    data = dict(orderId=oid, clientOrderId=p['newClientOrderId'], symbol=s, side=p['side'],
                        status='NEW', type='STOP_MARKET', origQty=p['quantity'], executedQty='0', avgPrice='0',
                        stopPrice=p['stopPrice'], reduceOnly=True, workingType=p['workingType'])
                    self.orders[oid] = data
                    return httpx.Response(200, json=data)
                data = self.fill(p)
                lose = self.lose_close_response if data['reduceOnly'] else self.lose_entry_response
                if lose:
                    if data['reduceOnly']: self.lose_close_response = False
                    else: self.lose_entry_response = False
                    raise httpx.ReadTimeout('simulated lost POST response', request=request)
            elif method == 'DELETE':
                data = self.orders.get(int(p['orderId'])) if 'orderId' in p else next(
                    (o for o in self.orders.values() if o['clientOrderId'] == p['origClientOrderId'] and o['symbol'] == s), None)
                if data and data['status'] not in ('FILLED', 'CANCELED', 'EXPIRED', 'REJECTED'):
                    data['status'] = 'CANCELED'
            else:
                data = self.orders.get(int(p['orderId'])) if 'orderId' in p else next(
                    (o for o in self.orders.values() if o['clientOrderId'] == p['origClientOrderId']), None)
        elif path.endswith('/userTrades'):
            data = [] if self.delay_accounting else [f for f in self.fills if f['symbol'] == s
                and ('orderId' not in p or str(f['orderId']) == p['orderId'])
                and ('startTime' not in p or f['time'] >= int(p['startTime']))
                and ('fromId' not in p or f['id'] >= int(p['fromId']))][:int(p.get('limit', 1000))]
        elif path.endswith('/openOrders'): data = [o for o in self.orders.values() if o['status'] in ('NEW', 'PARTIALLY_FILLED') and (not s or o['symbol'] == s)]
        elif path.endswith('/openAlgoOrders'):
            data = [o for o in self.algos.values() if o['algoStatus'] == 'NEW' and (not s or o['symbol'] == s)]
        elif path.endswith('/algoOrder'):
            if method == 'POST':
                if p['type'] == self.reject_role: return httpx.Response(400, json={'code': -2021})
                assert p['reduceOnly'] == 'true'
                assert D(p['quantity']) <= abs(self.positions[s]['qty'])
                aid = len(self.algos) + 2000
                data = dict(algoId=aid, clientAlgoId=p['clientAlgoId'], symbol=s, algoStatus='NEW',
                    side=p['side'], orderType=p['type'], quantity=p['quantity'],
                    reduceOnly=True, triggerPrice=p['triggerPrice'], actualOrderId='', workingType=p['workingType'])
                self.algos[aid] = data
            elif method == 'GET': data = next((o for o in self.algos.values() if o['clientAlgoId'] == p['clientAlgoId']), None)
            else:
                self.algos[int(p['algoId'])]['algoStatus'] = 'CANCELED'; data = {'code': 200}
        else: raise AssertionError('Unexpected adapter contract: ' + method + ' ' + path)
        return httpx.Response(400, json={'code': -2013}) if data is None else httpx.Response(200, json=data)


async def production_bot(tmp_path, monkeypatch, server, startup=True, legacy=False, hard_policy=False):
    if legacy:
        from legacy_config import legacy_execution_config
        cfg = legacy_execution_config()
    else:
        cfg = load_config()
    cfg['exchange'].update(mode='testnet', state_dir=str(tmp_path))
    cfg['rollout']['evidence_file'] = str(tmp_path / 'evidence.json')
    cfg['rollout']['operator_policy_file'] = str(tmp_path / '{mode}' / 'policy.json')
    monkeypatch.setenv('BINANCE_API_KEY', 'offline-key')
    monkeypatch.setenv('BINANCE_API_SECRET', 'offline-secret')
    monkeypatch.setattr('paper_runtime.load_config', lambda: copy.deepcopy(cfg))
    bot = V2PaperHunter('state.json')  # Actual Demo constructor and supervisor wiring.
    if hard_policy:
        # Explicitly exercise the retained hard policy with a simulated transport.
        from risk.kill_switch import KillSwitch
        saved_kill = bot.safety.kill.dump()
        bot.safety.kill = KillSwitch(cfg['kill_switch'])
        bot.safety.kill.restore(saved_kill)
        if bot.safety.kill.blocks_entries: bot.safety.ready = False
    live = bot.exchange
    live.on_api_error = bot.safety.kill.on_api_error
    await live.client.aclose()
    live.client = httpx.AsyncClient(base_url=cfg['exchange']['base_url_testnet'], transport=httpx.MockTransport(server))
    for name in ('rl', 'weights', 'orders_short', 'orders_long'):
        setattr(live, name, SimpleNamespace(acquire=AsyncMock()))
    # Fake the delivery worker only; all policy, reconciliation and evidence code runs.
    bot.safety.alerter.enabled = True
    async def delivery():
        while True:
            await bot.safety.alerter.queue.get()
            bot.safety.alerter.queue.task_done()
    bot.safety.alerter._worker = delivery
    atomic_json(tmp_path / 'testnet' / 'policy.json', dict(mode='testnet', dedicated_account=True,
        futures_only=True, withdrawals_disabled=True, ip_whitelist_verified=True,
        last_rotated_at=datetime.now(timezone.utc).isoformat()))
    await live.load_markets()
    if bot.initial_balance is None: bot.initial_balance = live.equity
    if startup: await bot.safety.startup()
    return bot
