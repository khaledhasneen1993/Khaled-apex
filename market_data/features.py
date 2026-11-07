"""Point-in-time flow, candles and auxiliary market measurements."""
from collections import deque
from dataclasses import dataclass
from math import isfinite, log, sqrt

from .book import OrderBook


@dataclass(frozen=True)
class Features:
    symbol: str
    asof_ms: int
    observed_ms: int
    price: float
    momentum: float
    trend: float
    short_vol: float
    long_vol: float
    spike: float
    cvd: float
    flow: float
    ofi: float
    absorption: float
    depletion_usdt: float
    spread: float
    depth_usdt: float
    atr: float
    range_high: float
    range_low: float
    recent_high: float
    recent_low: float
    funding: float | None
    oi_change: float | None
    btc_momentum: float | None


class MarketState:
    def __init__(self, symbol):
        self.book = OrderBook(symbol)
        self.trades = deque(maxlen=4000)
        self.flows = deque(maxlen=300)
        self.candles = deque(maxlen=60)
        self.oi = deque(maxlen=2)
        self.mark = None
        self.liquidations = deque(maxlen=300)
        self.last_trade = -1

    def disconnect(self):
        self.__init__(self.book.symbol)

    def ingest(self, data, received_ms, max_age_ms=1500):
        try:
            return self._ingest(data, received_ms, max_age_ms)
        except (ValueError, TypeError, KeyError, OverflowError):
            self.disconnect()
            raise

    def _ingest(self, data, received_ms, max_age_ms):
        kind, event = data["e"], int(data["E"])
        symbol = data.get("o", {}).get("s") if kind == "forceOrder" else data.get("s")
        if symbol != self.book.symbol:
            raise ValueError("Unexpected market symbol")
        if not 0 <= received_ms - event <= max_age_ms:
            raise ValueError("Late market event")
        if kind == "depthUpdate":
            result = self.book.update(data, received_ms)
            if result is not None:
                self.flows.append((event, *result))
        elif kind == "aggTrade":
            ident, ts = int(data["a"]), int(data["T"])
            if ident <= self.last_trade:
                return
            p, q = float(data["p"]), float(data["q"])
            if (not all(isfinite(v) and v > 0 for v in (p, q)) or type(data["m"]) is not bool
                    or not 0 <= received_ms-ts <= max_age_ms
                    or (self.trades and ts < self.trades[-1][0])):
                raise ValueError("Invalid trade")
            self.last_trade = ident
            self.trades.append((ts, p, -q if data["m"] else q))
        elif kind == "kline":
            candle = data["k"]
            if candle["i"] == "1m" and candle["x"] is True:
                if int(candle["T"]) > event:
                    raise ValueError("Candle not closed")
                self.add_candle(int(candle["t"]), *(float(candle[k]) for k in ("o", "h", "l", "c", "v")))
        elif kind == "markPriceUpdate":
            mark, index, funding = (float(data[k]) for k in ("p", "i", "r"))
            if not all(isfinite(v) for v in (mark, index, funding)) or min(mark, index) <= 0:
                raise ValueError("Invalid mark event")
            if self.mark is None or event > self.mark[0]:
                self.mark = event, mark, index, funding
        elif kind == "forceOrder":
            order = data["o"]
            price, executed = float(order["ap"]), float(order["z"])
            if not all(isfinite(v) and v >= 0 for v in (price, executed)) or order["S"] not in ("BUY", "SELL"):
                raise ValueError("Invalid liquidation event")
            self.liquidations.append((event, order["S"], price, executed))

    def add_candle(self, start, o, h, l, c, v):
        if (start % 60000 or not all(isfinite(x) for x in (o, h, l, c, v))
                or l <= 0 or l > min(o, c) or h < max(o, c) or v < 0):
            raise ValueError("Invalid completed candle")
        if self.candles:
            previous = self.candles[-1][0]
            if start <= previous:
                return
            if start != previous + 60000:
                self.candles.clear()
        self.candles.append((start, o, h, l, c, v))

    def open_interest(self, payload, received_ms):
        value, ts = float(payload["openInterest"]), int(payload["time"])
        if (payload["symbol"] != self.book.symbol or not isfinite(value)
                or value <= 0 or not 0 <= received_ms - ts <= 120000):
            raise ValueError("Invalid open interest")
        if not self.oi or ts > self.oi[-1][0]:
            self.oi.append((ts, value))

    def features(self, now_ms, cfg, btc_momentum=None):
        if (not self.book.fresh(now_ms, cfg.max_age_ms) or not self.trades
                or not 0 <= now_ms - self.trades[-1][0] <= cfg.max_age_ms):
            return None
        past = [c for c in self.candles if c[0] + 60000 <= now_ms]
        if len(past) < 21 or now_ms - past[-1][0] >= 120000:
            return None
        # Use equal wall-clock windows. The previous minute's denominator is
        # from completed candles. Require a full observed current-minute window.
        start = now_ms - now_ms % 60000
        elapsed = now_ms - start
        if elapsed < 5000 or self.trades[0][0] > start:
            return None
        current = [(t, p, q) for t, p, q in self.trades if start <= t <= now_ms]
        flow = [row for row in current if row[0] >= now_ms - 5000]
        if not flow:
            return None
        cvd, total = sum(q for _, _, q in flow), sum(abs(q) for _, _, q in flow)
        mid = self.book.quote.mid
        base = sum(c[5] for c in past[-20:]) / 20
        returns = [log(b[4]/a[4]) for a, b in zip(past[-21:-1], past[-20:])]
        atr = sum(max(b[2]-b[3], abs(b[2]-a[4]), abs(b[3]-a[4]))
                  for a, b in zip(past[-15:-1], past[-14:])) / 14
        active_flows = [(ofi, dep) for ts, ofi, dep in self.flows if 0 <= now_ms - ts <= 5000]
        oi = (self.oi[-1][1]/self.oi[0][1]-1
              if len(self.oi) == 2 and 0 <= now_ms - self.oi[-1][0] <= 120000 else None)
        funding = self.mark[3] if self.mark and 0 <= now_ms - self.mark[0] <= cfg.max_age_ms else None
        prices = [p for _, p, _ in current]
        return Features(self.book.symbol, now_ms, min(self.book.event_ms, self.trades[-1][0]), mid,
                        mid/past[-2][4]-1, mid/past[-10][4]-1,
                        sqrt(sum(r*r for r in returns[-5:])/5), sqrt(sum(r*r for r in returns)/20),
                        sum(abs(q) for _, _, q in current)*60000/elapsed/base if base else 0.,
                        cvd, cvd/total, sum(v for v, _ in active_flows)/max(1, len(active_flows)),
                        abs(cvd/total)/(1+abs(mid/flow[0][1]-1)/cfg.min_stop),
                        sum(d for _, d in active_flows), self.book.quote.spread, self.book.depth(), atr,
                        max(c[2] for c in past[-10:]), min(c[3] for c in past[-10:]),
                        max(prices), min(prices), funding, oi, btc_momentum)

    def volume_by_price(self, since_ms, asof_ms, tick):
        if not isfinite(tick) or tick <= 0:
            raise ValueError("Invalid volume bucket")
        buckets = {}
        for ts, price, qty in self.trades:
            if since_ms <= ts <= asof_ms:
                key = round(price / tick)
                buckets[key] = buckets.get(key, 0.) + abs(qty)
        return buckets
