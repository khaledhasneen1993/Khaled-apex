# language: Python 3.11+, file: binance_cascade_hunter_dynamic_v2.py
# target: Binance USDT-M Futures — shared entry/exit strategy; run_demo.py selects Demo
# dependencies: requirements.txt

import asyncio
import pandas as pd
from datetime import datetime, timezone
import logging
from types import SimpleNamespace
from risk.config import load_config
from risk.atr_levels import compute_atr
from risk.dynamic_stop import update_dynamic_stop

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("CASCADE")

# === CONFIG ===

_BASELINE = load_config()
LEVERAGE = _BASELINE['leverage']
MAX_POSITION_PCT = _BASELINE['margin_fraction_per_position']
CASCADE_THRESHOLD = _BASELINE['cascade_threshold']
VOLUME_SPIKE = _BASELINE['volume_spike']
COOLDOWN_SEC = _BASELINE['cooldown_sec']
SCAN_INTERVAL = _BASELINE['universe_refresh_sec']
MAX_SYMBOLS_TO_SCAN = _BASELINE['max_symbols']
MAX_CONCURRENT_POSITIONS = _BASELINE['max_concurrent_positions']
MIN_QUOTE_VOLUME = _BASELINE['min_quote_volume_usdt']

class CascadeHunter:
    """V2 strategy methods; execution is supplied by the virtual account runtime."""

    def __init__(self):
        raise RuntimeError("Use V2PaperHunter or run_paper_live.py for the paper account")

    async def exit_atr(self, symbol, price):
        """Reuse scanner candles; missing data never filters an entry signal."""
        cfg = getattr(self, "cfg", None) or load_config()
        cache = getattr(self, "_exit_atr_cache", None)
        if cache is None:
            self._exit_atr_cache = cache = {}
        rows = list(getattr(getattr(self, "scanner", None), "histories", {}).get(symbol, ()))
        try:
            if not rows:
                rows = await self.exchange.fetch_ohlcv(symbol, "1m", limit=cfg["atr"]["period"] + 1)
            candles = [{"high": r[2], "low": r[3], "close": r[4]} for r in rows]
            atr = compute_atr(candles, cfg["atr"]["period"])
            cache[symbol] = atr
            return atr, "candles"
        except (OSError, ValueError, KeyError, IndexError, TypeError) as error:
            log.warning("ATR unavailable %s (%s); using cached/fallback ATR", symbol, type(error).__name__)
        except Exception as error:
            log.warning("ATR feed unavailable %s (%s); using cached/fallback ATR", symbol, type(error).__name__)
        if symbol in cache:
            return cache[symbol], "cached"
        return price * cfg["atr"]["fallback_atr_pct"], "fallback"

    async def set_leverage(self, symbol):
        if getattr(self, 'execution_mode', 'paper') != 'paper':
            choice = self.exchange.selected_leverage.get(symbol)
            if not choice or not choice['confirmed']:
                raise ValueError('Leverage not confirmed before entry')
            return
        await self.exchange.set_leverage(self.cfg['leverage'], symbol)

    async def get_top_symbols(self):
        """يجيب أعلى العملات سيولة (USDT-M linear)"""
        try:
            tickers = await self.exchange.fetch_tickers()
            candidates = []
            for symbol, t in tickers.items():
                if not symbol.endswith(":USDT"):
                    continue
                if "/USDT:USDT" not in symbol:
                    continue
                quote_vol = float(t.get("quoteVolume") or 0)
                if quote_vol < MIN_QUOTE_VOLUME:
                    continue
                candidates.append((symbol, quote_vol))

            candidates.sort(key=lambda x: x[1], reverse=True)
            top = [s for s, _ in candidates[:MAX_SYMBOLS_TO_SCAN]]
            log.info(f"Scan universe: {len(top)} symbols")
            return top
        except Exception as e:
            log.error(f"Failed to fetch top symbols: {e}")
            return ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]

    async def get_orderbook_imbalance(self, symbol, depth=20):
        try:
            ob = await self.exchange.fetch_order_book(symbol, limit=depth)
            bid_vol = sum(x[1] for x in ob["bids"][:depth])
            ask_vol = sum(x[1] for x in ob["asks"][:depth])
            if bid_vol + ask_vol == 0:
                return 0.0
            return (bid_vol - ask_vol) / (bid_vol + ask_vol)
        except Exception:
            return 0.0

    async def detect_cascade(self, symbol):
        try:
            ohlcv_1m = await self.exchange.fetch_ohlcv(symbol, "1m", limit=30)
            df1 = pd.DataFrame(ohlcv_1m, columns=["ts", "o", "h", "l", "c", "v"])

            ret_1m = (df1["c"].iloc[-1] - df1["c"].iloc[-3]) / df1["c"].iloc[-3]
            vol_avg = df1["v"].iloc[-15:-3].mean()
            vol_now = df1["v"].iloc[-1]
            vol_spike = vol_now / vol_avg if vol_avg > 0 else 0

            imb = await self.get_orderbook_imbalance(symbol)

            long_sig = ret_1m > CASCADE_THRESHOLD and vol_spike > VOLUME_SPIKE and imb > 0.35
            short_sig = ret_1m < -CASCADE_THRESHOLD and vol_spike > VOLUME_SPIKE and imb < -0.35

            strength = abs(ret_1m) * vol_spike * abs(imb)

            return {
                "long": long_sig,
                "short": short_sig,
                "ret": ret_1m,
                "vol_spike": vol_spike,
                "imb": imb,
                "price": float(df1["c"].iloc[-1]),
                "strength": strength,
                "symbol": symbol
            }
        except Exception as e:
            log.debug(f"Detect failed {symbol}: {e}")
            return None

    async def size_position(self, symbol):
        bal = await self.exchange.fetch_balance()
        usdt = max(0.0, float(bal["USDT"].get("equity", bal["USDT"]["total"])))  # current equity, not initial capital
        price = (await self.exchange.fetch_ticker(symbol))["last"]
        if getattr(self, 'execution_mode', 'paper') != 'paper':
            choice = await self.exchange.select_leverage(symbol, usdt * self.cfg['margin_fraction_per_position'], price)
            return choice['amount'] if choice.get('entry_order_valid', True) else 0.0
        from risk.trading_policy import entry_amount, fixed_paper_notional
        notional = fixed_paper_notional(self.cfg)
        if notional is not None:
            return float(self.exchange.amount_to_precision(symbol, notional / price))
        return entry_amount(usdt, price, self.cfg['leverage'], self.cfg, self.exchange.fee_rate,
                            lambda q: float(self.exchange.amount_to_precision(symbol, q)))

    async def enter(self, symbol, side, price):
        if symbol in self.positions: return False
        now = datetime.now(timezone.utc).timestamp()
        if symbol in self.last_trade and now - self.last_trade[symbol] < COOLDOWN_SEC:
            return False
        if len(self.positions) >= MAX_CONCURRENT_POSITIONS:
            return False

        try:
            amount = await self.size_position(symbol)
        except Exception as error:
            from infra.errors import diagnostic_fields
            await self.safety.event('entry_configuration_failed', 'warning', symbol=symbol,
                stage='sizing_and_leverage', recovery_action='no_entry_sent', **diagnostic_fields(error))
            return False
        if amount <= 0:
            return False

        try:
            from risk.trading_policy import expected_slippage_bps, fixed_paper_notional
            if self.cfg['exit'].get('policy') == 'hard_stop_trailing':
                book = await self.exchange.fetch_order_book(symbol, limit=self.cfg['kill_switch']['book_depth'])
                expected_bps = expected_slippage_bps(book, side, amount, price)
                if expected_bps > self.cfg['slippage']['max_expected_entry_bps']:
                    await self.safety.event('entry_blocked', reason='expected_slippage', symbol=symbol,
                        report={'expected_bps': expected_bps if expected_bps != float('inf') else None})
                    return False
            if self.cfg['exit']['policy'] == 'hard_stop_trailing':
                atr, atr_source = None, 'not_used'
            else:
                atr, atr_source = await self.exit_atr(symbol, price)
            await self.set_leverage(symbol)
            params = {"reduceOnly": False, "exit_atr": atr,
                      "atr_source": atr_source, "expected_price": price}
            if fixed_paper_notional(self.cfg) is not None:
                params["use_paper_notional"] = True
            fill = await self.exchange.create_order(symbol, "market", side, amount, params=params)
            self.last_trade[symbol] = now
            self.positions[symbol] = fill["risk_position"]
            log.info(f"ENTER {side.upper()} {symbol} size={amount:.4f} @ {price:.4f}")
            return True
        except Exception as e:
            log.error("Entry failed %s (%s)", symbol, type(e).__name__)
            return False

    async def manage_exits(self):
        to_remove = []
        for symbol, pos in list(self.positions.items()):
            try:
                if getattr(self, 'execution_mode', 'paper') != 'paper' and hasattr(self, 'exit_reference'):
                    price = await self.exit_reference(symbol)
                    if price is None: continue
                else:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    price = ticker["last"]
                from paper_account import Position, make_position
                cfg = getattr(self, "cfg", None) or load_config()
                if cfg['exit']['policy'] == 'hard_stop_trailing':
                    atr, source = pos.get('atr_at_entry'), pos.get('atr_source', 'not_used')
                else:
                    atr, source = await self.exit_atr(symbol, price)
                if "stop_initial" not in pos:
                    if getattr(self, 'execution_mode', 'paper') != 'paper':
                        raise RuntimeError('Live legacy position requires explicit execution recovery')
                    # Upgrade pre-feature saved positions before their first exit check.
                    account = self.exchange.positions[symbol]
                    opening = next((t for t in reversed(self.exchange.trades)
                                    if t["symbol"] == symbol and not t["reduceOnly"]), None)
                    upgraded = make_position(pos["side"], account["entry"], pos["amount"],
                                             self.exchange.leverage, atr, cfg,
                                             self.exchange.equity, self.exchange.fee_rate,
                                             opened_at=datetime.fromisoformat(opening["timestamp"]).timestamp() if opening else None,
                                             signal_id=opening["id"] if opening else None,
                                             atr_source=source)
                    if opening:
                        upgraded["entry_fee"] = opening["fee_usdt"]
                    pos = self.positions[symbol] = upgraded
                    pos["legacy_migrated"] = True
                elif not isinstance(pos, Position):
                    pos = self.positions[symbol] = Position(pos)
                sm = 1 if pos["side"] == "buy" else -1
                previous_stop = pos.stop
                exit_cfg = cfg['exit']
                if exit_cfg['policy'] != 'hard_stop_trailing':
                    fee = max(self.exchange.fee_rate, pos.get("entry_fee", 0.) / (pos["entry"] * pos["amount"]))
                    exit_cfg = {**exit_cfg, 'be_offset_pct': max(
                        exit_cfg['be_offset_pct'], 2 * fee / (1 - sm * fee))}
                update_dynamic_stop(pos, SimpleNamespace(price=price, atr=atr), exit_cfg)
                if exit_cfg['policy'] != 'hard_stop_trailing':
                    be_stop = pos.entry * (1 + sm * exit_cfg['be_offset_pct'])
                    if sm * (pos.stop - previous_stop) > 0 and sm * (pos.stop - be_stop) > 0:
                        pos['trail_moved'] = True
                hit_tp = pos.get('tp') is not None and ((pos["side"] == "buy" and price >= pos["tp"]) or
                         (pos["side"] == "sell" and price <= pos["tp"]))
                hit_sl = (pos["side"] == "buy" and price <= pos["sl"]) or \
                         (pos["side"] == "sell" and price >= pos["sl"])

                if hit_tp or hit_sl:
                    close_side = "sell" if pos["side"] == "buy" else "buy"
                    from risk.trading_policy import stop_reason
                    reason = stop_reason(pos) if hit_sl else "TP"
                    if hasattr(self, "close_position"):
                        await self.close_position(symbol, reason, price)
                    else:
                        await self.exchange.create_order(symbol, "market", close_side, pos["amount"],
                                                         params={"reduceOnly": True, "expected_price": price})
                        if hasattr(self, "record_exit_reason"):
                            self.record_exit_reason(symbol, reason, price)
                    log.info(f"EXIT {reason} {symbol} @ {price:.4f}")
                    to_remove.append(symbol)
            except Exception as e:
                if getattr(self, 'execution_mode', 'paper') != 'paper': raise
                log.error(f"Exit check failed {symbol}: {e}")

        for s in to_remove:
            self.positions.pop(s, None)


    async def exit_watcher(self):
        """مراقبة الخروج بشكل مستمر بين المسحات"""
        while True:
            try:
                if hasattr(self, 'process_watchdog'): self.process_watchdog.heartbeat('order_manager')
                async with self.lock:
                    await self.manage_exits()
                await asyncio.sleep(self.cfg["exit"]["check_sec"])
            except Exception as e:
                from infra.resilience import transient_error
                if transient_error(e):
                    await asyncio.sleep(1)
                    continue
                if getattr(self, 'execution_mode', 'paper') != 'paper':
                    from infra.errors import diagnostic_fields
                    async with self.lock:
                        await self.safety.event('exit_monitor_exception', 'critical', stage='exit_management',
                            recovery_action='latch_and_reconcile', **diagnostic_fields(e))
                        await self.safety.trip('exit_monitor_failure')
                else:
                    log.error(f"Exit watcher error: {e}")
                await asyncio.sleep(15)


if __name__ == "__main__":
    raise SystemExit("Strategy module only. Run: python -u run_paper_live.py")
