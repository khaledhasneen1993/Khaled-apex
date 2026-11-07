"""Capture public USD-M events to replayable JSONL. No account endpoints or credentials."""
import argparse
import asyncio
import json
from pathlib import Path
import re
import time

from .book import OrderBook

REST = "https://fapi.binance.com"
WS = "wss://fstream.binance.com"
SYMBOL = re.compile(r"[A-Z0-9]{3,24}USDT")


async def get(session, endpoint, **params):
    async with session.get(REST+endpoint, params=params, allow_redirects=False) as response:
        response.raise_for_status()
        if response.status != 200:
            raise ValueError("Unexpected market data HTTP status")
        return await response.json()


async def produce(session, symbol, kind, queue):
    lower = symbol.lower()
    topics = (f"/public/stream?streams={lower}@depth@100ms" if kind == "depth" else
              "/market/stream?streams="+"/".join(f"{lower}@{channel}" for channel in
                  ("aggTrade", "kline_1m", "markPrice@1s", "forceOrder")))
    pause = 2
    while True:
        await queue.put((symbol, f"reset_{kind}", {}))
        try:
            async with session.ws_connect(WS+topics, heartbeat=30) as ws:
                if kind == "depth":
                    snapshot = await get(session, "/fapi/v1/depth", symbol=symbol, limit=1000)
                    book = OrderBook(symbol)
                    book.snapshot(snapshot)
                    await queue.put((symbol, "snapshot", snapshot))
                else:
                    candles = await get(session, "/fapi/v1/klines", symbol=symbol, interval="1m", limit=45)
                    await queue.put((symbol, "candles", candles))
                while True:
                    event = (await asyncio.wait_for(ws.receive_json(), timeout=20))["data"]
                    now = time.time_ns()//1_000_000
                    if event.get("st", 1) != 1:
                        continue
                    if not 0 <= now-int(event["E"]) <= 1500:
                        raise ValueError("Stale feed while capturing")
                    if kind == "depth" and book.update(event, now) is None:
                        continue
                    await queue.put((symbol, "event", event))
                    pause = 2
        except (OSError, TimeoutError, ValueError, KeyError, TypeError) as error:
            await queue.put((symbol, f"reset_{kind}", {"error": type(error).__name__}))
        except Exception as error:
            # aiohttp transport errors include a variety of protocol subclasses.
            import aiohttp
            if not isinstance(error, aiohttp.ClientError):
                raise
            await queue.put((symbol, f"reset_{kind}", {"error": type(error).__name__}))
        await asyncio.sleep(pause)
        pause = min(120, pause*2)


async def open_interest(session, symbols, queue):
    import aiohttp
    while True:
        for symbol in symbols:
            try:
                result = await get(session, "/fapi/v1/openInterest", symbol=symbol)
                await queue.put((symbol, "open_interest", result))
            except (aiohttp.ClientError, TimeoutError, ValueError):
                continue
        await asyncio.sleep(60)


def write_batch(stream, rows):
    stream.writelines(json.dumps(row, allow_nan=False, separators=(",", ":"))+"\n" for row in rows)
    stream.flush()


async def record(symbols, destination, seconds, replay=None):
    import aiohttp
    queue = asyncio.Queue(maxsize=2000)
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
        with destination.open("x", encoding="utf-8") as output:
            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(produce(session, symbol, feed, queue))
                         for symbol in symbols for feed in ("depth", "market")]
                tasks.append(group.create_task(open_interest(session, symbols, queue)))
                end = time.monotonic()+seconds
                last_timestamp = 0
                batch = []
                try:
                    while (left := end-time.monotonic()) > 0:
                        try:
                            symbol, kind, data = await asyncio.wait_for(queue.get(), min(1., left))
                            now = max(last_timestamp, time.time_ns()//1_000_000)
                            last_timestamp = now
                            row = dict(received_ms=now, symbol=symbol, kind=kind, data=data)
                            batch.append(row)
                            if replay is not None:
                                replay.apply(row)
                        except TimeoutError:
                            pass
                        if batch and (len(batch) >= 100 or queue.empty()):
                            await asyncio.to_thread(write_batch, output, batch)
                            batch.clear()
                finally:
                    for task in tasks:
                        task.cancel()
                if batch:
                    await asyncio.to_thread(write_batch, output, batch)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    parser.add_argument("symbols", nargs="+")
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--shadow-output", type=Path)
    parser.add_argument("--instruments", type=Path)
    parser.add_argument("--config", type=Path, default=Path("apex.json"))
    args = parser.parse_args()
    if (not 1 <= len(set(args.symbols)) == len(args.symbols) <= 8
            or not all(SYMBOL.fullmatch(s) for s in args.symbols) or args.seconds < 1):
        parser.error("Specify one to eight unique USD-M USDT symbols and a positive duration")
    if bool(args.shadow_output) != bool(args.instruments):
        parser.error("Shadow output and instrument filters are required together")
    replay = None
    if args.shadow_output:
        from apex.config import Config
        from apex.engine import Engine, Instrument
        from research.replay import Replay, export
        cfg = Config.load(args.config)
        instruments = {s: Instrument(**v) for s, v in json.loads(args.instruments.read_text()).items()}
        replay = Replay(Engine(cfg, instruments))
    asyncio.run(record(args.symbols, args.destination, args.seconds, replay))
    if replay is not None:
        export(replay, args.shadow_output)


if __name__ == "__main__":
    main()
