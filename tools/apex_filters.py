"""Fetch public MARKET_LOT_SIZE and MIN_NOTIONAL filters to a new snapshot file."""
import argparse
import asyncio
import json
from pathlib import Path

from apex.engine import Instrument
from market_data.capture import SYMBOL, get


def extract(payload, selected):
    instruments = {}
    for row in payload["symbols"]:
        name = row["symbol"]
        if name not in selected:
            continue
        if row["contractType"] != "PERPETUAL" or row["marginAsset"] != "USDT" or row["status"] != "TRADING":
            raise ValueError(f"Ineligible USD-M perpetual {name}")
        filters = {rule["filterType"]: rule for rule in row["filters"]}
        step = float(filters["MARKET_LOT_SIZE"]["stepSize"])
        minimum = float(filters["MIN_NOTIONAL"]["notional"])
        Instrument(step, minimum)
        instruments[name] = {"step": step, "min_notional": minimum}
    if set(instruments) != set(selected):
        raise ValueError("Missing requested instrument")
    return instruments


async def fetch(selected):
    import aiohttp
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12), trust_env=True) as session:
        return extract(await get(session, "/fapi/v1/exchangeInfo"), selected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    parser.add_argument("symbols", nargs="+")
    args = parser.parse_args()
    if not all(SYMBOL.fullmatch(s) for s in args.symbols):
        parser.error("Invalid USD-M symbols")
    result = asyncio.run(fetch(args.symbols))
    with args.destination.open("x") as file:
        json.dump(result, file, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
