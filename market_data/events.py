"""Apply normalized public events consistently in Paper, Replay and Demo."""
from .features import MarketState


def apply_market_row(states, row, max_age):
    now, symbol, kind, data = (row[k] for k in ("received_ms", "symbol", "kind", "data"))
    state = states.setdefault(symbol, MarketState(symbol))
    if kind == "event":
        state.ingest(data, now, max_age)
    elif kind == "snapshot":
        state.book.snapshot(data)
    elif kind == "reset_depth":
        state.book.invalidate()
        state.flows.clear()
    elif kind == "reset_market":
        book, flows = state.book, state.flows
        state.disconnect()
        state.book, state.flows = book, flows
    elif kind == "candles":
        for candle in data:
            if int(candle[0])+60000 <= now:
                state.add_candle(int(candle[0]), *(float(v) for v in candle[1:6]))
    elif kind == "open_interest":
        state.open_interest(data, now)
    else:
        raise ValueError("Unknown public market event")
    return state
