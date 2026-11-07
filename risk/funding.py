import time
from risk.liq_math import direction


async def check_funding_before_settlement(state, exchange, cfg, journal):
    if not cfg.get('close_positions', False): return
    for symbol, pos in list(state.open_positions.items()):
        data = await exchange.get_funding_rate(symbol)
        # Use Binance's actual nextFundingTime (intervals can vary by contract).
        minutes = (int(data['nextFundingTime']) / 1000 - time.time()) / 60
        if not 0 <= minutes <= cfg['warning_minutes']:
            continue
        rate = float(data['lastFundingRate'])
        if direction(pos['side']) * rate > cfg['close_threshold']:
            journal.log('funding_close', symbol=symbol, rate=rate)
            await state.close_position(symbol, 'funding_avoidance')
