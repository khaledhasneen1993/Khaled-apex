"""GET-only Demo account/endpoint check. Cannot establish order permissions."""
import argparse
import asyncio
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from risk.config import load_config, execution_mode
from exchange.demo import validate_demo
from exchange.live_exchange import LiveExchange
from infra.rate_limit import TokenBucket
from infra.errors import diagnostic_fields
from exchange.orders import TERMINAL_ORDER_STATUSES


class ReadOnlyDemo(LiveExchange):
    async def _request(self, method, *args, **kwargs):
        if method != 'GET': raise ValueError('Read-only tool refuses mutation')
        return await super()._request(method, *args, **kwargs)


def demo_config():
    original = load_config()
    if original['exchange']['mode'] == 'mainnet': raise ValueError('Demo tool refuses Mainnet baseline')
    with execution_mode('testnet'): cfg = load_config()
    validate_demo(cfg)
    return cfg


async def preflight(exchange, symbol, websocket=True):
    await exchange.load_markets()
    symbol = exchange.symbol(symbol)
    await exchange.refresh_account()
    account_config = await exchange._request('GET', '/fapi/v1/accountConfig', retry=True)
    # V3 balances omit canTrade. This is account enablement, not proof of
    # the API key's TRADE permission or native order acceptance.
    can_trade = account_config.get('canTrade')
    if type(can_trade) is not bool: can_trade = None
    orders = await exchange.get_open_orders()
    ticker = await exchange.fetch_ticker(symbol)
    choice = await exchange.select_leverage(symbol, exchange.equity * exchange.cfg['margin_fraction_per_position'], ticker['last'], confirm=False)
    config = await exchange._request('GET', '/fapi/v1/symbolConfig', {'symbol': exchange.raw(symbol)}, retry=True)
    await exchange._request('GET', '/fapi/v1/userTrades', {'symbol': exchange.raw(symbol), 'limit': 1}, retry=True)
    ws_ok = False
    if websocket:
        import aiohttp
        url = exchange.cfg['exchange']['ws_url_testnet'] + '?streams=' + exchange.raw(symbol).lower() + '@kline_1m'
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.ws_connect(url, timeout=aiohttp.ClientWSTimeout(ws_receive=15)) as socket:
                message = await socket.receive_json()
                ws_ok = message.get('data', {}).get('e') == 'kline'
    state_path = exchange.intent_path.parent / exchange.cfg['demo']['state_filename']
    local_state = json.loads(state_path.read_text()) if state_path.exists() else {}
    from binance_cascade_hunter_dynamic_v2_paper import settings_for_config
    policy_matches = not state_path.exists() or local_state.get('settings') == settings_for_config(exchange.cfg)
    operations = local_state.get('operations', {})
    kill = operations.get('kill', {}).get('triggered')
    guard_path = exchange.intent_path.parent / exchange.cfg['exchange']['session_guard_filename']
    guard = json.loads(guard_path.read_text()) if guard_path.exists() else {}
    storage_failed = operations.get('storage_failed', False)
    pending_accounting = sum(r.get('trade', {}).get('accounting_status') == 'pending' for r in exchange.executions.values())
    pending_prices = sum(bool(r.get('fill_price_pending')) for r in exchange.executions.values())
    pending_native = sum(bool(r.get('native_exit')) and (r['order'].get('status') not in TERMINAL_ORDER_STATUSES
        or bool(r.get('fill_price_pending')) or float(r['order'].get('executedQty', 0)) > 0 and not r.get('trade'))
        for r in exchange.executions.values())
    pending_management = [symbol for symbol, fault in exchange.management_faults.items() if fault.get('status') != 'resolved']
    unresolved = []
    for client_id, intent in exchange.intents.items():
        if intent['status'] == 'rejected': continue
        record = exchange.executions.get(exchange.execution_key(intent['symbol'], intent.get('order_id')))
        if intent['status'] == 'pending' or (intent['type'] == 'market' and (
                not record or record.get('fill_price_pending') or record['order'].get('status') not in ('FILLED', 'EXPIRED', 'CANCELED', 'REJECTED', 'EXPIRED_IN_MATCH'))):
            unresolved.append(client_id)
    return {'local_policy_matches': policy_matches, 'session_guard_active': bool(guard.get('active')), 'storage_failed': storage_failed,
        'unresolved_intents': unresolved, 'pending_accounting': pending_accounting, 'local_kill_latch': kill, 'mode': 'demo', 'equity_usdt': exchange.equity, 'available_usdt': exchange.free,
        'positions': len(exchange.positions), 'regular_orders': sum(not o.get('is_algo') for o in orders),
        'algo_orders': sum(bool(o.get('is_algo')) for o in orders), 'account_can_trade': can_trade,
        'account_can_trade_source': '/fapi/v1/accountConfig',
        'symbol': symbol, 'candidate': choice, 'current_symbol_configuration': config,
        'websocket_market_data_verified': ws_ok, 'signature_and_account_reads_verified': True,
        'order_submission_verified': False, 'protection_acceptance_verified': False,
        'apiRestrictions': 'not used; Demo 404 cannot establish permissions',
        'local_pending_intents': sum(i['status'] == 'pending' for i in exchange.intents.values()),
        'pending_execution_prices': pending_prices,
        'pending_native_exits': pending_native,
        'pending_symbol_management': pending_management,
        'symbol_entry_cooldowns': {symbol: fault['entry_block_until'] for symbol, fault in exchange.management_faults.items() if fault.get('entry_block_until')},
        'entry_blocks': operations.get('kill', {}).get('entry_blocks', {}),
        'same_symbol_loss_cooldowns': operations.get('loss_cooldowns', {}),
        'requires_operator_review': bool(pending_management or not policy_matches or exchange.positions or orders or kill or guard.get('active') or storage_failed or unresolved or pending_accounting or pending_prices or pending_native
                                         or not choice.get('entry_order_valid', False) or can_trade is not True or websocket and not ws_ok)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbol')
    args = parser.parse_args()
    async def run():
        cfg = demo_config()
        ec = cfg['exchange']
        exchange = ReadOnlyDemo(cfg, TokenBucket(ec['rate_per_sec'], ec['rate_burst']))
        try:
            result = await preflight(exchange, args.symbol or cfg['demo']['preflight_symbol'])
            print(json.dumps(result, indent=2))
            if result['requires_operator_review']: raise SystemExit(2)
        finally: await exchange.close()
    try: asyncio.run(run())
    except Exception as error:
        print(json.dumps({'stage': 'preflight', **diagnostic_fields(error)}))
        raise SystemExit(1) from None


if __name__ == '__main__': main()
