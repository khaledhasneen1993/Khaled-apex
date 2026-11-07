"""Preview/apply a flat Demo state's policy upgrade; GET-only exchange access.

Never resets a kill, drops trades, or starts trading. Requires a matching preview
hash for --apply and holds the same account lock as the trading process.
"""
import argparse
import asyncio
import copy
import fcntl
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journal.wal import WriteAheadLog
from infra.journal import atomic_json, OperationalJournal
from infra.rate_limit import TokenBucket
from tools.demo_preflight import demo_config, preflight, ReadOnlyDemo
from tools.reset_safety import reviewed_reset_state
from infra.errors import diagnostic_fields


from binance_cascade_hunter_dynamic_v2_paper import settings_for_config as current_settings


def prepare_upgrade(data, root, cfg, snapshot):
    if data['settings'].get('execution_mode') != 'testnet':
        raise ValueError('Upgrade accepts Demo state only')
    if any(snapshot.get(key) for key in ('positions', 'regular_orders', 'algo_orders',
            'local_pending_intents', 'pending_execution_prices', 'pending_native_exits', 'pending_accounting', 'unresolved_intents')):
        raise ValueError('Account or execution recovery is not flat/complete')
    # Reuse pending-intent/accounting checks on a disposable copy, without
    # accepting its reset result. The saved latch and counters remain unchanged.
    reviewed_reset_state(copy.deepcopy(data), root, cfg)
    upgraded = copy.deepcopy(data)
    upgraded['settings'] = current_settings(cfg)
    preview = {'version': cfg['version'], 'old_settings': data['settings'],
        'new_settings': upgraded['settings'], 'trades_retained': len(data['trades']),
        'kill_retained': data.get('operations', {}).get('kill', {}).get('triggered'),
        'trading_started': False, 'exchange_access': 'GET_only'}
    payload = {'state': data, 'target_settings': upgraded['settings']}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()
    preview['preview_sha256'] = digest
    return upgraded, preview


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--expected-sha256')
    args = parser.parse_args()
    if args.apply and not args.expected_sha256: parser.error('--apply requires the reviewed preview hash')
    cfg = demo_config()
    root = Path(cfg['exchange']['state_dir']) / 'testnet'
    state = root / cfg['demo']['state_filename']
    if not state.is_file(): parser.error('Existing Demo state not found; no upgrade needed for a fresh state')
    async def run():
        with (root / 'account.lock').open('a') as lock:
            try: fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError: raise ValueError('Stop the trading process before upgrading') from None
            exchange = ReadOnlyDemo(cfg, TokenBucket(cfg['exchange']['rate_per_sec'], cfg['exchange']['rate_burst']))
            try:
                snapshot = await preflight(exchange, cfg['demo']['preflight_symbol'], websocket=False)
                original = state.read_bytes()
                updated, preview = prepare_upgrade(json.loads(original), root, cfg, snapshot)
                print(json.dumps(preview, indent=2))
                if not args.apply: return
                if args.expected_sha256 != preview['preview_sha256']:
                    raise ValueError('Preview differs; review the fresh result before applying')
                backup = root / ('state.before-policy-' + preview['preview_sha256'] + '.json')
                if not backup.exists(): atomic_json(backup, json.loads(original))
                if state.read_bytes() != original: raise ValueError('State changed during upgrade')
                WriteAheadLog(state.parent / 'journal.wal').snapshot(state, updated)
                OperationalJournal(root / 'journal').log('policy_upgrade', report={
                    'version': cfg['version'], 'preview_sha256': preview['preview_sha256']})
                print('Policy upgraded locally. Trades, risk counters, kill latch and session guard retained. Trading remains stopped.')
            finally: await exchange.close()
    try: asyncio.run(run())
    except Exception as error:
        print(json.dumps({'stage': 'policy_upgrade', **diagnostic_fields(error)}))
        raise SystemExit(1) from None


if __name__ == '__main__': main()
