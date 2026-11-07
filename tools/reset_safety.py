"""Explicit offline reset after operator review; never starts a trading process."""
import argparse
import fcntl
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journal.wal import WriteAheadLog
from infra.journal import OperationalJournal, atomic_json
from risk.kill_switch import KillSwitch
from risk.config import load_config


def reviewed_reset_state(data, root, cfg):
    """Clear the reviewed latch, preserving risk history and pending evidence."""
    if data['account_positions'] or data['strategy_positions']:
        raise ValueError('Positions remain in saved state; reconcile and review first')
    intents_path, executions_path = root / 'order_intents.json', root / 'executions.json'
    intents = json.loads(intents_path.read_text()) if intents_path.exists() else {}
    executions = json.loads(executions_path.read_text()) if executions_path.exists() else {}
    terminal = {'FILLED', 'EXPIRED', 'CANCELED', 'REJECTED', 'EXPIRED_IN_MATCH'}
    for intent in intents.values():
        if intent['status'] == 'rejected': continue
        key = intent['symbol'] + ':' + str(intent.get('order_id'))
        record = executions.get(key)
        if intent['status'] == 'pending' or intent['type'] == 'market' and (
                not record or record.get('fill_price_pending') or record['order'].get('status') not in terminal):
            raise ValueError('Unresolved order intent remains; restore it before reset')
    if any(r.get('fill_price_pending') or r.get('trade', {}).get('accounting_status') == 'pending' for r in executions.values()):
        raise ValueError('Pending accounting or execution price remains; restore exact executions before reset')
    if any(r.get('native_exit') and (r['order'].get('status') not in terminal
            or float(r['order'].get('executedQty', 0)) > 0 and not r.get('trade')) for r in executions.values()):
        raise ValueError('Unresolved native exit remains; reconcile before reset')
    management_path = root / cfg['exchange']['management_filename']
    management = json.loads(management_path.read_text()) if management_path.exists() else {}
    if any(f.get('status') != 'resolved' for f in management.values()):
        raise ValueError('Pending symbol management remains; recover it before reset')
    kill = KillSwitch(cfg['kill_switch'])
    kill.restore(data['operations']['kill'])
    kill.roll_day(data.get('equity_usdt'))
    kill.triggered = None
    data['operations']['kill'] = kill.dump()
    data['operations']['announced_kill'] = False
    data['operations']['storage_failed'] = False
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', required=True)
    parser.add_argument('--reviewed', action='store_true', help='Confirm the incident has been reviewed manually')
    args = parser.parse_args()
    if not args.reviewed: parser.error('Manual incident review is required')
    path = Path(args.state)
    with (path.parent / 'account.lock').open('a') as lock:
        try: fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError: parser.error('Stop the running account process before reset')
        data = json.loads(path.read_text())
        cfg = load_config()
        try: data = reviewed_reset_state(data, path.parent, cfg)
        except ValueError as error: parser.error(str(error))
        WriteAheadLog(path.parent / 'journal.wal').snapshot(path, data)
        atomic_json(path.parent / cfg['exchange']['session_guard_filename'], {'active': False, 'reviewed': True})
        OperationalJournal(path.parent / 'journal').log('kill_manual_reset')
    print('Reviewed local latch reset. Risk counters retained; startup reconciliation and risk gates still apply.')


if __name__ == '__main__':
    main()
