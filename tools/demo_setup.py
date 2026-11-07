"""Record local operator checks for the Demo key; never sends requests."""
import sys
from pathlib import Path
from datetime import datetime, timezone
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.demo_preflight import demo_config
from infra.journal import atomic_json


def main():
    cfg = demo_config()
    path = Path(cfg['rollout']['operator_policy_file'].format(mode='testnet'))
    if path.exists(): raise ValueError('Policy already exists; review it locally instead of replacing it')
    checks = {
        'dedicated_account': 'Dedicated Demo account; no other bot or manual trading?',
        'futures_only': 'Demo key permissions reviewed for Futures trading?',
        'withdrawals_disabled': 'Withdrawals disabled / unavailable for this Demo key?',
        'ip_whitelist_verified': 'Available IP controls reviewed and configured for this device?',
    }
    print('This is operator attestation, not proof from apiRestrictions. No secrets are requested.')
    if input('New Demo key created/rotated and any previously exposed key revoked? Type yes: ').strip() != 'yes':
        raise ValueError('Review the Demo key before setup')
    data = {'mode': 'testnet'}
    for key, prompt in checks.items():
        if input(prompt + ' Type yes: ').strip() != 'yes': raise ValueError('Operator checks incomplete')
        data[key] = True
    data['last_rotated_at'] = datetime.now(timezone.utc).isoformat()
    atomic_json(path, data)
    print('Local policy recorded. Run tools/demo_preflight.py; trading has not started.')


if __name__ == '__main__': main()
