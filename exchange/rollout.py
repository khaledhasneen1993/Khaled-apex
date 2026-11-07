"""Evidence gate. Selecting mainnet alone never authorizes funded execution."""
import hashlib
import json
import math
import statistics
import time
from pathlib import Path
from infra.journal import atomic_json


def strategy_fingerprint(cfg):
    keys = ('leverage', 'margin_fraction_per_position', 'max_concurrent_positions', 'cascade_threshold',
            'volume_spike', 'orderbook_imbalance_threshold', 'cooldown_sec', 'tp_sl', 'atr', 'exit',
            'kill_switch', 'exposure', 'funding', 'time_stop')
    return hashlib.sha256(json.dumps({k: cfg[k] for k in keys}, sort_keys=True).encode()).hexdigest()


def code_fingerprint():
    root = Path(__file__).resolve().parents[1]
    code = sorted(p for p in root.rglob('*.py') if not any(s in p.parts for s in ('.venv', '__pycache__', 'tests', 'runtime')))
    return hashlib.sha256(b''.join(str(p.relative_to(root)).encode() + p.read_bytes() for p in code)).hexdigest()


class TestnetEvidence:
    def __init__(self, cfg, mode="testnet"):
        self.cfg = cfg
        self.mode = mode
        self.path = Path(cfg["rollout"]["evidence_file" if mode == "testnet" else "mainnet_evidence_file"])
        self.data = json.loads(self.path.read_text()) if self.path.exists() else {}
        if self.data.get('mode') != mode or self.data.get('config_hash') != strategy_fingerprint(cfg) or self.data.get('code_hash') != code_fingerprint():
            self.data = {'mode': mode, 'config_hash': strategy_fingerprint(cfg), 'code_hash': code_fingerprint(),
                         'monitored_seconds': 0., 'continuous_seconds': 0., 'samples': [], 'trades': [],
                         'technical_kills': 0, 'reconcile_mismatches': 0, 'last_sample': None, 'started_at': time.time(), 'open_trade_ages': []}

    def start_session(self):
        # A process restart breaks the continuous-uptime acceptance window.
        self.data['continuous_seconds'] = 0.
        self.data['last_sample'] = None
        atomic_json(self.path, self.data)

    def sample(self, equity, trades, positions=None):
        now = time.time()
        last = self.data['last_sample']
        if last is not None:
            gap = now - last
            if 0 <= gap <= self.cfg['rollout']['max_monitor_gap_sec']:
                self.data['monitored_seconds'] += gap
                self.data['continuous_seconds'] += gap
            else:
                self.data['continuous_seconds'] = 0.
        self.data['last_sample'] = now
        self.data['samples'].append({'ts': now, 'equity': equity})
        from datetime import datetime
        self.data['trades'] = [t for t in trades if t.get('reduceOnly') and t.get('exit_detail')
                               and t.get('mode') == self.mode
                               and datetime.fromisoformat(t['timestamp']).timestamp() >= self.data['started_at']]
        self.data['open_trade_ages'] = [max(0, now - p['opened_at']) for p in (positions or {}).values()]
        atomic_json(self.path, self.data)

    def metrics(self):
        d = self.data
        equities = [s['equity'] for s in d['samples']]
        high = equities[0] if equities else 0
        dd = 0.
        returns = []
        for previous, value in zip(equities, equities[1:]):
            if previous > 0: returns.append(value / previous - 1)
        for value in equities:
            high = max(high, value)
            if high > 0: dd = max(dd, (high - value) / high)
        sharpe = None
        window = self.cfg['rollout']['sharpe_window']
        if len(returns) >= window and statistics.stdev(returns[-window:]) > 0:
            sharpe = statistics.mean(returns[-window:]) / statistics.stdev(returns[-window:]) * math.sqrt(self.cfg['rollout']['sharpe_periods_per_year'])
        trades = d['trades']
        slips = [t.get('entry_slippage_bps') for t in trades if t.get('entry_slippage_bps') is not None]
        return {'closed_trades': len(trades), 'max_drawdown': dd, 'rolling_100_sharpe': sharpe,
                'mean_entry_slippage_bps': sum(slips) / len(slips) if slips else None,
                'stuck_fraction': (sum(t['exit_detail']['time_in_trade_sec'] > self.cfg['rollout']['stuck_minutes'] * 60 for t in trades) + sum(age > self.cfg['rollout']['stuck_minutes'] * 60 for age in d.get('open_trade_ages', []))) / (len(trades) + len(d.get('open_trade_ages', []))) if trades or d.get('open_trade_ages') else None}


def require_mainnet_readiness(cfg):
    gate = cfg['rollout']
    evidence_path, approval_path = Path(gate['evidence_file']), Path(gate['approval_file'])
    if not evidence_path.is_file() or not approval_path.is_file():
        raise ValueError('Mainnet locked: 14-day testnet evidence and explicit operator approval required')
    for state_path in (Path(cfg['exchange']['state_dir']) / 'testnet').glob('*.json'):
        try: saved = json.loads(state_path.read_text())
        except (ValueError, OSError): raise ValueError('Mainnet locked: unreadable Testnet state') from None
        if isinstance(saved, dict) and saved.get('operations', {}).get('kill', {}).get('triggered'):
            raise ValueError('Mainnet locked: review and reset the Testnet incident latch first')
    evidence = TestnetEvidence(cfg)
    data, metrics = evidence.data, evidence.metrics()
    approval = json.loads(approval_path.read_text())
    expected = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    checks = (data.get('mode') == 'testnet', data['monitored_seconds'] >= gate['testnet_days_required'] * 86400,
        data['continuous_seconds'] >= gate['continuous_days_required'] * 86400,
        data.get('technical_kills') == 0, data.get('reconcile_mismatches') == 0,
        metrics['closed_trades'] >= gate['min_closed_trades'],
        metrics['max_drawdown'] < gate['max_drawdown_pct'],
        metrics['mean_entry_slippage_bps'] is not None and metrics['mean_entry_slippage_bps'] < gate['max_mean_entry_slippage_bps'],
        metrics['rolling_100_sharpe'] is not None and metrics['rolling_100_sharpe'] >= gate['min_sharpe'],
        metrics['stuck_fraction'] is not None and metrics['stuck_fraction'] < gate['max_stuck_fraction'],
        approval.get('approved') is True, approval.get('evidence_sha256') == expected,
        approval.get('config_hash') == strategy_fingerprint(cfg), approval.get('code_hash') == code_fingerprint(),
        approval.get('max_fraction') == gate['initial_mainnet_fraction'])
    if not all(checks):
        raise ValueError('Mainnet locked: testnet acceptance or explicit approval is incomplete')


def require_operator_policy(cfg, mode):
    """Operator attestation for controls the Futures API cannot prove locally."""
    path = Path(cfg['rollout']['operator_policy_file'].format(mode=mode))
    if not path.is_file():
        raise ValueError('Live mode requires a local operator policy record; see README')
    data = json.loads(path.read_text())
    required = ('dedicated_account', 'futures_only', 'withdrawals_disabled', 'ip_whitelist_verified')
    from datetime import datetime, timezone
    try:
        rotated = datetime.fromisoformat(data['last_rotated_at'])
        if rotated.tzinfo is None: raise ValueError()
        age = (datetime.now(timezone.utc) - rotated).total_seconds()
    except (KeyError, TypeError, ValueError):
        raise ValueError('Operator policy requires a timezone-aware last_rotated_at') from None
    if data.get('mode') != mode or not all(data.get(k) is True for k in required):
        raise ValueError('Operator policy incomplete for this execution environment')
    if not 0 <= age < cfg['exchange']['credentials_rotation_days'] * 86400:
        raise ValueError('Rotate API credentials and update the local operator policy')


def mainnet_allocation_fraction(cfg):
    """1% initially. Each increase needs a separate reviewed evidence receipt.

    A certificate is consumed once on restart; the running process never raises
    funded exposure on a timer or from a Telegram message.
    """
    gate = cfg['rollout']
    state_path = Path(gate['scale_state_file'])
    data = json.loads(state_path.read_text()) if state_path.exists() else {}
    if data.get('config_hash') != strategy_fingerprint(cfg) or data.get('code_hash') != code_fingerprint():
        data = {'fraction': gate['initial_mainnet_fraction'], 'last_review_monitored_seconds': 0.,
                'approval_sha256': None, 'config_hash': strategy_fingerprint(cfg), 'code_hash': code_fingerprint()}
    approval_path = Path(gate['scale_approval_file'])
    if approval_path.is_file():
        digest = hashlib.sha256(approval_path.read_bytes()).hexdigest()
        if digest != data['approval_sha256']:
            approval = json.loads(approval_path.read_text())
            evidence = TestnetEvidence(cfg, 'mainnet')
            metrics = evidence.metrics()
            evidence_path = Path(gate['mainnet_evidence_file'])
            if not evidence_path.exists(): raise ValueError('Scaling locked: mainnet evidence missing')
            expected_hash = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
            fraction = approval.get('fraction', 0)
            checks = (approval.get('approved') is True, approval.get('evidence_sha256') == expected_hash,
                approval.get('config_hash') == strategy_fingerprint(cfg), approval.get('code_hash') == code_fingerprint(),
                data['fraction'] < fraction <= min(data['fraction'] * gate['scale_factor'], gate['max_mainnet_fraction']),
                evidence.data['monitored_seconds'] - data['last_review_monitored_seconds'] >= gate['scale_review_days'] * 86400,
                evidence.data['technical_kills'] == 0, evidence.data['reconcile_mismatches'] == 0,
                metrics['closed_trades'] >= gate['min_closed_trades'], metrics['max_drawdown'] < gate['max_drawdown_pct'],
                metrics['mean_entry_slippage_bps'] is not None and metrics['mean_entry_slippage_bps'] < gate['max_mean_entry_slippage_bps'],
                metrics['rolling_100_sharpe'] is not None and metrics['rolling_100_sharpe'] >= gate['min_sharpe'],
                metrics['stuck_fraction'] is not None and metrics['stuck_fraction'] < gate['max_stuck_fraction'])
            if not all(checks): raise ValueError('Scaling locked: 14 monitored days, performance and explicit review required')
            data.update(fraction=fraction, last_review_monitored_seconds=evidence.data['monitored_seconds'], approval_sha256=digest)
    if not 0 < data['fraction'] <= gate['max_mainnet_fraction']:
        raise ValueError('Invalid mainnet allocation record')
    atomic_json(state_path, data)
    return data['fraction']
