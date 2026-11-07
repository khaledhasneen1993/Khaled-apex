"""Baseline settings and scoped runner selection; no on-disk mode swapping."""
import json
import math
from pathlib import Path
from contextlib import contextmanager
from contextvars import ContextVar

_mode = ContextVar('execution_mode', default=None)


def load_config():
    root = Path(__file__).resolve().parents[1]
    cfg = json.loads((root / 'baseline.json').read_text())
    paper_notional = cfg.get('paper_position_notional_usdt')
    if paper_notional is not None and (
        type(paper_notional) not in (int, float)
        or not math.isfinite(paper_notional) or paper_notional <= 0
    ):
        raise ValueError('Paper position notional must be a finite positive amount')
    if cfg['exit'].get('policy') == 'hard_stop_trailing':
        if (cfg['leverage'] != cfg['exposure']['leverage']
                or cfg['max_concurrent_positions'] != cfg['exposure']['max_concurrent']):
            raise ValueError('Sizing and exposure configuration disagree')
        if not (0 < cfg['exit']['hard_stop_pct'] < 1
                and 0 < cfg['exit']['trail_distance_pct'] < cfg['exit']['trail_arm_pct'] < 1
                and 0 < cfg['tp_sl']['max_equity_risk_pct'] <= .03):
            raise ValueError('Invalid price-stop risk policy')
        if cfg['exit']['allow_partial_close'] is not False or cfg['exit']['trail_enabled'] is not True:
            raise ValueError('Current policy requires full exits and enabled trailing')
        if cfg['kill_switch']['daily_drawdown_action'] not in ('block_entries', 'flatten'):
            raise ValueError('Daily drawdown action must be explicit')
    if _mode.get() is not None: cfg['exchange']['mode'] = _mode.get()
    # A single install uses the same live state and lock from every cwd.
    for section, keys in (('exchange', ('state_dir',)), ('rollout', (
            'evidence_file', 'approval_file', 'operator_policy_file', 'mainnet_evidence_file',
            'scale_state_file', 'scale_approval_file'))):
        for key in keys:
            value = Path(cfg[section][key])
            if not value.is_absolute(): cfg[section][key] = str(root / value)
    return cfg


@contextmanager
def execution_mode(mode):
    if mode not in ('paper', 'testnet'): raise ValueError('Supported runner refuses Mainnet')
    token = _mode.set(mode)
    try: yield
    finally: _mode.reset(token)
