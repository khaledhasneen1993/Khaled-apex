"""Captured ATR-order settings for regression tests of legacy ledger recovery.

Not a runner or an alternative baseline. Current-policy tests load risk.config.
"""
from risk.config import load_config


def legacy_execution_config():
    cfg = load_config()
    cfg['exit']['policy'] = 'atr'
    cfg['exchange']['leverage_policy'] = 'maximum_by_bracket'
    cfg['tp_sl']['max_equity_risk_pct'] = .03
    cfg['funding']['block_new_entries'] = False
    return cfg
