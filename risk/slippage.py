from risk.liq_math import direction


def entry_slippage_bps(expected, actual, side):
    return (actual - expected) / expected * 10000 * direction(side)


def exit_slippage_bps(expected, actual, side):
    return (expected - actual) / expected * 10000 * direction(side)


def check_slippage_health(journal, cfg):
    recent = journal.last_n(cfg['window'])
    values = [t['entry_slippage_bps'] for t in recent if 'entry_slippage_bps' in t]
    if not values:
        return None
    average = sum(values) / len(values)
    return f'slippage_high:{average:.1f}bps' if average > cfg['alert_bps'] else None
