"""Explicit sample units and dependence-aware resampling."""
from math import sqrt
import random
from statistics import fmean, stdev


def max_drawdown(equity):
    peak = equity[0]
    worst = 0.
    for value in equity:
        peak = max(peak, value)
        if peak > 0:
            worst = max(worst, (peak-value)/peak)
    return worst


def summary(trades, equity, periodic_returns=(), periods_per_year=None):
    values = [trade["net_pnl"] for trade in trades]
    wins, losses = [x for x in values if x > 0], [-x for x in values if x < 0]
    sharpe = sortino = None
    if periods_per_year and len(periodic_returns) >= 2:
        mean = fmean(periodic_returns)
        deviation = stdev(periodic_returns)
        downside = sqrt(fmean(min(0., r)**2 for r in periodic_returns))
        sharpe = mean/deviation*sqrt(periods_per_year) if deviation else None
        sortino = mean/downside*sqrt(periods_per_year) if downside else None
    tail = sorted(values)[:max(1, len(values)//20)]
    return dict(trades=len(values), net_pnl=sum(values), expectancy=fmean(values) if values else None,
                profit_factor=sum(wins)/sum(losses) if losses else None,
                max_drawdown=max_drawdown(equity), sharpe=sharpe, sortino=sortino,
                win_rate=len(wins)/len(values) if values else None,
                average_win=fmean(wins) if wins else None, average_loss=fmean(losses) if losses else None,
                tail_loss=fmean(tail) if tail else None,
                mean_entry_slippage=fmean(t["slippage"] for t in trades) if trades else None)


def block_bootstrap(values, block=5, repetitions=1000, seed=7):
    if not 1 <= block <= len(values) or repetitions < 40:
        raise ValueError("Insufficient resampling data")
    rng = random.Random(seed)
    means = []
    for _ in range(repetitions):
        resampled = []
        while len(resampled) < len(values):
            start = rng.randrange(len(values)-block+1)
            resampled.extend(values[start:start+block])
        means.append(fmean(resampled[:len(values)]))
    means.sort()
    return means[int(.025*(repetitions-1))], means[int(.975*(repetitions-1))]
