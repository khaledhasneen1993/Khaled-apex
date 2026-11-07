"""Evidence check. Approval records remain separate from model fitting."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Evidence:
    trades: int
    net_pnl: float
    drawdown: float
    oos: bool
    costs_included: bool
    recovery_passed: bool
    lower_paired_edge_ci: float
    worst_regime_delta: float


def gate(champion, challenger, approved_by=None, *, min_trades=100, max_drawdown=.15):
    failures = []
    if min(champion.trades, challenger.trades) < min_trades:
        failures.append("INSUFFICIENT_SAMPLE")
    if not (champion.oos and challenger.oos and champion.costs_included and challenger.costs_included):
        failures.append("MISSING_OUT_OF_SAMPLE_COST_EVIDENCE")
    if not challenger.net_pnl > champion.net_pnl or not challenger.lower_paired_edge_ci > 0:
        failures.append("SUPERIORITY_UNPROVEN")
    if not 0 <= challenger.drawdown <= max_drawdown or not challenger.worst_regime_delta >= 0:
        failures.append("RISK_OR_REGIME_REGRESSION")
    if not challenger.recovery_passed:
        failures.append("RECOVERY_UNPROVEN")
    if not approved_by:
        failures.append("OPERATOR_APPROVAL_REQUIRED")
    return failures
