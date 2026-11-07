"""Entry permissions separated from position management and emergency policy."""
from enum import StrEnum


class Safety(StrEnum):
    READY = "ready"
    WARNING = "warning"
    DEGRADED = "degraded"
    PROTECTIVE = "protective"
    EMERGENCY = "emergency"


def state(*, data_ok, execution_ok=True, unresolved=False, protection_ok=True,
          storage_ok=True, confirmed_emergency=False, warning=False):
    if confirmed_emergency:
        return Safety.EMERGENCY
    if unresolved or not protection_ok or not storage_ok:
        return Safety.PROTECTIVE
    if not data_ok or not execution_ok:
        return Safety.DEGRADED
    return Safety.WARNING if warning else Safety.READY
