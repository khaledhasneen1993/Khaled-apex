"""Versioned configuration for the experimental decision engine."""
from dataclasses import asdict, dataclass, fields
from hashlib import sha256
import json
import math
from pathlib import Path


@dataclass(frozen=True)
class Config:
    version: int = 1
    max_age_ms: int = 1500
    rank_window_ms: int = 75
    max_spread: float = .002
    max_slippage: float = .003
    min_depth_usdt: float = 1000
    momentum: float = .012
    volume_spike: float = 3.5
    min_flow: float = .15
    trend: float = .005
    high_volatility: float = .012
    low_volatility: float = .0005
    fee: float = .0005
    risk_fraction: float = .0025
    leverage: int = 10
    mmr: float = .004
    liquidation_buffer: float = .003
    min_stop: float = .008
    atr_stop: float = 1.5
    trail_atr: float = 1.2
    reward_risk: float = 1.5
    margin_fraction: float = .1
    gross_fraction: float = 2.0
    direction_fraction: float = 1.5
    symbol_fraction: float = .75
    aggregate_risk: float = .01
    cluster_risk: float = .005
    volatility_exposure: float = .03
    correlation: float = .7
    drawdown_reduce: float = .025
    drawdown_block: float = .05

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if (type(value) not in (int, float) or not math.isfinite(value) or value <= 0
                    or (type(field.default) is int and type(value) is not int)):
                raise ValueError(f"Invalid Apex setting {field.name}")
        if (self.version != 1 or self.rank_window_ms >= self.max_age_ms
                or not .0025 <= self.risk_fraction <= .005
                or self.leverage > 10 or not 0 < self.fee < self.max_slippage < .1
                or not self.low_volatility < self.high_volatility < 1
                or not self.drawdown_reduce < self.drawdown_block < 1
                or not self.risk_fraction <= self.cluster_risk <= self.aggregate_risk < 1
                or not self.symbol_fraction <= self.direction_fraction <= self.gross_fraction
                or not self.min_stop + self.mmr + self.liquidation_buffer < 1 / self.leverage
                or self.margin_fraction > 1 or self.correlation > 1 or self.min_flow > 1):
            raise ValueError("Inconsistent Apex settings")

    @classmethod
    def load(cls, path):
        return cls(**json.loads(Path(path).read_text()))

    @property
    def fingerprint(self):
        return sha256(json.dumps(asdict(self), sort_keys=True, allow_nan=False).encode()).hexdigest()
