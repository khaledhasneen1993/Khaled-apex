import time
import math
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone


@dataclass
class KillState:
    daily_pnl: float = 0.
    consecutive_losses: int = 0
    last_ws_heartbeat: float = 0.
    api_errors_1m: int = 0
    last_api_error_ts: float = 0.


class KillSwitch:
    def __init__(self, cfg: dict, environment=None):
        self.environment = environment
        self.mode = 'hard' if environment is None else cfg.get('mode_per_environment', {}).get(environment, cfg.get('mode', 'hard'))
        if environment == 'mainnet': self.mode = 'hard'
        if environment == 'demo': self.mode = 'observe'
        if self.mode not in ('observe', 'soft', 'hard'): raise ValueError('Invalid kill mode')
        self.cfg = dict(cfg)
        if environment == 'mainnet': self.cfg['daily_drawdown_action'] = 'flatten'
        levels = cfg.get('thresholds', {}).get(self.mode, {}) if environment is not None else {}
        for key, target in (('ws_offline_sec','ws_timeout_sec'), ('api_errors_per_min','api_error_limit'), ('spread_spike_mult','spread_spike_mult'), ('daily_dd_pct','daily_dd_limit_pct'), ('consecutive_losses','max_consecutive_losses')):
            if key in levels: self.cfg[target] = levels[key]
        self.cooldown_sec = levels.get('cooldown_sec', 600)
        self.triggered_at = None
        self.last_observed = None
        self.state = KillState(last_ws_heartbeat=time.time())
        self.triggered = None
        self.errors = deque()
        self.day = datetime.now(timezone.utc).date().isoformat()
        self.day_start_equity = None
        self.entry_blocks = {}
        self.handled_loss_streak = 0

    def on_trade_closed(self, pnl: float, closed_at=None):
        self.roll_day()
        closed_day = datetime.fromisoformat(closed_at).astimezone(timezone.utc).date().isoformat() if closed_at else self.day
        if closed_day == self.day: self.state.daily_pnl += pnl
        self.state.consecutive_losses = self.state.consecutive_losses + 1 if pnl < 0 else 0
        if pnl >= 0: self.handled_loss_streak = 0

    def roll_day(self, equity=None):
        day = datetime.now(timezone.utc).date().isoformat()
        if day != self.day:
            self.day = day
            self.state.daily_pnl = 0.
            self.day_start_equity = equity
        # Never reset the latch, loss streak, API window or WS age at midnight.

    def on_ws_heartbeat(self):
        self.state.last_ws_heartbeat = time.time()

    def _prune(self):
        cutoff = time.time() - self.cfg['api_window_sec']
        while self.errors and self.errors[0] < cutoff:
            self.errors.popleft()
        self.state.api_errors_1m = len(self.errors)

    def on_api_error(self):
        self.errors.append(time.time())
        self.state.last_api_error_ts = self.errors[-1]
        self._prune()

    def reset_daily(self):
        self.state.daily_pnl = 0.
        self.state.consecutive_losses = 0
        self.triggered = None
        self.errors.clear()
        self.state.api_errors_1m = 0
        self.state.last_ws_heartbeat = time.time()
        self.day = datetime.now(timezone.utc).date().isoformat()
        self.day_start_equity = None

    @property
    def blocks_entries(self): return bool(self.triggered) and self.mode != 'observe'

    def check(self, state, market):
        previous = self.triggered
        if self.mode != 'hard': self.triggered = None
        reason = self._check(state, market)
        if self.mode == 'soft' or self.environment == 'mainnet':
            reason = reason or self.entry_block_reason_raw()
            self.triggered = reason
        if self.mode == 'observe':
            reason = reason or self.entry_block_reason_raw()
            self.last_observed = reason
            self.triggered = reason
            self.entry_blocks.clear()
            return None
        if self.mode == 'soft':
            if reason:
                if previous is None: self.triggered_at = time.time()
            elif previous and time.time() - (self.triggered_at or time.time()) < self.cooldown_sec:
                self.triggered = previous
            else:
                self.triggered = None
                self.triggered_at = None
        return self.triggered

    def entry_block_reason_raw(self):
        return next((reason for reason, until in self.entry_blocks.items() if until > time.time()), None)

    def _check(self, state, market):
        self._prune()
        equity = float(state.equity)
        self.roll_day(equity)
        if not math.isfinite(equity) or equity <= 0:
            self.triggered = 'invalid_equity'
            return self.triggered
        if self.triggered and self.should_flatten(self.triggered):
            return self.triggered
        if self.day_start_equity is None:
            self.day_start_equity = equity
        # Include unrealized losses; compare with a stable daily starting equity.
        day_loss = min(self.state.daily_pnl, equity - self.day_start_equity)
        if time.time() - self.state.last_ws_heartbeat > self.cfg['ws_timeout_sec']:
            self.triggered = 'ws_timeout'
        elif self.state.api_errors_1m >= self.cfg['api_error_limit']:
            self.triggered = 'api_errors'
        elif day_loss <= -self.cfg['daily_dd_limit_pct'] * self.day_start_equity:
            self.triggered = 'daily_drawdown'
        if (market is not None and getattr(market, 'samples', 0) >= self.cfg['spread_warmup']
                and math.isfinite(market.avg_spread) and market.avg_spread > 0
                and math.isfinite(market.spread()) and market.spread() > self.cfg['spread_spike_mult'] * market.avg_spread):
            self.entry_blocks['spread_spike'] = time.time() + self.cfg['spread_block_sec']
        losses = self.state.consecutive_losses
        if losses >= self.cfg['max_consecutive_losses'] and losses > self.handled_loss_streak:
            self.entry_blocks['consecutive_losses'] = time.time() + self.cfg['consecutive_loss_pause_sec']
            self.handled_loss_streak = losses
        return self.triggered

    def entry_block_reason(self):
        return None if self.mode == 'observe' else self.entry_block_reason_raw()

    def should_flatten(self, reason):
        if self.mode != 'hard': return False
        if self.environment == 'mainnet': return bool(reason)
        if reason == 'daily_drawdown': return self.cfg['daily_drawdown_action'] == 'flatten'
        if reason in ('spread_spike', 'consecutive_losses'): return False
        return True

    def dump(self):
        return {'triggered_at': self.triggered_at, 'mode': self.mode, 'state': asdict(self.state), 'triggered': self.triggered, 'day': self.day,
                'day_start_equity': self.day_start_equity, 'errors': list(self.errors),
                'entry_blocks': self.entry_blocks, 'handled_loss_streak': self.handled_loss_streak}

    def restore(self, data):
        self.state = KillState(**data['state'])
        self.triggered = data['triggered']
        self.triggered_at = data.get('triggered_at') or (time.time() if self.triggered else None)
        self.day = data['day']
        self.day_start_equity = data.get('day_start_equity')
        self.errors = deque(data.get('errors', []))
        self.entry_blocks = data.get('entry_blocks', {})
        self.handled_loss_streak = data.get('handled_loss_streak', 0)
