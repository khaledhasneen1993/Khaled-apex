"""Isolated Paper worker using the unmodified upstream trading classes."""
import asyncio
import copy
import fcntl
import os
import signal
import sys
import time
from contextlib import nullcontext
from paths import RUNTIME, SOURCES, STATE_PATHS


def strip_credentials():
    for key in tuple(os.environ):
        if key.startswith(('BINANCE_', 'TELEGRAM_', 'CASCADE_DASHBOARD_', 'OAI_SITES_')):
            os.environ.pop(key, None)


def snapshot(bot, name, status):
    sc = bot.scanner
    mono, wall = sc.clock(), sc.wall_clock()
    health = []
    for symbol in sc.symbols:
        received, event = sc.received.get(symbol), sc.event_times.get(symbol)
        age = max(0, mono-received) if received is not None else None
        event_age = wall-event/1000 if event is not None else None
        fresh = bool(sc.connected and age is not None and age <= 10
                     and event_age is not None and -10 <= event_age <= 10)
        health.append(dict(symbol=symbol, age=age, event_age=event_age,
                           fresh=fresh, seeded=symbol in sc.histories))
    settings = bot.settings()
    exit_cfg = settings.get('active_exit', {}).get('exit', {})
    display = dict(margin_fraction=settings['MAX_POSITION_PCT'], leverage=settings['LEVERAGE'],
        max_positions=settings['MAX_CONCURRENT_POSITIONS'],
        hard_stop_pct=exit_cfg.get('hard_stop_pct', settings.get('STOP_LOSS_PCT')),
        trail_arm_pct=exit_cfg.get('trail_arm_pct'), trail_distance_pct=exit_cfg.get('trail_distance_pct'),
        fixed_tp_pct=settings.get('TAKE_PROFIT_PCT'),
        max_equity_risk_pct=settings.get('active_exit', {}).get('tp_sl', {}).get('max_equity_risk_pct'))
    state = copy.deepcopy(dict(version=1, bot=name, status=status, execution_mode='paper',
        initial_balance_usdt=getattr(bot, 'initial_balance', 100.), settings=settings,
        display_settings=display, fee_rate=bot.exchange.fee_rate,
        balance_usdt=bot.exchange.balance, equity_usdt=bot.exchange.equity,
        free_usdt=bot.exchange.free, used_usdt=bot.exchange.used,
        account_positions=bot.exchange.positions, strategy_positions=bot.positions,
        prices=bot.exchange.prices, trades=bot.exchange.trades,
        updated_at=time.time(), scanner=dict(connected=sc.connected, symbols=list(sc.symbols),
            events=sc.events, entries=sc.entries, book_checks=sc.book_checks,
            last_event_age=min((x['age'] for x in health if x['age'] is not None), default=None),
            fresh_symbols=sum(x['fresh'] for x in health), symbol_health=health)))
    if name == 'khaled':
        state['operational_health'] = dict(version=bot.cfg['version'],
            kill_switch_mode=bot.safety.kill.mode, ready=bot.safety.ready,
            storage_failed=bot.safety.storage_failed)
    target = RUNTIME/'data'/f'{name}.snapshot.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix('.tmp')
    import json
    temporary.write_text(json.dumps(state, ensure_ascii=False, allow_nan=False))
    temporary.replace(target)
    return state


async def serve(bot, name):
    bot.save()
    snapshot(bot, name, 'starting')
    async def report():
        while True:
            # Telemetry reads memory; durable saving remains owned by the bot.
            snapshot(bot, name, 'running')
            await asyncio.sleep(3)
    run = asyncio.create_task(bot.start())
    if name == 'khaled':
        from infra.shutdown import GracefulShutdown
        shutdown = GracefulShutdown(bot, run)
        shutdown.install()
        stopped = shutdown.stop_event
    else:
        shutdown = None
        stopped = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().add_signal_handler(sig, stopped.set)
    reporter = asyncio.create_task(report())
    stop = asyncio.create_task(stopped.wait())
    status = 'stopped'
    try:
        done, _ = await asyncio.wait((run, reporter, stop), return_when=asyncio.FIRST_COMPLETED)
        if stop in done and shutdown:
            try:
                await run  # Current Khaled shutdown drains before cancellation.
            except asyncio.CancelledError:
                if not shutdown.started: raise
        else:
            for task in (run, reporter):
                if task in done: await task
    except Exception:
        status = 'error'
        raise
    finally:
        for task in (run, reporter, stop):
            if not task.done(): task.cancel()
        await asyncio.gather(run, reporter, stop, return_exceptions=True)
        if shutdown: shutdown.close()
        snapshot(bot, name, status)


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in SOURCES:
        raise SystemExit('Choose final or khaled')
    name = sys.argv[1]
    strip_credentials()
    sys.path.insert(0, str(SOURCES[name]))
    state_path = STATE_PATHS[name]
    state_path.parent.mkdir(parents=True, exist_ok=True)
    (RUNTIME/'data').mkdir(parents=True, exist_ok=True)
    with (RUNTIME/'data'/f'{name}.lock').open('a') as worker_lock, \
         (state_path.parent/'account.lock').open('a') as account_lock:
        for lock in (worker_lock, account_lock):
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if name == 'khaled':
            from risk.config import execution_mode
            mode = execution_mode('paper')
        else:
            mode = nullcontext()
        with mode:
            from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
            bot = V2PaperHunter(state_path=state_path)
            if getattr(bot, 'execution_mode', 'paper') != 'paper':
                raise SystemExit('Dashboard permits Paper only')
            if name == 'khaled':
                # The dashboard supplies the health UI; avoid Demo's port 8080.
                bot.cfg['health']['enabled'] = False
            asyncio.run(serve(bot, name))


if __name__ == '__main__':
    main()
