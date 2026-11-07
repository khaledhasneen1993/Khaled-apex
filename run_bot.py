"""Run Khaled in the single execution mode selected by baseline.json."""
import argparse
import asyncio
import logging
from pathlib import Path

from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from risk.config import load_config


async def serve(bot, runner=None):
    from infra.shutdown import GracefulShutdown
    task = asyncio.create_task(runner if runner is not None else bot.start())
    shutdown = GracefulShutdown(bot, task)
    shutdown.install()
    try:
        await task
    except asyncio.CancelledError:
        if not shutdown.started: raise
    finally:
        shutdown.close()


def main():
    from infra.json_logging import configure
    configure()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', help='State filename; live state is isolated by mode')
    args = parser.parse_args()
    cfg = load_config()
    state = args.state or cfg['exchange']['state_filename']
    if cfg['exchange']['mode'] == 'paper' and args.state is None:
        state = str(Path(cfg['exchange']['state_dir']) / 'paper' / state)
    # Constructor refuses mainnet without evidence and explicit operator approval.
    bot = V2PaperHunter(state_path=state, fee_rate=cfg['fee_rate'])
    logging.getLogger('CASCADE').info('KHALED %s | MODE=%s | %sx | equity margin ceiling=10%% | max_positions=4', cfg['version'], bot.execution_mode, bot.settings()['LEVERAGE'])
    try:
        asyncio.run(serve(bot))
    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.getLogger('CASCADE').info('Stopped; state persisted')


if __name__ == '__main__':
    main()
