"""Explicit Binance USDT-M Futures Demo runner; never starts on import."""
import argparse
import asyncio
import os
from risk.config import load_config, execution_mode
from exchange.demo import validate_demo


def main():
    from infra.json_logging import configure
    configure()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dashboard', action='store_true', help='Publish to the existing dashboard using local token')
    args = parser.parse_args()
    cfg = load_config()
    if cfg['exchange']['mode'] == 'mainnet':
        raise ValueError('Demo runner refuses Mainnet baseline')
    with execution_mode('testnet'):
        cfg = load_config()
        validate_demo(cfg)
        from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
        bot = V2PaperHunter(state_path=cfg['demo']['state_filename'], fee_rate=cfg['fee_rate'])
        if args.dashboard:
            from run_paper_live import CloudPublisher, run
            token = os.getenv('CASCADE_DASHBOARD_TOKEN')
            if not token: raise ValueError('Set dashboard token locally for --dashboard')
            from run_bot import serve
            asyncio.run(serve(bot, run(bot, cloud_publisher=CloudPublisher(bot, 'khaled-demo', token))))
        else:
            from run_bot import serve
            asyncio.run(serve(bot))


if __name__ == '__main__':
    main()
