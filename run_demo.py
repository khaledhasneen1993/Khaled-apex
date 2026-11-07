"""Explicit Binance USDT-M Futures Demo runner; never starts on import."""
import argparse
import asyncio
import os
import json
from pathlib import Path
from risk.config import load_config, execution_mode
from exchange.demo import validate_demo


def main():
    from infra.json_logging import configure
    configure()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--strategy', choices=('legacy', 'apex'), default='legacy')
    parser.add_argument('--instruments', type=Path)
    parser.add_argument('--apex-config', type=Path, default=Path('apex.json'))
    parser.add_argument('--dashboard', action='store_true', help='Publish to the existing dashboard using local token')
    parser.add_argument('--enable-demo', action='store_true', help='Explicitly authorize this Demo process')
    args = parser.parse_args()
    if not args.enable_demo:
        parser.error('Demo requires --enable-demo')
    if args.strategy == 'apex':
        from apex.demo_execution import require_demo_release
        try:
            require_demo_release()
        except ValueError as exc:
            parser.error(str(exc))
        if args.instruments is None or args.dashboard:
            parser.error('Apex Demo requires --instruments and local monitoring')
    cfg = load_config()
    if cfg['exchange']['mode'] == 'mainnet':
        raise ValueError('Demo runner refuses Mainnet baseline')
    with execution_mode('testnet'):
        cfg = load_config()
        validate_demo(cfg)
        from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
        if args.strategy == 'apex':
            from apex.config import Config
            from apex.demo_runtime import ApexDemoRuntime
            from apex.engine import Instrument
            instruments = {s: Instrument(**v) for s, v in json.loads(args.instruments.read_text()).items()}
            bot = ApexDemoRuntime(Config.load(args.apex_config), instruments,
                                  state_path=cfg['demo']['state_filename'], fee_rate=cfg['fee_rate'])
        else:
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
