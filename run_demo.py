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
    parser.add_argument('--qualification', action='store_true', help='Restricted Demo qualification')
    parser.add_argument('--describe-qualification', action='store_true', help='Print local approval fingerprint; no network')
    parser.add_argument('--approval', type=Path)
    parser.add_argument('--enable-demo', action='store_true', help='Explicitly authorize this Demo process')
    args = parser.parse_args()
    if args.qualification or args.describe_qualification or args.approval:
        if args.strategy != 'apex' or args.instruments is None or args.dashboard:
            parser.error('Qualification requires Apex, --instruments and local monitoring')
        if args.describe_qualification:
            if args.enable_demo or args.qualification or args.approval:
                parser.error('Describe qualification cannot start a session')
        elif not (args.qualification and args.enable_demo and args.approval):
            parser.error('Qualification requires --qualification --enable-demo --approval')
    elif not args.enable_demo:
        parser.error('Demo requires --enable-demo')
    elif args.strategy == 'apex':
        from apex.demo_execution import require_demo_release
        try:
            require_demo_release()
        except ValueError as exc:
            parser.error(str(exc))
    cfg = load_config()
    if cfg['exchange']['mode'] == 'mainnet':
        raise ValueError('Demo runner refuses Mainnet baseline')
    with execution_mode('testnet'):
        cfg = load_config()
        validate_demo(cfg)
        from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
        if args.strategy == 'apex':
            from apex.config import Config
            from apex.engine import Instrument
            from apex.qualification import Approval, qualification_config, redis_endpoint, settings_digest
            instruments = {s: Instrument(**v) for s, v in json.loads(args.instruments.read_text()).items()}
            config = qualification_config(Config.load(args.apex_config))
            endpoint = redis_endpoint()
            digest = settings_digest(config, instruments, cfg, endpoint)
            if args.describe_qualification:
                print(json.dumps(dict(scope="binance-demo-qualification", fingerprint=digest)))
                return
            approval = Approval.load(args.approval, digest)
            token = os.environ.get("APEX_FENCE_TOKEN")
            if not token:
                parser.error("Set APEX_FENCE_TOKEN locally")
            import httpx
            from apex.demo_runtime import ApexDemoRuntime
            from apex.qualification import AccountFence
            from run_bot import serve
            async def qualify():
                async with httpx.AsyncClient(base_url=endpoint, headers={"Authorization": "Bearer "+token},
                                             timeout=3, follow_redirects=False, trust_env=False) as client:
                    bot = ApexDemoRuntime(config, instruments, approval=approval,
                                          state_path=cfg['demo']['state_filename'], fee_rate=cfg['fee_rate'])
                    bot.exchange.approval = approval
                    bot.exchange.fence = AccountFence(client, approval.account_alias)
                    await serve(bot)
            asyncio.run(qualify())
            return
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
