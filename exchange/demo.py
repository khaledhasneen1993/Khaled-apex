"""The supported runner has an exact Demo endpoint allowlist."""
def validate_demo(cfg):
    ec = cfg['exchange']
    if (ec.get('leverage_policy') not in ('fixed', 'maximum_by_bracket') or ec['mode'] != 'testnet' or ec['base_url_testnet'] != 'https://demo-fapi.binance.com'
            or ec['ws_url_testnet'] != 'wss://demo-fstream.binance.com/market/stream'):
        raise ValueError('Demo runner refuses Mainnet and unapproved endpoints')
