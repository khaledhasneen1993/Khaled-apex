"""Fresh processes reproduce standalone CLI imports, without alerts side effects."""
import subprocess
import sys
from pathlib import Path
import pytest


@pytest.mark.parametrize('level', ['INFO', 'DEBUG'])
@pytest.mark.parametrize('status', [200, 401])
def test_standalone_signed_requests_never_print_transport_urls(level, status):
    root = Path(__file__).resolve().parents[1]
    code = '''
import asyncio, logging, os, tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock
import httpx
from risk.config import load_config
from exchange.live_exchange import LiveExchange
from exchange.orders import ExchangeError

# Same import path as standalone preflight; no infra.alerts import.
logging.basicConfig(level=logging.LEVEL)
logging.getLogger().setLevel(logging.LEVEL)
logging.getLogger('httpx').setLevel(logging.LEVEL)
logging.getLogger('httpcore.connection').setLevel(logging.DEBUG)
os.environ['BINANCE_API_KEY'] = 'unit-test-api-key'
os.environ['BINANCE_API_SECRET'] = 'unit-test-secret'
os.environ.pop('BINANCE_RSA_PRIVATE_KEY_PATH', None)

async def run():
    with tempfile.TemporaryDirectory() as temp:
        cfg = load_config()
        cfg['exchange'].update(mode='testnet', state_dir=temp)
        ex = LiveExchange(cfg, SimpleNamespace(acquire=AsyncMock()))
        await ex.client.aclose()
        ex.weights = SimpleNamespace(acquire=AsyncMock())
        async def handler(request):
            assert request.url.params.get('signature')
            return httpx.Response(STATUS, json={'code': -2015} if STATUS != 200 else {'ok': True})
        ex.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        try:
            result = await ex._request('GET', '/fapi/v1/accountConfig')
            assert result['ok'] is True
        except ExchangeError as error:
            assert error.status == STATUS
            ex.journal.log('request_failed', stage='preflight', http_status=error.status, exchange_error_code=error.code)
            print('Safe diagnostic:', str(error))
        logging.getLogger('httpcore.connection').debug('URL: https://example.test/private?signature=sensitive-signature')
        await ex.close()
        print('CHECK_COMPLETE')
asyncio.run(run())
'''.replace('logging.LEVEL', 'logging.' + level).replace('STATUS', str(status))
    result = subprocess.run([sys.executable, '-c', code], cwd=root, text=True,
                            capture_output=True, check=True, timeout=30)
    output = result.stdout + result.stderr
    assert 'CHECK_COMPLETE' in output
    for sensitive in ('example.test', 'signature=', 'unit-test-api-key', 'unit-test-secret', 'sensitive-signature'):
        assert sensitive not in output
