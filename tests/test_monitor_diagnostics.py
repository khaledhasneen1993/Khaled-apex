import json
import time
from unittest.mock import AsyncMock, Mock
import httpx
import pytest
from test_live_transport import live, run, runtime_with_transport
from exchange.orders import ExchangeError


class MonitorStopped(BaseException):
    pass


@pytest.mark.parametrize('stage', ['account_refresh', 'reconciliation', 'evidence_sample', 'state_save'])
def test_monitor_stage_is_recorded_without_exception_text(live, tmp_path, stage):
    sensitive = 'secret https://example.test/private?signature=do-not-log-this'
    async def check():
        bot, _ = await runtime_with_transport(live, tmp_path)
        await live.refresh_account()
        safety = bot.safety
        now = time.time()
        safety.last_account = safety.last_recon = safety.last_spread = safety.last_funding = safety.last_evidence = now
        safety.last_report_day = time.strftime('%Y-%m-%d', time.gmtime())
        safety.trip = AsyncMock(side_effect=MonitorStopped())
        if stage == 'account_refresh':
            safety.last_account = 0
            live.refresh_account = AsyncMock(side_effect=ExchangeError(503, -1007))
        elif stage == 'reconciliation':
            safety.last_recon = 0
            live.refresh_account = AsyncMock()
            safety.reconcile_once = AsyncMock(side_effect=KeyError(sensitive))
        elif stage == 'evidence_sample':
            safety.last_evidence = 0
            safety.evidence = Mock()
            safety.evidence.sample.side_effect = ValueError(sensitive)
        else:
            bot.save = Mock(side_effect=OSError(sensitive))
        if stage in ('account_refresh', 'state_save'):
            from unittest.mock import patch
            with patch('infra.supervisor.asyncio.sleep', side_effect=MonitorStopped()):
                with pytest.raises(MonitorStopped): await safety.monitor()
            safety.trip.assert_not_awaited()
        else:
            with pytest.raises(MonitorStopped): await safety.monitor()
            safety.trip.assert_awaited_once_with('monitor_failure')
        content = (safety.journal.root / 'events.log').read_text()
        records = [json.loads(line) for line in content.splitlines()]
        event = {'account_refresh':'monitor_retry', 'state_save':'storage_snapshot_failed'}.get(stage, 'monitor_exception')
        record = next(r for r in records if r['event'] == event)
        assert record['stage'] == stage and record['error_type']
        if stage == 'account_refresh':
            assert record['http_status'] == 503 and record['exchange_error_code'] == -1007
        assert sensitive not in content and 'signature=' not in content
        assert record['source'] == 'infra/supervisor.py'
        assert record['function'] == ('save_safely' if stage == 'state_save' else 'monitor') and type(record['line']) is int
        await live.close()
    run(check())


def test_transport_type_retained_without_url_or_error_body(live):
    async def handler(request):
        raise httpx.ReadTimeout('secret signed URL ' + str(request.url), request=request)
    async def check():
        from infra.errors import diagnostic_fields
        await live.client.aclose()
        live.client = httpx.AsyncClient(base_url='https://example.test', transport=httpx.MockTransport(handler))
        with pytest.raises(ExchangeError) as caught:
            await live._request('GET', '/fapi/v1/accountConfig')
        fields = diagnostic_fields(caught.value)
        assert fields['transport_error_type'] == 'ReadTimeout'
        assert 'signature=' not in json.dumps(fields) and 'example.test' not in json.dumps(fields)
        assert 'secret' not in str(caught.value)
        await live.close()
    run(check())


def test_flat_monitor_completes_real_reconciliation_evidence_and_save(live, tmp_path):
    from exchange.rollout import TestnetEvidence
    from unittest.mock import patch
    async def check():
        bot, _ = await runtime_with_transport(live, tmp_path)
        await live.refresh_account()
        bot.cfg['rollout']['evidence_file'] = str(tmp_path / 'evidence.json')
        bot.safety.evidence = TestnetEvidence(bot.cfg)
        bot.safety.last_report_day = time.strftime('%Y-%m-%d', time.gmtime())
        with patch('infra.supervisor.asyncio.sleep', side_effect=MonitorStopped()):
            with pytest.raises(MonitorStopped): await bot.safety.monitor()
        assert not bot.safety.kill.triggered
        assert bot.safety.last_account and bot.safety.last_recon and bot.safety.last_evidence
        assert json.loads((tmp_path / 'evidence.json').read_text())['samples'][-1]['equity'] == 100
        assert json.loads(bot.state_path.read_text())['operations']['kill']['triggered'] is None
        await live.close()
    run(check())
