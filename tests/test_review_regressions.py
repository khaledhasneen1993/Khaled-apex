import copy
from unittest.mock import AsyncMock, patch
import httpx
import pytest
from test_live_transport import live, run, runtime_with_transport
from exchange.orders import ExchangeError

S='BTC/USDT:USDT'


def test_review_fill_tier_change_must_not_block_emergency_close(live,tmp_path):
    async def check():
        bot, server = await runtime_with_transport(live,tmp_path)
        original=server.__call__
        async def handler(request):
            if request.url.path.endswith('/leverageBracket'):
                return httpx.Response(200,json=[{'brackets':[
                    {'initialLeverage':100,'notionalFloor':0,'notionalCap':1005,'maintMarginRatio':.004,'cum':0},
                    {'initialLeverage':50,'notionalFloor':1005,'notionalCap':100000,'maintMarginRatio':.008,'cum':4.02}]}])
            if request.url.path=='/fapi/v1/order' and request.method=='POST':
                server.price=101
            return await original(request)
        await live.client.aclose()
        live.client=httpx.AsyncClient(base_url='https://example.test',transport=httpx.MockTransport(handler))
        with patch('asyncio.sleep',new_callable=AsyncMock):
            assert not await bot.enter(S,'buy',100)
        await live.close()
        assert server.qty == 0, 'Filled entry is unprotected and emergency close is blocked by recovery of invalid risk levels'
    run(check())


def test_review_partial_close_is_not_final_trade(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        server.partial_close=True; server.price=100.2
        with pytest.raises(ExchangeError): await bot.close_position(S,'manual')
        assert server.qty==.5
        bot.merge_execution_state()
        await live.close()
        assert 'exit_detail' not in live.trades[-1], 'A partial close must not produce a final position result'
    run(check())


def test_review_sync_old_pending_entry_cannot_loosen_new_position(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        original=server.__call__
        async def handler(request):
            if request.url.path.endswith('/userTrades'):
                oid=request.url.params.get('orderId')
                if (oid=='1' and len(server.algos)<4) or oid=='3':
                    return httpx.Response(200,json=[])
            return await original(request)
        await live.client.aclose()
        live.client=httpx.AsyncClient(base_url='https://example.test',transport=httpx.MockTransport(handler))
        assert await bot.enter(S,'buy',100)
        assert live.trades[0]['accounting_status']=='pending'
        await bot.close_position(S,'manual')
        bot.last_trade.clear()
        server.price=110
        assert await bot.enter(S,'buy',110)
        current=bot.positions[S]
        native=server.algos[current['protection']['sl']['orderId']]['triggerPrice']
        await live.close()
        assert float(native)>=current['stop_initial'], 'Old entry accounting loosens the actual new native stop'
    run(check())


def test_review_kill_with_disk_error_still_attempts_flatten(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        with patch.object(bot,'save',side_effect=OSError('disk full')):
            await bot.safety.trip('protection_failure')
            assert bot.safety.storage_failed and bot.safety.kill.triggered
        await live.close()
        assert not server.qty, 'A local state write failure aborts all emergency network attempts'
    run(check())


def test_review_closed_pending_protection_is_removed(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        await bot.close_position(S,'manual')
        assert not bot.positions and not server.qty
        await live.close()
        assert S not in live.pending_protection, 'Normal closes leave stale strategy positions in pending_protection'
    run(check())


def test_review_mmr_deduction_must_be_used_in_stop_cap():
    from risk.config import load_config
    from risk.atr_levels import compute_tp_sl
    from risk.liq_math import compute_liq_price
    cfg=load_config()['tp_sl']
    cfg.update(mmr=.008,maintenance_amount=4.02,quantity=10)
    liquidation=compute_liq_price('buy',101,100,.008,4.02,10)
    assert 101>100.8>liquidation*1.003
    compute_tp_sl('buy',101,.1,100,cfg)


def test_review_stale_closed_position_must_not_count_loss_twice(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        await bot.close_position(S,'manual')
        previous=bot.safety.kill.state.daily_pnl
        live.execution_fault='order_execution_uncertain'
        with patch.object(bot.strategy.CascadeHunter,'enter',new=AsyncMock(return_value=False)):
            await bot.enter(S,'buy',100)
        await live.close()
        assert bot.safety.kill.state.daily_pnl==previous
    run(check())


def test_review_rejected_startup_policy_must_not_place_orders(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        server.qty=1
        bot.exchange.load_markets=AsyncMock(return_value=live.markets)
        bot.cfg['rollout']['operator_policy_file']=str(tmp_path/'missing-policy-{mode}.json')
        with pytest.raises(ValueError,match='operator policy'):
            await bot.start()
        assert not server.orders, 'Startup policy rejection still invokes flatten and submits orders'
    run(check())


def test_risk_builder_failure_records_fill_and_emergency_accounting(live,tmp_path):
    from paper_account import make_position
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        original=server.__call__
        async def handler(request):
            if request.url.path=='/fapi/v1/order' and request.method=='POST': server.price=101
            return await original(request)
        await live.client.aclose()
        live.client=httpx.AsyncClient(base_url='https://example.test',transport=httpx.MockTransport(handler))
        def model(side,entry,*args,**kwargs):
            if entry==101: raise ValueError('model failure')
            return make_position(side,entry,*args,**kwargs)
        with patch('exchange.execution.make_position',side_effect=model):
            assert not await bot.enter(S,'buy',100)
        assert not server.qty and len(server.orders)==2
        entry=live.executions[live.execution_key(S,1)]
        assert entry['risk_error']=='risk_levels_unavailable' and entry['trade']['amount']==1
        assert live.trades[-1]['exit_detail']['net_pnl_usdt']==pytest.approx(server.wallet-100)
        assert live.trades[-1]['exit_detail']['tp'] is None
        await live.close()
    run(check())


def test_full_disk_fsync_failure_preserves_native_protection_and_latches(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        with patch('infra.journal.os.fsync',side_effect=OSError('full disk')):
            await bot.safety.trip('storage_failure')
        assert server.qty and len(server.orders)==1
        assert all(a['algoStatus']=='NEW' for a in server.algos.values())
        assert live.storage_fault and bot.safety.storage_failed
        assert bot.safety.kill.triggered and not bot.safety.ready
        assert not await bot.enter(S,'buy',100)
        await live.close()
    run(check())


def test_close_accounting_and_kill_counter_idempotent_after_restart(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        server.partial_close=True
        with pytest.raises(ExchangeError): await bot.close_position(S,'manual')
        bot.merge_execution_state()
        assert not bot._notifications
        await bot.close_position(S,'manual')
        assert len([t for t in live.trades if t.get('exit_detail')])==1
        pnl=bot.safety.kill.state.daily_pnl
        bot.restore()
        await live.refresh_account(); await live.recover_execution()
        bot.merge_execution_state(); await bot._drain_notifications()
        assert bot.safety.kill.state.daily_pnl==pnl
        assert len(bot.safety.accounted_closes)==1
        await live.close()
    run(check())


def test_alert_queue_full_cannot_block_kill(live,tmp_path):
    import asyncio
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        bot.safety.alerter.enabled=True
        bot.safety.alerter.queue=asyncio.Queue(maxsize=1)
        bot.safety.alerter.queue.put_nowait('busy')
        await asyncio.wait_for(bot.safety.trip('queue_failure'),2)
        assert not server.qty and bot.safety.kill.triggered
        assert bot.safety.alerter.failed
        await live.close()
    run(check())


def test_startup_missing_telegram_cannot_flatten(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        server.qty=1
        live.load_markets=AsyncMock(return_value=live.markets)
        with patch('exchange.rollout.require_operator_policy'):
            with pytest.raises(ValueError,match='TELEGRAM'): await bot.start()
        assert not server.orders and server.qty==1
    run(check())


def test_wrong_native_protection_response_cannot_cancel_original(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        pos=bot.positions[S]
        old=copy.deepcopy(pos['protection']['sl'])
        pos['sl']=99.5
        original=server.__call__
        async def handler(request):
            response=await original(request)
            if request.method=='GET' and request.url.path=='/fapi/v1/algoOrder':
                data=response.json(); data['quantity']='0.001'
                return httpx.Response(200,json=data)
            return response
        await live.client.aclose()
        live.client=httpx.AsyncClient(base_url='https://example.test',transport=httpx.MockTransport(handler))
        with pytest.raises(ExchangeError): await live.protect(S,pos)
        assert server.algos[old['orderId']]['algoStatus']=='NEW'
        assert pos['protection']['sl']==old
        await live.close()
    run(check())


def test_session_guard_blocks_restart_without_manual_review(live,tmp_path):
    import asyncio,json
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        async def worker(): await asyncio.Future()
        bot.safety.alerter.enabled=True
        bot.safety.alerter._worker=worker
        bot.safety.alerter.send=AsyncMock(return_value=True)
        with patch('exchange.rollout.require_operator_policy'):
            await bot.safety.startup()
            guard=bot.state_path.parent / live.cfg['exchange']['session_guard_filename']
            assert json.loads(guard.read_text())['active']
            assert bot._management_authorized
            with pytest.raises(ValueError,match='Kill switch'): await bot.safety.startup()
        assert bot.safety.kill.triggered=='unclean_session_review'
        assert not server.orders
        await live.close()
    run(check())


def test_crash_between_fill_fact_and_risk_position_restores_protection(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        key=live.execution_key(S,1)
        live.executions[key].pop('position')
        live.pending_protection.clear()
        for algo in server.algos.values(): algo['algoStatus']='CANCELED'
        assert await live.recover_execution()
        assert 'position' in live.executions[key]
        assert len([a for a in server.algos.values() if a['algoStatus']=='NEW'])==2
        assert len(server.orders)==1
        await live.close()
    run(check())


@pytest.mark.parametrize('side,sign',[('buy',1),('sell',-1)])
def test_bracket_deduction_stop_boundary_matches_equity_equation(side,sign):
    from risk.liq_math import compute_liq_price,liquidation_stop_cap
    from risk.atr_levels import compute_tp_sl
    from risk.config import load_config
    import math
    entry,qty,leverage,mmr,cum=101.,10.,100.,.008,4.02
    liq=compute_liq_price(side,entry,leverage,mmr,cum,qty)
    equity=qty*(entry/leverage+sign*(liq-entry))
    maintenance=qty*liq*mmr-cum
    assert equity==pytest.approx(maintenance)
    cfg=load_config()['tp_sl']
    cfg.update(mmr=mmr,maintenance_amount=cum,quantity=qty)
    levels=compute_tp_sl(side,entry,3,leverage,cfg)
    assert sign*(levels['stop']-liq*(1+sign*cfg['liq_safety_margin']))>0
    assert levels['stop_pct']<=liquidation_stop_cap(side,entry,leverage,mmr,cfg['liq_safety_margin'],cum,qty)+math.ulp(1.)


def test_legacy_saved_closed_trade_not_applied_to_kill_twice(live,tmp_path):
    import json
    from infra.journal import atomic_json
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        await bot.close_position(S,'manual')
        old=json.loads(bot.state_path.read_text())
        old['operations'].pop('accounted_closes')
        atomic_json(bot.state_path,old)
        previous=bot.safety.kill.state.daily_pnl
        bot.restore()
        await live.recover_execution(); bot.merge_execution_state(); await bot._drain_notifications()
        assert bot.safety.kill.state.daily_pnl==previous
        await live.close()
    run(check())


def test_stale_order_query_cannot_reduce_confirmed_execution(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        record=live.executions[live.execution_key(S,1)]
        stale={**record['order'],'status':'PARTIALLY_FILLED','executedQty':'.5'}
        await live._accept_fill(S,stale,record['options'],'buy',recovered=True)
        assert live.trades[0]['amount']==1
        assert live.executions[live.execution_key(S,1)]['order']['status']=='FILLED'
        assert len(server.orders)==1
        await live.close()
    run(check())


def test_zero_entry_commission_survives_risk_rebuild(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        record=live.executions[live.execution_key(S,1)]
        record['trade']['fee_usdt']=0.
        record['trade']['fee_exact']='0'
        record['trade']['accounting_status']='complete'
        record.pop('position')
        live.pending_protection.clear()
        for algo in server.algos.values(): algo['algoStatus']='CANCELED'
        await live.recover_execution()
        assert live.executions[live.execution_key(S,1)]['position']['entry_fee']==0.
        await live.close()
    run(check())


def test_disk_failure_and_lost_close_response_do_not_resend(live,tmp_path):
    async def check():
        bot,server=await runtime_with_transport(live,tmp_path)
        assert await bot.enter(S,'buy',100)
        original=server.__call__
        posts=[]
        async def handler(request):
            result=await original(request)
            if request.method=='POST' and request.url.path=='/fapi/v1/order':
                posts.append(request.url.params['newClientOrderId'])
                raise httpx.ReadTimeout('lost close response',request=request)
            return result
        await live.client.aclose()
        live.client=httpx.AsyncClient(base_url='https://example.test',transport=httpx.MockTransport(handler))
        with patch('infra.journal.os.fsync',side_effect=OSError('full disk')):
            await bot.safety.trip('storage_failure')
        assert server.qty and not posts and len(server.orders)==1
        assert bot.safety.kill.triggered and live.storage_fault
        await live.close()
    run(check())
