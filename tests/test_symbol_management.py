"""Per-symbol post-fill fault isolation; no authenticated network requests."""
import asyncio
import copy
from unittest.mock import AsyncMock, patch
import httpx
import pytest
from futures_simulator import FuturesSimulator, production_bot


def run(coro): return asyncio.run(coro)


def symbols_server():
    server=FuturesSimulator()
    for old,new in (('AIOUSDT','AKEUSDT'),('SOLUSDT','IDOLUSDT')):
        server.specs[new]=server.specs.pop(old)
        server.prices[new]=server.prices.pop(old)
    return server


async def replace_transport(bot,handler):
    await bot.exchange.client.aclose()
    bot.exchange.client=httpx.AsyncClient(base_url=bot.cfg['exchange']['base_url_testnet'],transport=httpx.MockTransport(handler))


def test_first_protection_rejection_then_retry_keeps_both_positions(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        attempts=0
        async def send(request):
            nonlocal attempts
            if request.method=='POST' and request.url.path=='/fapi/v1/algoOrder' and request.url.params['symbol']=='AKEUSDT':
                attempts+=1
                if attempts==1:return httpx.Response(400,json={'code':-2021})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):
            assert await bot.enter(ake,'sell',.05)
        assert attempts==2
        assert set(bot.positions)=={idol,ake} and not bot.safety.kill.triggered
        assert len(server.orders)==2
        assert bot.positions[ake]['protection']['sl']
        await bot.exchange.close()
    run(check())


def test_final_protection_failure_closes_only_ake_and_keeps_idol(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        healthy=copy.deepcopy(bot.positions[idol]['protection'])
        async def send(request):
            if request.method=='POST' and request.url.path=='/fapi/v1/algoOrder' and request.url.params['symbol']=='AKEUSDT':
                return httpx.Response(400,json={'code':-2021})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):
            assert not await bot.enter(ake,'sell',.05)
        assert set(bot.positions)=={idol}
        assert server.positions['IDOLUSDT']['qty'] and not server.positions['AKEUSDT']['qty']
        assert bot.positions[idol]['protection']==healthy
        assert not bot.safety.kill.triggered and bot.exchange.execution_fault is None
        assert len(server.orders)==3
        assert len([t for t in bot.exchange.trades if t['symbol']==ake and t.get('exit_detail')])==1
        assert await bot.safety.reconcile_once()
        assert idol in bot.positions and not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_unresolved_algo_retirement_does_not_fail_close_and_recovers(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        assert await bot.enter(ake,'sell',.05)
        retiring=bot.positions[ake]['protection']['sl']['orderId']
        stuck=True
        async def send(request):
            if stuck and request.method=='DELETE' and request.url.path=='/fapi/v1/algoOrder' and request.url.params.get('algoId')==str(retiring):
                return httpx.Response(200,json={'code':200})  # DELETE ack, GET still NEW.
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):
            await bot.close_position(ake,'operator_close')
        assert set(bot.positions)=={idol} and not server.positions['AKEUSDT']['qty']
        assert not bot.safety.kill.triggered and bot.exchange.execution_fault is None
        assert bot.exchange.management_faults[ake]['retirement_pending']
        before=len(server.orders)
        assert not await bot.enter(ake,'sell',.05)
        stuck=False
        bot.exchange.management_faults[ake]['last_attempt']-=10
        assert await bot.safety.reconcile_once()
        assert not bot.exchange.management_faults[ake]['retirement_pending']
        assert len(server.orders)==before and idol in bot.positions
        assert len([t for t in bot.exchange.trades if t['symbol']==ake and t.get('exit_detail')])==1
        await bot.exchange.close()
    run(check())


def test_lost_algo_post_response_is_queried_without_second_post(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        ake=bot.exchange.symbol('AKEUSDT')
        posts=0;queries=0
        async def send(request):
            nonlocal posts,queries
            if request.url.path=='/fapi/v1/algoOrder' and request.method=='GET':
                queries+=1
                if queries<=3:return httpx.Response(400,json={'code':-2013})
            response=await server(request)
            if request.url.path=='/fapi/v1/algoOrder' and request.method=='POST':
                posts+=1
                raise httpx.ReadTimeout('untrusted signed URL must not be logged',request=request)
            return response
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert await bot.enter(ake,'sell',.05)
        assert posts==1 and queries>=4 and len(server.orders)==1
        intents=[i for i in bot.exchange.intents.values() if i['type']=='STOP_MARKET']
        assert len(intents)==1 and intents[0]['protection_committed']
        assert not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_supported_regular_stop_fallback_is_confirmed_and_native_fill_recovered(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();server.advertise_regular_stop=server.regular_stops=True
        bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        async def send(request):
            if request.method=='POST' and request.url.path=='/fapi/v1/algoOrder' and request.url.params['symbol']=='AKEUSDT':
                return httpx.Response(400,json={'code':-2021})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert await bot.enter(ake,'sell',.05)
        stop=bot.positions[ake]['protection']['sl']
        assert stop['is_algo'] is False and not bot.safety.kill.triggered
        server.prices['AKEUSDT']=__import__('decimal').Decimal(stop['trigger'])
        server.complete_native_order(stop['orderId'])
        assert await bot.safety.reconcile_once()
        assert set(bot.positions)=={idol} and not bot.safety.kill.triggered
        result=next(t['exit_detail'] for t in bot.exchange.trades if t['symbol']==ake and t.get('exit_detail'))
        assert result['exit_reason']=='HARD_SL'
        await bot.exchange.close()
    run(check())


def test_unsupported_regular_stop_is_not_treated_as_protection(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();server.advertise_regular_stop=True
        bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        async def send(request):
            if request.method=='POST' and request.url.path=='/fapi/v1/algoOrder' and request.url.params['symbol']=='AKEUSDT':
                return httpx.Response(400,json={'code':-2021})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert not await bot.enter(ake,'sell',.05)
        assert set(bot.positions)=={idol} and not bot.safety.kill.triggered
        assert bot.exchange.management_faults[ake]['regular_unavailable']
        assert bot.safety.kill.state.api_errors_1m==0
        await bot.exchange.close()
    run(check())


def test_retirement_queue_survives_restart_with_other_healthy_position(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        assert await bot.enter(ake,'sell',.05)
        retiring=bot.positions[ake]['protection']['sl']['orderId']
        async def send(request):
            if request.method=='DELETE' and request.url.path=='/fapi/v1/algoOrder' and request.url.params.get('algoId')==str(retiring):
                return httpx.Response(200,json={'code':200})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):await bot.close_position(ake,'operator_close')
        assert bot.exchange.management_faults[ake]['retirement_pending']
        count=len(server.orders)
        await bot.exchange.close()
        bot=await production_bot(tmp_path,monkeypatch,server,startup=False)
        bot.exchange.management_faults[ake]['last_attempt']-=10
        assert await bot.safety.reconcile_once()
        assert set(bot.positions)=={idol} and not bot.exchange.management_faults[ake]['retirement_pending']
        assert len(server.orders)==count and not bot.safety.kill.triggered
        assert len([t for t in bot.exchange.trades if t['symbol']==ake and t.get('exit_detail')])==1
        await bot.exchange.close()
    run(check())


def test_failed_scoped_emergency_close_is_explicit_account_escalation(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server, hard_policy=True)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        async def send(request):
            if request.method=='POST' and request.url.path=='/fapi/v1/algoOrder' and request.url.params['symbol']=='AKEUSDT':
                return httpx.Response(400,json={'code':-2021})
            if request.method=='POST' and request.url.path=='/fapi/v1/order' and request.url.params['symbol']=='AKEUSDT' and request.url.params.get('reduceOnly')=='true':
                return httpx.Response(400,json={'code':-2022})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert not await bot.enter(ake,'sell',.05)
        assert bot.safety.kill.triggered=='symbol_emergency_close_failed'
        assert server.positions['AKEUSDT']['qty'] and not server.positions['IDOLUSDT']['qty']
        assert not bot.safety.ready
        await bot.exchange.close()
    run(check())


def test_regular_fallback_trailing_replaces_regular_stop_without_new_algo_attempts(tmp_path,monkeypatch):
    from decimal import Decimal as D
    async def check():
        server=symbols_server();server.advertise_regular_stop=server.regular_stops=True
        bot=await production_bot(tmp_path,monkeypatch,server)
        ake=bot.exchange.symbol('AKEUSDT');algos=0
        async def send(request):
            nonlocal algos
            if request.method=='POST' and request.url.path=='/fapi/v1/algoOrder':
                algos+=1
                return httpx.Response(400,json={'code':-2021})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert await bot.enter(ake,'sell',.05)
        old=bot.positions[ake]['protection']['sl']['orderId']
        before=algos
        server.prices['AKEUSDT']=D('.04925')
        await bot.manage_exits()
        stop=bot.positions[ake]['protection']['sl']
        assert bot.positions[ake]['trailing_armed'] and stop['is_algo'] is False
        assert stop['orderId']!=old and server.orders[old]['status']=='CANCELED'
        assert algos==before and not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_confirmed_entry_bookkeeping_exception_is_scoped_even_with_confirmed_sl(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        original=bot.exchange._create_order
        async def create(symbol,*args,**kwargs):
            result=await original(symbol,*args,**kwargs)
            if symbol==ake and not result['reduceOnly']:raise ValueError('private data must not be logged')
            return result
        bot.exchange._create_order=create
        with patch('asyncio.sleep',new_callable=AsyncMock):assert not await bot.enter(ake,'sell',.05)
        assert set(bot.positions)=={idol} and not bot.safety.kill.triggered
        assert bot.exchange.management_faults[ake]['reason']=='entry_confirmed_management_failed'
        assert bot.exchange.execution_fault is None
        assert len(server.orders)==3
        await bot.exchange.close()
    run(check())


def test_scoped_close_lost_response_never_closes_healthy_symbol(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        server.lose_close_response=True
        async def send(request):
            if request.method=='POST' and request.url.path=='/fapi/v1/algoOrder' and request.url.params['symbol']=='AKEUSDT':
                return httpx.Response(400,json={'code':-2021})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert not await bot.enter(ake,'sell',.05)
        assert set(bot.positions)=={idol} and not bot.safety.kill.triggered
        assert len(server.orders)==3
        closes=[t for t in bot.exchange.trades if t['symbol']==ake and t.get('exit_detail')]
        assert len(closes)==1
        pnl=closes[0]['exit_detail']['net_pnl_exact']
        assert await bot.safety.reconcile_once()
        assert len(server.orders)==3 and closes[0]['exit_detail']['net_pnl_exact']==pnl
        await bot.exchange.close()
    run(check())


def test_retirement_retry_succeeds_without_a_persistent_fault(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        ake=bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(ake,'sell',.05)
        attempts=0
        async def send(request):
            nonlocal attempts
            if request.method=='DELETE' and request.url.path=='/fapi/v1/algoOrder':
                attempts+=1
                if attempts==1:return httpx.Response(200,json={'code':200})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):await bot.close_position(ake,'operator_close')
        assert attempts==2 and not bot.positions and not bot.safety.kill.triggered
        assert not bot.exchange.management_blocked(ake) and len(server.orders)==2
        await bot.exchange.close()
    run(check())


def test_pending_algo_never_uses_new_protection_id_or_regular_fallback(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();server.advertise_regular_stop=server.regular_stops=True
        bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        posts=0
        async def send(request):
            nonlocal posts
            if request.url.path=='/fapi/v1/algoOrder':
                if request.method=='POST' and request.url.params['symbol']=='AKEUSDT':
                    posts+=1
                    raise httpx.ReadTimeout('untrusted timeout text',request=request)
                if request.method=='GET' and request.url.params.get('clientAlgoId') not in [a['clientAlgoId'] for a in server.algos.values()]:
                    return httpx.Response(400,json={'code':-2013})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert not await bot.enter(ake,'sell',.05)
        assert posts==1 and set(bot.positions)=={idol} and not bot.safety.kill.triggered
        fault=bot.exchange.management_faults[ake]
        assert fault['retirement_pending']
        cid=next(cid for cid,i in bot.exchange.intents.items() if i['symbol']==ake and i['type']=='STOP_MARKET')
        assert cid in [ref['clientOrderId'] for _,ref in fault['obsolete']]
        # Intent commitment is not proof that an unknown parent was retired.
        bot.exchange.intents[cid]['protection_committed']=True
        fault['last_attempt']-=10
        assert not [o for o in server.orders.values() if o.get('type')=='STOP_MARKET']
        assert len(server.orders)==3
        assert await bot.safety.reconcile_once()
        assert bot.exchange.management_faults[ake]['retirement_pending']
        assert idol in bot.positions and not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_flat_native_exit_with_delayed_price_keeps_fault_local_until_recovered(tmp_path,monkeypatch):
    from decimal import Decimal as D
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        assert await bot.enter(ake,'sell',.05)
        stop=bot.positions[ake]['protection']['sl']
        server.prices['AKEUSDT']=D(stop['trigger'])
        child=server.trigger_algo(stop['orderId']);unpriced=True
        async def send(request):
            response=await server(request)
            if unpriced and request.url.path=='/fapi/v1/order' and request.method=='GET' and request.url.params.get('orderId')==str(child['orderId']):
                data=response.json();data.update(avgPrice='0',cumQuote='0')
                return httpx.Response(200,json=data)
            return response
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert await bot.safety.reconcile_once()
        assert idol in bot.positions and not bot.safety.kill.triggered
        assert bot.exchange.execution_fault is None
        assert not server.positions['AKEUSDT']['qty']
        assert bot.exchange.management_faults[ake]['accounting_pending']
        count=len(server.orders)
        unpriced=False
        bot.exchange.management_faults[ake]['last_attempt']-=10
        assert await bot.safety.reconcile_once()
        assert set(bot.positions)=={idol} and len(server.orders)==count
        assert len([t for t in bot.exchange.trades if t['symbol']==ake and t.get('exit_detail')])==1
        assert not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_restart_after_fill_fact_before_position_build_closes_only_failed_symbol(tmp_path,monkeypatch):
    import exchange.execution as execution
    class Abort(BaseException):pass
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        original=execution.make_position;calls=0
        def build(*args,**kwargs):
            nonlocal calls
            calls+=1
            if calls==2:raise Abort()
            return original(*args,**kwargs)
        with patch('exchange.execution.make_position',build):
            with pytest.raises(Abort):await bot.enter(ake,'sell',.05)
        assert server.positions['AKEUSDT']['qty'] and len(server.orders)==2
        await bot.exchange.close()
        bot=await production_bot(tmp_path,monkeypatch,server,startup=False)
        async def send(request):
            if request.method=='POST' and request.url.path=='/fapi/v1/algoOrder' and request.url.params['symbol']=='AKEUSDT':
                return httpx.Response(400,json={'code':-2021})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert await bot.safety.reconcile_once()
        assert set(bot.positions)=={idol} and not bot.safety.kill.triggered
        assert not server.positions['AKEUSDT']['qty'] and len(server.orders)==3
        assert bot.exchange.management_faults[ake]['reason']=='entry_confirmed_management_failed'
        await bot.exchange.close()
    run(check())


def test_protection_retry_budget_bounds_rate_rejection_posts(tmp_path,monkeypatch):
    async def check():
        server=symbols_server();bot=await production_bot(tmp_path,monkeypatch,server)
        idol,ake=bot.exchange.symbol('IDOLUSDT'),bot.exchange.symbol('AKEUSDT')
        assert await bot.enter(idol,'buy',120)
        attempts=0
        async def send(request):
            nonlocal attempts
            if request.method=='POST' and request.url.path=='/fapi/v1/algoOrder' and request.url.params['symbol']=='AKEUSDT':
                attempts+=1
                return httpx.Response(429,json={'code':-1003},headers={'Retry-After':'1'})
            return await server(request)
        await replace_transport(bot,send)
        with patch('asyncio.sleep',new_callable=AsyncMock):assert not await bot.enter(ake,'sell',.05)
        assert attempts==bot.cfg['exchange']['protection_attempts']==3
        assert set(bot.positions)=={idol} and not bot.safety.kill.triggered
        assert bot.safety.kill.state.api_errors_1m==3  # Real rate failures still count.
        await bot.exchange.close()
    run(check())
