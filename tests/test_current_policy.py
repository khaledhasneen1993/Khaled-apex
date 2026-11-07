"""Current baseline acceptance: full-position price stops and entry-only pauses."""
import asyncio
import copy
import json
import time
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from risk.config import load_config
from risk.trading_policy import entry_amount, expected_slippage_bps
from risk.kill_switch import KillSwitch
from risk.exposure_limits import can_open
from risk.dynamic_stop import update_dynamic_stop
from paper_account import make_position
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from futures_simulator import FuturesSimulator, production_bot
from test_continuous import StreamData
from exchange.orders import ExchangeError
from exchange.quantity import OrderValidationError


def run(coro): return asyncio.run(coro)


@pytest.mark.parametrize('side,stop', [('buy',92),('sell',108)])
def test_exact_hard_stop_with_arbitrary_atr(side, stop):
    cfg=load_config()
    for atr in (0,.1,50):
        pos=make_position(side,100,.2,10,atr,cfg,100,.0005)
        assert pos['sl']==stop and pos['stop_pct_initial']==.08
        assert pos['tp'] is None and not pos['trailing_armed']


@pytest.mark.parametrize('leverage',[10,50,125])
@pytest.mark.parametrize('equity',[17.,100.,4912.1995,50000.])
def test_high_leverage_cannot_inflate_risk_budget(leverage,equity):
    cfg=load_config()
    amount=entry_amount(equity,100,leverage,cfg,.0005)
    for sign in (1,-1):
        price=100*1.003
        stop=price*(1-sign*.08)
        loss=amount*(abs(price-stop)+.0005*(price+stop))
        assert loss<=equity*.025+1e-9
    assert amount*100/leverage<=equity*.1
    assert entry_amount(equity,100,10,cfg,.0005)==amount


@pytest.mark.parametrize('side,sign',[('buy',1),('sell',-1)])
def test_trail_arms_exactly_at_one_point_five_and_is_monotone(side,sign):
    cfg=load_config()
    pos=make_position(side,100,.2,10,.4,cfg,100,.0005)
    update_dynamic_stop(pos,SimpleNamespace(price=100*(1+sign*.0149),atr=900),cfg['exit'])
    assert not pos['trailing_armed'] and pos['sl']==100*(1-sign*.08)
    update_dynamic_stop(pos,SimpleNamespace(price=100*(1+sign*.015),atr=900),cfg['exit'])
    assert pos['trailing_armed']
    assert pos['sl']==pytest.approx(100*(1+sign*.015)*(1-sign*.006))
    update_dynamic_stop(pos,SimpleNamespace(price=100*(1+sign*.03),atr=0),cfg['exit'])
    peak_stop=pos['sl']
    assert peak_stop==pytest.approx(100*(1+sign*.03)*(1-sign*.006))
    update_dynamic_stop(pos,SimpleNamespace(price=100*(1+sign*.02),atr=.001),cfg['exit'])
    assert pos['sl']==peak_stop


def test_four_ten_percent_margins_are_permitted_even_same_direction():
    cfg=load_config()['exposure']
    state=SimpleNamespace(equity=100.,open_positions={})
    for symbol in ('BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT'):
        assert can_open(state,symbol,'buy',100,cfg)==(True,'ok')
        state.open_positions[symbol]={'side':'buy','entry':100.,'amount':1.,'leverage':10}
    assert can_open(state,'FIFTH','buy',100,cfg)==(False,'max_positions')


def test_spread_requires_real_warmup_and_pause_expires():
    cfg=load_config()['kill_switch']; kill=KillSwitch(cfg)
    account=SimpleNamespace(equity=100.)
    market=SimpleNamespace(avg_spread=.001,spread=lambda:.01,samples=19)
    with patch('risk.kill_switch.time.time',return_value=100):
        kill.state.last_ws_heartbeat=100
        assert kill.check(account,market) is None and kill.entry_block_reason() is None
        market.samples=20
        assert kill.check(account,market) is None
        assert kill.entry_block_reason()=='spread_spike'
    with patch('risk.kill_switch.time.time',return_value=191):
        assert kill.entry_block_reason() is None and kill.triggered is None


def test_spread_block_preserves_open_position_and_native_order(tmp_path,monkeypatch):
    async def check():
        server=FuturesSimulator()
        bot=await production_bot(tmp_path,monkeypatch,server, hard_policy=True)
        symbol=bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol,'buy',.05)
        old=copy.deepcopy(bot.positions[symbol]['protection'])
        for _ in range(bot.cfg['kill_switch']['spread_warmup']):
            bot.safety.observe_book(symbol,{'bids':[[.04999,1]],'asks':[[.05001,1]]})
        bot.safety.observe_book(symbol,{'bids':[[.049,1]],'asks':[[.051,1]]})
        assert not await bot.safety.allow_entry(symbol,'buy',.05)
        assert bot.safety.kill.entry_block_reason()=='spread_spike'
        assert bot.safety.kill.triggered is None
        await bot.safety.trip('spread_spike')
        assert server.position_rows() and len(server.orders)==1
        assert bot.positions[symbol]['protection']==old
        assert all(a['algoStatus']=='NEW' for a in server.algos.values())
        await bot.exchange.close()
    run(check())


def test_daily_drawdown_blocks_entries_but_keeps_normal_exit_management(tmp_path):
    async def check():
        data=StreamData();bot=V2PaperHunter(tmp_path/'state.json',market_data=data)
        bot.safety.kill.mode = 'hard'
        assert await bot.enter('BTC/USDT:USDT','buy',100)
        await bot.safety.trip('daily_drawdown')
        assert bot.positions and len(bot.exchange.trades)==1
        assert not await bot.enter('ETH/USDT:USDT','buy',100)
        data.price=92
        await bot.manage_exits()
        assert not bot.positions and bot.exchange.trades[-1]['exit_reason']=='HARD_SL'
        assert bot.safety.kill.triggered=='daily_drawdown'
    run(check())


def test_daily_dd_flatten_is_explicit_operator_policy(tmp_path):
    async def check():
        bot=V2PaperHunter(tmp_path/'state.json',market_data=StreamData())
        bot.safety.kill.mode = 'hard'
        bot.cfg['kill_switch']['daily_drawdown_action']='flatten'
        bot.safety.kill.cfg['daily_drawdown_action']='flatten'
        assert await bot.enter('BTC/USDT:USDT','buy',100)
        await bot.safety.trip('daily_drawdown')
        assert not bot.positions
    run(check())


def test_loss_pause_persists_without_flatten_or_permanent_retrigger():
    cfg=load_config()['kill_switch']; kill=KillSwitch(cfg)
    with patch('risk.kill_switch.time.time',return_value=100):
        kill.state.last_ws_heartbeat=100
        for _ in range(4):kill.on_trade_closed(-.1)
        assert kill.check(SimpleNamespace(equity=100),None) is None
        saved=kill.dump()
        assert kill.entry_block_reason()=='consecutive_losses'
    restored=KillSwitch(cfg);restored.restore(saved)
    with patch('risk.kill_switch.time.time',return_value=701):
        restored.on_ws_heartbeat()
        assert restored.check(SimpleNamespace(equity=100),None) is None
        assert restored.entry_block_reason() is None
        assert restored.state.consecutive_losses==4


def test_same_symbol_loss_cooldown_does_not_block_other_symbol(tmp_path):
    async def check():
        data=StreamData();bot=V2PaperHunter(tmp_path/'state.json',market_data=data)
        assert await bot.enter('BTC/USDT:USDT','buy',100)
        data.price=99.9
        await bot.close_position('BTC/USDT:USDT','manual_close')
        assert not await bot.enter('BTC/USDT:USDT','buy',99.9)
        assert await bot.enter('ETH/USDT:USDT','buy',99.9)
        saved=json.loads(bot.state_path.read_text())
        assert saved['operations']['loss_cooldowns']['BTC/USDT:USDT']>time.time()
    run(check())


@pytest.mark.parametrize('side',['buy','sell'])
def test_intentional_partial_close_is_rejected_before_post(tmp_path,monkeypatch,side):
    async def check():
        server=FuturesSimulator();bot=await production_bot(tmp_path,monkeypatch,server)
        symbol=bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol,side,.05)
        quantity=bot.positions[symbol]['amount']
        with pytest.raises(OrderValidationError, match="partial_close_disabled"):
            await bot.exchange.create_order(symbol,'market','sell' if side=='buy' else 'buy',quantity/2,{'reduceOnly':True})
        assert len(server.orders)==1 and abs(server.positions['AIOUSDT']['qty'])==D(str(quantity))
        await bot.exchange.close()
    run(check())


@pytest.mark.parametrize('minutes,rate,blocked',[(4,'.001',True),(10,'.001',False),(-1,'.001',False),(4,'-.001',False)])
def test_funding_blocks_only_adverse_actual_window(tmp_path,monkeypatch,minutes,rate,blocked):
    async def check():
        server=FuturesSimulator();bot=await production_bot(tmp_path,monkeypatch,server)
        bot.exchange.get_funding_rate=AsyncMock(return_value={'nextFundingTime':(time.time()+minutes*60)*1000,'lastFundingRate':rate})
        result=await bot.safety.allow_entry(bot.exchange.symbol('AIOUSDT'),'buy',.05)
        assert result is not blocked
        assert not server.orders and not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_slippage_uses_weighted_depth_and_explicit_cap():
    book={'asks':[[100,1],[101,1]],'bids':[[100,1],[99,1]]}
    assert expected_slippage_bps(book,'buy',2,100)==pytest.approx(50)
    assert expected_slippage_bps(book,'sell',2,100)==pytest.approx(50)
    assert expected_slippage_bps(book,'buy',3,100)==float('inf')


def test_expected_slippage_blocks_without_post_or_kill(tmp_path,monkeypatch):
    async def check():
        server=FuturesSimulator();bot=await production_bot(tmp_path,monkeypatch,server)
        bot.exchange.fetch_order_book=AsyncMock(return_value={'asks':[[.0502,1e9]],'bids':[[.0498,1e9]]})
        assert not await bot.enter(bot.exchange.symbol('AIOUSDT'),'buy',.05)
        assert not server.orders and not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_leverage_rejection_never_sends_entry(tmp_path,monkeypatch):
    async def check():
        server=FuturesSimulator();bot=await production_bot(tmp_path,monkeypatch,server)
        bot.exchange.set_leverage=AsyncMock(side_effect=ExchangeError(400,-4028))
        assert not await bot.enter(bot.exchange.symbol('AIOUSDT'),'buy',.05)
        assert not server.orders
        await bot.exchange.close()
    run(check())


def test_trail_native_replace_and_restart_preserve_extreme_and_exact_accounting(tmp_path,monkeypatch):
    async def check():
        server=FuturesSimulator();bot=await production_bot(tmp_path,monkeypatch,server)
        symbol=bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol,'buy',.05)
        first=bot.positions[symbol]['protection']['sl']['orderId']
        server.prices['AIOUSDT']=D('.05075')
        await bot.manage_exits()
        pos=copy.deepcopy(bot.positions[symbol])
        assert pos['trailing_armed'] and pos['sl']==pytest.approx(.05075*.994)
        assert set(pos['protection'])=={'sl'}
        assert pos['protection']['sl']['orderId']!=first
        assert server.algos[first]['algoStatus']=='CANCELED'
        await bot.exchange.close()
        bot=await production_bot(tmp_path,monkeypatch,server,startup=False)
        assert bot.positions[symbol]['trailing_extreme']==pos['trailing_extreme']
        server.prices['AIOUSDT']=D(str(pos['sl']))-D('.00001')
        await bot.manage_exits()
        assert not bot.positions and not server.position_rows()
        result=next(t for t in bot.exchange.trades if t.get('exit_detail'))
        assert result['exit_detail']['exit_reason']=='TRAILING_SL'
        assert D(result['exit_detail']['net_pnl_exact'])==server.wallet-server.start_wallet
        assert not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_critical_protection_failure_escalates_entry_only_daily_latch(tmp_path):
    async def check():
        bot=V2PaperHunter(tmp_path/'state.json',market_data=StreamData())
        bot.safety.kill.mode = 'hard'
        assert await bot.enter('BTC/USDT:USDT','buy',100)
        await bot.safety.trip('daily_drawdown')
        assert bot.positions
        await bot.safety.trip('protection_update_failed')
        assert not bot.positions and bot.safety.kill.triggered=='protection_update_failed'
        saved=json.loads(bot.state_path.read_text())
        assert saved['operations']['kill']['triggered']=='protection_update_failed'
        events=(bot.safety.journal.root/'kill_events.log').read_text()
        assert 'daily_drawdown' in events and 'protection_update_failed' in events
    run(check())


def test_existing_position_prevents_scale_in_hedge_and_reversal_without_kill(tmp_path,monkeypatch):
    async def check():
        server=FuturesSimulator();bot=await production_bot(tmp_path,monkeypatch,server)
        symbol=bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol,'buy',.05)
        bot.last_trade[symbol]-=600
        assert not await bot.enter(symbol,'buy',.05)
        assert not await bot.enter(symbol,'sell',.05)
        assert len(server.orders)==1 and not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_policy_upgrade_preserves_history_risk_and_latch_and_rejects_open_account(tmp_path):
    from tools.upgrade_demo_policy import prepare_upgrade
    cfg=load_config(); kill=KillSwitch(cfg['kill_switch'])
    kill.triggered='protection_recovery_failed';kill.state.daily_pnl=-75
    old={'version':1,'settings':{'execution_mode':'testnet','MAX_CONCURRENT_POSITIONS':3},
         'account_positions':{},'strategy_positions':{},'equity_usdt':4823.,
         'trades':[{'id':'old-history'}],'operations':{'kill':kill.dump(),'accounted_closes':['old'],
         'loss_cooldowns':{'AIOUSDT':1000000}}}
    before=copy.deepcopy(old)
    new,preview=prepare_upgrade(old,tmp_path,cfg,{})
    assert old==before and new['trades']==old['trades'] and new['operations']==old['operations']
    assert new['settings']['MAX_CONCURRENT_POSITIONS']==4
    assert new['settings']['LEVERAGE']==10 and not preview['trading_started']
    again,preview2=prepare_upgrade(new,tmp_path,cfg,{})
    assert again==new
    with pytest.raises(ValueError):prepare_upgrade(old,tmp_path,cfg,{'positions':1})
    with pytest.raises(ValueError):prepare_upgrade(old,tmp_path,cfg,{'pending_accounting':1})


def test_trade_closed_alert_contains_side_reason_pnl_and_equity(tmp_path,monkeypatch):
    async def check():
        server=FuturesSimulator();bot=await production_bot(tmp_path,monkeypatch,server)
        symbol=bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol,'buy',.05)
        bot.safety.event=AsyncMock()
        await bot.close_position(symbol,'operator_close')
        call=next(c for c in bot.safety.event.await_args_list if c.args[0]=='trade_closed')
        assert {'symbol','side','reason','pnl','equity'}<=call.kwargs.keys()
        assert call.kwargs['symbol']==symbol and call.kwargs['side']=='long'
        assert call.kwargs['equity']==bot.exchange.equity
        await bot.exchange.close()
    run(check())


def test_invalid_equity_and_stale_feed_override_entry_only_latch():
    cfg=load_config()['kill_switch'];kill=KillSwitch(cfg)
    kill.triggered='daily_drawdown'
    assert kill.check(SimpleNamespace(equity=float('nan')),None)=='invalid_equity'
    kill=KillSwitch(cfg);kill.triggered='daily_drawdown'
    kill.state.last_ws_heartbeat-=cfg['ws_timeout_sec']+1
    assert kill.check(SimpleNamespace(equity=100),None)=='ws_timeout'


def test_small_rounding_difference_does_not_flatten_healthy_position(tmp_path,monkeypatch):
    async def check():
        server=FuturesSimulator();bot=await production_bot(tmp_path,monkeypatch,server)
        symbol=bot.exchange.symbol('AIOUSDT')
        assert await bot.enter(symbol,'buy',.05)
        original=bot.positions[symbol]['amount']
        # A rounded account snapshot is not a new execution; retain entry facts.
        base=server.position_rows
        def rounded(symbol=None):
            rows=base(symbol)
            for row in rows:row['positionAmt']=str(D(row['positionAmt'])*D('.999999'))
            return rows
        server.position_rows=rounded
        assert await bot.safety.reconcile_once()
        assert len(server.orders)==1 and bot.positions[symbol]['amount']==original
        assert not bot.safety.kill.triggered
        await bot.exchange.close()
    run(check())


def test_failed_json_state_save_cannot_block_emergency_close(tmp_path):
    async def check():
        bot=V2PaperHunter(tmp_path/'state.json',market_data=StreamData())
        bot.safety.kill.mode = 'hard'
        assert await bot.enter('BTC/USDT:USDT','buy',100)
        bot.save=lambda: (_ for _ in ()).throw(ValueError('nonfinite JSON snapshot'))
        with patch('asyncio.sleep',new_callable=AsyncMock):
            await bot.safety.trip('invalid_equity')
        assert not bot.exchange.positions
        assert bot.safety.kill.triggered=='invalid_equity' and bot.safety.storage_failed
    run(check())


def test_fixed_leverage_uses_risk_limited_notional_to_choose_bracket(tmp_path,monkeypatch):
    async def check():
        server=FuturesSimulator();bot=await production_bot(tmp_path,monkeypatch,server)
        symbol=bot.exchange.symbol('AIOUSDT')
        bot.exchange.leverage_brackets=AsyncMock(return_value=[
            {'initialLeverage':10,'notionalFloor':0,'notionalCap':5000,'maintMarginRatio':.004,'cum':0},
            {'initialLeverage':5,'notionalFloor':5000,'notionalCap':100000,'maintMarginRatio':.025,'cum':0}])
        bot.exchange.set_leverage=AsyncMock(return_value=5000)
        # Target-margin notional=10000 crosses the tier, but risk-sized notional
        # is only about3075, where10x is valid. Select the actual size's tier.
        choice=await bot.exchange.select_leverage(symbol,1000,100)
        assert choice['confirmed'] and choice['leverage']==10
        assert choice['notional']<5000 and choice['bracket']['initialLeverage']==10
        assert choice['margin']<1000
        await bot.exchange.close()
    run(check())


def test_upgrade_cli_is_get_only_and_keeps_backup_guard_and_trade_history(tmp_path,monkeypatch,capsys):
    from tools import upgrade_demo_policy as upgrade
    from infra.journal import atomic_json
    server=FuturesSimulator()
    async def setup():
        bot=await production_bot(tmp_path,monkeypatch,server,startup=False)
        await bot.exchange.refresh_account()
        bot.safety.kill.triggered='protection_recovery_failed'
        bot.safety.kill.state.daily_pnl=-.5
        bot.save()
        cfg=copy.deepcopy(bot.cfg)
        await bot.exchange.close()
        return cfg,bot.state_path
    cfg,state=run(setup())
    data=json.loads(state.read_text())
    data['settings']['MAX_CONCURRENT_POSITIONS']=3
    data['trades']=[{'id':'historical-record','reduceOnly':True}]
    atomic_json(state,data)
    guard=state.parent/cfg['exchange']['session_guard_filename']
    atomic_json(guard,{'active':True,'previous_session':'retained'})
    before_guard=guard.read_bytes()
    _,preview=upgrade.prepare_upgrade(data,state.parent,cfg,{})
    original_client=httpx.AsyncClient
    def client(*args,**kwargs):
        kwargs['transport']=httpx.MockTransport(server)
        return original_client(*args,**kwargs)
    monkeypatch.setattr(httpx,'AsyncClient',client)
    monkeypatch.setattr(upgrade,'demo_config',lambda:copy.deepcopy(cfg))
    monkeypatch.setattr('sys.argv',['upgrade_demo_policy.py','--apply','--expected-sha256',preview['preview_sha256']])
    upgrade.main()
    updated=json.loads(state.read_text())
    assert updated['settings']['MAX_CONCURRENT_POSITIONS']==4
    assert updated['trades']==data['trades'] and updated['operations']==data['operations']
    assert guard.read_bytes()==before_guard
    backups=list(state.parent.glob('state.before-policy-*.json'))
    assert len(backups)==1 and json.loads(backups[0].read_text())==data
    assert all(method=='GET' for method,_,_ in server.requests)
    assert 'Trading remains stopped' in capsys.readouterr().out


@pytest.mark.parametrize('equity',['17','50000'])
def test_demo_size_uses_actual_account_equity_not_paper_balance(tmp_path,monkeypatch,equity):
    async def check():
        server=FuturesSimulator(equity);bot=await production_bot(tmp_path,monkeypatch,server)
        symbol=bot.exchange.symbol('AIOUSDT')
        expected=entry_amount(float(equity),.05,10,bot.cfg,bot.exchange.fee_rate,
            lambda q:float(bot.exchange.amount_to_precision(symbol,q)))
        assert await bot.enter(symbol,'buy',.05)
        pos=bot.positions[symbol]
        assert pos['amount']==expected and pos['equity_at_entry']==float(equity)
        assert pos['leverage']==server.leverage['AIOUSDT']==10
        assert pos['margin']<=float(equity)*.1
        assert pos['amount']*(.004+.0005*(.05+.046))<=float(equity)*.025
        await bot.exchange.close()
    run(check())


def test_current_policy_does_not_fetch_unused_atr_for_entry_or_exit(tmp_path):
    async def check():
        data=StreamData()
        data.fetch_ohlcv=AsyncMock(side_effect=AssertionError('ATR must not delay current execution'))
        bot=V2PaperHunter(tmp_path/'state.json',market_data=data)
        assert await bot.enter('BTC/USDT:USDT','buy',100)
        pos=bot.positions['BTC/USDT:USDT']
        assert pos['atr_at_entry'] is None and pos['atr_source']=='not_used'
        data.price=101.5
        await bot.manage_exits()
        assert pos['trailing_armed']
        data.price=pos['sl']
        await bot.manage_exits()
        assert not bot.positions
        assert bot.exchange.trades[-1]['exit_reason']=='TRAILING_SL'
        data.fetch_ohlcv.assert_not_awaited()
    run(check())
