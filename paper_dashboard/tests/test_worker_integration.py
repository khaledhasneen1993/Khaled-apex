"""Actual worker/runtime tests using public-data fakes; no exchange connection."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]


def worker_code(source, name):
    return f'''
import asyncio, os, runpy, sys, time
from pathlib import Path
sys.path.insert(0,{str(source)!r})
if {name!r} == 'khaled':
    from risk import config
    config._mode.set('testnet')  # Dashboard must override this outer choice.
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter as Hunter
class Public:
    def __init__(self): self.price=100.
    async def load_markets(self): return {{}}
    async def fetch_ticker(self,symbol): return {{'last':self.price,'timestamp':int(time.time()*1000)}}
    async def fetch_order_book(self,symbol,limit): return {{'bids':[[self.price,900000000]],'asks':[[self.price,100000000]]}}
    async def fetch_tickers(self): return {{}}
    def amount_to_precision(self,symbol,amount): return str(int(amount*1000)/1000)
    def market(self,symbol): return {{'linear':True,'swap':True,'settle':'USDT','limits':{{'amount':{{'min':.001}},'cost':{{'min':5}}}}}}
    async def close(self): Path(os.environ['KHALED_PAPER_SESSION'],'client-closed-{name}').write_text('yes')
original_init=Hunter.__init__
def initialize(self,*args,**kwargs):
    assert not any(k.startswith(('BINANCE_','TELEGRAM_','CASCADE_DASHBOARD_')) for k in os.environ)
    kwargs['market_data']=Public()
    original_init(self,*args,**kwargs)
    assert getattr(self,'execution_mode','paper')=='paper'
    async def idle(): await asyncio.Future()
    async def trade_then_wait():
        if not self.exchange.trades:
            assert self.exchange.balance==100
            assert await self.enter('BTC/USDT:USDT','buy',100)
            self.exchange.market_data.price=102
            await self.manage_exits()
            self.exchange.market_data.price=101
            await self.manage_exits()
            assert not self.positions
            self.save()
        await asyncio.Future()
    self.scan_loop=trade_then_wait
    self.exit_watcher=idle
Hunter.__init__=initialize
sys.path.insert(0,{str(ROOT/'paper_dashboard')!r})
sys.argv=['worker.py',{name!r}]
runpy.run_path({str(ROOT/'paper_dashboard/worker.py')!r},run_name='__main__')
'''


def wait_closed(session, name, proc):
    snapshot = session/'data'/f'{name}.snapshot.json'
    for _ in range(160):
        if proc.poll() is not None:
            raise AssertionError(proc.communicate()[0])
        if snapshot.exists():
            data=json.loads(snapshot.read_text())
            if (len(data['trades'])==2 and data.get('status')=='running'
                    and data['updated_at'] >= getattr(proc,'paper_started_at',0)):
                return data
        time.sleep(.05)
    raise AssertionError('Worker did not publish completed fake trade')


class WorkerIntegrationTests(unittest.TestCase):
    def test_paper_override_fills_locks_graceful_stop_and_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            session=Path(directory)
            env=dict(os.environ,KHALED_PAPER_SESSION=str(session),BINANCE_API_KEY='dummy-not-used',
                     BINANCE_API_SECRET='dummy-not-used',TELEGRAM_BOT_TOKEN='dummy-not-used',
                     CASCADE_DASHBOARD_TOKEN='dummy-not-used')
            code=worker_code(ROOT,'khaled')
            for attempt in range(2):
                proc=subprocess.Popen([sys.executable,'-c',code],cwd=ROOT,env=env,
                    stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
                proc.paper_started_at=time.time()
                try:
                    data=wait_closed(session,'khaled',proc)
                    self.assertEqual(data['execution_mode'],'paper')
                    self.assertEqual(data['initial_balance_usdt'],100)
                    self.assertEqual(data['settings']['execution_mode'],'paper')
                    self.assertEqual(data['display_settings']['hard_stop_pct'],.08)
                    self.assertEqual(data['display_settings']['trail_arm_pct'],.015)
                    self.assertEqual(data['trades'][-1]['exit_reason'],'TRAILING_SL')
                    if attempt==0:
                        initial=data
                        duplicate=subprocess.run([sys.executable,'-c',code],cwd=ROOT,env=env,
                            capture_output=True,text=True,timeout=15)
                        self.assertNotEqual(duplicate.returncode,0)
                        self.assertIn('BlockingIOError',duplicate.stderr)
                    else:
                        self.assertEqual(data['balance_usdt'],initial['balance_usdt'])
                        self.assertEqual(data['trades'],initial['trades'])
                    proc.send_signal(signal.SIGINT)
                    out=proc.communicate(timeout=15)[0]
                    self.assertEqual(proc.returncode,0,out)
                    self.assertNotIn('Unclosed',out)
                    self.assertNotIn('dummy-not-used',out)
                    self.assertTrue((session/'client-closed-khaled').exists())
                    saved=json.loads((session/'accounts/khaled/state.json').read_text())
                    self.assertEqual(saved['trades'],initial['trades'])
                    self.assertTrue((session/'accounts/khaled/journal.wal').exists())
                finally:
                    if proc.poll() is None: proc.kill();proc.communicate()
