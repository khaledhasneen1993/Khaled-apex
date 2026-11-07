import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
import asyncio
import binance_cascade_hunter_dynamic_v2 as v2
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from continuous_scanner import ContinuousScanner
from test_continuous import NOW, StreamData, event


class V2Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'v2.json'
        self.data = StreamData()
        self.data.tickers['DOGE/USDT:USDT'] = {'quoteVolume': 25_000_000}
        self.bot = V2PaperHunter(self.path, fee_rate=0, market_data=self.data)

    async def test_fixed_notional_allows_four_positions(self):
        for symbol in ('BTC/USDT:USDT','ETH/USDT:USDT','SOL/USDT:USDT','DOGE/USDT:USDT'):
            self.assertTrue(await self.bot.enter(symbol, 'buy', 100))
            pos = self.bot.positions[symbol]
            self.assertEqual(pos['amount'] * pos['entry'], 100)
            self.assertEqual(self.bot.exchange.positions[symbol]['margin'], 10)
        self.assertFalse(await self.bot.enter('FIFTH/USDT:USDT','buy',100))
        self.assertEqual(len(self.bot.positions), 4)

    async def test_long_hard_stop_is_eight_percent_without_tp(self):
        self.assertTrue(await self.bot.enter('BTC/USDT:USDT','buy',100))
        pos = self.bot.positions['BTC/USDT:USDT']
        self.assertEqual(pos['sl'],92)
        self.assertIsNone(pos['tp'])
        self.data.price=95
        await self.bot.manage_exits()
        self.assertTrue(self.bot.positions)
        self.data.price=92
        await self.bot.manage_exits()
        self.assertFalse(self.bot.positions)
        self.assertEqual(self.bot.exchange.trades[-1]['exit_reason'],'HARD_SL')

    async def test_short_hard_stop_is_eight_percent(self):
        self.assertTrue(await self.bot.enter('ETH/USDT:USDT','sell',100))
        self.assertEqual(self.bot.positions['ETH/USDT:USDT']['sl'],108)
        self.data.price=105
        await self.bot.manage_exits()
        self.assertTrue(self.bot.positions)
        self.data.price=108
        await self.bot.manage_exits()
        self.assertFalse(self.bot.positions)
        self.assertEqual(self.bot.exchange.trades[-1]['exit_reason'],'HARD_SL')

    async def test_incompatible_state_rejected_without_modifying_file(self):
        self.bot.save()
        state=json.loads(self.path.read_text())
        state['settings']['MAX_POSITION_PCT']=.85
        (self.path.parent / 'journal.wal').unlink(missing_ok=True)
        self.path.write_text(json.dumps(state))
        before=self.path.read_bytes()
        with self.assertRaises(ValueError): V2PaperHunter(self.path,fee_rate=0,market_data=self.data)
        self.assertEqual(self.path.read_bytes(),before)

    async def test_saved_policy_restores_positions_and_settings(self):
        self.bot.cfg.pop('paper_position_notional_usdt')
        await self.bot.enter('SOL/USDT:USDT','buy',100)
        restored=V2PaperHunter(self.path,fee_rate=0,market_data=self.data)
        self.assertEqual(restored.positions,self.bot.positions)
        self.assertEqual(restored.settings()['paper_position_notional_usdt'], 100)
        self.assertEqual(json.loads(self.path.read_text())['settings']['MAX_CONCURRENT_POSITIONS'],4)

    async def test_continuous_scanner_opens_four_on_updates(self):
        scanner=ContinuousScanner(self.bot,config=v2,clock=lambda:100,wall_clock=lambda:NOW/1000)
        await scanner.refresh_universe()
        await scanner.seed(scanner.symbols)
        scanner.connected=True
        self.data.price=102
        for symbol in ('BTCUSDT','ETHUSDT','SOLUSDT','DOGEUSDT'): scanner.ingest(event(symbol))
        with patch('continuous_scanner.BOOK_REQUEST_SPACING',0):
            await asyncio.gather(*(scanner.evaluate(s) for s in scanner.symbols))
        self.assertEqual(len(self.bot.positions),4)
        self.assertEqual(len(self.bot.exchange.trades),4)

    async def test_fixed_notional_uses_execution_price(self):
        symbol = 'BTC/USDT:USDT'
        self.data.fetch_ticker = AsyncMock(side_effect=[{'last': 100}, {'last': 100.1}])
        self.assertTrue(await self.bot.enter(symbol, 'buy', 100))
        fill = self.bot.exchange.trades[0]
        self.assertEqual(fill['amount'], .999)
        self.assertAlmostEqual(fill['amount'] * fill['price'], 99.9999)
        self.assertAlmostEqual(self.bot.positions[symbol]['sl'], 100.1 * .92)

    async def test_sizing_uses_current_equity_and_fee_budget(self):
        bot=V2PaperHunter(self.path,market_data=self.data)
        bot.cfg.pop('paper_position_notional_usdt')
        await bot.enter('BTC/USDT:USDT','buy',100)
        opening=bot.exchange.trades[0]
        self.assertAlmostEqual(bot.exchange.balance,100-opening['fee_usdt'])
        await bot.enter('ETH/USDT:USDT','buy',100)
        pos=bot.positions['ETH/USDT:USDT']
        loss=8*pos['amount']+pos['entry_fee']+92*pos['amount']*.0005
        self.assertLessEqual(loss,pos['equity_at_entry']*.025)
        self.assertGreater(bot.exchange.free,0)
