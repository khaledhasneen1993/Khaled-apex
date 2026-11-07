import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ledger import round_trips

class LedgerTests(unittest.TestCase):
    def test_scale_fees_short_reentry(self):
        def fill(side,amount,price,fee,close=False,pnl=0):
            return dict(symbol='BTC/USDT:USDT',side=side,amount=amount,price=price,
                        fee_usdt=fee,reduceOnly=close,pnl_usdt=pnl,timestamp='2026-10-30')
        fills=[fill('sell',2,100,.1),fill('sell',1,110,.055),
               fill('buy',3,90,.135,True,40), fill('buy',1,91,.0455)]
        opened,closed=round_trips(fills)
        self.assertEqual(len(opened),1)
        self.assertEqual(len(closed),1)
        self.assertEqual(closed[0]['adds'],1)
        self.assertAlmostEqual(closed[0]['average_entry'],310/3)
        self.assertAlmostEqual(closed[0]['net_pnl_usdt'],39.71)
        self.assertEqual(opened[0]['side'],'buy')
    def test_empty(self):
        self.assertEqual(round_trips([]),([],[]))

if __name__=='__main__': unittest.main()

class PartialAndSignalTests(unittest.TestCase):
    def test_partial_fills_report_one_close_with_all_fees(self):
        rows=[dict(symbol='BTC',signal_id='one',side='buy',amount=2,price=100,fee_usdt=.1,reduceOnly=False),
              dict(symbol='BTC',signal_id='one',side='sell',amount=1,price=102,fee_usdt=.051,reduceOnly=True,pnl_usdt=2),
              dict(symbol='BTC',signal_id='one',side='sell',amount=1,price=104,fee_usdt=.052,reduceOnly=True,pnl_usdt=4,exit_reason='TRAILING_SL',exit_detail={'stop_final':103})]
        opened,closed=round_trips(rows[:2]);self.assertEqual(len(opened),1);self.assertEqual(closed,[])
        opened,closed=round_trips(rows);self.assertEqual(opened,[]);self.assertEqual(len(closed),1)
        self.assertEqual(closed[0]['exit_price'],103)
        self.assertAlmostEqual(closed[0]['net_pnl_usdt'],5.797)
        self.assertEqual(closed[0]['exit_detail']['stop_final'],103)

    def test_unmatched_signal_does_not_close_another_trade(self):
        rows=[dict(symbol='BTC',signal_id='one',side='buy',amount=1,price=100,reduceOnly=False),
              dict(symbol='BTC',signal_id='other',side='sell',amount=1,price=101,reduceOnly=True,pnl_usdt=1)]
        opened,closed=round_trips(rows);self.assertEqual(len(opened),1);self.assertEqual(closed,[])
