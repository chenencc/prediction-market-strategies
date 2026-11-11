import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))
import unittest
from decimal import Decimal as D
from prediction_market_strategies.strategies.pair_convergence import (
    Config, Depth, Leg, Pair, PairConvergence, fee_per_share, NS,
)
from prediction_market_strategies.backtest.pair_research import Metadata


def terms(venue, market, at):
    return dict(feesEnabled=False, orderMinSize=1)


def setup(**kwargs):
    values = dict(quantity=D("5"), entry_edge=D("0.01"), fee_buffer_per_share=D(0),
                  slippage_limit=D(0), max_age_ns=100*NS, max_skew_ns=10*NS,
                  decision_ns=0, hold_seconds=5)
    values.update(kwargs)
    model = PairConvergence(Config(**values), terms)
    pair = Pair("p", (Leg("a", "polymarket", "1"), Leg("b", "predictfun", "2")), NS)
    model.update("a", [(".39", "20")], [(".40", "20")], NS)
    model.update("b", [(".49", "20")], [(".50", "20")], NS)
    model.advance(NS)
    return model, pair


class PairConvergenceTests(unittest.TestCase):
    def test_metadata_never_uses_future_version(self):
        m = Metadata()
        m.add("polymarket", "1", 10, {"rate":1})
        m.add("polymarket", "1", 20, {"rate":2})
        m.finish()
        self.assertIsNone(m("polymarket", "1", 9))
        self.assertEqual(m("polymarket", "1", 19), {"rate":1})

    def test_conflicting_metadata_rejected(self):
        m = Metadata()
        m.add("polymarket", "1", 10, {"rate":1})
        with self.assertRaises(ValueError):
            m.add("polymarket", "1", 10, {"rate":2})

    def test_unknown_fees_never_zero(self):
        with self.assertRaises(ValueError):
            fee_per_share("polymarket", {}, D(".4"))
        self.assertEqual(fee_per_share("predictfun", {"feeRateBps":200}, D(".4")), D(".008"))
        self.assertEqual(fee_per_share("polymarket", {"feeSchedule":{"rate":".04","exponent":1}}, D(".4")), D(".0096"))

    def test_mirror_shares_same_liquidity(self):
        b = Depth()
        b.update([(".4","10")], [(".5","10")], NS)
        l = Leg("x", "predictfun", "1", True)
        self.assertEqual(b.ladder(l, True)[0][:2], (D(".6"),D(10)))
        b.available["bids"][D(".4")] -= 5
        b.update([(".4","10")], [(".5","10")], 2*NS)
        self.assertEqual(b.ladder(l,True)[0][1], D(5))
        b.update([(".4","12")], [(".5","10")], 3*NS)
        self.assertEqual(b.ladder(l,True)[0][1], D(7))

    def test_independent_leg_latency_and_early_exit(self):
        m,p = setup()
        m.evaluate(p, NS)
        self.assertEqual(len(m.fills), 0)
        m.advance(NS+300_000_000)
        self.assertEqual(m.positions["p"].quantities, [D(5),D(0)])
        m.advance(2*NS)
        self.assertEqual(m.positions["p"].quantities, [D(5),D(5)])
        m.update("a", [(".44","20")], [(".45","20")], 2*NS)
        m.update("b", [(".50","20")], [(".51","20")], 2*NS)
        m.evaluate(p,2*NS)
        m.advance(3*NS)
        self.assertFalse(m.positions)
        self.assertEqual(m.trades[0]["pnl"], "0.20")
        self.assertEqual(m.trades[0]["reason"], "take_profit")
        self.assertEqual(sum(m.cash.values())-2000, D(".20"))

    def test_price_change_before_arrival_causes_single_leg_exit(self):
        m,p = setup()
        m.evaluate(p, NS)
        m.advance(NS+300_000_000)
        m.update("b", [(".69","20")], [(".70","20")], NS+300_000_000)
        m.advance(2*NS)
        self.assertEqual(m.trades[0]["reason"], "incomplete_entry")
        self.assertLess(D(m.trades[0]["pnl"]), 0)
        self.assertEqual(m.stats["partial_or_zero_leg_fills"], 1)

    def test_timeout_sells_at_loss_without_settlement_credit(self):
        m,p = setup()
        m.evaluate(p,NS)
        m.advance(2*NS)
        m.advance(6*NS)
        m.advance(7*NS)
        self.assertEqual(m.trades[0]["reason"], "timeout")
        self.assertLess(D(m.trades[0]["pnl"]), 0)

    def test_partial_entry_does_not_invent_hedge(self):
        m,p = setup()
        m.evaluate(p,NS)
        m.advance(NS+300_000_000)
        m.update("b", [(".49","20")], [(".50","2")], NS+300_000_000)
        m.advance(2*NS)
        self.assertFalse(m.positions)
        self.assertEqual([f["quantity"] for f in m.fills], ["5","2","5","2"])
        self.assertEqual(m.trades[0]["reason"], "incomplete_entry")

    def test_stale_exit_stays_open_not_profitable(self):
        m,p = setup(max_age_ns=NS)
        m.evaluate(p,NS)
        m.advance(2*NS)
        m.advance(7*NS)
        r=m.report()
        self.assertEqual(r["closed_round_trips"],0)
        self.assertEqual(r["realized_cash_equivalent_pnl"],"0")
        self.assertEqual(len(r["open_positions"]),1)
        self.assertEqual(r["open_positions"][0]["exit_reason"],"timeout")

    def test_capital_reserved_before_fill(self):
        m,p=setup(initial_cash_per_venue=D("3"))
        m.evaluate(p,NS)
        p2=Pair("p2",p.legs,NS)
        m.evaluate(p2,NS)
        self.assertNotIn("p2",m.positions)
        self.assertEqual(m.stats["entry_rejected:venue_capital_limit"],1)

    def test_no_trading_before_pair_known(self):
        m,p=setup()
        m.evaluate(Pair("later",p.legs,2*NS),NS)
        self.assertFalse(m.queue)

    def test_depth_insufficiency_rejects_entry(self):
        m,p=setup()
        m.update("b", [(".49","2")], [(".50","20")],NS)
        m.evaluate(p,NS)
        self.assertFalse(m.queue)
        self.assertEqual(m.stats["entry_rejected:entry_or_exit_depth_insufficient"],1)

    def test_clock_reversal_rejected(self):
        m,p=setup()
        with self.assertRaises(ValueError):
            m.advance(0)

    def test_zero_quantity_rejected(self):
        with self.assertRaises(ValueError):
            Config(quantity=D(0))


if __name__ == "__main__":
    unittest.main()
