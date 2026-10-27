from decimal import Decimal
from dataclasses import replace
import unittest

from prediction_market_strategies.strategies.binary_pair_arbitrage import (
    BinaryPairPaperStrategy,
    PairRules,
    BookView,
    FeeTerms,
    evaluate,
)
from prediction_market_strategies.adapters.common import (
    binary_instrument,
    book_snapshot,
)


D = Decimal
NOW = 1_793_000_000_000_000_000


def instruments(rate="0"):
    items = []
    for outcome, token in [("Yes", "111"), ("No", "222")]:
        items.append(
            binary_instrument(
                venue="polymarket",
                market_id="test",
                condition_id="0xsynthetic-condition",
                token=token,
                outcome=outcome,
                observed_ns=NOW,
                revision="synthetic",
                terms={"question": "Synthetic complementary binary test"},
                spec=dict(
                    currency="USDC",
                    currency_precision=6,
                    price_precision=2,
                    size_precision=2,
                    price_increment="0.01",
                    size_increment="0.01",
                ),
                extra_info=dict(
                    condition_id="0xsynthetic-condition",
                    market_version="v1",
                    min_order_size="5",
                    fee_schedule=dict(
                        rate=rate, exponent="1", takerOnly=True, rebateRate="0"
                    ),
                ),
            )
        )
    return items


def view(ask="0.45", bid="0.44", size="100", ts=NOW):
    return BookView(((D(bid), D(size)),), ((D(ask), D(size)),), ts, ts)


class Decisions(unittest.TestCase):
    def decide(self, books=None, fees=None, **kwargs):
        return evaluate(
            books or [view(), view()],
            fees or [FeeTerms(D(0), 1, D(5))] * 2,
            PairRules(),
            now_ns=NOW,
            step=D("0.01"),
            cash=D(1000),
            currency_precision=6,
            **kwargs,
        )

    def test_known_fees_and_depth_constrain_edge(self):
        entry, reason = self.decide()
        self.assertEqual(reason, "eligible")
        self.assertEqual(entry.net_edge, D("0.10"))
        _, reason = self.decide(
            [view("0.49", "0.48"), view("0.49", "0.48")],
            [FeeTerms(D("0.03"), 1, D(5))] * 2,
        )
        self.assertEqual(reason, "no_net_edge")
        self.assertEqual(self.decide([view(size="4"), view()])[1], "insufficient_depth")

    def test_stale_future_skew_and_empty_block(self):
        self.assertEqual(
            self.decide([view(), view(ts=NOW - 3_000_000_000)])[1], "stale_book"
        )
        self.assertEqual(self.decide([view(), view(ts=NOW + 1)])[1], "stale_book")
        self.assertEqual(
            self.decide([view(), view(ts=NOW - 600_000_000)])[1], "receipt_skew"
        )
        self.assertEqual(
            self.decide([view(), replace(view(), asks=())])[1], "empty_side"
        )

    def test_missing_fee_is_not_zero(self):
        item = instruments()[0].to_dict()
        item["info"].pop("fee_schedule")
        from nautilus_trader.model import BinaryOption

        with self.assertRaisesRegex(ValueError, "missing observed fee"):
            FeeTerms.from_instrument(BinaryOption.from_dict(item))

    def test_limits_are_worst_case_not_only_average(self):
        book = replace(view(), asks=((D("0.45"), D(5)), (D("0.56"), D(5))))
        # Average cost 0.955 is attractive but the IOC price caps can permit
        # a total 1.01 per pair. Reject rather than assume first-level fills.
        self.assertEqual(self.decide([book, view()])[1], "no_net_edge")

    def test_unknown_and_bad_config_are_rejected(self):
        with self.assertRaises(ValueError):
            PairRules(quantity=float("nan"))
        with self.assertRaises(ValueError):
            PairRules(decision_delay_ns=0)


def replay(*, rate="0", fail_second=False, partial_second=False, no_edge=False):
    from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
    from nautilus_trader.model import Venue, OmsType, AccountType, Money, BookType
    from nautilus_trader.adapters.polymarket import PolymarketFeeModel
    from nautilus_trader.execution import StaticLatencyModel

    engine = BacktestEngine(
        BacktestEngineConfig(bypass_logging=True, run_analysis=False)
    )
    items = instruments(rate)
    from nautilus_trader.model import BinaryOption

    # Research instrument builders deliberately leave closure unknown (0).
    # An order simulation requires an explicit *synthetic* future expiration.
    values = [i.to_dict() for i in items]
    for v in values:
        v["expiration_ns"] = NOW + 86_400_000_000_000
    items = [BinaryOption.from_dict(v) for v in values]
    engine.add_venue(
        venue=Venue("POLYMARKET"),
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        starting_balances=[Money(D(1000), items[0].currency)],
        book_type=BookType.L2_MBP,
        liquidity_consumption=True,
        trade_execution=False,
        bar_execution=False,
        queue_position=False,
        fee_model=PolymarketFeeModel(),
        latency_model=StaticLatencyModel(insert_latency_nanos=100_000_000),
    )
    strategy = BinaryPairPaperStrategy([i.id for i in items], PairRules())
    for item in items:
        engine.add_instrument(item)
    engine.add_strategy(strategy)
    batches = []
    for ms in (0, 10, 300, 310, 350, 360, 600, 610, 900, 910, 1500, 1510, 2500):
        index = 0 if ms % 100 == 0 else 1
        ts = NOW + ms * 1_000_000
        ask = "0.51" if no_edge else "0.45"
        if fail_second and index == 1 and ms >= 350:
            ask = "0.55"
        size = "3" if partial_second and index == 1 and ms >= 350 else "100"
        batches.append(
            book_snapshot(
                items[index], [("0.44", "100")], [(ask, size)], ts_event=ts, ts_init=ts
            )
        )
    try:
        engine.add_data(batches)
        engine.run()
        from prediction_market_strategies.paper.polymarket_pair import summarize

        report = summarize(strategy, engine.cache, currency=items[0].currency)
        return report, list(strategy.records)
    finally:
        engine.dispose()


class NativeSimulation(unittest.TestCase):
    def test_windows_long_native_token_paths_are_not_omitted_from_inventory(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        from prediction_market_strategies.paper.polymarket_pair import (
            artifact_inventory,
            filesystem_path,
        )

        with TemporaryDirectory() as folder:
            root = Path(folder)
            target = root / ("a" * 180) / ("b" * 100) / "book.parquet"
            physical = filesystem_path(target)
            physical.parent.mkdir(parents=True)
            physical.write_bytes(b"test exact bytes")
            try:
                result = artifact_inventory(root)
                self.assertEqual(len(result), 1)
                self.assertEqual(next(iter(result.values()))["bytes"], 16)
            finally:
                # tempfile's plain-path cleanup has the same Windows limit;
                # unlink only the three exact paths owned by this test.
                physical.unlink()
                physical.parent.rmdir()
                physical.parent.parent.rmdir()

    def test_paper_input_archive_cold_read_preserves_clocks_and_metadata(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        from prediction_market_strategies.paper.polymarket_pair import archive_inputs

        items = instruments()
        values = [
            book_snapshot(
                i, [("0.44", "100")], [("0.45", "100")], ts_event=NOW, ts_init=NOW + 10
            )
            for i in items
        ]
        with TemporaryDirectory() as folder:
            result = archive_inputs(Path(folder) / "catalog", items, values)
            self.assertEqual(result["catalog_readback_batches"], 2)
            self.assertTrue(result["catalog_roundtrip_semantics_match"])

    def test_two_leg_ioc_fills_and_native_fees(self):
        result, events = replay(rate="0.03")
        self.assertTrue(result["entered"])
        fills = [r for r in events if r["kind"] == "fill"]
        self.assertEqual(len(fills), 2)
        self.assertEqual({D(r["quantity"]) for r in fills}, {D("10")})
        self.assertGreater(D(result["simulated_commissions"]), 0)
        self.assertEqual(D(result["residual_quantity"]), 0)
        self.assertFalse(result["unresolved_orders"])

    def test_no_edge_does_not_get_forced_into_order(self):
        result, events = replay(no_edge=True)
        self.assertFalse(result["entered"])
        self.assertFalse(result["orders"])

    def test_one_leg_price_move_unwinds_excess(self):
        result, events = replay(fail_second=True)
        self.assertTrue(result["entered"])
        self.assertTrue(any(r["kind"] == "unwind" for r in events))
        self.assertEqual(D(result["residual_quantity"]), 0)
        self.assertFalse(result["unresolved_orders"])

    def test_partial_second_leg_keeps_only_matched_inventory(self):
        result, events = replay(partial_second=True)
        self.assertTrue(any(r["kind"] == "unwind" for r in events))
        self.assertEqual(
            {D(v) for v in result["simulated_net_inventory"].values()}, {D(3)}
        )
        self.assertEqual(D(result["residual_quantity"]), 0)
        self.assertFalse(result["unresolved_orders"])

    def test_duplicate_native_fill_and_terminal_events_do_not_double_count(self):
        from types import SimpleNamespace as NS

        strategy = BinaryPairPaperStrategy([i.id for i in instruments()])
        strategy._clock = NS(timestamp_ns=lambda: NOW)
        strategy._cache = NS(order=lambda ident: NS(is_closed=True))
        event = NS(
            client_order_id="synthetic-id",
            trade_id="synthetic-fill",
            instrument_id=strategy.ids[0],
            order_side="BUY",
            last_qty=NS(as_decimal=lambda: D(5)),
            last_px=NS(as_decimal=lambda: D("0.45")),
            commission=None,
            currency="USDC",
        )
        for _ in range(2):
            strategy.on_order_filled(event)
            strategy.on_order_event(event)
        self.assertEqual(sum(r["kind"] == "fill" for r in strategy.records), 1)
        self.assertEqual(strategy.terminal, {"synthetic-id"})


if __name__ == "__main__":
    unittest.main()
