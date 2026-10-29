from decimal import Decimal as D
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from collections import Counter

from prediction_market_strategies.data.compact_source import (
    scaled,
    trade_identity,
    canonical_economics,
    gap_mask,
    side_for_outcome,
)
from prediction_market_strategies.adapters.predictfun.accounting import (
    estimate_fee_atoms,
    recorded_taker_flow,
)


def trade(**kw):
    return dict(
        market_id="1",
        asset_id="99",
        side="Bid",
        execution_ns=123,
        price_native="500000000000000000",
        size_native="10000000000000000000",
        price_unit="native_wei",
        size_unit="native_wei",
        transaction_hash="0xab",
        settlement_id="unique",
        fee_bundle_id=1,
        **kw,
    )


def fee(side="Bid", amount="200000000000000000"):
    return [
        dict(
            leg="taker",
            fee_amount_native=amount,
            fee_unit="SHARES" if side == "Bid" else "COLLATERAL",
            fee_status="amount_reported",
            amount_is_estimate=False,
        )
    ]


class CompactSafety(unittest.TestCase):
    def test_units_preserve_18_decimal_values(self):
        self.assertEqual(
            scaled("123456789012345678", "native_wei"), D("0.123456789012345678")
        )
        for value, unit in [(1.5, "shares"), ("1", "unknown"), ("1.2", "native_wei")]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                scaled(value, unit)

    def test_storage_ids_and_fee_amendment_are_not_new_executions(self):
        row = trade()
        revised = {**row, "payload_id": 900, "fee_bundle_id": 92}
        self.assertEqual(
            trade_identity("predictfun", row), trade_identity("predictfun", revised)
        )
        self.assertEqual(canonical_economics(row), canonical_economics(revised))
        other = {**row, "settlement_id": "second"}
        self.assertNotEqual(
            trade_identity("predictfun", row), trade_identity("predictfun", other)
        )
        with self.assertRaises(ValueError):
            trade_identity("predictfun", {**row, "settlement_id": None})

    def test_taker_direction_is_on_own_outcome_including_no(self):
        self.assertEqual(side_for_outcome("Bid"), "BUY")
        self.assertEqual(side_for_outcome("Ask"), "SELL")
        with self.assertRaises(ValueError):
            side_for_outcome("unknown")

    def test_gap_clears_all_ready_books_and_update_cannot_heal(self):
        events = [
            (1, 1, "book", "a", {}),
            (2, 2, "book", "b", {}),
            (5, 3, "price_change", "a", {}),
            (9, 4, "price_change", "b", {}),
            (10, 5, "book", "a", {}),
            (11, 6, "price_change", "a", {}),
        ]
        out, report = gap_mask(events, [dict(start_ns=4, end_ns=8)])
        self.assertEqual(sum(v[2] == "gap_clear" for v in out), 2)
        self.assertNotIn(9, [v[0] for v in out])
        self.assertEqual(report["awaiting_snapshot"], ["b"])
        self.assertIn(11, [v[0] for v in out])

    def test_update_before_first_snapshot_is_suppressed(self):
        out, report = gap_mask(
            [(1, 1, "price_change", "a", {}), (2, 2, "book", "a", {})], []
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(report["suppressed"]["update_without_valid_snapshot"], 1)

    def test_share_fee_is_inventory_and_cash_fee_is_cash(self):
        buy = recorded_taker_flow(trade(), fee())
        self.assertEqual(D(buy["share_delta"]), D("9.8"))
        self.assertEqual(D(buy["cash_delta"]), D(-5))
        row = {**trade(), "side": "Ask"}
        sell = recorded_taker_flow(row, fee("Ask", "100000000000000000"))
        self.assertEqual(D(sell["share_delta"]), D(-10))
        self.assertEqual(D(sell["cash_delta"]), D("4.9"))
        with self.assertRaises(ValueError):
            recorded_taker_flow(row, fee())

    def test_native_fee_floor_and_outcome_symmetry(self):
        self.assertEqual(
            estimate_fee_atoms(5 * 10**17, 10 * 10**18, 200, "Bid"),
            (2 * 10**17, "SHARES"),
        )
        self.assertEqual(
            estimate_fee_atoms(5 * 10**17, 10 * 10**18, 200, "Ask"),
            (10**17, "COLLATERAL"),
        )
        self.assertEqual(
            estimate_fee_atoms(4 * 10**17, 10**18, 200, "Ask"),
            estimate_fee_atoms(6 * 10**17, 10**18, 200, "Ask"),
        )

    def test_runtime_preserves_high_precision_objects(self):
        from nautilus_trader.model import Price, Quantity

        for cls in (Price, Quantity):
            raw = "0.123456789012345678"
            self.assertEqual(str(cls.from_str(raw)), raw)

    def test_polymarket_fee_uses_fill_role_and_floor_not_old_wheel_rounding(self):
        from types import SimpleNamespace
        from nautilus_trader.model import Price, Quantity, Currency
        from prediction_market_strategies.paper.fees import PolymarketNoRewardFeeModel

        inst = SimpleNamespace(
            currency=Currency.from_str("USDC"),
            info=dict(
                fee_schedule=dict(rate="0.05", exponent="1", takerOnly=True),
                min_order_size="5",
            ),
        )
        model = PolymarketNoRewardFeeModel()
        q, p = Quantity.from_str("5"), Price.from_str("0.97")
        self.assertEqual(
            model.get_commission(
                SimpleNamespace(liquidity_side="TAKER"), q, p, inst
            ).as_decimal(),
            D("0.00727"),
        )
        self.assertEqual(
            model.get_commission(
                SimpleNamespace(liquidity_side="MAKER", is_post_only=False), q, p, inst
            ).as_decimal(),
            0,
        )
        with self.assertRaises(ValueError):
            model.get_commission(SimpleNamespace(liquidity_side=None), q, p, inst)

    def test_exact_overflow_cold_read_and_tamper_detection(self):
        from nautilus_trader.model import (
            Price,
            Quantity,
            TradeTick,
            TradeId,
            AggressorSide,
        )
        from prediction_market_strategies.adapters.common import (
            binary_instrument,
            book_snapshot,
        )
        from prediction_market_strategies.data.nautilus_exporter import (
            NautilusExporter,
            ExportedGroup,
            event_hash,
        )

        spec = dict(
            currency="USDT",
            currency_precision=18,
            price_precision=18,
            size_precision=18,
            price_increment="0.001000000000000000",
            size_increment="0.000000000000000001",
        )
        inst = binary_instrument(
            venue="predictfun",
            market_id="1",
            condition_id="c",
            token="99",
            outcome="Yes",
            terms={},
            spec=spec,
            observed_ns=100,
            revision="test",
        )
        values = [
            book_snapshot(
                inst, [("0.4", "10")], [("0.5", "10")], ts_event=101, ts_init=102
            ),
            TradeTick(
                inst.id,
                Price.from_str("0.500000000000000000"),
                Quantity.from_str("1.123456789012345670"),
                AggressorSide.BUY,
                TradeId("exact"),
                103,
                104,
            ),
        ]
        export = ExportedGroup(
            [inst],
            values,
            dict(data_records=2, instrument_inputs=[dict(parameters=spec)]),
        )
        with TemporaryDirectory() as folder:
            target = Path(folder) / "catalog"
            cold = NautilusExporter.write_catalog(export, target, exact_spill=True)
            self.assertEqual(
                [event_hash(v) for v in cold.data], [event_hash(v) for v in values]
            )
            self.assertEqual(str(cold.data[1].size), "1.123456789012345670")
            self.assertEqual(
                cold.report["native_catalog_precision_contract"]["overflow_events"], 1
            )
            p = target / "exact-overflow.parquet"
            p.write_bytes(p.read_bytes() + b"tamper")
            with self.assertRaisesRegex(ValueError, "CATALOG_FILE_CHANGED"):
                NautilusExporter.read_export(target)

    def api_fixture(self, venue, rows, occurrences):
        from prediction_market_strategies.data.compact_source import CompactSource

        s = CompactSource.__new__(CompactSource)
        s.start = 100
        s.end = 200
        tables = {
            "trade_content": rows,
            "trade_page_content": [
                dict(
                    content_id=i,
                    occurrences=[
                        dict(row_index=n, payload_id=p) for n, p in enumerate(values)
                    ],
                )
                for i, values in enumerate(occurrences)
            ],
            "trade_pages": [
                dict(
                    sequence=i,
                    market_id="1",
                    available_at_ns=210 + i,
                    returned_rows=len(values),
                    content_id=i,
                )
                for i, values in enumerate(occurrences)
            ],
        }
        s.rows = lambda kind, v: tables[kind]
        return s.api_trades(venue, {"1"})

    def test_api_polls_and_fee_amendments_do_not_double_volume(self):
        base = {**trade(), "payload_id": 0, "execution_ns": 123}
        rows = [base, {**base, "payload_id": 1, "fee_bundle_id": 2}]
        values, report = self.api_fixture("predictfun", rows, [[0, 0], [1]])
        self.assertEqual(len(values), 1)
        self.assertEqual(values[0]["available_at_ns"], 210)
        self.assertEqual(len(report["fee_revisions"]), 1)
        self.assertEqual(report["counts"]["repeat_observations"], 2)

    def test_poly_multiplicity_is_max_per_page_not_sum_of_polls(self):
        base = {
            **trade(),
            "payload_id": 0,
            "execution_ns": 123,
            "side": "BUY",
            "price_unit": "probability",
            "size_unit": "shares",
        }
        values, report = self.api_fixture("polymarket", [base], [[0, 0], [0]])
        self.assertEqual(len(values), 2)
        self.assertEqual(report["counts"]["ambiguous_multiplicity_groups"], 1)
        self.assertEqual(len({r["identity"] for r in values}), 2)

    def test_conflicting_settlement_economics_and_page_multiplicity_fail(self):
        base = {**trade(), "payload_id": 0, "execution_ns": 123}
        with self.assertRaisesRegex(ValueError, "CONFLICTING_STABLE_TRADE_ID"):
            self.api_fixture(
                "predictfun",
                [base, {**base, "payload_id": 1, "size_native": "2000000000000000000"}],
                [[0], [1]],
            )

    def test_shared_paper_lifecycle_cancels_and_disarms_on_gap_control(self):
        from types import SimpleNamespace
        from nautilus_trader.model import (
            InstrumentId,
            OrderBook,
            BookType,
            OrderBookDelta,
            OrderBookDeltas,
        )
        from prediction_market_strategies.strategies.binary_market import (
            BinaryMarketPaperStrategy,
        )

        ident = InstrumentId.from_str("test.POLYMARKET")
        cancelled = []
        records = []
        obj = SimpleNamespace(
            native_books={ident: OrderBook(ident, BookType.L2_MBP)},
            counts=Counter(),
            raw_batches=[],
            views={ident: object()},
            candidate_ns=123,
            cancel_all_orders=lambda i: cancelled.append(i),
            record=lambda *a, **k: records.append(a),
        )
        delta = OrderBookDelta.clear(ident, 0, 100, 101)
        # A control CLEAR has no F_SNAPSHOT; it is not permission to quote.
        from nautilus_trader.model import RecordFlag

        delta = OrderBookDelta(
            ident, delta.action, delta.order, int(RecordFlag.F_LAST), 0, 100, 101
        )
        BinaryMarketPaperStrategy.on_book_deltas(obj, OrderBookDeltas(ident, [delta]))
        self.assertEqual(cancelled, [ident])
        self.assertNotIn(ident, obj.views)
        self.assertIsNone(obj.candidate_ns)


if __name__ == "__main__":
    unittest.main()
