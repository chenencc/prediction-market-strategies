from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from prediction_market_strategies.adapters.predictfun import PredictDataLoader
from prediction_market_strategies.adapters.common import parameters
from prediction_market_strategies.adapters.predictfun.loader import (
    atoms,
    execution_time,
)
from prediction_market_strategies.data.recording_group import InputError
from prediction_market_strategies.data.nautilus_exporter import (
    NautilusExporter,
    event_hash,
)
from predict_support import KNOWN, market, definition, book, match, page


def loader(**options):
    return PredictDataLoader.from_market_json(
        market(), received_ns=KNOWN, definition=definition(), **options
    )


class PredictNativeTests(unittest.TestCase):
    def test_decimal_precision_is_quote_grid_not_quantity_or_aggregate_price_precision(
        self,
    ):
        value = loader()
        self.assertEqual(value.spec["price_precision"], 12)
        self.assertEqual(value.spec["size_precision"], 8)
        self.assertEqual(value.spec["price_increment"], "0.001000000000")
        self.assertEqual(
            {i.currency.precision for i in value.instruments.values()}, {18}
        )
        raw = {**market()["data"], **definition()["fields"]}
        raw.pop("pricePrecision")
        self.assertEqual(parameters(raw, "predictfun")["price_precision"], 3)
        raw.pop("sizePrecision")
        with self.assertRaisesRegex(InputError, "MISSING_QUANTITY_PRECISION"):
            parameters(raw, "predictfun")

    def test_definitions_are_explicit_market_bound_and_not_future_information(self):
        for field, value, code in [
            ("market_id", "9", "DEFINITION_MARKET"),
            ("known_at_ns", KNOWN + 1, "FUTURE_DEFINITION"),
            ("source_ref", "", "DEFINITION_PROVENANCE"),
        ]:
            d = definition()
            d[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(InputError, code):
                PredictDataLoader.from_market_json(
                    market(), received_ns=KNOWN, definition=d
                )
        with self.assertRaisesRegex(InputError, "MISSING_QUANTITY_PRECISION"):
            PredictDataLoader.from_market_json(market(), received_ns=KNOWN)
        d = definition()
        d["fields"]["decimalPrecision"] = 2
        with self.assertRaisesRegex(InputError, "DEFINITION_FIELDS"):
            PredictDataLoader.from_market_json(
                market(), received_ns=KNOWN, definition=d
            )

    def test_native_index_set_and_full_depth_without_double_liquidity(self):
        value = loader()
        out = value.parse_orderbook(book(), received_ns=KNOWN + 1_000_000)
        self.assertEqual(str(out.instrument_id), "517145-10001.PREDICTFUN")
        self.assertEqual(len(out.deltas), 5)
        self.assertEqual(str(out.deltas[0].action), "CLEAR")
        self.assertEqual(str(out.deltas[1].order.price), "0.491000000000")
        self.assertFalse(value.report["complement_generated"])
        self.assertEqual(value.instruments["10001"].outcome, "Higher")

    def test_empty_books_clear_stale_levels_and_preserve_receipt_time(self):
        value = loader()
        native = dict(
            success=True,
            data=dict(marketId=517145, tsMs=KNOWN // 1_000_000, bids=[], asks=[]),
        )
        out = value.parse_orderbook(native, received_ns=KNOWN + 999)
        self.assertEqual(len(out.deltas), 1)
        self.assertEqual(str(out.deltas[0].action), "CLEAR")
        self.assertEqual(out.ts_init, KNOWN + 999)

    def test_wrong_market_unknown_outcome_and_ambiguous_clock_fail(self):
        data = dict(
            success=True,
            data=dict(marketId=9, tsMs=KNOWN // 1_000_000, bids=[], asks=[]),
        )
        with self.assertRaisesRegex(InputError, "BOOK_MARKET"):
            loader().parse_orderbook(data, received_ns=KNOWN + 1)
        data["data"].update(marketId=517145, updateTimestampMs=1)
        with self.assertRaisesRegex(InputError, "SOURCE_CLOCK"):
            loader().parse_orderbook(data, received_ns=KNOWN + 1)
        with self.assertRaisesRegex(InputError, "TRADE_TOKEN"):
            loader().parse_matches(
                page(match(token="999")), received_ns=KNOWN + 10_000_000
            )

    def test_invalid_float_and_off_tick_quotes_are_not_rounded(self):
        for price, code in [
            (0.491, "EXACT_DECIMAL_REQUIRED"),
            ("0.4911", "LEVEL_OFF_PRICE_TICK"),
        ]:
            data = dict(
                success=True,
                data=dict(
                    marketId=517145,
                    tsMs=KNOWN // 1_000_000,
                    bids=[[price, "2"]],
                    asks=[],
                ),
            )
            with self.subTest(price=price), self.assertRaisesRegex(InputError, code):
                loader().parse_orderbook(data, received_ns=KNOWN + 1)

    def test_aggregate_trade_price_no_side_and_nine_digit_execution_time(self):
        value = loader()
        out = value.parse_matches(
            page(match(token="10002", side="Bid")), received_ns=KNOWN + 10_000_000
        )
        tick = out.ticks[0]
        self.assertEqual(str(tick.instrument_id), "517145-10002.PREDICTFUN")
        self.assertEqual(str(tick.price), "0.121369800000")
        self.assertEqual(str(tick.size), "100.00000000")
        self.assertEqual(str(tick.aggressor_side), "BUY")
        self.assertEqual(tick.ts_event, 1792497600123456789)
        self.assertEqual(tick.ts_init, KNOWN + 10_000_000)

    def test_cursor_overlap_repeated_rows_and_fee_amendments_do_not_inflate_trades(
        self,
    ):
        value = loader()
        row = match()
        first = value.parse_matches(
            page(row, row, cursor="page2"), received_ns=KNOWN + 10_000_000
        )
        amended = deepcopy(row)
        amended["taker"]["fee"]["amount"] = "3500000000000000"
        second = value.parse_matches(page(amended), received_ns=KNOWN + 20_000_000)
        self.assertEqual(first.cursor, "page2")
        self.assertEqual(len(first.ticks), 1)
        self.assertEqual(first.counts["duplicate_observations"], 1)
        self.assertEqual(second.ticks, [])
        self.assertEqual(
            first.observations[0]["fees"][0]["amount"], "0.002500000000000000"
        )
        self.assertEqual(
            second.observations[0]["fees"][0]["amount"], "0.003500000000000000"
        )
        self.assertEqual(
            second.observations[0]["first_received_ns"], KNOWN + 10_000_000
        )

    def test_same_transaction_different_settlements_are_distinct(self):
        out = loader().parse_matches(
            page(match(sid="a"), match(sid="b")), received_ns=KNOWN + 10_000_000
        )
        self.assertEqual(len(out.ticks), 2)
        self.assertNotEqual(out.ticks[0].trade_id, out.ticks[1].trade_id)

    def test_missing_trade_id_is_quarantined_and_unknown_fee_unit_stays_unknown(self):
        row = match(sid=None)
        row["makers"][0]["fee"]["type"] = "NEW_UNIT"
        out = loader().parse_matches(page(row), received_ns=KNOWN + 10_000_000)
        self.assertEqual(out.ticks, [])
        self.assertEqual(out.counts["ambiguous_quarantined"], 1)
        fee = out.observations[0]["fees"][1]
        self.assertIsNone(fee["amount"])
        self.assertEqual(fee["status"], "unknown_type")

    def test_page_failure_does_not_commit_partial_trade_identity_state(self):
        value = loader()
        bad = match(sid="bad")
        bad["priceExecuted"] = "-1"
        with self.assertRaisesRegex(InputError, "NATIVE_ID"):
            value.parse_matches(page(match(), bad), received_ns=KNOWN + 10_000_000)
        self.assertEqual(
            len(
                value.parse_matches(page(match()), received_ns=KNOWN + 10_000_000).ticks
            ),
            1,
        )
        conflict = match()
        conflict["amountFilled"] = "200000000000000000000"
        with self.assertRaisesRegex(InputError, "CONFLICTING_STABLE_TRADE_ID"):
            value.parse_matches(page(conflict), received_ns=KNOWN + 20_000_000)

    def test_identity_and_receipt_memory_bounds(self):
        value = loader(max_trade_ids=1)
        with self.assertRaisesRegex(InputError, "IDENTITY_CAP"):
            value.parse_matches(
                page(match(sid="a"), match(sid="b")), received_ns=KNOWN + 10_000_000
            )
        self.assertEqual(
            len(
                value.parse_matches(
                    page(match(sid="a")), received_ns=KNOWN + 10_000_000
                ).ticks
            ),
            1,
        )
        limited = loader(max_receipts=1)
        limited.parse_orderbook(book(), received_ns=KNOWN + 1)
        with self.assertRaisesRegex(InputError, "RECEIPT_CAP"):
            limited.parse_orderbook(book(), received_ns=KNOWN + 2)

    def test_no_nanosecond_or_uint256_precision_loss(self):
        self.assertEqual(atoms("1000000000000000001"), "1.000000000000000001")
        self.assertEqual(
            execution_time("2026-10-20T13:00:00.123456789+01:00"),
            execution_time("2026-10-20T12:00:00.123456789Z"),
        )
        with self.assertRaisesRegex(InputError, "EXACT_TIMEZONE"):
            execution_time("2026-10-20T12:00:00")
        with self.assertRaisesRegex(InputError, "UINT256"):
            atoms(str(2**256))
        row = match()
        row["amountFilled"] = "1000000000000000001"
        with self.assertRaisesRegex(InputError, "UNREPRESENTABLE_PRECISION"):
            loader().parse_matches(page(row), received_ns=KNOWN + 10_000_000)

    def test_native_adapter_output_cold_reads_in_real_nautilus_catalog(self):
        value = loader()
        snapshot = value.parse_orderbook(book(), received_ns=KNOWN + 1)
        trades = value.parse_matches(page(match()), received_ns=KNOWN + 10_000_000)
        group = value.as_exported_group([snapshot, *trades.ticks])
        with TemporaryDirectory() as folder:
            out = NautilusExporter.write_catalog(group, Path(folder) / "native")
            self.assertEqual(
                [event_hash(x) for x in group.data], [event_hash(x) for x in out.data]
            )
            self.assertTrue(out.report["catalog_cold_readback_verified"])
            self.assertEqual({i.currency.precision for i in out.instruments}, {18})

    def test_native_grid_does_not_justify_rounding_aggregate_trades(self):
        d = definition()
        d["fields"].pop("pricePrecision")
        value = PredictDataLoader.from_market_json(
            market(), received_ns=KNOWN, definition=d
        )
        with self.assertRaisesRegex(InputError, "UNREPRESENTABLE_PRECISION"):
            value.parse_matches(page(match()), received_ns=KNOWN + 10_000_000)
        d["fields"]["pricePrecision"] = 12
        d["fields"]["priceIncrement"] = "0.0001"
        with self.assertRaisesRegex(InputError, "TICK_CONFLICTS_WITH_NATIVE_PRECISION"):
            PredictDataLoader.from_market_json(
                market(), received_ns=KNOWN, definition=d
            )

    def test_engine_probe_separates_data_acceptance_from_missing_l2_inputs(self):
        from prediction_market_strategies.backtest.replay_probe import replay_probe

        value = loader()
        trade = value.parse_matches(
            page(match(token="10002")), received_ns=KNOWN + 10_000_000
        )
        probe = replay_probe(value.as_exported_group(trade.ticks))
        self.assertEqual(probe["iterations"], 1)
        self.assertEqual(probe["venue_book_types"]["PREDICTFUN"]["book_type"], "L1_MBP")
        self.assertFalse(probe["l2_execution_inputs_complete"])
        self.assertFalse(probe["profitability_tested"])


if __name__ == "__main__":
    unittest.main()
