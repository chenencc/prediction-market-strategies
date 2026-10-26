from copy import deepcopy
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from prediction_market_strategies.data.nautilus_exporter import (
    NautilusExporter,
    event_hash,
)
from prediction_market_strategies.data.recording_group import (
    RecordingGroup,
    InputError,
    file_hash,
    digest,
)
from fixture_support import samples, write_group, modify_terms, with_initial_gap


class SourceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixtures = samples()

    def test_all_four_actual_recorder_formats_verify_without_recorder_import(self):
        self.assertEqual(len(self.fixtures["groups"]), 4)
        with TemporaryDirectory() as folder:
            for value in self.fixtures["groups"]:
                root = write_group(Path(folder) / value["task_type"], value)
                g = RecordingGroup(root)
                for name in ("orderbook.parquet", "trades.parquet"):
                    self.assertEqual(
                        len(list(g.iter_rows(name))),
                        value["files"][name]["metadata"]["rows"],
                    )

    def test_changed_payload_is_rejected_before_conversion(self):
        with TemporaryDirectory() as folder:
            root = write_group(Path(folder) / "source", self.fixtures["groups"][1])
            p = root / "orderbook.parquet"
            p.write_bytes(p.read_bytes() + b"changed")
            with self.assertRaisesRegex(InputError, "SOURCE_FILE_HASH"):
                RecordingGroup(root)

    def test_delta_without_checkpoint_is_rejected(self):
        value = deepcopy(self.fixtures["groups"][2])
        row = next(
            r
            for r in value["files"]["orderbook.parquet"]["rows"]
            if r["kind"] == "book"
        )
        op = json.loads(row["operation_json"])
        op.update(type="delta", base=0)
        row["operation_json"] = json.dumps(op)
        with TemporaryDirectory() as folder:
            g = RecordingGroup(write_group(Path(folder) / "source", value))
            with self.assertRaisesRegex(InputError, "MISSING_CHECKPOINT"):
                list(g.iter_rows("orderbook.parquet"))

    def test_metadata_cannot_be_read_before_its_observation(self):
        with TemporaryDirectory() as folder:
            group = RecordingGroup(
                write_group(Path(folder) / "source", self.fixtures["groups"][1])
            )
            key = next(iter(group.markets))
            first = min(
                o["received_unix_ns"] for o in group.markets[key]["observations"]
            )
            with self.assertRaisesRegex(InputError, "NO_PRIOR_OBSERVED_METADATA"):
                group.terms_at(key, first - 1)
            self.assertEqual(
                group.terms_at(key, first),
                group.versions[
                    key + ":" + group.markets[key]["observations"][0]["revision"]
                ]["terms"],
            )


class NautilusConversionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixtures = samples()

    def test_all_four_groups_convert_preserving_namespaces_and_snapshot_flags(self):
        from nautilus_trader.model import RecordFlag

        with TemporaryDirectory() as folder:
            for value in self.fixtures["groups"]:
                root = write_group(Path(folder) / value["task_type"], value)
                out = NautilusExporter().convert(root)
                self.assertGreater(out.report["book_batches"], 0)
                for item in out.data:
                    if type(item).__name__ != "OrderBookDeltas":
                        continue
                    self.assertEqual(str(item.deltas[0].action), "CLEAR")
                    self.assertTrue(
                        all(
                            int(d.flags) & int(RecordFlag.F_SNAPSHOT)
                            for d in item.deltas
                        )
                    )
                    self.assertTrue(int(item.deltas[-1].flags) & int(RecordFlag.F_LAST))
                self.assertEqual(
                    [v.ts_init for v in out.data], sorted(v.ts_init for v in out.data)
                )
                self.assertFalse(out.report["predict_complement_generated"])
                if value["task_type"] == "cross_platform_pair":
                    self.assertEqual(
                        {str(i.id.venue) for i in out.instruments},
                        {"POLYMARKET", "PREDICTFUN"},
                    )
                    self.assertEqual(len(out.instruments), 4)

    def test_duplicate_observations_are_not_multiple_fills_and_ambiguity_is_quarantined(
        self,
    ):
        value = next(
            v
            for v in self.fixtures["groups"]
            if v["task_type"] == "cross_platform_pair"
        )
        with TemporaryDirectory() as folder:
            out = NautilusExporter().convert(
                write_group(Path(folder) / "source", value)
            )
            self.assertEqual(
                out.report["trade_counts"],
                dict(
                    observations=8,
                    ambiguous_quarantined=4,
                    canonical_observed_trades=1,
                    duplicate_observations=3,
                ),
            )
            trade = next(v for v in out.data if type(v).__name__ == "TradeTick")
            self.assertGreater(trade.ts_init, trade.ts_event)
            self.assertEqual(str(trade.aggressor_side), "BUY")

    def test_missing_precision_does_not_get_inferred_from_price_or_protocol(self):
        value = modify_terms(
            self.fixtures["groups"][1], lambda terms: terms.pop("pricePrecision")
        )
        with TemporaryDirectory() as folder:
            with self.assertRaisesRegex(InputError, "MISSING_PRICE_PRECISION"):
                NautilusExporter().convert(write_group(Path(folder) / "source", value))

    def test_missing_currency_and_conflicting_ticks_are_refused(self):
        for field, message in [
            ("collateralCurrency", "MISSING_COLLATERAL_CURRENCY"),
            ("priceIncrement", "CONFLICTING_ALIASES"),
        ]:

            def mutate(terms):
                if field == "priceIncrement":
                    terms[field] = "0.01"
                else:
                    terms.pop(field)

            value = modify_terms(self.fixtures["groups"][1], mutate)
            with self.subTest(field=field), TemporaryDirectory() as folder:
                with self.assertRaisesRegex(InputError, message):
                    NautilusExporter().convert(
                        write_group(Path(folder) / "source", value)
                    )

    def test_bounded_export_and_readonly_source(self):
        with TemporaryDirectory() as folder:
            root = write_group(Path(folder) / "source", self.fixtures["groups"][2])
            before = {p.name: file_hash(p) for p in root.iterdir()}
            with self.assertRaisesRegex(InputError, "EXPORT_RECORD_CAP"):
                NautilusExporter(max_records=1).convert(root)
            with self.assertRaisesRegex(InputError, "EXPORT_DELTA_CAP"):
                NautilusExporter(max_deltas=1).convert(root)
            self.assertEqual(before, {p.name: file_hash(p) for p in root.iterdir()})

    def test_observed_fees_are_preserved_without_claiming_simulation(self):
        with TemporaryDirectory() as folder:
            out = NautilusExporter().convert(
                write_group(Path(folder) / "source", self.fixtures["groups"][1])
            )
            for instrument in out.instruments:
                self.assertEqual(instrument.info["fee_schedule"]["rate"], "0.04")
                self.assertTrue(instrument.info["scheduled_end_not_observed_closure"])
                self.assertEqual(instrument.expiration_ns, 0)
            self.assertFalse(out.report["execution_simulation_certified"])

    def test_later_fee_semantic_changes_are_not_silently_ignored(self):
        value = deepcopy(self.fixtures["groups"][1])
        market = value["context"]["markets"][0]
        version = deepcopy(market["versions"][0])
        observation = deepcopy(market["observations"][0])
        version["terms"]["feeType"] = "new_execution_fee_type"
        version["revision"] = digest(version["terms"])
        market["versions"].append(version)
        observation["revision"] = version["revision"]
        observation["received_unix_ns"] += 1
        observation["receipt_sequence"] += 1
        market["observations"].append(observation)
        with TemporaryDirectory() as folder:
            source = write_group(Path(folder) / "source", value)
            with self.assertRaisesRegex(
                InputError, "INSTRUMENT_EXECUTION_TERMS_CHANGED"
            ):
                NautilusExporter().convert(source)

    def test_catalog_cold_read_preserves_currencies_and_exact_event_semantics(self):
        from nautilus_trader.model import Currency

        pair = next(
            v
            for v in self.fixtures["groups"]
            if v["task_type"] == "cross_platform_pair"
        )
        with TemporaryDirectory() as folder:
            source = write_group(Path(folder) / "source", pair)
            target = Path(folder) / "exported"
            before = Currency.from_str("USDT").precision
            original = NautilusExporter().convert(source)
            out = NautilusExporter().export(source, target)
            self.assertEqual(Currency.from_str("USDT").precision, before)
            self.assertEqual(
                [event_hash(v) for v in original.data],
                [event_hash(v) for v in out.data],
            )
            self.assertTrue(out.report["catalog_cold_readback_verified"])
            self.assertEqual(
                {
                    i.currency.precision
                    for i in out.instruments
                    if str(i.id.venue) == "PREDICTFUN"
                },
                {18},
            )
            p = next((target / "catalog").rglob("*.parquet"))
            p.write_bytes(p.read_bytes() + b"changed")
            with self.assertRaisesRegex(InputError, "CATALOG_FILE_CHANGED"):
                NautilusExporter.read_export(target)

    def test_unsafe_output_paths_and_overwrites_are_refused(self):
        with TemporaryDirectory() as folder:
            root = write_group(Path(folder) / "source", self.fixtures["groups"][1])
            with self.assertRaisesRegex(InputError, "OUTPUT_MUST_BE_NEW"):
                NautilusExporter().export(root, root / "export")
            with self.assertRaisesRegex(InputError, "OUTPUT_MUST_BE_NEW"):
                NautilusExporter().export(root, root)

    def test_exclude_trade_mode_is_explicit_and_still_verifies_trade_source(self):
        with TemporaryDirectory() as folder:
            root = write_group(Path(folder) / "source", self.fixtures["groups"][1])
            out = NautilusExporter(trade_mode="exclude").convert(root)
            self.assertEqual(out.report["trade_ticks"], 0)
            self.assertEqual(out.report["trade_counts"]["excluded_by_policy"], 8)

    def test_transport_gap_fences_survive_native_catalog_readback(self):
        value, gap = with_initial_gap(self.fixtures["groups"][1])
        with TemporaryDirectory() as folder:
            root = write_group(Path(folder) / "source", value)
            out = NautilusExporter().export(root, Path(folder) / "export")
            self.assertEqual(len(out.report["gaps"]), 1)
            self.assertIn("synthetic_test_timeout", out.report["gaps"][0]["context"])
            fences = [v for v in out.data if v.ts_init == gap["received_unix_ns"]]
            self.assertEqual(len(fences), 2)
            self.assertTrue(
                all(
                    len(v.deltas) == 1 and str(v.deltas[0].action) == "CLEAR"
                    for v in fences
                )
            )


if __name__ == "__main__":
    unittest.main()
