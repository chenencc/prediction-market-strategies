from decimal import Decimal as D
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from prediction_market_strategies.paper.replay_monitor import replay, synthetic_inputs
from prediction_market_strategies.paper.monitor import MonitorWriter, wire
from prediction_market_strategies.strategies.single_market import SingleRules


class NativeRoundTrip(unittest.TestCase):
    def run_case(self, **kwargs):
        with TemporaryDirectory() as d:
            report, monitor = replay(Path(d) / "run", **kwargs)
            state = json.loads((Path(d) / "run/monitor-state.json").read_text())
            self.assertEqual(state["persisted_event_seq"], len(monitor.events))
            self.assertTrue(report["catalog_roundtrip_semantics_match"])
            return report, monitor, state

    def test_buy_sell_reconciles_cash_fees_and_no_position(self):
        report, monitor, state = self.run_case()
        self.assertEqual(report["status"], "completed")
        fills = state["fills"]
        self.assertEqual([f["side"] for f in fills], ["BUY", "SELL"])
        self.assertEqual(len(state["orders"]), 2)
        self.assertEqual(
            D(state["account"]["equity"]) - 1000, D(state["account"]["realized_pnl"])
        )
        self.assertGreater(D(state["account"]["fees"]), 0)
        self.assertTrue(all(D(b["held"]) == 0 for b in state["books"]))
        self.assertGreater(len(monitor.snapshots), 100)
        self.assertTrue(any(s["phase"] == "holding" for s in monitor.snapshots))

    def test_threshold_not_met_has_no_orders_or_fake_fills(self):
        report, _, state = self.run_case(rules=SingleRules(buy_below=D("0.10")))
        self.assertEqual(report["status"], "completed")
        self.assertEqual(state["orders"], [])
        self.assertEqual(state["reason"], "above_buy_threshold")
        self.assertEqual(D(state["account"]["cash_total"]), 1000)

    def test_partial_entry_below_minimum_retains_real_residual(self):
        from prediction_market_strategies.adapters.common import book_snapshot

        items, data = synthetic_inputs()
        ts = data[0].ts_init + 50_000_000
        data.append(
            book_snapshot(
                items[0], [("0.48", "100")], [("0.49", "3")], ts_event=ts, ts_init=ts
            )
        )
        data.sort(key=lambda b: b.ts_init)
        report, _, state = self.run_case(inputs=(items, data))
        self.assertEqual(report["status"], "failed")
        self.assertEqual(state["reason"], "residual_below_market_minimum")
        self.assertEqual(D(state["books"][0]["held"]), 3)
        self.assertEqual(len(state["orders"]), 1)

    def test_bad_rules_rejected(self):
        for kw in (
            {"quantity": D("NaN")},
            {"buy_below": D(1)},
            {"hold_ns": 0},
            {"quantity": 10.0},
        ):
            with self.assertRaises(ValueError):
                SingleRules(**kw)

    def test_stale_held_book_keeps_inventory_but_removes_valuation(self):
        from prediction_market_strategies.adapters.common import book_snapshot

        items, data = synthetic_inputs()
        ts = data[0].ts_init + 20_000_000_000
        data = data[:2] + [
            book_snapshot(
                items[1], [("0.50", "100")], [("0.51", "100")], ts_event=ts, ts_init=ts
            )
        ]
        report, _, state = self.run_case(
            inputs=(items, data), rules=SingleRules(hold_ns=60_000_000_000)
        )
        self.assertEqual(report["status"], "failed")
        self.assertIsNone(state["account"]["equity"])
        self.assertIsNone(state["account"]["unrealized_pnl"])
        self.assertEqual(D(state["books"][0]["held"]), 10)


class Observations(unittest.TestCase):
    def test_shutdown_drains_critical_events_and_latest_state(self):
        with TemporaryDirectory() as d:
            writer = MonitorWriter(d, {"run_id": "test"})
            for i in range(1000):
                self.assertTrue(
                    writer.event({"kind": "test", "now_ns": 1793000000000000000 + i})
                )
            writer.snapshot({"event_count": 1000, "published_ns": 1793000000000000000})
            writer.close()
            events = [
                json.loads(line)
                for line in (Path(d) / "monitor-events.jsonl").read_text().splitlines()
            ]
            self.assertEqual([r["seq"] for r in events], list(range(1, 1001)))
            state = json.loads((Path(d) / "monitor-state.json").read_text())
            self.assertEqual(state["persisted_event_seq"], 1000)
            self.assertEqual(state["published_ns"], "1793000000000000000")

    def test_critical_overflow_is_explicit(self):
        from queue import Queue

        writer = object.__new__(MonitorWriter)
        writer.seq, writer.error, writer.queue = 0, None, Queue(1)
        self.assertTrue(writer.event({"kind": "first"}))
        self.assertFalse(writer.event({"kind": "second"}))
        self.assertEqual(writer.error, "critical_observation_queue_full")

    def test_exact_values_are_strings_on_wire(self):
        self.assertEqual(
            wire(
                {"price": D("0.123456789123456789"), "observed_ns": 1793000000000000000}
            ),
            {"price": "0.123456789123456789", "observed_ns": "1793000000000000000"},
        )

    def test_readonly_routes_no_execution_or_path_escape(self):
        from prediction_market_strategies.paper.server import create_app

        with TemporaryDirectory() as d:
            app = create_app(d)
            routes = [r for r in app.routes if r.path.startswith("/api/")]
            self.assertTrue(all(r.methods == {"GET"} for r in routes))
            state_route = next(r for r in routes if r.path.endswith("/state"))
            from fastapi import HTTPException

            with self.assertRaises(HTTPException):
                state_route.endpoint("../secret")


if __name__ == "__main__":
    unittest.main()
