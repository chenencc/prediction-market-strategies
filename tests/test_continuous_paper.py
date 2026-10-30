from decimal import Decimal as D
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from nautilus_trader.model import BinaryOption
from prediction_market_strategies.adapters.common import book_snapshot
from prediction_market_strategies.paper.continuous import (
    choose_market,
    next_balance,
    SessionLock,
    carry_from,
)
from prediction_market_strategies.paper.replay_monitor import replay, synthetic_inputs
from prediction_market_strategies.strategies.single_market import SingleRules


class ContinuousPaper(unittest.TestCase):
    def test_up_down_multiple_native_cycles_and_cost_basis(self):
        original, original_data = synthetic_inputs()
        first = original_data[0].ts_init
        items = []
        for item, outcome in zip(original, ["Up", "Down"]):
            row = item.to_dict()
            row["outcome"] = outcome
            items.append(BinaryOption.from_dict(row))
        data = []
        for tick in range(240):
            for index, item in enumerate(items):
                # Later entries at a different price must have their own basis.
                bid = D("0.40") if tick < 80 else D("0.55")
                if index:
                    bid = 1 - bid - D("0.02")
                ts = first + tick * 250_000_000 + index * 10_000_000
                data.append(
                    book_snapshot(
                        item,
                        [(str(bid), "200")],
                        [(str(bid + D("0.01")), "200")],
                        ts_event=ts,
                        ts_init=ts,
                    )
                )
        rules = SingleRules(
            repeat=True,
            buy_below=D("0.95"),
            hold_ns=4_000_000_000,
            cooldown_ns=2_000_000_000,
            entry_cutoff_ns=first + 45_000_000_000,
            exit_cutoff_ns=first + 50_000_000_000,
        )
        with TemporaryDirectory() as d:
            report, monitor = replay(
                Path(d) / "run",
                rules=rules,
                inputs=(items, data),
                capture_raw_inputs=False,
            )
            final = monitor.snapshots[-1]
            self.assertEqual(report["status"], "completed")
            self.assertGreaterEqual(final["cycles"], 5)
            self.assertEqual(report["retained_native_batches"], 0)
            self.assertEqual(sum(b["batches"] for b in final["books"]), len(data))
            self.assertTrue(all(D(b["held"]) == 0 for b in final["books"]))
            self.assertTrue(all(o["status"] == "FILLED" for o in final["orders"]))
            self.assertEqual(
                D(final["account"]["cash_total"]) - 1000,
                D(final["account"]["realized_pnl"]),
            )
            later_sales = [
                e
                for e in monitor.events
                if e["kind"] == "decision"
                and e["side"] == "SELL"
                and int(e["now_ns"]) > first + 25_000_000_000
            ]
            self.assertTrue(later_sales)
            self.assertTrue(
                all(
                    e["reason"] in {"hold_time_elapsed", "window_closing"}
                    for e in later_sales
                )
            )
            self.assertTrue(
                all(
                    int(e["now_ns"]) < rules.entry_cutoff_ns
                    for e in monitor.events
                    if e["kind"] == "decision" and e["side"] == "BUY"
                )
            )

    def test_carry_rejects_residual_unconfirmed_or_changed_collateral(self):
        report = dict(
            status="completed",
            node_exited_cleanly=True,
            result=dict(
                halted=False,
                unresolved_orders=[],
                simulated_net_inventory={"up": "0", "down": "0"},
                cash_total="987.12345",
                currency="pUSD",
            ),
        )
        state = dict(account=dict(fees="1.20"))
        self.assertEqual(next_balance(report, state), (D("987.12345"), "pUSD"))
        with self.assertRaises(ValueError):
            next_balance(report, state, "USDC")
        report["result"]["simulated_net_inventory"]["up"] = "1"
        with self.assertRaises(ValueError):
            next_balance(report, state)
        report["result"]["simulated_net_inventory"]["up"] = "0"
        report["result"]["unresolved_orders"] = ["unknown"]
        with self.assertRaises(ValueError):
            next_balance(report, state)

    def test_discovery_proves_current_window_and_official_flags(self):
        start = 1793377800
        market = dict(
            slug=f"btc-updown-15m-{start}",
            outcomes='["Up","Down"]',
            endDate="2026-10-30T16:45:00Z",
            active=True,
            closed=False,
            acceptingOrders=True,
            enableOrderBook=True,
            conditionId="0xabc",
            id="test",
            question="BTC",
        )
        event = dict(slug=market["slug"], markets=[market], title="BTC")
        self.assertEqual(
            choose_market(event, start, start + 10)["condition_id"], "0xabc"
        )
        self.assertIsNone(choose_market(event, start, start - 1))
        self.assertIsNone(choose_market(event, start, start + 850))
        market["acceptingOrders"] = False
        self.assertIsNone(choose_market(event, start, start + 10))

    def test_singleton_owner_and_release(self):
        with TemporaryDirectory() as d:
            path = Path(d) / "owner"
            with SessionLock(path):
                with self.assertRaises(RuntimeError):
                    with SessionLock(path):
                        pass
            with SessionLock(path):
                pass

    def test_restart_carries_verified_losses_and_fees_without_reset(self):
        with TemporaryDirectory() as d:
            root = Path(d).resolve()
            control = root / "_sessions" / "old"
            control.mkdir(parents=True)
            product = root / "old-0001"
            product.mkdir()
            report = dict(
                status="completed",
                node_exited_cleanly=True,
                result=dict(
                    halted=False,
                    unresolved_orders=[],
                    simulated_net_inventory={"up": "0", "down": "0"},
                    cash_total="987.12",
                    currency="pUSD",
                ),
            )
            (product / "report.json").write_text(json.dumps(report))
            (product / "monitor-state.json").write_text(
                json.dumps(dict(account=dict(fees="1.2")))
            )
            status = dict(
                status="stopped",
                runs=[dict(run_id=product.name)],
                balance="987.12",
                fees="2.30",
                currency="pUSD",
            )
            (control / "session.json").write_text(json.dumps(status))
            self.assertEqual(carry_from(root, "old"), (D("987.12"), D("2.30"), "pUSD"))
            status["balance"] = "1000"
            (control / "session.json").write_text(json.dumps(status))
            with self.assertRaises(ValueError):
                carry_from(root, "old")


if __name__ == "__main__":
    unittest.main()
