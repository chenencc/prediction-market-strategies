from dataclasses import replace
from decimal import Decimal
import unittest

from prediction_market_strategies.strategies.binary_market import BookView
from prediction_market_strategies.strategies.depth_maker import (
    MakerRules,
    RewardTerms,
    FillProtection,
    MODES,
    plan_quotes,
    front_depth,
)
from prediction_market_strategies.paper.rewards import from_gamma
from test_binary_pair_paper import instruments, NOW

D = Decimal


def book(*, bid="0.50", ask="0.52", size="3000", ts=NOW):
    return BookView(
        tuple((D(bid) - D("0.01") * i, D(size)) for i in range(5)),
        tuple((D(ask) + D("0.01") * i, D(size)) for i in range(5)),
        ts,
        ts,
    )


def reward():
    return RewardTerms(
        "polymarket",
        "test",
        "0xsynthetic-condition",
        NOW - 1,
        D("5"),
        D("0.10"),
        "midpoint_distance",
        True,
        "synthetic-test",
    )


def decide(*, books=None, inventory=None, rules=None, rewards=None, cash=D("1000")):
    return plan_quotes(
        books or [book(), book()],
        inventory or [D(0), D(0)],
        rules or MakerRules(),
        rewards or reward(),
        ticks=[D("0.01")] * 2,
        steps=[D("0.01")] * 2,
        minimums=[D("5")] * 2,
        cash=cash,
        now_ns=NOW,
        expiry_ns=NOW + 2 * 86400 * 10**9,
    )


class DepthPolicy(unittest.TestCase):
    def test_tightly_spaced_tiers_retreat_farther_instead_of_breaking_distance_guard(
        self,
    ):
        b = replace(
            book(),
            bids=tuple((D("0.500") - D("0.001") * i, D(3000)) for i in range(5)),
            asks=tuple((D("0.502") + D("0.001") * i, D(3000)) for i in range(5)),
        )
        terms = replace(reward(), maximum_distance=D("0.045"))
        result = plan_quotes(
            [b, b],
            [D(0), D(0)],
            MakerRules(),
            terms,
            ticks=[D("0.001")] * 2,
            steps=[D("0.01")] * 2,
            minimums=[D(5)] * 2,
            cash=D(1000),
            now_ns=NOW,
            expiry_ns=NOW + 2 * 86400 * 10**9,
        )
        self.assertEqual({q.price for q in result.quotes}, {D("0.465")})

    def test_risk_cap_allows_inventory_reducing_sells_without_fake_cash(self):
        result = decide(inventory=[D(200), D(200)], cash=D(0))
        self.assertTrue(result.quotes)
        self.assertTrue(all(q.side == "SELL" for q in result.quotes))

    def test_near_close_invalid_cash_and_ambiguous_reward_flag_are_rejected(self):
        result = plan_quotes(
            [book(), book()],
            [D(0), D(0)],
            MakerRules(),
            reward(),
            ticks=[D("0.01")] * 2,
            steps=[D("0.01")] * 2,
            minimums=[D(5)] * 2,
            cash=D(1000),
            now_ns=NOW,
            expiry_ns=NOW + 20 * 3600 * 10**9,
        )
        self.assertEqual(result.reason, "near_or_unknown_close")
        self.assertEqual(decide(cash=float("nan")).reason, "invalid_cash")
        with self.assertRaises(ValueError):
            replace(reward(), active_allocation="false")

    def test_fourth_and_third_tier_retreat_and_tick_rounding(self):
        c = decide()
        a = decide(rules=MakerRules(mode="aggressive"))
        self.assertEqual(c.reason, "quotable")
        self.assertEqual({q.price for q in c.quotes}, {D("0.45")})
        self.assertEqual({q.price for q in a.quotes}, {D("0.46")})
        self.assertTrue(all(q.side == "BUY" for q in c.quotes))

    def test_sells_use_only_available_inventory(self):
        result = decide(inventory=[D("7"), D("10")])
        self.assertEqual(
            [q.quantity for q in result.quotes if q.side == "SELL"], [D(7), D(10)]
        )
        self.assertTrue(
            all(q.price == D("0.57") for q in result.quotes if q.side == "SELL")
        )

    def test_insufficient_native_depth_and_empty_tiers_do_not_get_fabricated(self):
        self.assertFalse(decide(books=[book(size="100"), book()]).quotes)
        self.assertEqual(
            decide(books=[replace(book(), bids=book().bids[:2]), book()]).reason,
            "insufficient_native_tiers",
        )

    def test_stale_future_skewed_and_closed_inputs_block(self):
        self.assertEqual(
            decide(books=[book(ts=NOW - 3 * 10**9), book()]).reason, "stale_book"
        )
        self.assertEqual(decide(books=[book(ts=NOW + 1), book()]).reason, "stale_book")
        self.assertEqual(
            decide(books=[book(ts=NOW - 600_000_000), book()]).reason, "receipt_skew"
        )
        self.assertEqual(
            decide(rewards=replace(reward(), observed_ns=NOW + 1)).reason,
            "reward_not_yet_available",
        )

    def test_reward_band_is_midpoint_distance_and_does_not_pull_quote_forward(self):
        self.assertEqual(
            decide(rewards=replace(reward(), maximum_distance=D("0.045"))).reason,
            "outside_reward_band",
        )
        self.assertFalse(
            decide(rewards=replace(reward(), active_allocation=False)).quotes
        )
        self.assertEqual(
            decide(rewards=replace(reward(), minimum_shares=D("200"))).reason,
            "below_venue_or_reward_minimum",
        )

    def test_own_order_subtraction_keeps_other_orders_at_same_price(self):
        b = book()
        before = front_depth(b, D("0.45"), "BUY")
        after = front_depth(b, D("0.45"), "BUY", [("BUY", D("0.50"), D("100"))])
        self.assertEqual(before - after, 100)

    def test_gap_fill_is_revalidated_after_tick_rounding(self):
        b = replace(
            book(),
            bids=(
                (D("0.50"), D(3000)),
                (D("0.49"), D(3000)),
                (D("0.48"), D(3000)),
                (D("0.44"), D(3000)),
                (D("0.43"), D(3000)),
            ),
        )
        result = decide(books=[b, b])
        self.assertEqual({q.price for q in result.quotes}, {D("0.46")})
        self.assertTrue(all(q.front >= D(6000) for q in result.quotes))

    def test_budget_reserves_hedging_and_no_short_inventory(self):
        self.assertEqual(decide(cash=D("10")).reason, "cash_and_hedge_reserve")
        self.assertEqual(
            decide(inventory=[D(-1), D(0)]).reason, "invalid_or_short_inventory"
        )

    def test_predict_native_reward_basis_is_explicit_not_polymarket_relabeling(self):
        terms = replace(
            reward(), venue="predictfun", basis="quote_width", maximum_distance=D("0.2")
        )
        self.assertTrue(decide(rewards=terms).quotes)
        with self.assertRaises(ValueError):
            replace(reward(), basis="quote_width")

    def test_fill_cooldown_blacklist_and_duplicate_partial_fills(self):
        guard = FillProtection(MODES["conservative"])
        guard.filled("order-a", NOW)
        guard.filled("order-a", NOW + 1)
        self.assertEqual(guard.count, 1)
        self.assertEqual(guard.blocked(NOW + 1), "fill_cooldown")
        guard.filled("order-b", NOW + 2)
        self.assertEqual(guard.blocked(NOW + 3), "fill_blacklist")
        self.assertIsNone(guard.blocked(NOW + 8 * 86400 * 10**9))

    def test_gamma_settings_without_current_allocation_are_not_active_rewards(self):
        market = dict(
            id="test",
            conditionId="0xsynthetic-condition",
            rewardsMinSize=5,
            rewardsMaxSpread="4.5",
            acceptingOrders=True,
            closed=False,
            clobRewards=[],
        )
        terms = from_gamma(
            market,
            condition_id="0xsynthetic-condition",
            market_id="test",
            received_ns=NOW,
            source="synthetic",
        )
        self.assertEqual(terms.maximum_distance, D("0.045"))
        self.assertFalse(terms.active_allocation)
        market["clobRewards"] = [
            dict(
                conditionId="wrong",
                rewardsDailyRate="10",
                startDate="2026-01-01",
                endDate="2027-01-01",
            )
        ]
        with self.assertRaisesRegex(ValueError, "condition mismatch"):
            from_gamma(
                market,
                condition_id="0xsynthetic-condition",
                market_id="test",
                received_ns=NOW,
                source="synthetic",
            )


def native_replay(
    *,
    touch=False,
    reprice=False,
    hedge_mode="FLATTEN",
    gap_loss=False,
    fill_during_cancel=False,
    partial_fill=False,
):
    from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
    from nautilus_trader.model import (
        BinaryOption,
        Venue,
        OmsType,
        AccountType,
        Money,
        BookType,
    )
    from nautilus_trader.execution import StaticLatencyModel
    from prediction_market_strategies.paper.fees import (
        PolymarketNoRewardFeeModel,
        ThroughPriceFillModel,
    )
    from prediction_market_strategies.strategies.depth_maker_nautilus import (
        DepthMakerPaperStrategy,
    )
    from prediction_market_strategies.adapters.common import book_snapshot
    from prediction_market_strategies.paper.polymarket_pair import summarize

    items = []
    for i in instruments("0.03"):
        value = i.to_dict()
        value["expiration_ns"] = NOW + 2 * 86400 * 10**9
        value["info"]["market_id"] = "test"
        value["info"]["fee_schedule"]["rebateRate"] = "0.25"
        items.append(BinaryOption.from_dict(value))
    engine = BacktestEngine(
        BacktestEngineConfig(bypass_logging=True, run_analysis=False)
    )
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
        fee_model=PolymarketNoRewardFeeModel(),
        fill_model=ThroughPriceFillModel(),
        latency_model=StaticLatencyModel(
            insert_latency_nanos=100_000_000, cancel_latency_nanos=100_000_000
        ),
    )
    strategy = DepthMakerPaperStrategy(
        [i.id for i in items], MakerRules(hedge_mode=hedge_mode), reward()
    )
    for i in items:
        engine.add_instrument(i)
    engine.add_strategy(strategy)
    data = []
    times = (
        0,
        10,
        500,
        510,
        1200,
        1210,
        1500,
        1510,
        1800,
        1810,
        2500,
        2510,
        3000,
        3010,
        5000,
        5010,
    )
    if fill_during_cancel:
        reprice = True
        times = tuple(sorted(times + (2550, 2600, 2610, 2800, 2810)))
    if partial_fill:
        times = tuple(sorted(times + (7000, 7010)))
    for ms in times:
        idx = 0 if ms % 100 == 0 else 1
        b = book(ts=NOW + ms * 10**6)
        if not reprice and idx == 0 and ms >= 1200:
            b = book(
                bid="0.44" if touch else "0.43",
                ask="0.45" if touch else "0.44",
                ts=b.received_ns,
            )
        if gap_loss and idx == 1 and ms >= 1200:
            b = book(bid="0.69", ask="0.71", ts=b.received_ns)
        if reprice and ms >= 2500:
            b = book(bid="0.51", ask="0.53", ts=b.received_ns)
        if fill_during_cancel and idx == 1 and ms >= 2550:
            b = book(bid="0.43", ask="0.44", ts=b.received_ns)
        if partial_fill and idx == 0 and ms >= 1200:
            b = replace(
                b,
                asks=(
                    (D("0.44"), D(3)),
                    (D("0.46"), D(3000)),
                    (D("0.47"), D(3000)),
                    (D("0.48"), D(3000)),
                    (D("0.49"), D(3000)),
                ),
            )
        data.append(
            book_snapshot(
                items[idx],
                [(str(p), str(q)) for p, q in b.bids],
                [(str(p), str(q)) for p, q in b.asks],
                ts_event=b.source_ns,
                ts_init=b.received_ns,
            )
        )
    try:
        engine.add_data(data)
        engine.run()
        result = summarize(strategy, engine.cache, currency=items[0].currency)
        result.update(
            quote_cycles=strategy.quote_cycles, total_orders=strategy.total_orders
        )
        return result, strategy.records
    finally:
        engine.dispose()


class NativeDepthMaker(unittest.TestCase):
    def test_strategy_callback_exception_halts_and_cancels_instead_of_only_logging(
        self,
    ):
        from types import SimpleNamespace as NS
        from prediction_market_strategies.strategies.depth_maker_nautilus import (
            DepthMakerPaperStrategy,
        )

        class Broken(DepthMakerPaperStrategy):
            def _tick(self):
                raise RuntimeError("synthetic callback failure")

            def cancel_all_orders(self, ident):
                self.cancelled.append(ident)

        strategy = Broken([i.id for i in instruments()], MakerRules(), reward())
        strategy._clock = NS(timestamp_ns=lambda: NOW)
        strategy.cancelled = []
        strategy.guarded_tick()
        self.assertTrue(strategy.halted)
        self.assertEqual(strategy.cancelled, list(strategy.ids))
        self.assertTrue(
            any(r["kind"] == "strategy_exception" for r in strategy.records)
        )

    def test_successful_opposite_hedge_holds_a_pair_without_claiming_cash_profit(self):
        result, events = native_replay(hedge_mode="BUY_OPPOSITE")
        self.assertFalse(result["halted"])
        self.assertEqual(
            {D(v) for v in result["simulated_net_inventory"].values()}, {D(10)}
        )
        self.assertEqual(D(result["residual_quantity"]), 0)
        self.assertLess(D(result["cash_total"]), 1000)
        self.assertFalse(result["unresolved_orders"])

    def test_fill_during_cancel_is_hedged_once_not_replaced_into_more_exposure(self):
        result, events = native_replay(fill_during_cancel=True)
        self.assertEqual(D(result["residual_quantity"]), 0)
        self.assertEqual(len([r for r in events if r["kind"] == "maker_hedge"]), 1)
        first_fill = min(r["now_ns"] for r in events if r["kind"] == "fill")
        self.assertTrue(
            all(r["now_ns"] < first_fill for r in events if r["kind"] == "maker_quote")
        )
        self.assertFalse(result["unresolved_orders"])

    def test_tiny_partial_fill_is_retained_as_blocked_not_rounded_up_into_a_hedge(self):
        result, events = native_replay(partial_fill=True)
        self.assertTrue(result["halted"])
        self.assertEqual(D(result["residual_quantity"]), 3)
        self.assertFalse(any(r["kind"] == "maker_hedge" for r in events))
        self.assertFalse(result["unresolved_orders"])

    def test_missing_cancel_ack_blocks_replacement(self):
        from types import SimpleNamespace as NS
        from prediction_market_strategies.strategies.depth_maker_nautilus import (
            DepthMakerPaperStrategy,
        )

        strategy = DepthMakerPaperStrategy(
            [i.id for i in instruments()], MakerRules(), reward()
        )
        strategy._clock = NS(timestamp_ns=lambda: NOW + 4 * 10**9)
        strategy._cache = NS(order=lambda ident: NS(is_closed=False))
        strategy.makers["stuck"] = {}
        strategy.cancel_requested["stuck"] = NOW
        strategy._tick()
        self.assertTrue(strategy.halted)
        self.assertTrue(
            any(r.get("reason") == "maker_cancel_ack_timeout" for r in strategy.records)
        )
        self.assertFalse(any(r["kind"] == "maker_quote" for r in strategy.records))

    def test_passive_fill_then_flatten_and_cooldown_without_reward_credit(self):
        result, events = native_replay()
        self.assertTrue(result["entered"])
        self.assertTrue(any(r["kind"] == "maker_hedge" for r in events))
        self.assertEqual(D(result["residual_quantity"]), 0)
        self.assertEqual(
            {D(v) for v in result["simulated_net_inventory"].values()}, {D(0)}
        )
        fills = [r for r in events if r["kind"] == "fill"]
        self.assertEqual(len(fills), 2)
        self.assertEqual(D(fills[0]["commission"]), 0)
        self.assertGreater(D(fills[1]["commission"]), 0)
        self.assertLess(D(result["cash_total"]), 1000)
        self.assertFalse(result["unresolved_orders"])
        self.assertEqual(result["quote_cycles"], 1)

    def test_a_mere_quote_touch_is_not_a_guaranteed_passive_fill(self):
        result, events = native_replay(touch=True)
        self.assertFalse(any(r["kind"] == "fill" for r in events))
        self.assertEqual(D(result["cash_total"]), 1000)

    def test_replacement_follows_actual_cancel_acknowledgements(self):
        result, events = native_replay(reprice=True)
        quotes = [r for r in events if r["kind"] == "maker_quote"]
        self.assertGreaterEqual(len(quotes), 4)
        first = {r["client_order_id"] for r in quotes[:2]}
        closes = [
            r
            for r in events
            if r["kind"] == "order_event"
            and r["event_type"] == "OrderCanceled"
            and r["client_order_id"] in first
        ]
        self.assertEqual(len(closes), 2)
        self.assertGreaterEqual(quotes[2]["now_ns"], max(r["now_ns"] for r in closes))
        self.assertFalse(result["unresolved_orders"])

    def test_opposite_token_hedge_rejects_unbounded_locked_loss(self):
        result, events = native_replay(hedge_mode="BUY_OPPOSITE", gap_loss=True)
        self.assertTrue(result["halted"])
        self.assertTrue(
            any(
                r["kind"] == "protection_halt"
                and r["reason"] == "opposite_hedge_cost_limit"
                for r in events
            )
        )
        self.assertGreater(D(result["residual_quantity"]), 0)


class PublicPreflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_transient_tls_failure_retries_without_mutating_strategy_state(self):
        from prediction_market_strategies.paper.polymarket_pair import public_preflight

        attempts = []

        async def fake():
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("tls handshake eof")
            return "synthetic-receipt"

        receipts = []
        result = await public_preflight(fake, stage="synthetic", receipts=receipts)
        self.assertEqual(result, "synthetic-receipt")
        self.assertEqual(len(receipts), 2)
        self.assertEqual(receipts[0]["status"], "failed")

    async def test_permanent_metadata_failure_is_not_retried(self):
        from prediction_market_strategies.paper.polymarket_pair import public_preflight

        async def fake():
            raise RuntimeError("404 market not found")

        receipts = []
        with self.assertRaises(RuntimeError):
            await public_preflight(fake, stage="synthetic", receipts=receipts)
        self.assertEqual(len(receipts), 1)


if __name__ == "__main__":
    unittest.main()
