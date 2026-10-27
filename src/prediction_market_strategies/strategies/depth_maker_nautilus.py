# SPDX-License-Identifier: LGPL-3.0-or-later
"""Depth-maker policy adapter sharing the bounded native paper lifecycle."""

from nautilus_trader.model import OrderSide, Price, Quantity, TimeInForce

from .binary_market import BinaryMarketPaperStrategy, walk
from .depth_maker import (
    D,
    MODES,
    FillProtection,
    MakerRules,
    plan_quotes,
    validate_quote,
    snap,
)


class DepthMakerPaperStrategy(BinaryMarketPaperStrategy):
    def __init__(self, instrument_ids, rules=None, rewards=None):
        super().__init__(instrument_ids, rules or MakerRules(), "DEPTH-MAKER-001")
        self.rewards = rewards
        self.protection = FillProtection(MODES[self.rules.mode])
        self.makers, self.hedges, self.cancel_requested = {}, {}, {}
        self.quote_cycles, self.total_orders, self.hedge_attempts = 0, 0, 0
        self.last_submit_ns = None
        self.unhedged_since_ns = None
        self.initial_cash = None

    def on_ready(self):
        self.initial_cash = self._cash()
        if self.rewards is not None:
            info = self.instruments[self.ids[0]].info
            if (
                self.rewards.venue != "polymarket"
                or self.rewards.market_id != str(info.get("market_id"))
                or self.rewards.condition_id != info.get("condition_id")
            ):
                self._block("reward_identity_mismatch")

    def _block(self, reason):
        self.halted = True
        self.record("protection_halt", reason=reason)
        self._cancel_makers(reason)

    def _active(self, collection):
        from nautilus_trader.model import ClientOrderId

        result = {}
        for ident, spec in collection.items():
            order = self._cache.order(ClientOrderId(ident))
            if order is not None and order.is_closed:
                self.terminal.add(ident)
            if ident not in self.terminal:
                result[ident] = spec
        return result

    def _cancel_makers(self, reason):
        from nautilus_trader.model import ClientOrderId

        for ident in self._active(self.makers):
            if ident in self.cancel_requested:
                continue
            order = self._cache.order(ClientOrderId(ident))
            if order is not None and not order.is_closed:
                self.cancel_requested[ident] = self._clock.timestamp_ns()
                self.record("maker_cancel", client_order_id=ident, reason=reason)
                self.cancel_order(order.client_order_id)

    def _observe(self, reason):
        self.counts[reason] += 1
        now = self._clock.timestamp_ns()
        if now - self.last_sample_ns >= 1_000_000_000:
            self.samples.append(
                dict(
                    now_ns=now,
                    reason=reason,
                    quote_cycles=self.quote_cycles,
                    active_makers=len(self._active(self.makers)),
                    active_hedges=len(self._active(self.hedges)),
                    inventory={k: str(v) for k, v in self.inventory().items()},
                    books={
                        str(i): dict(
                            received_ns=b.received_ns,
                            source_ns=b.source_ns,
                            bid=str(max((p for p, _ in b.bids), default=0)),
                            ask=str(min((p for p, _ in b.asks), default=1)),
                        )
                        for i, b in self.views.items()
                    },
                )
            )
            self.last_sample_ns = now

    def _mark_loss(self, inventory):
        account = self._cache.account_for_venue(self.ids[0].venue)
        total = account.balance_total(self.currency) if account else None
        if total is None or self.initial_cash is None:
            return False
        equity = total.as_decimal()
        for ident, quantity in zip(self.ids, inventory):
            book = self.views.get(ident)
            if quantity and (book is None or not book.bids):
                return False
            if quantity:
                equity += quantity * max(p for p, _ in book.bids)
        if self.initial_cash - equity > self.rules.max_run_loss:
            self._block("executable_bid_mark_loss_limit")
            return False
        return True

    def _tick(self):
        if self.halted:
            self._cancel_makers("halted")
            return
        now = self._clock.timestamp_ns()
        makers, hedges = self._active(self.makers), self._active(self.hedges)
        for ident, sent in self.cancel_requested.items():
            if ident in makers and now - sent > self.rules.order_timeout_ns:
                self._block("maker_cancel_ack_timeout")
                return
        if hedges:
            self._cancel_makers("hedge_pending")
            if any(
                now - spec["sent_ns"] > self.rules.order_timeout_ns
                for spec in hedges.values()
            ):
                self._block("hedge_order_timeout")
            self._observe("hedge_pending")
            return
        inventory = [self.inventory()[str(i)] for i in self.ids]
        if any(q < 0 for q in inventory):
            self._block("short_inventory_forbidden")
            return
        if inventory[0] != inventory[1]:
            self._cancel_makers("unbalanced_inventory")
            if self.unhedged_since_ns is None:
                self.unhedged_since_ns = now
            if now - self.unhedged_since_ns > self.rules.max_unhedged_ns:
                self._block("unhedged_inventory_deadline")
                return
            if makers:
                self._observe("cancel_before_hedge")
                return
            self._hedge(inventory)
            return
        self.unhedged_since_ns, self.hedge_attempts = None, 0
        protection = self.protection.blocked(now)
        if protection:
            self._cancel_makers(protection)
            self._observe(protection)
            return
        if (
            self.quote_cycles >= self.rules.max_quote_cycles
            or self.total_orders >= self.rules.max_orders
        ):
            self._cancel_makers("finite_quote_budget")
            self._observe("finite_quote_budget")
            return
        if not self._mark_loss(inventory):
            self._cancel_makers("account_unavailable_or_loss_limit")
            return
        # Include only our currently reserved buy cash in a *replacement plan*;
        # no new order is sent until the old IDs have terminal acknowledgements.
        planning_cash = self._cash()
        if planning_cash is not None:
            planning_cash += sum(
                (
                    spec["price"] * spec["quantity"]
                    for spec in makers.values()
                    if spec["side"] == "BUY"
                ),
                D(0),
            )
        plan = plan_quotes(
            [self.views.get(i) for i in self.ids],
            inventory,
            self.rules,
            self.rewards,
            ticks=[self.instruments[i].price_increment.as_decimal() for i in self.ids],
            steps=[self.instruments[i].size_increment.as_decimal() for i in self.ids],
            minimums=[f.min_quantity for f in self.fees],
            cash=planning_cash,
            now_ns=now,
            expiry_ns=min(self.instruments[i].expiration_ns for i in self.ids),
        )
        self._observe(plan.reason)
        if not plan.quotes:
            self._cancel_makers(plan.reason)
            return
        if makers:
            unsafe = None
            for spec in makers.values():
                i = spec["index"]
                reason, front = validate_quote(
                    self.views[self.ids[i]],
                    spec["price"],
                    spec["side"],
                    tick=self.instruments[self.ids[i]].price_increment.as_decimal(),
                    reward=self.rewards,
                    mode=MODES[self.rules.mode],
                )
                if reason != "valid" or front < spec["front"] * (
                    1 - MODES[self.rules.mode].depth_drop_fraction
                ):
                    unsafe = "protection_depth_or_distance_changed"
                    break
            if unsafe:
                self._cancel_makers(unsafe)
                return
            if any(ident in self.cancel_requested for ident in makers):
                return
            desired = {(q.index, q.side, q.price, q.quantity) for q in plan.quotes}
            existing = {
                (s["index"], s["side"], s["price"], s["quantity"])
                for s in makers.values()
            }
            if (
                desired != existing
                and now - self.last_submit_ns >= self.rules.quote_interval_ns
            ):
                self._cancel_makers("target_quote_changed")
            return
        if (
            self.last_submit_ns is not None
            and now - self.last_submit_ns < self.rules.quote_interval_ns
        ):
            return
        if (
            self.total_orders + len(plan.quotes) + self.rules.max_hedge_attempts
            > self.rules.max_orders
        ):
            self._observe("finite_order_budget")
            return
        orders = []
        for quote in plan.quotes:
            ident = self.ids[quote.index]
            order = self.order_factory.limit(
                instrument_id=ident,
                order_side=OrderSide.BUY if quote.side == "BUY" else OrderSide.SELL,
                quantity=Quantity.from_str(format(quote.quantity, "f")),
                price=Price.from_str(
                    format(quote.price, f".{self.instruments[ident].price_precision}f")
                ),
                time_in_force=TimeInForce.GTC,
                post_only=True,
                tags=["depth_tier_maker"],
            )
            key = str(order.client_order_id)
            self.makers[key] = dict(
                index=quote.index,
                side=quote.side,
                price=quote.price,
                quantity=quote.quantity,
                front=quote.front,
                sent_ns=now,
            )
            self.record(
                "maker_quote",
                client_order_id=key,
                instrument=str(ident),
                side=quote.side,
                price=str(quote.price),
                quantity=str(quote.quantity),
                front_shares=str(quote.front),
            )
            orders.append(order)
        self.quote_cycles += 1
        self.total_orders += len(orders)
        self.last_submit_ns, self.entered = now, True
        for order in orders:
            self.submit_order(order)

    def _hedge(self, inventory):
        if (
            self.hedge_attempts >= self.rules.max_hedge_attempts
            or self.total_orders >= self.rules.max_orders
        ):
            self._block("finite_hedge_budget")
            return
        heavy = 0 if inventory[0] > inventory[1] else 1
        index = heavy if self.rules.hedge_mode == "FLATTEN" else 1 - heavy
        side = "SELL" if self.rules.hedge_mode == "FLATTEN" else "BUY"
        ident = self.ids[index]
        quantity = abs(inventory[0] - inventory[1])
        book = self.views.get(ident)
        now = self._clock.timestamp_ns()
        if (
            book is None
            or now < book.received_ns
            or now - book.received_ns > self.rules.max_age_ns
        ):
            self._observe("hedge_waiting_for_fresh_native_book")
            return
        if quantity < self.fees[index].min_quantity:
            self._observe("hedge_residual_below_minimum")
            return
        tick = self.instruments[ident].price_increment.as_decimal()
        if side == "SELL":
            if not book.bids:
                self._observe("hedge_empty_native_bid")
                return
            limit = snap(
                max(p for p, _ in book.bids) * (1 - self.rules.hedge_slippage),
                tick,
                "SELL",
            )
            available = sum((q for p, q in book.bids if p >= limit), D(0))
            if available < quantity:
                self._observe("hedge_insufficient_native_bid_depth")
                return
        else:
            if not book.asks:
                self._observe("hedge_empty_native_ask")
                return
            limit = snap(
                min(p for p, _ in book.asks) * (1 + self.rules.hedge_slippage),
                tick,
                "BUY",
            )
            values = walk(tuple((p, q) for p, q in book.asks if p <= limit), quantity)
            if values is None:
                self._observe("hedge_insufficient_native_ask_depth")
                return
            hedge_fee = self.fees[index].cost(
                min(limit, D("0.5")), quantity, self.currency.precision
            )
            buys = [
                r
                for r in self.records
                if r["kind"] == "fill"
                and r["side"] == "BUY"
                and r["instrument"] == str(self.ids[heavy])
            ]
            if not buys:
                self._block("opposite_hedge_unknown_entry_cost")
                return
            # A conservative per-share upper bound prevents older cheap fills
            # from hiding a newly expensive unpaired lot in an average.
            entry_average = max(
                D(r["price"]) + D(r["commission"] or "0") / D(r["quantity"])
                for r in buys
            )
            # A complement locks a payout vector, not a profit. Reject excessive
            # locked loss instead of buying any available price after a fill.
            if quantity * (entry_average + limit) + hedge_fee > quantity * (
                1 + self.rules.max_pair_lock_loss
            ):
                self._block("opposite_hedge_cost_limit")
                return
            if self._cash() is None or self._cash() < quantity * limit + hedge_fee:
                self._block("opposite_hedge_cash_limit")
                return
        if not 0 < limit < 1:
            self._block("hedge_price_out_of_bounds")
            return
        order = self.order_factory.limit(
            instrument_id=ident,
            order_side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
            quantity=Quantity.from_str(format(quantity, "f")),
            price=Price.from_str(
                format(limit, f".{self.instruments[ident].price_precision}f")
            ),
            time_in_force=TimeInForce.IOC,
            tags=["depth_maker_fill_hedge"],
        )
        self.hedges[str(order.client_order_id)] = dict(sent_ns=now)
        self.hedge_attempts += 1
        self.total_orders += 1
        self.record(
            "maker_hedge",
            client_order_id=str(order.client_order_id),
            mode=self.rules.hedge_mode,
            instrument=str(ident),
            side=side,
            quantity=str(quantity),
            price=str(limit),
        )
        self.submit_order(order)

    def on_order_filled(self, event):
        count = len(self.seen_fills)
        super().on_order_filled(event)
        if len(self.seen_fills) != count and str(event.client_order_id) in self.makers:
            self.protection.filled(
                str(event.client_order_id), self._clock.timestamp_ns()
            )
            self.record(
                "maker_fill_protection",
                cooldown_until_ns=self.protection.cooldown_until_ns,
                blacklist_until_ns=self.protection.blacklist_until_ns,
            )

    def on_socket_state(self, event):
        super().on_socket_state(event)
        self._cancel_makers("socket_transition")
