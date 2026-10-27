# SPDX-License-Identifier: LGPL-3.0-or-later
# Reference: evan-kolberg/prediction-market-backtesting, commit c76e77af00ef53472a9da8f66dae7fdd2d3e5928.
# Original copyright (C) 2015-2026 Nautech Systems Pty Ltd. See THIRD_PARTY_NOTICES.md.
"""One complementary Polymarket binary pair, for isolated simulated execution.

The pure decision code uses Decimal throughout. The Nautilus strategy deliberately
uses price-capped IOC limits rather than the reference's unbounded market orders.
No discovery, network client, wallet or real execution factory lives here.
"""

from .binary_market import BinaryMarketPaperStrategy, walk
from .binary_market import BookView as BookView, FeeTerms as FeeTerms
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from nautilus_trader.model import OrderSide, Price, Quantity, TimeInForce
from ..adapters.common import decimal as D


@dataclass(frozen=True)
class PairRules:
    quantity: Decimal = Decimal("10")
    min_net_edge: Decimal = Decimal("0.02")
    max_pair_notional: Decimal = Decimal("20")
    max_spread: Decimal = Decimal("0.08")
    max_age_ns: int = 2000000000
    max_skew_ns: int = 500000000
    decision_delay_ns: int = 250000000
    order_timeout_ns: int = 3000000000

    def __post_init__(self):
        for name in ("quantity", "min_net_edge", "max_pair_notional", "max_spread"):
            v = D(getattr(self, name))
            if not isinstance(getattr(self, name), Decimal) or v <= 0:
                raise ValueError("positive Decimal required: " + name)
        if self.min_net_edge >= 1 or self.max_spread >= 1:
            raise ValueError("probability bounds")
        for name in (
            "max_age_ns",
            "max_skew_ns",
            "decision_delay_ns",
            "order_timeout_ns",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError("positive clock bound required: " + name)


@dataclass(frozen=True)
class Entry:
    quantity: Decimal
    limits: tuple
    cost: Decimal
    fee_ceiling: Decimal
    net_edge: Decimal
    receipt_ns: tuple


def evaluate(books, fees, rules, *, now_ns, step, cash, currency_precision):
    if len(books) != 2 or any((b is None or not b.ready for b in books)):
        return (None, "waiting_for_two_snapshots")
    for b in books:
        if b.received_ns > now_ns or now_ns - b.received_ns > rules.max_age_ns:
            return (None, "stale_book")
        if not b.bids or not b.asks:
            return (None, "empty_side")
        bid, ask = (max((p for p, _ in b.bids)), min((p for p, _ in b.asks)))
        if bid >= ask or ask - bid > rules.max_spread:
            return (None, "crossed_or_wide")
    if abs(books[0].received_ns - books[1].received_ns) > rules.max_skew_ns:
        return (None, "receipt_skew")
    q = (rules.quantity / step).to_integral_value(rounding=ROUND_DOWN) * step
    if q < max((f.min_quantity for f in fees)):
        return (None, "below_minimum")
    legs = [walk(b.asks, q) for b in books]
    if any((leg is None for leg in legs)):
        return (None, "insufficient_depth")
    cost = sum((v[0] for v in legs), Decimal(0))
    fee = sum(
        (
            f.cost(p, size, currency_precision)
            for f, leg in zip(fees, legs)
            for p, size in leg[2]
        ),
        Decimal(0),
    )
    worst_fee = sum(
        (
            f.cost(min(leg[1], Decimal("0.5")), q, currency_precision)
            for f, leg in zip(fees, legs)
        ),
        Decimal(0),
    )
    reservation = q * sum((v[1] for v in legs), Decimal(0)) + worst_fee
    if cash is None or reservation > cash * Decimal("0.9"):
        return (None, "insufficient_cash")
    if reservation > rules.max_pair_notional:
        return (None, "notional_cap")
    edge = 1 - (cost + fee) / q
    worst_edge = 1 - reservation / q
    if min(edge, worst_edge) < rules.min_net_edge:
        return (None, "no_net_edge")
    return (
        Entry(
            q,
            tuple((v[1] for v in legs)),
            cost,
            fee,
            edge,
            tuple((b.received_ns for b in books)),
        ),
        "eligible",
    )


class BinaryPairPaperStrategy(BinaryMarketPaperStrategy):
    """Bounded one-entry strategy; pending IDs are terminal sets, not counters."""

    def __init__(self, instrument_ids, rules=None):
        super().__init__(instrument_ids, rules or PairRules(), "BINARY-PAIR-001")
        self.entry_ids, self.unwind_ids = (set(), set())
        self.entry_sent_ns = None
        self.unwind_attempts = 0
        self.orders_to_cancel = set()

    def _decision(self):
        return evaluate(
            [self.views.get(i) for i in self.ids],
            self.fees,
            self.rules,
            now_ns=self._clock.timestamp_ns(),
            step=self.step,
            cash=self._cash(),
            currency_precision=self.currency.precision,
        )

    def _tick(self):
        if self.halted:
            return
        if self.entered:
            self._recover()
            return
        entry, reason = self._decision()
        self.counts[reason] += 1
        now = self._clock.timestamp_ns()
        if now - self.last_sample_ns >= 1000000000:
            self.samples.append(
                dict(
                    now_ns=now,
                    reason=reason,
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
        if entry is None:
            self.candidate_ns = None
            return
        if self.candidate_ns is None:
            self.candidate_ns = now
            self.record("candidate", edge=str(entry.net_edge))
            return
        if now - self.candidate_ns < self.rules.decision_delay_ns:
            return
        orders = [
            self.order_factory.limit(
                instrument_id=i,
                order_side=OrderSide.BUY,
                quantity=Quantity.from_str(format(entry.quantity, "f")),
                price=Price.from_str(
                    format(entry.limits[k], f".{self.instruments[i].price_precision}f")
                ),
                time_in_force=TimeInForce.IOC,
                tags=["paper_pair_entry"],
            )
            for k, i in enumerate(self.ids)
        ]
        self.entered = True
        self.entry_sent_ns = now
        self.entry_ids = {str(o.client_order_id) for o in orders}
        self.record(
            "entry",
            quantity=str(entry.quantity),
            limits=[str(p) for p in entry.limits],
            estimated_cost=str(entry.cost),
            estimated_fee_ceiling=str(entry.fee_ceiling),
            net_edge=str(entry.net_edge),
            receipt_ns=list(entry.receipt_ns),
        )
        for order in orders:
            self.submit_order(order)

    def _recover(self):
        pending = (self.entry_ids | self.unwind_ids) - self.terminal
        if pending:
            if (
                self._clock.timestamp_ns() - self.entry_sent_ns
                > self.rules.order_timeout_ns
            ):
                for ident in pending - self.orders_to_cancel:
                    from nautilus_trader.model import ClientOrderId

                    order = self._cache.order(ClientOrderId(ident))
                    if order is not None and (not order.is_closed):
                        self.cancel_order(order.client_order_id)
                    self.orders_to_cancel.add(ident)
                self.record("pending_timeout", ids=sorted(pending))
                self.halted = True
            return
        inventory = {str(i): Decimal(0) for i in self.ids}
        for r in self.records:
            if r["kind"] == "fill":
                inventory[r["instrument"]] += D(r["quantity"]) * (
                    1 if r["side"] == "BUY" else -1
                )
        a, b = [inventory[str(i)] for i in self.ids]
        if a == b:
            return
        if self.unwind_attempts >= 2:
            self.halted = True
            self.record(
                "residual_blocked", inventory={k: str(v) for k, v in inventory.items()}
            )
            return
        index = 0 if a > b else 1
        ident, amount = (self.ids[index], abs(a - b))
        book = self.views.get(ident)
        now = self._clock.timestamp_ns()
        if (
            book is None
            or not book.bids
            or now < book.received_ns
            or (now - book.received_ns > self.rules.max_age_ns)
        ):
            self.counts["unwind_waiting_for_fresh_book"] += 1
            return
        if amount < self.fees[index].min_quantity:
            self.halted = True
            self.record("residual_below_venue_minimum", quantity=str(amount))
            return
        price = max((p for p, _ in book.bids))
        order = self.order_factory.limit(
            instrument_id=ident,
            order_side=OrderSide.SELL,
            quantity=Quantity.from_str(format(amount, "f")),
            price=Price.from_str(
                format(price, f".{self.instruments[ident].price_precision}f")
            ),
            time_in_force=TimeInForce.IOC,
            tags=["paper_pair_residual_exit"],
        )
        self.unwind_ids.add(str(order.client_order_id))
        self.unwind_attempts += 1
        self.entry_sent_ns = now
        self.record(
            "unwind", instrument=str(ident), quantity=str(amount), price=str(price)
        )
        self.submit_order(order)
