# SPDX-License-Identifier: LGPL-3.0-or-later
# Strategy concept adapted from evan-kolberg/prediction-market-backtesting,
# strategies/binary_pair_arbitrage.py, commit c76e77af00ef53472a9da8f66dae7fdd2d3e5928.
# Original copyright (C) 2015-2026 Nautech Systems Pty Ltd. See THIRD_PARTY_NOTICES.md.
"""One complementary Polymarket binary pair, for isolated simulated execution.

The pure decision code uses Decimal throughout. The Nautilus strategy deliberately
uses price-capped IOC limits rather than the reference's unbounded market orders.
No discovery, network client, wallet or real execution factory lives here.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from math import lcm

from nautilus_trader.model import (
    BookType,
    ClientId,
    OrderBook,
    OrderSide,
    Price,
    Quantity,
    RecordFlag,
    StrategyId,
    TimeInForce,
)
from nautilus_trader.trading import Strategy, StrategyConfig

from ..adapters.common import decimal as D


@dataclass(frozen=True)
class PairRules:
    quantity: Decimal = Decimal("10")
    min_net_edge: Decimal = Decimal("0.02")
    max_pair_notional: Decimal = Decimal("20")
    max_spread: Decimal = Decimal("0.08")
    max_age_ns: int = 2_000_000_000
    max_skew_ns: int = 500_000_000
    decision_delay_ns: int = 250_000_000
    order_timeout_ns: int = 3_000_000_000

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
class FeeTerms:
    rate: Decimal
    exponent: int
    min_quantity: Decimal

    @classmethod
    def from_instrument(cls, instrument):
        info = instrument.info or {}
        fee = info.get("fee_schedule")
        if not isinstance(fee, dict) or "rate" not in fee or "exponent" not in fee:
            raise ValueError("missing observed fee schedule")
        rate, exponent = D(fee["rate"]), D(fee["exponent"])
        minimum = D(info.get("min_order_size", "0"))
        if rate < 0 or rate > 1 or exponent != 1 or minimum <= 0:
            raise ValueError("unsupported fee schedule or missing venue minimum")
        if fee.get("takerOnly") is not True:
            raise ValueError("unverified fee role")
        return cls(rate, int(exponent), minimum)

    def cost(self, price, quantity, currency_precision):
        # Conservative signal ceiling. Simulator's native venue rounding is
        # recorded separately and must never be substituted for an actual fee.
        value = quantity * self.rate * (price * (1 - price)) ** self.exponent
        return value.quantize(
            Decimal(1).scaleb(-currency_precision), rounding=ROUND_CEILING
        )


@dataclass(frozen=True)
class BookView:
    bids: tuple
    asks: tuple
    received_ns: int
    source_ns: int
    ready: bool = True


@dataclass(frozen=True)
class Entry:
    quantity: Decimal
    limits: tuple
    cost: Decimal
    fee_ceiling: Decimal
    net_edge: Decimal
    receipt_ns: tuple


def common_step(instruments):
    steps = [i.size_increment.as_decimal() for i in instruments]
    places = max(-s.as_tuple().exponent for s in steps)
    scale = 10**places
    return Decimal(lcm(*(int(s * scale) for s in steps))) / scale


def walk(asks, quantity):
    remaining, cost, fills = quantity, Decimal(0), []
    for price, available in sorted(asks):
        price, available = D(price), D(available)
        if not 0 < price < 1 or available <= 0:
            raise ValueError("invalid depth")
        q = min(remaining, available)
        if q:
            fills.append((price, q))
            cost += price * q
            remaining -= q
        if not remaining:
            return cost, fills[-1][0], fills
    return None


def evaluate(books, fees, rules, *, now_ns, step, cash, currency_precision):
    if len(books) != 2 or any(b is None or not b.ready for b in books):
        return None, "waiting_for_two_snapshots"
    for b in books:
        # Event timestamp is the last *change*, not a heartbeat. Freshness is
        # bounded by our local receipt, never by a timer relabeling old data.
        if b.received_ns > now_ns or now_ns - b.received_ns > rules.max_age_ns:
            return None, "stale_book"
        if not b.bids or not b.asks:
            return None, "empty_side"
        bid, ask = max(p for p, _ in b.bids), min(p for p, _ in b.asks)
        if bid >= ask or ask - bid > rules.max_spread:
            return None, "crossed_or_wide"
    if abs(books[0].received_ns - books[1].received_ns) > rules.max_skew_ns:
        return None, "receipt_skew"
    q = (rules.quantity / step).to_integral_value(rounding=ROUND_DOWN) * step
    if q < max(f.min_quantity for f in fees):
        return None, "below_minimum"
    legs = [walk(b.asks, q) for b in books]
    if any(leg is None for leg in legs):
        return None, "insufficient_depth"
    cost = sum((v[0] for v in legs), Decimal(0))
    fee = sum(
        (
            f.cost(p, size, currency_precision)
            for f, leg in zip(fees, legs)
            for p, size in leg[2]
        ),
        Decimal(0),
    )
    # Cap by worst permitted IOC prices, including conservative fee ceiling.
    worst_fee = sum(
        (
            f.cost(min(leg[1], Decimal("0.5")), q, currency_precision)
            for f, leg in zip(fees, legs)
        ),
        Decimal(0),
    )
    reservation = q * sum((v[1] for v in legs), Decimal(0)) + worst_fee
    if cash is None or reservation > cash * Decimal("0.9"):
        return None, "insufficient_cash"
    if reservation > rules.max_pair_notional:
        return None, "notional_cap"
    edge = 1 - (cost + fee) / q
    worst_edge = 1 - reservation / q
    if min(edge, worst_edge) < rules.min_net_edge:
        return None, "no_net_edge"
    return Entry(
        q,
        tuple(v[1] for v in legs),
        cost,
        fee,
        edge,
        tuple(b.received_ns for b in books),
    ), "eligible"


class BinaryPairPaperStrategy(Strategy):
    """Bounded one-entry strategy; pending IDs are terminal sets, not counters."""

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def __init__(self, instrument_ids, rules=None):
        super().__init__(
            StrategyConfig(
                strategy_id=StrategyId("BINARY-PAIR-001"),
                log_events=False,
                log_commands=False,
            )
        )
        self.ids = tuple(instrument_ids)
        if len(self.ids) != 2 or len(set(self.ids)) != 2:
            raise ValueError("two distinct outcomes required")
        self.rules = rules or PairRules()
        self.native_books, self.views, self.instruments = {}, {}, {}
        self.counts, self.records, self.samples = Counter(), [], []
        self.entry_ids, self.unwind_ids, self.terminal = set(), set(), set()
        self.entry_sent_ns = None
        self.candidate_ns = None
        self.entered = False
        self.halted = False
        self.unwind_attempts = 0
        self.orders_to_cancel = set()
        self.raw_batches = []
        self.last_sample_ns = 0
        self.seen_fills = set()

    def record(self, kind, **values):
        self.records.append(
            dict(kind=kind, now_ns=self._clock.timestamp_ns(), **values)
        )

    def on_start(self):
        # Python callbacks can be re-entered synchronously by submit_order.
        # Capture handles once; accessing the Strategy getter during that
        # mutable native borrow raises rather than reporting the order event.
        self._clock, self._cache = self.clock, self.cache
        try:
            self.instruments = {i: self._cache.instrument(i) for i in self.ids}
            if any(v is None for v in self.instruments.values()):
                raise ValueError("instrument not loaded")
            instruments = list(self.instruments.values())
            infos = [i.info or {} for i in instruments]
            if (
                len({str(i.id.venue) for i in instruments}) != 1
                or str(instruments[0].id.venue) != "POLYMARKET"
                or len({i.get("condition_id") for i in infos}) != 1
                or not infos[0].get("condition_id")
                or {i.outcome.lower() for i in instruments} != {"yes", "no"}
                or len({str(i.currency) for i in instruments}) != 1
                or any(i.get("market_version") != "v1" for i in infos)
            ):
                raise ValueError("same v1 binary condition and collateral required")
            self.fees = [FeeTerms.from_instrument(i) for i in instruments]
            self.step = common_step(instruments)
            self.currency = instruments[0].currency
            self.subscribe_socket_state(client_id=ClientId("POLYMARKET"))
            for ident in self.ids:
                self.native_books[ident] = OrderBook(ident, BookType.L2_MBP)
                self.subscribe_instrument(ident)
                self.subscribe_book_deltas(ident, BookType.L2_MBP, managed=True)
            self._clock.set_timer(
                "pair-watchdog",
                timedelta(milliseconds=100),
                callback=self.on_time_event,
            )
            self.record(
                "started",
                fees=[dict(rate=str(f.rate), exponent=f.exponent) for f in self.fees],
            )
        except Exception as e:
            self.halted = True
            self.record("initialization_failed", error=str(e))
            self.stop()

    def on_socket_state(self, event):
        state = str(event.state)
        self.record("socket_state", state=state)
        # Reset on *every* socket transition, including reconnect. Only fresh
        # complete snapshots can re-arm; incremental changes cannot do so.
        self.views.clear()
        self.candidate_ns = None
        for book in self.native_books.values():
            book.reset()

    def on_book_deltas(self, deltas):
        ident = deltas.instrument_id
        if ident not in self.native_books:
            return
        self.counts[str(ident)] += 1
        if len(self.raw_batches) >= 100_000:
            self.halted = True
            self.record("input_cap")
            return
        self.raw_batches.append(deltas)
        snapshot = bool(
            deltas.deltas and int(deltas.deltas[-1].flags) & int(RecordFlag.F_SNAPSHOT)
        )
        if ident not in self.views and not snapshot:
            self.counts["delta_before_snapshot"] += 1
            return
        book = self.native_books[ident]
        book.apply_deltas(deltas)
        view = BookView(
            tuple(book.bids_to_dict().items()),
            tuple(book.asks_to_dict().items()),
            deltas.ts_init,
            deltas.ts_event,
        )
        self.views[ident] = view
        self._tick()

    def _cash(self):
        account = self._cache.account_for_venue(self.ids[0].venue)
        value = account.balance_free(self.currency) if account else None
        return value.as_decimal() if value is not None else None

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
        if now - self.last_sample_ns >= 1_000_000_000:
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
        # Re-evaluated against currently available books *after* decision delay.
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

    def on_time_event(self, event):
        if event.name == "pair-watchdog" and hasattr(self, "fees"):
            self._tick()

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
                    if order is not None and not order.is_closed:
                        self.cancel_order(order.client_order_id)
                    self.orders_to_cancel.add(ident)
                self.record("pending_timeout", ids=sorted(pending))
                self.halted = True
            return
        # Net *actual simulated fills*, including any partial unwind, determine
        # residual exposure. A rejected or expired leg is never assumed filled.
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
        ident, amount = self.ids[index], abs(a - b)
        book = self.views.get(ident)
        now = self._clock.timestamp_ns()
        if (
            book is None
            or not book.bids
            or now < book.received_ns
            or now - book.received_ns > self.rules.max_age_ns
        ):
            self.counts["unwind_waiting_for_fresh_book"] += 1
            return
        if amount < self.fees[index].min_quantity:
            self.halted = True
            self.record("residual_below_venue_minimum", quantity=str(amount))
            return
        price = max(p for p, _ in book.bids)
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

    def on_order_event(self, event):
        ident = str(getattr(event, "client_order_id", ""))
        self.record(
            "order_event", event_type=type(event).__name__, client_order_id=ident
        )
        if ident:
            order = self._cache.order(event.client_order_id)
            if order is not None and order.is_closed:
                self.terminal.add(ident)

    def on_order_filled(self, event):
        identity = (str(event.client_order_id), str(event.trade_id))
        if identity in self.seen_fills:
            self.counts["duplicate_fill_event"] += 1
            return
        self.seen_fills.add(identity)
        self.record(
            "fill",
            instrument=str(event.instrument_id),
            client_order_id=str(event.client_order_id),
            side=str(event.order_side),
            quantity=str(event.last_qty.as_decimal()),
            price=str(event.last_px.as_decimal()),
            commission=str(event.commission.as_decimal()) if event.commission else None,
            currency=str(event.currency),
            trade_id=str(event.trade_id),
        )

    def on_instrument(self, instrument):
        if (
            instrument.id in self.instruments
            and instrument.to_dict() != self.instruments[instrument.id].to_dict()
        ):
            # Dynamic tick/fee migration needs a new run, never silently carry
            # cached parameters into a changed market.
            old, new = self.instruments[instrument.id], instrument
            if (
                old.price_increment != new.price_increment
                or old.size_increment != new.size_increment
                or old.currency != new.currency
                or old.info != new.info
            ):
                self.halted = True
                self.record("instrument_terms_changed", instrument=str(instrument.id))

    def on_stop(self):
        for ident in self.ids:
            self.cancel_all_orders(ident)
        self.record("stopped", entered=self.entered, halted=self.halted)
