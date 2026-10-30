# SPDX-License-Identifier: LGPL-3.0-or-later
# Shared lifecycle extracted from our binary-pair adapter. See THIRD_PARTY_NOTICES.md.
"""Shared native binary-market paper lifecycle; strategy policy lives elsewhere."""

from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, ROUND_CEILING
from math import lcm
from nautilus_trader.model import BookType, ClientId, OrderBook, RecordFlag, StrategyId
from nautilus_trader.trading import Strategy, StrategyConfig
from ..adapters.common import decimal as D


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


class BinaryMarketPaperStrategy(Strategy):
    outcome_sets = (frozenset({"yes", "no"}),)

    def __init__(self, instrument_ids, rules, strategy_id):
        super().__init__(
            StrategyConfig(
                strategy_id=StrategyId(strategy_id),
                log_events=False,
                log_commands=False,
            )
        )
        self.ids = tuple(instrument_ids)
        if len(self.ids) != 2 or len(set(self.ids)) != 2:
            raise ValueError("two distinct outcomes required")
        self.rules = rules
        self.native_books, self.views, self.instruments = {}, {}, {}
        self.counts, self.records, self.samples = Counter(), [], []
        self.terminal = set()
        self.entered, self.halted = False, False
        self.raw_batches = []
        self.capture_raw_inputs = True
        self.policy_interval_ns = 0
        self.next_policy_ns = 0
        self.last_sample_ns = 0
        self.seen_fills = set()
        self.candidate_ns = None
        self.monitor = None
        self.last_monitor_ns = 0

    def publish_monitor(self, status="running", error=None, force=False):
        if self.monitor is None or not hasattr(self, "currency"):
            return
        now = self._clock.timestamp_ns()
        if not force and now - self.last_monitor_ns < 250_000_000:
            return
        self.last_monitor_ns = now
        from ..paper.monitor import capture

        try:
            self.monitor.snapshot(capture(self, status, error))
        except Exception as exc:
            self.monitor.error = "observation_capture_failure: " + str(exc)
        if self.monitor.error and not self.halted:
            self.halted = True
            self.records.append(
                dict(kind="observation_failure", now_ns=now, error=self.monitor.error)
            )
            for ident in self.ids:
                self.cancel_all_orders(ident)

    def on_ready(self):
        pass

    def inventory(self):
        result = {str(i): Decimal(0) for i in self.ids}
        for row in self.records:
            if row["kind"] == "fill":
                result[row["instrument"]] += D(row["quantity"]) * (
                    1 if row["side"] == "BUY" else -1
                )
        return result

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def record(self, kind, **values):
        row = dict(kind=kind, now_ns=self._clock.timestamp_ns(), **values)
        self.records.append(row)
        if self.monitor is not None:
            self.monitor.event(row)

    def guarded_tick(self):
        # The native Python callback bridge logs exceptions; logging alone
        # must not leave resting orders active or certify a quiet strategy.
        if self.halted:
            return
        now = self._clock.timestamp_ns()
        if now < self.next_policy_ns:
            return
        self.next_policy_ns = now + self.policy_interval_ns
        try:
            self._tick()
        except Exception as exc:
            self.halted = True
            self.record(
                "strategy_exception", error=type(exc).__name__ + ": " + str(exc)
            )
            for ident in self.ids:
                self.cancel_all_orders(ident)

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
                or frozenset(i.outcome.lower() for i in instruments)
                not in self.outcome_sets
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
                "market-watchdog",
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
        if not self.halted:
            try:
                self.on_ready()
            except Exception as exc:
                self.halted = True
                self.record("initialization_failed", error=str(exc))
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
        if getattr(self, "capture_raw_inputs", True):
            if len(self.raw_batches) >= 100_000:
                self.halted = True
                self.record("input_cap")
                return
            self.raw_batches.append(deltas)
        snapshot = bool(
            deltas.deltas and int(deltas.deltas[-1].flags) & int(RecordFlag.F_SNAPSHOT)
        )
        control_clear = not snapshot and any(
            str(d.action) == "CLEAR" for d in deltas.deltas
        )
        if control_clear:
            # Imported gap/terminal controls invalidate depth and resting
            # orders. Ordinary changes cannot re-arm after a lost connection.
            self.cancel_all_orders(ident)
            self.native_books[ident].reset()
            self.views.pop(ident, None)
            self.candidate_ns = None
            self.record("source_book_invalidated", instrument=str(ident))
            return
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
        self.guarded_tick()

    def _cash(self):
        account = self._cache.account_for_venue(self.ids[0].venue)
        value = account.balance_free(self.currency) if account else None
        return value.as_decimal() if value is not None else None

    def on_time_event(self, event):
        if event.name == "market-watchdog" and hasattr(self, "fees"):
            self.guarded_tick()
            self.publish_monitor()

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
