"""One bounded YES round trip for observing local Paper execution."""

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from nautilus_trader.model import OrderSide, Price, Quantity, TimeInForce
from .binary_market import BinaryMarketPaperStrategy, walk

D = Decimal


@dataclass(frozen=True)
class SingleRules:
    quantity: Decimal = D("10")
    buy_below: Decimal = D("0.50")
    take_profit: Decimal = D("0.02")
    stop_loss: Decimal = D("0.02")
    max_notional: Decimal = D("20")
    max_spread: Decimal = D("0.04")
    hold_ns: int = 15_000_000_000
    max_age_ns: int = 3_000_000_000
    order_timeout_ns: int = 5_000_000_000

    def __post_init__(self):
        for name in (
            "quantity",
            "buy_below",
            "take_profit",
            "stop_loss",
            "max_notional",
            "max_spread",
        ):
            v = getattr(self, name)
            if not isinstance(v, Decimal) or not v.is_finite() or v <= 0:
                raise ValueError("positive finite Decimal required: " + name)
        if self.buy_below >= 1 or self.max_spread >= 1:
            raise ValueError("invalid probability bound")
        for name in ("hold_ns", "max_age_ns", "order_timeout_ns"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError("positive clock bound required")


class SingleMarketPaperStrategy(BinaryMarketPaperStrategy):
    def __init__(self, instrument_ids, rules=None):
        super().__init__(instrument_ids, rules or SingleRules(), "SINGLE-MARKET-001")
        self.phase, self.reason = "waiting", "waiting_for_snapshot"
        self.order_ids = []
        self.sent_ns = self.bought_ns = None
        self.entry_price = None
        self.exit_attempts = 0

    def _block(self, reason):
        self.halted, self.phase, self.reason = True, "blocked", reason
        self.record("protection_halt", reason=reason)
        for i in self.ids:
            self.cancel_all_orders(i)

    def _tick(self):
        now = self._clock.timestamp_ns()
        ident = next(
            i for i in self.ids if self.instruments[i].outcome.lower() == "yes"
        )
        idx = self.ids.index(ident)
        active = [self._cache.order(x) for x in self.order_ids]
        if any(o is not None and not o.is_closed for o in active):
            self.reason = "waiting_for_order_confirmation"
            if now - self.sent_ns > self.rules.order_timeout_ns:
                self._block("order_confirmation_timeout")
            return
        held = self.inventory()[str(ident)]
        if held < 0:
            self._block("unexpected_short_position")
            return
        if self.entered and held == 0:
            self.phase, self.reason = (
                "completed",
                "round_trip_complete" if self.bought_ns else "entry_unfilled",
            )
            return
        view = self.views.get(ident)
        if view is None or not view.bids or not view.asks:
            self.reason = "waiting_for_snapshot"
            return
        if not 0 <= now - view.received_ns <= self.rules.max_age_ns:
            self.reason = "stale_book"
            return
        bid, ask = max(p for p, _ in view.bids), min(p for p, _ in view.asks)
        if bid >= ask or ask - bid > self.rules.max_spread:
            self.reason = "spread_too_wide"
            return
        if (
            self.instruments[ident].expiration_ns
            and now >= self.instruments[ident].expiration_ns
        ):
            self.reason = "market_expired"
            return
        if not self.entered:
            if ask > self.rules.buy_below:
                self.reason = "above_buy_threshold"
                return
            q = (self.rules.quantity / self.step).to_integral_value(
                rounding=ROUND_DOWN
            ) * self.step
            if q < self.fees[idx].min_quantity:
                self.reason = "below_market_minimum"
                return
            depth = walk(view.asks, q)
            if depth is None or depth[1] > self.rules.buy_below:
                self.reason = "insufficient_depth"
                return
            price = depth[1]
            reserve = price * q + self.fees[idx].cost(
                min(price, D("0.5")), q, self.currency.precision
            )
            cash = self._cash()
            if (
                reserve > self.rules.max_notional
                or cash is None
                or reserve > cash * D("0.9")
            ):
                self.reason = "cash_or_notional_limit"
                return
            self.entered, self.phase, self.reason = True, "buying", "buy_threshold_met"
            side = OrderSide.BUY
        else:
            self.phase = "holding"
            if held < self.fees[idx].min_quantity:
                self._block("residual_below_market_minimum")
                return
            if self.bought_ns is None or self.entry_price is None:
                self._block("missing_fill_basis")
                return
            reason = (
                "take_profit"
                if bid >= self.entry_price + self.rules.take_profit
                else "stop_loss"
                if bid <= self.entry_price - self.rules.stop_loss
                else "hold_time_elapsed"
                if now - self.bought_ns >= self.rules.hold_ns
                else "holding_for_exit"
            )
            self.reason = reason
            if reason == "holding_for_exit":
                return
            if self.exit_attempts >= 2:
                self._block("exit_attempt_limit")
                return
            q = held
            # Use the genuine bid ladder; IOC limit bounds the exit price.
            levels = sorted(view.bids, reverse=True)
            remaining, price = q, None
            for p, available in levels:
                remaining -= min(remaining, available)
                price = p
                if remaining == 0:
                    break
            if remaining or price is None:
                self.reason = "insufficient_exit_depth"
                return
            self.exit_attempts += 1
            self.phase, side = "selling", OrderSide.SELL
        order = self.order_factory.limit(
            instrument_id=ident,
            order_side=side,
            quantity=Quantity.from_str(format(q, "f")),
            price=Price.from_str(
                format(price, f".{self.instruments[ident].price_precision}f")
            ),
            time_in_force=TimeInForce.IOC,
            tags=["single_market_paper"],
        )
        self.order_ids.append(order.client_order_id)
        self.sent_ns = now
        self.record(
            "decision",
            reason=self.reason,
            side=str(side),
            price=str(price),
            quantity=str(q),
            client_order_id=str(order.client_order_id),
            instrument=str(ident),
        )
        self.submit_order(order)

    def on_order_filled(self, event):
        super().on_order_filled(event)
        buys = [r for r in self.records if r["kind"] == "fill" and r["side"] == "BUY"]
        if buys:
            self.bought_ns = min(r["now_ns"] for r in buys)
            self.entry_price = sum(
                D(r["price"]) * D(r["quantity"]) for r in buys
            ) / sum(D(r["quantity"]) for r in buys)
