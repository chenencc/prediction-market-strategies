"""Independent Decimal implementation of depth-tier passive market making.

Public behavior reference: ccjingeth/predict-fun-marketmaker at
9346c7abefaff73758586e6cb8d7193fcbc79afb. No upstream TypeScript/runtime is imported.
This policy has no network, account, clock, rewards-credit or execution side effect.
"""

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

from ..adapters.common import decimal as D


@dataclass(frozen=True)
class MakerMode:
    tier: int
    minimum_front: Decimal
    minimum_distance: Decimal
    retreat: Decimal
    depth_drop_fraction: Decimal
    balance_check: bool
    cooldown_ns: int
    blacklist_ns: int


MODES = {
    "conservative": MakerMode(
        4,
        D("6000"),
        D("0.035"),
        D("0.015"),
        D("0.05"),
        True,
        6 * 3600 * 10**9,
        7 * 86400 * 10**9,
    ),
    "aggressive": MakerMode(
        3,
        D("4000"),
        D("0.03"),
        D("0.015"),
        D("0.08"),
        False,
        4 * 3600 * 10**9,
        48 * 3600 * 10**9,
    ),
}


@dataclass(frozen=True)
class MakerRules:
    mode: str = "conservative"
    quantity: Decimal = D("10")
    max_inventory_per_outcome: Decimal = D("200")
    max_reserved_notional: Decimal = D("200")
    max_run_loss: Decimal = D("20")
    hedge_slippage: Decimal = D("0.025")
    max_pair_lock_loss: Decimal = D("0.02")
    hedge_mode: str = "FLATTEN"
    max_quote_cycles: int = 5
    max_orders: int = 30
    max_hedge_attempts: int = 2
    quote_interval_ns: int = 2_000_000_000
    max_age_ns: int = 2_000_000_000
    max_skew_ns: int = 500_000_000
    order_timeout_ns: int = 3_000_000_000
    max_unhedged_ns: int = 5_000_000_000
    min_time_to_close_ns: int = 24 * 3600 * 10**9

    def __post_init__(self):
        if self.mode not in MODES or self.hedge_mode not in {"FLATTEN", "BUY_OPPOSITE"}:
            raise ValueError("unsupported maker/hedge mode")
        for name in (
            "quantity",
            "max_inventory_per_outcome",
            "max_reserved_notional",
            "max_run_loss",
            "hedge_slippage",
            "max_pair_lock_loss",
        ):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError("positive Decimal required: " + name)
        if self.hedge_slippage >= 1 or self.max_pair_lock_loss >= 1:
            raise ValueError("invalid price bound")
        if self.quantity > self.max_inventory_per_outcome:
            raise ValueError("quantity exceeds inventory cap")
        for name in (
            "max_quote_cycles",
            "max_orders",
            "max_hedge_attempts",
            "quote_interval_ns",
            "max_age_ns",
            "max_skew_ns",
            "order_timeout_ns",
            "max_unhedged_ns",
            "min_time_to_close_ns",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError("positive limit required: " + name)


@dataclass(frozen=True)
class RewardTerms:
    venue: str
    market_id: str
    condition_id: str
    observed_ns: int
    minimum_shares: Decimal
    maximum_distance: Decimal
    basis: str
    active_allocation: bool
    source: str

    def __post_init__(self):
        if self.venue not in {"polymarket", "predictfun"}:
            raise ValueError("unsupported venue")
        if self.basis not in {"midpoint_distance", "quote_width"}:
            raise ValueError("explicit reward spread basis required")
        if (
            type(self.observed_ns) is not int
            or type(self.active_allocation) is not bool
        ):
            raise ValueError("exact reward clock and allocation flag required")
        if (
            not self.market_id
            or not self.condition_id
            or not self.source
            or self.observed_ns <= 0
        ):
            raise ValueError("reward identity/provenance required")
        if (
            not isinstance(self.minimum_shares, Decimal)
            or not isinstance(self.maximum_distance, Decimal)
            or not self.minimum_shares.is_finite()
            or not self.maximum_distance.is_finite()
            or self.minimum_shares <= 0
            or not 0 < self.maximum_distance < 1
        ):
            raise ValueError("invalid reward terms")
        if self.venue == "polymarket" and self.basis != "midpoint_distance":
            raise ValueError(
                "Polymarket reward spread is distance from adjusted midpoint"
            )


@dataclass(frozen=True)
class Quote:
    index: int
    side: str
    price: Decimal
    quantity: Decimal
    front: Decimal


@dataclass(frozen=True)
class QuotePlan:
    quotes: tuple
    reason: str


def snap(value, increment, side):
    return (value / increment).to_integral_value(
        rounding=ROUND_FLOOR if side == "BUY" else ROUND_CEILING
    ) * increment


def levels(values, *, reverse=False):
    values = tuple((D(p), D(q)) for p, q in values)
    if any(not 0 < p < 1 or q <= 0 for p, q in values) or len(
        {p for p, _ in values}
    ) != len(values):
        raise ValueError("invalid or duplicate book levels")
    return tuple(sorted(values, reverse=reverse))


def front_depth(book, price, side, own=()):
    visible = dict(levels(book.bids if side == "BUY" else book.asks))
    for own_side, own_price, own_quantity in own:
        if own_side == side and own_price in visible:
            visible[own_price] = max(D(0), visible[own_price] - own_quantity)
    return sum(
        (q for p, q in visible.items() if (p > price if side == "BUY" else p < price)),
        D(0),
    )


def validate_quote(book, price, side, *, tick, reward, mode, own=()):
    bids, asks = levels(book.bids, reverse=True), levels(book.asks)
    if not bids or not asks or bids[0][0] >= asks[0][0]:
        return "empty_or_crossed", D(0)
    best_bid, best_ask = bids[0][0], asks[0][0]
    if not 0 < price < 1 or price % tick:
        return "invalid_quote_grid", D(0)
    distance = best_bid - price if side == "BUY" else price - best_ask
    if distance < mode.minimum_distance:
        return "distance_guard", D(0)
    front = front_depth(book, price, side, own)
    if front < mode.minimum_front:
        return "front_depth_guard", front
    # Polymarket scoring uses a size-cutoff-adjusted midpoint. It is not the
    # same quantity as the quote's total bid/ask width. Eligibility is only
    # a local constraint; no money/rebate is credited from this estimate.
    if reward.basis == "midpoint_distance":
        rb = next((p for p, q in bids if q >= reward.minimum_shares), None)
        ra = next((p for p, q in asks if q >= reward.minimum_shares), None)
        if rb is None or ra is None:
            return "reward_midpoint_unavailable", front
        if abs(price - (rb + ra) / 2) >= reward.maximum_distance:
            return "outside_reward_band", front
    return "valid", front


def plan_quotes(
    books, inventory, rules, rewards, *, ticks, steps, minimums, cash, now_ns, expiry_ns
):
    if any(len(values) != 2 for values in [ticks, steps, minimums]):
        return QuotePlan((), "invalid_instrument_parameters")
    if any(
        not isinstance(v, Decimal) or not v.is_finite() or v <= 0
        for values in [ticks, steps, minimums]
        for v in values
    ):
        return QuotePlan((), "invalid_instrument_parameters")
    if cash is not None and (
        not isinstance(cash, Decimal) or not cash.is_finite() or cash < 0
    ):
        return QuotePlan((), "invalid_cash")
    if len(books) != 2 or any(b is None or not b.ready for b in books):
        return QuotePlan((), "waiting_for_two_snapshots")
    if rewards is None or not rewards.active_allocation:
        return QuotePlan((), "no_verified_active_reward_allocation")
    if now_ns < rewards.observed_ns:
        return QuotePlan((), "reward_not_yet_available")
    if expiry_ns is None or expiry_ns - now_ns <= rules.min_time_to_close_ns:
        return QuotePlan((), "near_or_unknown_close")
    if any(
        b.received_ns > now_ns or now_ns - b.received_ns > rules.max_age_ns
        for b in books
    ):
        return QuotePlan((), "stale_book")
    if abs(books[0].received_ns - books[1].received_ns) > rules.max_skew_ns:
        return QuotePlan((), "receipt_skew")
    if len(inventory) != 2 or any(
        not isinstance(q, Decimal) or not q.is_finite() or q < 0 for q in inventory
    ):
        return QuotePlan((), "invalid_or_short_inventory")
    mode, quotes = MODES[rules.mode], []
    for index, book in enumerate(books):
        bids, asks = levels(book.bids, reverse=True), levels(book.asks)
        if len(bids) < mode.tier or len(asks) < mode.tier:
            return QuotePlan((), "insufficient_native_tiers")
        if bids[0][0] >= asks[0][0]:
            return QuotePlan((), "empty_or_crossed")
        if mode.balance_check:
            bd = sum((q for _, q in bids[: mode.tier]), D(0))
            ad = sum((q for _, q in asks[: mode.tier]), D(0))
            if min(bd, ad) / max(bd, ad) < D("0.25"):
                return QuotePlan((), "depth_imbalance")
        targets = []
        for side, rows in (("BUY", bids), ("SELL", asks)):
            p = rows[mode.tier - 1][0] + (
                -mode.retreat if side == "BUY" else mode.retreat
            )
            # A tightly spaced native ladder may leave the Nth tier too close
            # to the touch. Retreat farther; never advance a quote merely to
            # make it fit a reward band.
            p = (
                min(p, bids[0][0] - mode.minimum_distance)
                if side == "BUY"
                else max(p, asks[0][0] + mode.minimum_distance)
            )
            previous = rows[mode.tier - 2][0]
            if abs(previous - rows[mode.tier - 1][0]) > D("0.01"):
                candidate = snap(
                    (previous + rows[mode.tier - 1][0]) / 2, ticks[index], side
                )
                if (
                    validate_quote(
                        book,
                        candidate,
                        side,
                        tick=ticks[index],
                        reward=rewards,
                        mode=mode,
                    )[0]
                    == "valid"
                ):
                    p = candidate
            p = snap(p, ticks[index], side)
            reason, front = validate_quote(
                book, p, side, tick=ticks[index], reward=rewards, mode=mode
            )
            if reason != "valid":
                return QuotePlan((), reason)
            targets.append((p, front))
        if (
            rewards.basis == "quote_width"
            and targets[1][0] - targets[0][0] > rewards.maximum_distance
        ):
            return QuotePlan((), "outside_reward_width")
        quantities = [
            min(
                rules.quantity,
                max(D(0), rules.max_inventory_per_outcome - inventory[index]),
            ),
            min(rules.quantity, inventory[index]),
        ]
        for side, (p, front), quantity in zip(("BUY", "SELL"), targets, quantities):
            quantity = snap(quantity, steps[index], "BUY")
            if quantity >= max(minimums[index], rewards.minimum_shares):
                quotes.append(Quote(index, side, p, quantity, front))
    if not quotes:
        return QuotePlan((), "below_venue_or_reward_minimum")
    buy_cost = sum((q.price * q.quantity for q in quotes if q.side == "BUY"), D(0))
    if buy_cost + sum(inventory) > rules.max_reserved_notional:
        quotes = [q for q in quotes if q.side == "SELL"]
        if not quotes:
            return QuotePlan((), "inventory_and_open_order_cap")
        buy_cost = D(0)
    # Reserve a full one-unit opposite-token hedge, independent of hoped-for
    # sell proceeds or rewards. Existing inventory is capped separately.
    hedge_reserve = max(
        (
            q.quantity
            for q in quotes
            if q.side == "BUY" or rules.hedge_mode == "BUY_OPPOSITE"
        ),
        default=D(0),
    )
    if cash is None or cash < (buy_cost + hedge_reserve) / D("0.9"):
        return QuotePlan((), "cash_and_hedge_reserve")
    return QuotePlan(tuple(quotes), "quotable")


class FillProtection:
    def __init__(self, mode):
        self.mode = mode
        self.seen_orders = set()
        self.count, self.last_fill_ns = 0, None
        self.cooldown_until_ns, self.blacklist_until_ns = 0, 0

    def filled(self, order_id, now_ns):
        if order_id in self.seen_orders:
            return
        self.seen_orders.add(order_id)
        if self.last_fill_ns is not None and now_ns - self.last_fill_ns > 3600 * 10**9:
            self.count = 0
        self.count += 1
        self.last_fill_ns = now_ns
        self.cooldown_until_ns = max(
            self.cooldown_until_ns, now_ns + self.mode.cooldown_ns
        )
        if self.count >= 2:
            self.blacklist_until_ns = max(
                self.blacklist_until_ns, now_ns + self.mode.blacklist_ns
            )

    def blocked(self, now_ns):
        if now_ns < self.blacklist_until_ns:
            return "fill_blacklist"
        if now_ns < self.cooldown_until_ns:
            return "fill_cooldown"
        return None
