"""Offline cross-venue pair convergence with independent delayed IOC legs.

Research cash-equivalent fees/collateral, not a live execution implementation.
No guaranteed settlement payoff is booked. Unclosed inventory remains open.
"""
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal as D, ROUND_CEILING
import heapq

ZERO = D(0)
ONE = D(1)
NS = 1_000_000_000


@dataclass(frozen=True)
class Leg:
    instrument: str
    venue: str
    market: str
    mirror: bool = False


@dataclass(frozen=True)
class Pair:
    identity: str
    legs: tuple[Leg, Leg]
    start_ns: int
    end_ns: int | None = None


class Depth:
    """Shared depth budget. Unchanged public size does NOT replenish own fills."""
    def __init__(self):
        self.raw = {"bids": {}, "asks": {}}
        self.available = {"bids": {}, "asks": {}}
        self.at = 0

    def update(self, bids, asks, at):
        if at < self.at:
            raise ValueError("book_availability_reversal")
        for side, levels in (("bids", bids), ("asks", asks)):
            new = {D(p): D(q) for p, q in levels if D(q) > 0}
            if any(not p.is_finite() or not q.is_finite() or not 0 <= p <= 1 for p, q in new.items()):
                raise ValueError("invalid_depth")
            old, available = self.raw[side], self.available[side]
            self.available[side] = {p: min(q, available.get(p, ZERO) + max(ZERO, q-old.get(p, ZERO)))
                                    for p, q in new.items()}
            self.raw[side] = new
        self.at = at

    def ladder(self, leg, buy):
        side = ("bids" if buy else "asks") if leg.mirror else ("asks" if buy else "bids")
        items = [(ONE-p if leg.mirror else p, q, p, side)
                 for p, q in self.available[side].items() if q > 0]
        return sorted(items, key=lambda x: x[0], reverse=not buy)

    def valid(self):
        return bool(self.raw["bids"] and self.raw["asks"]
                    and max(self.raw["bids"]) < min(self.raw["asks"]))


@dataclass(frozen=True)
class Config:
    hold_seconds: int = 300
    quantity: D = D("5")
    entry_edge: D = D("0.02")
    take_profit: D = D("0.005")
    stop_loss: D = D("0.03")
    slippage_limit: D = D("0.005")
    fee_buffer_per_share: D = D("0.001")
    max_age_ns: int = 5 * NS
    max_skew_ns: int = 2 * NS
    decision_ns: int = NS
    poly_latency_ns: int = 250_000_000
    predict_latency_ns: int = 500_000_000
    initial_cash_per_venue: D = D("1000")
    cooldown_ns: int = 10 * NS

    def __post_init__(self):
        for name in ("quantity", "entry_edge", "take_profit", "stop_loss", "slippage_limit",
                     "fee_buffer_per_share", "initial_cash_per_venue"):
            value = getattr(self, name)
            if not isinstance(value, D) or not value.is_finite() or value < 0:
                raise ValueError("nonnegative_exact_config_required:" + name)
        if self.quantity <= 0 or self.initial_cash_per_venue <= 0 or self.hold_seconds <= 0:
            raise ValueError("positive_size_capital_and_hold_required")
        if min(self.max_age_ns, self.max_skew_ns, self.decision_ns, self.poly_latency_ns,
               self.predict_latency_ns, self.cooldown_ns) < 0:
            raise ValueError("negative_time_configuration")


def fee_per_share(venue, raw, price):
    """Conservative rounded CASH EQUIVALENT; actual share deductions uncertified."""
    p = D(price)
    if not 0 < p < 1:
        raise ValueError("invalid_execution_price")
    if raw.get("feesEnabled") is False:
        return ZERO
    if venue == "polymarket":
        schedule = raw.get("feeSchedule") or {}
        if str(schedule.get("exponent")) != "1" or schedule.get("rate") is None:
            raise ValueError("unknown_polymarket_fee_formula")
        value = D(str(schedule["rate"])) * p * (1-p)
    elif venue == "predictfun" and raw.get("feeRateBps") is not None:
        value = D(str(raw["feeRateBps"])) / 10000 * min(p, 1-p)
    else:
        raise ValueError("unknown_fee")
    if not value.is_finite() or value < 0:
        raise ValueError("invalid_fee")
    return value.quantize(D("0.00001"), rounding=ROUND_CEILING)


@dataclass
class Position:
    pair: Pair
    opened_ns: int
    quantities: list = field(default_factory=lambda: [ZERO, ZERO])
    spent: D = ZERO
    proceeds: D = ZERO
    pending: int = 0
    exiting: bool = False
    reason: str = ""
    last_exit_attempt: int = 0
    capital_ns: D = ZERO
    last_account_ns: int = 0


class PairConvergence:
    def __init__(self, config, metadata):
        self.config = config
        self.metadata = metadata
        self.books = {}
        self.cash = {v: config.initial_cash_per_venue for v in ("polymarket", "predictfun")}
        self.reserved = dict.fromkeys(self.cash, ZERO)
        self.positions = {}
        self.last_decision = {}
        self.cooldown = {}
        self.queue = []
        self.serial = 0
        self.now = 0
        self.start_ns = None
        self.stats = Counter()
        self.trades = []
        self.fills = []

    def update(self, instrument, bids, asks, at):
        self.books.setdefault(instrument, Depth()).update(bids, asks, at)

    def _raw(self, leg, at):
        raw = self.metadata(leg.venue, leg.market, at)
        if raw is None:
            raise ValueError("metadata_unavailable_asof")
        if raw.get("closed") is True or raw.get("acceptingOrders") is False or raw.get("tradingStatus") in {"CLOSED", "HALTED", "PAUSED"}:
            raise ValueError("market_not_open")
        return raw

    def quote(self, leg, quantity, buy, at, limit=None):
        book = self.books.get(leg.instrument)
        if book is None or not book.valid() or not 0 <= at-book.at <= self.config.max_age_ns:
            raise ValueError("stale_or_invalid_book")
        raw = self._raw(leg, at)
        left, notional, fee, levels = quantity, ZERO, ZERO, []
        for price, available, original, side in book.ladder(leg, buy):
            if limit is not None and ((buy and price > limit) or (not buy and price < limit)):
                break
            amount = min(left, available)
            if amount <= 0:
                continue
            charge = amount * (fee_per_share(leg.venue, raw, price) + self.config.fee_buffer_per_share)
            notional += amount * price
            fee += charge
            levels.append((original, side, amount, price))
            left -= amount
            if left == 0:
                break
        return dict(quantity=quantity-left, notional=notional, fee=fee, levels=levels,
                    total=notional+fee if buy else notional-fee)

    def _schedule(self, pos, index, buy, quantity, limit, reserve=ZERO):
        leg = pos.pair.legs[index]
        latency = self.config.poly_latency_ns if leg.venue == "polymarket" else self.config.predict_latency_ns
        self.serial += 1
        heapq.heappush(self.queue, (self.now+latency, self.serial, pos.pair.identity, index, buy, quantity, limit, reserve))
        pos.pending += 1
        self.reserved[leg.venue] += reserve

    def _account(self, pos, at):
        if pos.last_account_ns:
            pos.capital_ns += max(ZERO, pos.spent-pos.proceeds) * (at-pos.last_account_ns)
        pos.last_account_ns = at

    def advance(self, at):
        if at < self.now:
            raise ValueError("research_clock_reversed")
        if self.start_ns is None:
            self.start_ns = at
        while self.queue and self.queue[0][0] <= at:
            due, _, ident, index, buy, quantity, limit, reserve = heapq.heappop(self.queue)
            self.now = due
            pos = self.positions[ident]
            self._account(pos, due)
            leg = pos.pair.legs[index]
            self.reserved[leg.venue] -= reserve
            pos.pending -= 1
            try:
                fill = self.quote(leg, quantity, buy, due, limit)
                if buy and fill["total"] > self.cash[leg.venue]-self.reserved[leg.venue]:
                    raise ValueError("cash_unavailable_at_arrival")
                if buy and pos.pair.end_ns is not None and due >= pos.pair.end_ns:
                    raise ValueError("pair_closed_before_arrival")
            except ValueError as error:
                self.stats["arrival_rejected:" + str(error)] += 1
                fill = dict(quantity=ZERO, total=ZERO, fee=ZERO, notional=ZERO, levels=[])
            q = fill["quantity"]
            if q:
                for price, side, amount, _ in fill["levels"]:
                    self.books[leg.instrument].available[side][price] -= amount
                if buy:
                    pos.quantities[index] += q
                    pos.spent += fill["total"]
                    self.cash[leg.venue] -= fill["total"]
                else:
                    pos.quantities[index] -= q
                    pos.proceeds += fill["total"]
                    self.cash[leg.venue] += fill["total"]
                self.fills.append(dict(pair=ident, venue=leg.venue, leg=index, buy=buy, at_ns=due,
                                       quantity=str(q), cash=str(fill["total"]), fee=str(fill["fee"])))
            if q < quantity:
                self.stats["partial_or_zero_leg_fills"] += 1
            if pos.pending == 0:
                if not any(pos.quantities):
                    self._close(pos)
                elif pos.exiting or pos.quantities != [self.config.quantity]*2:
                    pos.exiting = True
                    pos.reason = pos.reason or "incomplete_entry"
                    self.stats["residual_exit_required"] += 1
                    self._exit(pos)
        self.now = at
        for pos in list(self.positions.values()):
            if pos.pending:
                continue
            if at-pos.opened_ns >= self.config.hold_seconds*NS:
                pos.exiting = True
                pos.reason = pos.reason or "timeout"
            if pos.pair.end_ns is not None and at >= pos.pair.end_ns:
                pos.exiting = True
                pos.reason = pos.reason or "pair_membership_ended"
            if pos.exiting:
                self._exit(pos)

    def _close(self, pos):
        self._account(pos, self.now)
        self.trades.append(dict(pair=pos.pair.identity, opened_ns=pos.opened_ns, closed_ns=self.now,
                               spent=str(pos.spent), proceeds=str(pos.proceeds), pnl=str(pos.proceeds-pos.spent),
                               hold_seconds=(self.now-pos.opened_ns)/NS, reason=pos.reason or "entry_unfilled",
                               capital_hours=str(pos.capital_ns/D(NS*3600))))
        self.cooldown[pos.pair.identity] = self.now+self.config.cooldown_ns
        del self.positions[pos.pair.identity]

    def _exit(self, pos):
        if pos.pending or self.now-pos.last_exit_attempt < max(NS, self.config.decision_ns):
            return
        pos.last_exit_attempt = self.now
        for i, q in enumerate(pos.quantities):
            if not q:
                continue
            try:
                quote = self.quote(pos.pair.legs[i], q, False, self.now)
                if not quote["levels"]:
                    raise ValueError("no_exit_depth")
                limit = max(D("0.00001"), quote["levels"][-1][3]-self.config.slippage_limit)
                self._schedule(pos, i, False, q, limit)
            except ValueError as error:
                self.stats["exit_blocked:" + str(error)] += 1

    def evaluate(self, pair, at):
        if at != self.now:
            raise ValueError("advance_clock_before_evaluation")
        if at-self.last_decision.get(pair.identity, -10**30) < self.config.decision_ns:
            return
        self.last_decision[pair.identity] = at
        pos = self.positions.get(pair.identity)
        if pos:
            if pos.pending or pos.exiting:
                return
            try:
                proceeds = pos.proceeds
                for i, leg in enumerate(pair.legs):
                    quote = self.quote(leg, pos.quantities[i], False, at)
                    if quote["quantity"] != pos.quantities[i]:
                        raise ValueError("insufficient_exit_depth")
                    proceeds += quote["total"]
                pnl = proceeds-pos.spent
                if pnl >= self.config.take_profit*self.config.quantity or pnl <= -self.config.stop_loss*self.config.quantity:
                    pos.exiting = True
                    pos.reason = "take_profit" if pnl >= 0 else "stop_loss"
                    self._exit(pos)
            except ValueError as error:
                self.stats["position_mark_blocked:"+str(error)] += 1
            return
        if at < pair.start_ns or (pair.end_ns is not None and at >= pair.end_ns) or at < self.cooldown.get(pair.identity, 0):
            return
        self.stats["entry_checks"] += 1
        try:
            books = [self.books.get(leg.instrument) for leg in pair.legs]
            if any(b is None for b in books) or abs(books[0].at-books[1].at) > self.config.max_skew_ns:
                raise ValueError("quote_skew_or_missing")
            quotes = [self.quote(leg, self.config.quantity, True, at) for leg in pair.legs]
            exit_quotes = [self.quote(leg, self.config.quantity, False, at) for leg in pair.legs]
            if any(q["quantity"] != self.config.quantity for q in quotes+exit_quotes):
                raise ValueError("entry_or_exit_depth_insufficient")
            # Explicit venue minimum evidence; Predict product minimum is 1 USDT.
            for leg, q in zip(pair.legs, quotes):
                raw = self._raw(leg, at)
                if leg.venue == "polymarket":
                    minimum = raw.get("orderMinSize", raw.get("minimum_order_size"))
                    if minimum is None or self.config.quantity < D(str(minimum)):
                        raise ValueError("minimum_size_unknown_or_unmet")
                elif q["notional"] < ONE:
                    raise ValueError("predict_minimum_notional_unmet")
            entry = sum(q["total"] for q in quotes)
            exit_fee = sum(q["fee"] for q in exit_quotes)
            buffer = self.config.slippage_limit*self.config.quantity*4
            if self.config.quantity-entry-exit_fee-buffer < self.config.entry_edge*self.config.quantity:
                raise ValueError("insufficient_net_edge")
            reserves = [q["total"]+self.config.quantity*(self.config.slippage_limit+D("0.01")) for q in quotes]
            if any(self.cash[leg.venue]-self.reserved[leg.venue] < reserve for leg, reserve in zip(pair.legs, reserves)):
                raise ValueError("venue_capital_limit")
        except ValueError as error:
            self.stats["entry_rejected:"+str(error)] += 1
            return
        pos = Position(pair, at, last_account_ns=at)
        self.positions[pair.identity] = pos
        self.stats["entry_signals"] += 1
        for i, q in enumerate(quotes):
            self._schedule(pos, i, True, self.config.quantity,
                           min(D("0.99999"), q["levels"][-1][3]+self.config.slippage_limit), reserves[i])

    def report(self):
        for pos in self.positions.values():
            self._account(pos, self.now)
        elapsed_hours = D(self.now-(self.start_ns or self.now))/D(NS*3600)
        pnl = sum((D(t["pnl"]) for t in self.trades), ZERO)
        return dict(schema="pair-convergence-research/1", stats=dict(self.stats),
                    closed_round_trips=len(self.trades), realized_cash_equivalent_pnl=str(pnl),
                    cash_by_venue={v:str(x) for v,x in self.cash.items()},
                    prefunded_capital_hours=str(elapsed_hours*self.config.initial_cash_per_venue*2),
                    committed_capital_hours=str(sum((D(t["capital_hours"]) for t in self.trades),ZERO)
                        +sum((p.capital_ns/D(NS*3600) for p in self.positions.values()),ZERO)),
                    open_positions=[dict(pair=p.pair.identity, quantities=list(map(str,p.quantities)),
                        spent=str(p.spent), proceeds=str(p.proceeds), exit_reason=p.reason,
                        pending_legs=p.pending, holding_seconds=(self.now-p.opened_ns)/NS) for p in self.positions.values()],
                    pending_orders=len(self.queue), trades=self.trades, fills=self.fills,
                    execution_ready=False, settlement_pnl_booked=False,
                    assumptions=["conditional_on_pair_equivalence_not_certified", "collateral_parity_assumed",
                        "cash_equivalent_fee_model_not_actual_share_deductions", "no_market_impact_or_hidden_liquidity",
                        "independent_price_capped_IOC_legs", "unfinished_positions_not_counted_as_profit"])
