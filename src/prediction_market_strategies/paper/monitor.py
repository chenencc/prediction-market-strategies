"""Bounded non-blocking observations. Native cache is read by its owner only."""

from decimal import Decimal
import json
from pathlib import Path
from queue import Queue, Empty, Full
import threading
import time
from .polymarket_pair import save_json


class MonitorWriter:
    def __init__(self, root, metadata, capacity=8192):
        self.root = Path(root)
        self.metadata = dict(metadata)
        self.queue = Queue(capacity)
        self.latest = None
        self.lock = threading.Lock()
        self.closed = threading.Event()
        self.error = None
        self.persisted_seq = 0
        self.seq = 0
        self.coalesced = 0
        self.thread = threading.Thread(
            target=self._work, name="paper-observer", daemon=True
        )
        self.thread.start()

    def event(self, row):
        self.seq += 1
        row = dict(row, seq=self.seq)
        try:
            self.queue.put_nowait(row)
        except Full:
            self.error = "critical_observation_queue_full"
        return self.error is None

    def snapshot(self, row):
        with self.lock:
            if self.latest is not None:
                self.coalesced += 1
            self.latest = dict(self.metadata, **row)

    def _work(self):
        last_snapshot = None
        try:
            with (
                (self.root / "monitor-events.jsonl").open(
                    "a", encoding="utf-8"
                ) as events,
                (self.root / "monitor-history.jsonl").open(
                    "a", encoding="utf-8"
                ) as history,
            ):
                while (
                    not self.closed.is_set()
                    or not self.queue.empty()
                    or self.latest is not None
                ):
                    try:
                        event = self.queue.get(timeout=0.05)
                        events.write(
                            json.dumps(event, default=str, ensure_ascii=False) + "\n"
                        )
                        events.flush()
                        self.persisted_seq = event["seq"]
                        for _ in range(255):
                            try:
                                event = self.queue.get_nowait()
                            except Empty:
                                break
                            events.write(
                                json.dumps(event, default=str, ensure_ascii=False)
                                + "\n"
                            )
                            self.persisted_seq = event["seq"]
                        events.flush()
                    except Empty:
                        pass
                    with self.lock:
                        snapshot, self.latest = self.latest, None
                    if snapshot is not None:
                        snapshot.update(
                            persisted_event_seq=self.persisted_seq,
                            observation_coalesced=self.coalesced,
                            writer_error=self.error,
                        )
                        # Nanosecond clocks and financial values remain strings on the wire.
                        snapshot = wire(snapshot)
                        last_snapshot = snapshot
                        history.write(json.dumps(snapshot, ensure_ascii=False) + "\n")
                        history.flush()
                        save_json(self.root / "monitor-state.json", snapshot)
                if last_snapshot is not None:
                    last_snapshot["persisted_event_seq"] = self.persisted_seq
                    last_snapshot["writer_error"] = self.error
                    save_json(self.root / "monitor-state.json", last_snapshot)
        except Exception as exc:
            self.error = type(exc).__name__ + ": " + str(exc)

    def close(self):
        self.closed.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            self.error = "observation_writer_shutdown_timeout"
        if self.error:
            raise RuntimeError(self.error)


def wire(value, key=""):
    if isinstance(value, dict):
        return {k: wire(v, k) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [wire(x) for x in value]
    if isinstance(value, Decimal) or (
        isinstance(value, int) and (key.endswith("_ns") or abs(value) > 2**53)
    ):
        return str(value)
    return value


def capture(strategy, status="running", error=None):
    now = strategy._clock.timestamp_ns()
    books = []
    inventory = strategy.inventory()
    account = strategy._cache.account_for_venue(strategy.ids[0].venue)
    currency = strategy.currency
    total = account.balance_total(currency) if account else None
    free = account.balance_free(currency) if account else None
    rows, realized, fees = {}, Decimal(0), Decimal(0)
    fees_known = True
    for event in strategy.records:
        if event["kind"] != "fill":
            continue
        item = rows.setdefault(
            event["instrument"], {"q": Decimal(0), "cost": Decimal(0)}
        )
        q, p = Decimal(event["quantity"]), Decimal(event["price"])
        fee = (
            Decimal(event["commission"])
            if event["commission"] is not None
            else Decimal(0)
        )
        fees_known &= event["commission"] is not None
        fees += fee
        if event["side"] == "BUY":
            item["q"] += q
            item["cost"] += q * p + fee
        else:
            basis = item["cost"] * q / item["q"] if item["q"] else Decimal(0)
            realized += q * p - fee - basis
            item["q"] -= q
            item["cost"] -= basis
    liquidation, valuation_valid = Decimal(0), True
    for ident in strategy.ids:
        view = strategy.views.get(ident)
        item = strategy.instruments[ident]
        age = now - view.received_ns if view else None
        fresh = age is not None and 0 <= age <= strategy.rules.max_age_ns
        bids = sorted(view.bids, reverse=True) if view else []
        asks = sorted(view.asks) if view else []
        held = inventory[str(ident)]
        remain, value = held, Decimal(0)
        for p, q in bids:
            take = min(remain, q)
            value += p * take
            remain -= take
        valid = held == 0 or (fresh and remain == 0)
        valuation_valid &= valid
        liquidation += value if valid else 0
        books.append(
            dict(
                instrument_id=str(ident),
                outcome=item.outcome,
                bids=[[str(p), str(q)] for p, q in bids[:12]],
                asks=[[str(p), str(q)] for p, q in asks[:12]],
                source_ns=view.source_ns if view else None,
                received_ns=view.received_ns if view else None,
                age_ms=age / 1e6 if age is not None else None,
                fresh=fresh,
                held=str(held),
                cost=str(rows.get(str(ident), {}).get("cost", 0)),
                liquidation_value=str(value) if valid else None,
                batches=strategy.counts[str(ident)],
            )
        )
    cash = total.as_decimal() if total is not None else None
    cost = sum((x["cost"] for x in rows.values()), Decimal(0))
    metadata = strategy.monitor.metadata if strategy.monitor is not None else {}
    prior_pnl = Decimal(metadata.get("prior_realized_pnl", "0"))
    prior_fees = Decimal(metadata.get("prior_fees", "0"))
    return wire(
        dict(
            observed_ns=now,
            published_ns=time.time_ns(),
            status=status,
            error=error,
            phase=getattr(
                strategy, "phase", "halted" if strategy.halted else "evaluating"
            ),
            reason=getattr(strategy, "reason", ""),
            halted=strategy.halted,
            cycles=getattr(strategy, "cycles", 0),
            books=books,
            orders=[x.to_dict() for x in strategy._cache.orders()],
            events=strategy.records[-100:],
            event_count=len(strategy.records),
            fills=[r for r in strategy.records if r["kind"] == "fill"],
            account=dict(
                currency=str(currency),
                cash_total=str(cash) if cash is not None else None,
                cash_free=str(free.as_decimal()) if free is not None else None,
                realized_pnl=str(realized) if fees_known else None,
                session_realized_pnl=str(prior_pnl + realized) if fees_known else None,
                session_fees=str(prior_fees + fees) if fees_known else None,
                unrealized_pnl=str(liquidation - cost)
                if valuation_valid and fees_known
                else None,
                equity=str(cash + liquidation)
                if cash is not None and valuation_valid
                else None,
                fees=str(fees) if fees_known else None,
                valuation="native_bid_depth_before_exit_fees",
                valuation_valid=valuation_valid,
            ),
        )
    )
