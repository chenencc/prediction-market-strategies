"""A deterministic plumbing strategy over real V3 events, never a live client.

The simulation uses artificial collateral, zero fees and a future fixture expiry.
Its fills demonstrate the engine connection, not executable exchange economics.
"""
from collections import Counter
from decimal import Decimal as D
import hashlib
import json


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def run_minimal(native_root):
    from pathlib import Path
    import pyarrow.parquet as pq
    from local_postprocess.nautilus import iter_events, fingerprint, clear_batch
    from nautilus_trader.persistence import ParquetDataCatalog
    from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
    from nautilus_trader.trading import Strategy, StrategyConfig
    from nautilus_trader.execution import MakerTakerFeeModel, FillModel
    from nautilus_trader.model import (
        BinaryOption, Currency, CurrencyType, Venue, OmsType, AccountType,
        Money, BookType, OrderBook, OrderSide, TimeInForce, Price, Quantity,
        StrategyId,
    )

    root = Path(native_root)
    # This reader includes exact-overflow; a standalone native catalog does not.
    events = list(iter_events(root))
    if not events or any(a.ts_init > b.ts_init for a, b in zip(events, events[1:])):
        raise ValueError("nonempty_availability_ordered_events_required")
    state = json.loads((root / "state.json").read_bytes())
    index = [r for commit in state["commits"] for r in
             pq.ParquetFile(root / "releases" / commit["identity"] / "event-index.parquet").read().to_pylist()]
    baseline_fingerprints = {r["fingerprint"] for r in index if r["policy"] == "baseline_seed"}
    observed_start_ns = min(r["ts_init"] for r in index if r["policy"] != "baseline_seed")
    Currency.register(Currency("XPROB", 0, 0, "Dimensionless probability replay", CurrencyType.CRYPTO), overwrite=True)
    instruments = {}
    for commit in state["commits"]:
        catalog = ParquetDataCatalog(str(root / "releases" / commit["identity"] / "catalog"))
        for inst in catalog.instruments():
            instruments.setdefault(str(inst.id), inst)

    source_events = events
    book_ids = {str(e.instrument_id) for e in events if type(e).__name__ == "OrderBookDeltas"}
    first_seen = {}
    for event in events:
        first_seen.setdefault(str(event.instrument_id), event.ts_init)
    # A trade-only outcome has no executable L2. The engine requires a book
    # input for each registered L2 instrument: explicitly inject an EMPTY book,
    # never infer its liquidity from the opposite outcome. Keep controls separate.
    controls = [clear_batch(instruments[key], first_seen[key])
                for key in sorted(set(first_seen) - book_ids)]
    events = sorted(controls + events, key=lambda e: e.ts_init)

    # All instruments are present before the run; selection and orders occur only
    # in the callback, from the book visible at that point. No future liquidity scan.
    currency = Currency("SIMUSD", 8, 0, "Artificial smoke-test collateral", CurrencyType.FIAT)
    Currency.register(currency, overwrite=True)
    simulated = []
    for inst in instruments.values():
        obj = inst.to_dict()
        obj["currency"] = "SIMUSD"
        obj["expiration_ns"] = events[-1].ts_init + 86_400_000_000_000
        simulated.append(BinaryOption.from_dict(obj))
    ids = sorted((i.id for i in simulated), key=str)
    expected_fingerprints = [fingerprint(e) for e in events]
    expected_books = {str(i): OrderBook(i, BookType.L2_MBP) for i in ids}
    for event in events:
        if type(event).__name__ == "OrderBookDeltas":
            expected_books[str(event.instrument_id)].apply_deltas(event)

    class ConservativeFill(FillModel):
        def is_limit_filled(self):
            return False

        def is_slipped(self):
            return False

        def fill_limit_inside_spread(self):
            return False

    class OneIOC(Strategy):
        def __init__(self):
            super().__init__(StrategyConfig(strategy_id=StrategyId("V3-SMOKE-001"), log_events=False, log_commands=False))
            self.books = {str(i): OrderBook(i, BookType.L2_MBP) for i in ids}
            self.seen = []
            self.counts = Counter()
            self.sent = None
            self.fills = []
            self.rejects = []

        def on_start(self):
            for ident in ids:
                self.subscribe_book_deltas(ident, BookType.L2_MBP, managed=True)
                self.subscribe_trades(ident)

        def on_trade(self, event):
            self.seen.append(fingerprint(event))
            self.counts["TradeTick"] += 1

        def on_book_deltas(self, event):
            self.seen.append(fingerprint(event))
            self.counts["OrderBookDeltas"] += 1
            book = self.books[str(event.instrument_id)]
            book.apply_deltas(event)
            if (self.sent is not None or str(event.instrument_id.venue) != "POLYMARKET"
                    or fingerprint(event) in baseline_fingerprints or event.ts_init < observed_start_ns):
                return
            bid, ask, size = book.best_bid_price(), book.best_ask_price(), book.best_ask_size()
            if bid is None or ask is None or size is None:
                return
            if not D("0.01") < bid.as_decimal() < ask.as_decimal() < D("0.99") or size.as_decimal() < 1:
                return
            self.sent = dict(instrument_id=str(event.instrument_id), available_at_ns=event.ts_init,
                             source_ns=event.ts_event, best_bid=str(bid), best_ask=str(ask),
                             available_ask_size=str(size), quantity="1", limit_price=str(ask))
            self.submit_order(self.order_factory.limit(
                event.instrument_id, OrderSide.BUY, Quantity.from_str("1"), Price.from_str(str(ask)),
                time_in_force=TimeInForce.IOC,
            ))

        def on_order_filled(self, event):
            self.fills.append(dict(instrument_id=str(event.instrument_id), quantity=str(event.last_qty),
                                   price=str(event.last_px), commission=str(event.commission),
                                   liquidity_side=str(event.liquidity_side), ts_event=event.ts_event))

        def on_order_rejected(self, event):
            self.rejects.append(str(event))

    engine = BacktestEngine(BacktestEngineConfig(bypass_logging=True, run_analysis=False))
    strategy = OneIOC()
    try:
        for venue in sorted({str(i.venue) for i in ids}):
            engine.add_venue(venue=Venue(venue), oms_type=OmsType.NETTING, account_type=AccountType.CASH,
                             starting_balances=[Money(D("10000"), currency)], book_type=BookType.L2_MBP,
                             trade_execution=False, bar_execution=False, queue_position=False,
                             liquidity_consumption=True, fill_model=ConservativeFill(),
                             fee_model=MakerTakerFeeModel(maker_rate=D(0), taker_rate=D(0)))
        for instrument in simulated:
            engine.add_instrument(instrument)
        engine.add_strategy(strategy)
        engine.add_data(events)
        engine.run()
        if engine.iteration != len(events) or strategy.seen != expected_fingerprints:
            raise ValueError("engine_callback_event_loss_or_reordering")
        if not strategy.sent or strategy.rejects or sum(D(f["quantity"]) for f in strategy.fills) != 1:
            raise ValueError("one_simulated_IOC_fill_required")
        if strategy.sent["available_at_ns"] < observed_start_ns:
            raise ValueError("warmup_baseline_must_not_place_orders")
        if any(D(f["price"]) != D(strategy.sent["best_ask"]) for f in strategy.fills):
            raise ValueError("fill_does_not_match_observed_ask")

        def top(book):
            return {k: str(getattr(book, k)()) for k in
                    ("best_bid_price", "best_ask_price", "best_bid_size", "best_ask_size")}

        expected = {k: top(v) for k, v in expected_books.items()}
        actual = {k: top(v) for k, v in strategy.books.items()}
        if actual != expected:
            raise ValueError("strategy_book_replay_changed")
        return dict(schema="v3-minimal-strategy-result/1", status="passed",
                    engine_iterations=engine.iteration, instruments=len(ids), input_events=len(events),
                    source_events=len(source_events), unavailable_book_controls=len(controls),
                    unavailable_book_instruments=sorted(set(first_seen) - book_ids),
                    source_event_counts=dict(Counter(type(e).__name__ for e in source_events)),
                    source_event_stream_sha256=digest([fingerprint(e) for e in source_events]),
                    baseline_seed_events=len(baseline_fingerprints), baseline_orders_allowed=False,
                    observed_start_ns=observed_start_ns,
                    event_counts=dict(strategy.counts), event_stream_sha256=digest(strategy.seen),
                    final_book_top_sha256=digest(actual), callback_order_matches_source=True,
                    start_available_ns=events[0].ts_init, end_available_ns=events[-1].ts_init,
                    simulated_orders=1, order=strategy.sent, fills=strategy.fills,
                    live_orders=0, execution_ready=False, profitability_certified=False,
                    simulation=dict(currency="SIMUSD", starting_balance_per_venue="10000", fees="zero",
                                    quantity="1", fixture_expiry="last_received_plus_one_day",
                                    queue_position=False, trade_based_fills=False, settlement_modeled=False,
                                    representation_grid_is_not_exchange_tick_size=True,
                                    pnl_certified=False, run_analysis=False))
    finally:
        engine.dispose()
