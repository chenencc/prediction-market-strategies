"""No-order engine smoke replay, distinct from a strategy/P&L validation."""

from collections import defaultdict


def replay_probe(exported):
    from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
    from nautilus_trader.model import (
        Venue,
        OmsType,
        AccountType,
        Money,
        BookType,
        OrderBook,
    )
    from nautilus_trader.execution import MakerTakerFeeModel
    from decimal import Decimal
    from ..data.recording_group import require

    engine = BacktestEngine(
        BacktestEngineConfig(bypass_logging=True, run_analysis=False)
    )
    expected = {}
    books = {}
    currencies = defaultdict(dict)
    data_ids = {str(v.instrument_id) for v in exported.data}
    book_ids = {
        str(v.instrument_id)
        for v in exported.data
        if type(v).__name__ == "OrderBookDeltas"
    }
    venue_book_types = {}
    try:
        for instrument in exported.instruments:
            currencies[str(instrument.id.venue)][str(instrument.currency)] = (
                instrument.currency
            )
        for venue, values in currencies.items():
            active = {
                str(i.id)
                for i in exported.instruments
                if str(i.id.venue) == venue and str(i.id) in data_ids
            }
            # L2 matching requires books for every active instrument, including
            # outcomes with trade ticks. Use L1 only for this no-order plumbing
            # probe when a native book is absent; never invent complementary depth.
            l2_complete = bool(active) and active <= book_ids
            venue_book_type = BookType.L2_MBP if l2_complete else BookType.L1_MBP
            venue_book_types[venue] = dict(
                book_type=str(venue_book_type), l2_inputs_complete=l2_complete
            )
            engine.add_venue(
                venue=Venue(venue),
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                # A data-only probe needs no funding. Raw zero is exact for
                # observed 18-decimal collateral too; no float/precision cast.
                starting_balances=[Money.from_raw(0, c) for c in values.values()],
                book_type=venue_book_type,
                trade_execution=False,
                bar_execution=False,
                queue_position=False,
                fee_model=MakerTakerFeeModel(
                    maker_rate=Decimal(0), taker_rate=Decimal(0)
                ),
            )
        for instrument in exported.instruments:
            engine.add_instrument(instrument)
            books[str(instrument.id)] = OrderBook(instrument.id, BookType.L2_MBP)
        for item in exported.data:
            if type(item).__name__ == "OrderBookDeltas":
                books[str(item.instrument_id)].apply_deltas(item)
        engine.add_data(exported.data)
        engine.run()
        require(
            engine.iteration == len(exported.data), "ENGINE_DID_NOT_CONSUME_ALL_RECORDS"
        )
        # The inspected Windows 2.x wheel's aggregate result path converts
        # account money to f64 and aborts for 18-decimal currencies. Data
        # replay itself accepts them. Do not request an unsafe P&L summary.
        high_precision = any(
            c.precision > 16 for values in currencies.values() for c in values.values()
        )
        result = None if high_precision else engine.get_result()
        for ident, book in books.items():
            expected[ident] = dict(
                best_bid=str(book.best_bid_price())
                if book.best_bid_price() is not None
                else None,
                best_ask=str(book.best_ask_price())
                if book.best_ask_price() is not None
                else None,
            )
        return dict(
            schema="nautilus-offline-replay-probe/1",
            success=True,
            instruments=len(exported.instruments),
            input_records=len(exported.data),
            iterations=engine.iteration,
            total_events=getattr(result, "total_events", None),
            total_orders=getattr(result, "total_orders", None),
            independent_l2_replay=expected,
            account_result_aggregation_skipped=high_precision,
            account_result_limitation="installed wheel f64 aggregation cannot handle currency precision above 16"
            if high_precision
            else None,
            no_strategy_attached=True,
            trade_based_fills_enabled=False,
            profitability_tested=False,
            fee_model="explicit_zero_for_no_order_plumbing_only_not_a_venue_fee_claim",
            source_availability_clocks_preserved=True,
            venue_book_types=venue_book_types,
            l2_execution_inputs_complete=all(
                v["l2_inputs_complete"] for v in venue_book_types.values()
            ),
        )
    finally:
        engine.dispose()
