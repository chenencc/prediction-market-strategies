"""One bounded simulated IOC against a retained real L2 snapshot, never live.

This proves matching plumbing, not latency/queue placement or profitable trading.
Predict's native account remains gross; its share fee is reconciled separately.
"""

from decimal import Decimal as D, localcontext


def matching_probe(exported, venue):
    from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
    from nautilus_trader.model import (
        BinaryOption,
        Venue,
        OmsType,
        AccountType,
        Money,
        BookType,
        OrderBook,
        OrderSide,
        TimeInForce,
        Price,
        Quantity,
        StrategyId,
        RecordFlag,
    )
    from nautilus_trader.trading import Strategy, StrategyConfig
    from nautilus_trader.execution import MakerTakerFeeModel
    from ..paper.fees import PolymarketNoRewardFeeModel, ThroughPriceFillModel
    from ..adapters.predictfun.accounting import estimate_fee_atoms
    from ..data.recording_group import require

    candidates = {
        str(i.id): i
        for i in exported.instruments
        if str(i.id.venue) == venue and i.info.get("book_origin") == "native"
    }
    chosen = None
    for event in exported.data:
        if (
            str(event.instrument_id) not in candidates
            or type(event).__name__ != "OrderBookDeltas"
        ):
            continue
        instrument = candidates[str(event.instrument_id)]
        if instrument.info.get("native_index_set") not in {None, 1}:
            continue
        if not any(
            str(d.action) == "CLEAR" and int(d.flags) & int(RecordFlag.F_SNAPSHOT)
            for d in event.deltas
        ):
            continue
        book = OrderBook(instrument.id, BookType.L2_MBP)
        book.apply_deltas(event)
        bid, ask = book.best_bid_price(), book.best_ask_price()
        if (
            bid is None
            or ask is None
            or not D("0.01") < bid.as_decimal() < ask.as_decimal() < D("0.99")
        ):
            continue
        if instrument.info.get("material_revision_at_ns"):
            continue
        quantity = D("5") if venue == "POLYMARKET" else D("1")
        if book.best_ask_size().as_decimal() < quantity:
            continue
        chosen = (instrument, event, bid, ask, quantity)
        break
    require(chosen is not None, "NO_SUITABLE_RETAINED_NATIVE_BOOK")
    instrument, event, bid, ask, quantity = chosen
    values = instrument.to_dict()
    # The real market's terminal payout is not inferred. Only this simulated
    # order fixture gets an explicit future expiry to exercise engine matching.
    values["expiration_ns"] = (
        exported.report["source_window"]["end_ns"] + 86_400_000_000_000
    )
    instrument = BinaryOption.from_dict(values)

    class OneIOC(Strategy):
        def __init__(self):
            super().__init__(
                StrategyConfig(
                    strategy_id=StrategyId("INPUT-ACCEPTANCE-001"),
                    log_events=False,
                    log_commands=False,
                )
            )
            self.sent = False
            self.fills = []
            self.rejects = []

        def on_start(self):
            self.subscribe_book_deltas(instrument.id, BookType.L2_MBP, managed=True)

        def on_book_deltas(self, deltas):
            if self.sent:
                return
            self.sent = True
            self.submit_order(
                self.order_factory.limit(
                    instrument.id,
                    OrderSide.BUY,
                    Quantity.from_str(format(quantity, ".18f")),
                    Price.from_str(str(ask)),
                    time_in_force=TimeInForce.IOC,
                )
            )

        def on_order_filled(self, fill):
            self.fills.append(
                dict(
                    quantity=str(fill.last_qty),
                    price=str(fill.last_px),
                    commission=str(fill.commission),
                    liquidity_side=str(fill.liquidity_side),
                    ts_event=fill.ts_event,
                )
            )

        def on_order_rejected(self, rejection):
            self.rejects.append(str(rejection))

    engine = BacktestEngine(
        BacktestEngineConfig(bypass_logging=True, run_analysis=False)
    )
    strategy = OneIOC()
    try:
        fee = (
            PolymarketNoRewardFeeModel()
            if venue == "POLYMARKET"
            else MakerTakerFeeModel(maker_rate=D(0), taker_rate=D(0))
        )
        engine.add_venue(
            venue=Venue(venue),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money(D("10000"), instrument.currency)],
            book_type=BookType.L2_MBP,
            trade_execution=False,
            bar_execution=False,
            queue_position=False,
            liquidity_consumption=True,
            fill_model=ThroughPriceFillModel(),
            fee_model=fee,
        )
        engine.add_instrument(instrument)
        engine.add_strategy(strategy)
        # Only the original captured batch; no synthetic additional liquidity.
        engine.add_data([event])
        engine.run()
        require(strategy.sent and not strategy.rejects, "SIMULATED_IOC_NOT_ACCEPTED")
        require(
            sum(D(r["quantity"]) for r in strategy.fills) == quantity,
            "SIMULATED_IOC_NOT_FULLY_MATCHED",
        )
        require(
            all(D(r["price"]) == ask.as_decimal() for r in strategy.fills),
            "SIMULATED_PRICE_DIFFERS_FROM_SOURCE_DEPTH",
        )
        result = dict(
            success=True,
            venue=venue,
            instrument_id=str(instrument.id),
            source_snapshot_received_ns=event.ts_init,
            source_snapshot_event_ns=event.ts_event,
            source_best_bid=str(bid),
            source_best_ask=str(ask),
            simulated_fills=strategy.fills,
            simulated_orders=1,
            live_orders=0,
            matching="retained_native_L2_taker_IOC_no_trade_based_fills_no_queue_priority",
            expiry="explicit_synthetic_future_for_matching_fixture_only",
            engine_pnl_summary_requested=False,
            profitability_certified=False,
        )
        if venue == "PREDICTFUN":
            with localcontext() as ctx:
                ctx.prec = 100
                fee_atoms, unit = estimate_fee_atoms(
                    int(ask.as_decimal() * 10**18),
                    int(quantity * 10**18),
                    instrument.info["observed_fee_policy"]["taker_fee_bps"],
                    "Bid",
                )
                result["predict_fee_inventory_overlay"] = dict(
                    status="estimate_separate_from_native_gross_position",
                    fee_unit=unit,
                    fee_atoms=str(fee_atoms),
                    gross_shares=str(quantity),
                    net_shares=str(quantity - D(fee_atoms) / 10**18),
                    quote_cash_spend=str(quantity * ask.as_decimal()),
                    native_engine_position_is_gross=True,
                    native_pnl_with_share_fees_certified=False,
                )
        return result
    finally:
        engine.dispose()
