"""Exact shared instrument/data mapping for offline venue adapters.

Native decimalPrecision means price precision for Predict.fun, never size.
No network, venue currency defaults, or global precision changes.
"""

from decimal import Decimal, InvalidOperation
from ..data.recording_group import InputError, require, digest


def binary_instrument(
    *,
    venue,
    market_id,
    condition_id,
    token,
    outcome,
    terms,
    spec,
    observed_ns,
    revision,
    parent_event_id=None,
    extra_info=None,
):
    from nautilus_trader.model import (
        BinaryOption,
        InstrumentId,
        Symbol,
        Currency,
        CurrencyType,
        AssetClass,
        Price,
        Quantity,
    )

    require(venue in {"polymarket", "predictfun"}, "UNSUPPORTED_VENUE")
    require(isinstance(token, str) and token, "TOKEN_ID_REQUIRED")
    symbol = (condition_id if venue == "polymarket" else market_id) + "-" + token
    native_venue = "POLYMARKET" if venue == "polymarket" else "PREDICTFUN"
    currency = Currency(
        spec["currency"],
        spec["currency_precision"],
        0,
        spec["currency"],
        CurrencyType.CRYPTO,
    )
    info = dict(
        source_market_key=venue + ":" + market_id,
        source_metadata_revision=revision,
        observed_terms=terms,
        scheduled_end_not_observed_closure=True,
        fee_schedule=terms.get("feeSchedule"),
    )
    if extra_info:
        info.update(extra_info)
    return BinaryOption(
        instrument_id=InstrumentId.from_str(symbol + "." + native_venue),
        raw_symbol=Symbol(token),
        asset_class=AssetClass.ALTERNATIVE,
        currency=currency,
        activation_ns=0,
        expiration_ns=0,
        price_precision=spec["price_precision"],
        size_precision=spec["size_precision"],
        price_increment=Price.from_str(spec["price_increment"]),
        size_increment=Quantity.from_str(spec["size_increment"]),
        ts_event=observed_ns,
        ts_init=observed_ns,
        outcome=outcome,
        description=str(
            terms.get("question", terms.get("title", venue + ":" + market_id))
        ),
        event_id=parent_event_id,
        info=info,
    )


def book_snapshot(instrument, bids, asks, *, ts_event, ts_init, max_deltas=2_000_000):
    """Independent sampled L2 snapshot, including a lone CLEAR for empty books."""
    from nautilus_trader.model import (
        BookOrder,
        OrderBookDelta,
        OrderBookDeltas,
        BookAction,
        OrderSide,
        RecordFlag,
        Price,
        Quantity,
    )

    require(
        type(ts_event) is int and ts_event > 0 and type(ts_init) is int and ts_init > 0,
        "BOOK_CLOCK_REQUIRED",
    )
    require(max_deltas >= 1, "EXPORT_DELTA_CAP_REACHED")
    rows = [OrderBookDelta.clear(instrument.id, 0, ts_event, ts_init)]
    seen = set()
    for levels, side in ((bids, OrderSide.BUY), (asks, OrderSide.SELL)):
        for price, size in levels:
            require(len(rows) < max_deltas, "EXPORT_DELTA_CAP_REACHED")
            px, q = decimal(price), decimal(size)
            require(
                0 <= px <= 1 and q > 0 and (side, px) not in seen, "INVALID_L2_LEVEL"
            )
            seen.add((side, px))
            require(
                px % instrument.price_increment.as_decimal() == 0,
                "LEVEL_OFF_PRICE_TICK",
            )
            order = BookOrder(
                side,
                Price.from_str(fixed(px, instrument.price_precision)),
                Quantity.from_str(fixed(q, instrument.size_precision)),
                0,
            )
            rows.append(
                OrderBookDelta(
                    instrument.id,
                    BookAction.ADD,
                    order,
                    int(RecordFlag.F_SNAPSHOT),
                    0,
                    ts_event,
                    ts_init,
                )
            )
    last = rows[-1]
    flags = int(RecordFlag.F_SNAPSHOT) | int(RecordFlag.F_LAST)
    rows[-1] = OrderBookDelta(
        instrument.id,
        last.action,
        last.order,
        int(last.flags) | flags,
        0,
        ts_event,
        ts_init,
    )
    return OrderBookDeltas(instrument.id, rows)


def predict_trade_tick(
    instrument, *, market_id, event_key, price, quantity, side_raw, ts_event, ts_init
):
    from nautilus_trader.model import Price, Quantity, TradeTick, TradeId, AggressorSide

    side = {
        "Bid": AggressorSide.BUY,
        "Ask": AggressorSide.SELL,
        "BUY": AggressorSide.BUY,
        "SELL": AggressorSide.SELL,
    }.get(side_raw)
    require(side is not None, "UNKNOWN_AGGRESSOR_SIDE")
    require(
        type(ts_event) is int
        and ts_event > 0
        and type(ts_init) is int
        and ts_init >= ts_event,
        "TRADE_EXECUTION_AFTER_RECEIPT",
    )
    require(0 <= decimal(price) <= 1 and decimal(quantity) > 0, "INVALID_TRADE_VALUE")
    require(str(instrument.id.venue) == "PREDICTFUN", "PREDICT_TRADE_VENUE")
    identity = ("predictfun:" + str(market_id), str(instrument.raw_symbol), event_key)
    return TradeTick(
        instrument.id,
        Price.from_str(fixed(price, instrument.price_precision)),
        Quantity.from_str(fixed(quantity, instrument.size_precision)),
        side,
        TradeId("pms-" + digest(identity)[:32]),
        ts_event,
        ts_init,
    )


def decimal(value):
    require(not isinstance(value, (bool, float)), "EXACT_DECIMAL_REQUIRED")
    try:
        v = Decimal(str(value))
    except InvalidOperation:
        raise InputError("INVALID_DECIMAL") from None
    require(v.is_finite(), "NONFINITE_DECIMAL")
    return v


def observed(terms, keys, code, convert=lambda x: x):
    values = [convert(terms[k]) for k in keys if terms.get(k) not in (None, "")]
    require(values, code)
    require(all(v == values[0] for v in values), "CONFLICTING_ALIASES:" + code)
    return values[0]


def precision(value):
    v = decimal(value)
    require(v == int(v) and 0 <= v <= 16, "INVALID_PRECISION")
    return int(v)


def currency_precision(value):
    v = decimal(value)
    require(v == int(v) and 0 <= v <= 18, "INVALID_CURRENCY_PRECISION")
    return int(v)


def fixed(value, places):
    v = decimal(value)
    require(
        v >= 0 and v == v.quantize(Decimal(1).scaleb(-places)),
        "UNREPRESENTABLE_PRECISION",
    )
    return format(v, f".{places}f")


def parameters(terms, venue):
    require(venue in {"polymarket", "predictfun"}, "UNSUPPORTED_VENUE")
    # Native quote-grid precision and model representation precision are distinct:
    # an aggregate executed price can have more digits than a resting quote.
    native_price_precision = (
        precision(terms["decimalPrecision"])
        if venue == "predictfun" and terms.get("decimalPrecision") is not None
        else None
    )
    pp = (
        observed(terms, ("pricePrecision",), "MISSING_PRICE_PRECISION", precision)
        if terms.get("pricePrecision") is not None or native_price_precision is None
        else native_price_precision
    )
    if native_price_precision is not None:
        require(pp >= native_price_precision, "MODEL_PRICE_PRECISION_BELOW_NATIVE_GRID")
    sp = observed(
        terms,
        (
            ("sizePrecision", "quantityPrecision")
            if venue == "predictfun"
            else ("sizePrecision", "quantityPrecision", "decimalPrecision")
        ),
        "MISSING_QUANTITY_PRECISION",
        precision,
    )
    price_terms = dict(terms)
    tick_keys = (
        "orderPriceMinTickSize",
        "minimum_tick_size",
        "priceTick",
        "tickSize",
        "minTickSize",
        "priceIncrement",
    )
    if native_price_precision is not None and not any(
        price_terms.get(k) not in (None, "") for k in tick_keys
    ):
        price_terms["priceTick"] = format(
            Decimal(1).scaleb(-native_price_precision), "f"
        )
    tick = observed(
        price_terms,
        (
            "orderPriceMinTickSize",
            "minimum_tick_size",
            "priceTick",
            "tickSize",
            "minTickSize",
            "priceIncrement",
        ),
        "MISSING_PRICE_TICK",
        decimal,
    )
    step = observed(
        terms,
        ("sizeIncrement", "quantityIncrement"),
        "MISSING_QUANTITY_INCREMENT",
        decimal,
    )
    currency = observed(
        terms,
        ("collateralCurrency", "currency", "quoteCurrency", "settlementCurrency"),
        "MISSING_COLLATERAL_CURRENCY",
        str,
    )
    decimals = observed(
        terms,
        ("collateralDecimals", "quoteTokenDecimals"),
        "MISSING_COLLATERAL_DECIMALS",
        currency_precision,
    )
    require(tick > 0 and step > 0, "NONPOSITIVE_INCREMENT")
    if native_price_precision is not None:
        unit = Decimal(1).scaleb(-native_price_precision)
        require(
            tick >= unit and tick % unit == 0,
            "PREDICT_TICK_CONFLICTS_WITH_NATIVE_PRECISION",
        )
    protocol = (
        observed(
            terms,
            ("version", "protocolVersion", "marketVersion"),
            "MISSING_PROTOCOL_VERSION",
            str,
        )
        if venue == "polymarket"
        else None
    )
    if venue == "polymarket":
        require(protocol == "v1", "UNSUPPORTED_POLYMARKET_PROTOCOL")
    return dict(
        price_precision=pp,
        size_precision=sp,
        price_increment=fixed(tick, pp),
        size_increment=fixed(step, sp),
        currency=currency,
        currency_precision=decimals,
        protocol=protocol,
        native_quote_decimal_precision=native_price_precision,
        price_tick_origin="documented_native_decimalPrecision_grid"
        if native_price_precision is not None
        and not any(terms.get(k) not in (None, "") for k in tick_keys)
        else "explicit_increment_field",
        fee_schedule=terms.get("feeSchedule"),
        fee_rate_bps=terms.get("feeRateBps"),
        fees_enabled=terms.get("feesEnabled"),
        fee_terms={
            k: terms[k]
            for k in (
                "feeSchedule",
                "feeRateBps",
                "feesEnabled",
                "feeType",
                "makerBaseFee",
                "takerBaseFee",
                "makerRebatesFeeShareBps",
            )
            if k in terms
        },
        collateral_contract_terms={
            k: terms[k]
            for k in (
                "collateralToken",
                "collateralTokenAddress",
                "collateralAddress",
                "quoteToken",
                "quoteTokenAddress",
                "settlementToken",
                "chainId",
                "chainID",
                "chain",
                "exchangeAddress",
                "negRiskExchangeAddress",
                "ctfAddress",
                "conditionalTokensAddress",
            )
            if k in terms
        },
    )
