"""Offline, bounded conversion of verified recording groups to Nautilus objects.

No HTTP clients, account credentials or live execution. Explicit source inputs
are mandatory: there are no venue precision, currency or fee defaults here.
"""

from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import platform
import threading

from .recording_group import (
    RecordingGroup,
    InputError,
    require,
    digest,
    file_hash,
)

_currency_read_lock = threading.RLock()


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
    pp = observed(terms, ("pricePrecision",), "MISSING_PRICE_PRECISION", precision)
    sp = observed(
        terms,
        ("sizePrecision", "quantityPrecision", "decimalPrecision"),
        "MISSING_QUANTITY_PRECISION",
        precision,
    )
    tick = observed(
        terms,
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


@dataclass
class ExportedGroup:
    instruments: list
    data: list
    report: dict


def event_hash(value):
    """Exact event semantics; CLEAR's unused precision is catalog-normalized."""
    base = [
        type(value).__name__,
        str(value.instrument_id),
        int(value.ts_event),
        int(value.ts_init),
    ]
    if type(value).__name__ == "OrderBookDeltas":
        rows = []
        for d in value.deltas:
            clear = str(d.action) == "CLEAR"
            rows.append(
                [
                    str(d.action),
                    str(d.order.side),
                    None if clear else str(d.order.price.as_decimal().normalize()),
                    None if clear else str(d.order.size.as_decimal().normalize()),
                    d.order.order_id,
                    int(d.flags),
                    d.sequence,
                    int(d.ts_event),
                    int(d.ts_init),
                ]
            )
        return digest(base + [rows])
    return digest(
        base
        + [
            str(value.price.as_decimal().normalize()),
            str(value.size.as_decimal().normalize()),
            str(value.aggressor_side),
            str(value.trade_id),
        ]
    )


def instrument_hash(value):
    return digest(
        [
            str(value.id),
            str(value.raw_symbol),
            value.price_precision,
            value.size_precision,
            str(value.price_increment),
            str(value.size_increment),
            str(value.currency),
            value.currency.precision,
            value.activation_ns,
            value.expiration_ns,
            value.outcome,
            value.info,
        ]
    )


class NautilusExporter:
    def __init__(
        self, *, max_records=250_000, max_deltas=2_000_000, trade_mode="observed"
    ):
        require(
            type(max_records) is int and 1 <= max_records <= 2_000_000,
            "EXPORT_RECORD_BOUND",
        )
        require(
            type(max_deltas) is int and 1 <= max_deltas <= 10_000_000,
            "EXPORT_DELTA_BOUND",
        )
        require(trade_mode in {"exclude", "observed"}, "TRADE_MODE")
        self.max_records = max_records
        self.max_deltas = max_deltas
        self.trade_mode = trade_mode

    def convert(self, source):
        import nautilus_trader as nt

        require(nt.__version__.startswith("2."), "NAUTILUS_2_RUNTIME_REQUIRED")
        from nautilus_trader.model import (
            BinaryOption,
            InstrumentId,
            Symbol,
            Currency,
            CurrencyType,
            Price,
            Quantity,
            AssetClass,
            BookOrder,
            OrderBookDelta,
            OrderBookDeltas,
            BookAction,
            OrderSide,
            RecordFlag,
            TradeTick,
            TradeId,
            AggressorSide,
        )

        group = RecordingGroup(source)
        instruments = []
        mapping = {}
        by_market = {}
        specs = {}
        data = []
        serial = 0
        delta_count = 0
        report = dict(
            schema="nautilus-recording-export/1",
            source_plan_id=group.manifest["plan_id"],
            source_group_id=group.manifest["group_id"],
            source_manifest_hash=group.manifest["manifest_hash"],
            source_metadata_policy="instrument-metadata/1",
            task_type=group.context["task_type"],
            nautilus_version=nt.__version__,
            python=platform.python_version(),
            trade_mode=self.trade_mode,
            sampled_book_policy="independent_L2_snapshots_not_exchange_event_deltas",
            ts_event_policy="recorded_source_clock",
            ts_init_policy="recorded_response_availability",
            synthetic_timestamp_offsets=False,
            source_modified=False,
            gaps=[],
            trade_counts={},
            instrument_inputs=[],
            live_execution_enabled=False,
            execution_simulation_certified=False,
            complete_trade_coverage=False,
            inherited_limitations=group.context.get("backtest_assumptions", {}),
        )
        for key, market in sorted(group.markets.items()):
            require(market["observations"], "NO_OBSERVED_INSTRUMENT:" + key)
            first = min(
                market["observations"],
                key=lambda o: (o["received_unix_ns"], o["receipt_sequence"]),
            )
            terms = group.terms_at(key, first["received_unix_ns"])
            p = parameters(terms, market["venue"])
            specs[key] = p
            for observation in market["observations"]:
                later = group.terms_at(
                    key,
                    observation["received_unix_ns"],
                    key + ":" + observation["revision"],
                )
                require(
                    parameters(later, market["venue"]) == p,
                    "INSTRUMENT_EXECUTION_TERMS_CHANGED:" + key,
                )
            # Register only observed currencies. No USDC/pUSD or decimal default.
            cur = Currency(
                p["currency"],
                p["currency_precision"],
                0,
                p["currency"],
                CurrencyType.CRYPTO,
            )
            parents = {
                str(e["id"])
                for e in terms.get("events") or []
                if e.get("id") is not None
            }
            for field in ("event", "parentEvent"):
                parent = terms.get(field)
                items = parent if isinstance(parent, list) else [parent]
                parents.update(
                    str(e["id"])
                    for e in items
                    if isinstance(e, dict) and e.get("id") is not None
                )
            parents.update(
                str(terms[f])
                for f in ("eventId", "event_id")
                if terms.get(f) not in (None, "")
            )
            event_id = next(iter(parents)) if len(parents) == 1 else None
            by_market[key] = []
            for outcome, token in sorted(market["instrument_mapping"].items()):
                require(isinstance(token, str) and token, "TOKEN_ID_REQUIRED")
                venue = (
                    "POLYMARKET" if market["venue"] == "polymarket" else "PREDICTFUN"
                )
                symbol = (
                    (
                        market["condition_id"]
                        if market["venue"] == "polymarket"
                        else market["market_id"]
                    )
                    + "-"
                    + token
                )
                ident = InstrumentId.from_str(symbol + "." + venue)
                info = dict(
                    source_market_key=key,
                    source_metadata_revision=first["revision"],
                    observed_terms=terms,
                    scheduled_end_not_observed_closure=True,
                    fee_schedule=terms.get("feeSchedule"),
                )
                # A scheduled endDate is not proof of actual trading closure.
                instrument = BinaryOption(
                    instrument_id=ident,
                    raw_symbol=Symbol(token),
                    asset_class=AssetClass.ALTERNATIVE,
                    currency=cur,
                    activation_ns=0,
                    expiration_ns=0,
                    price_precision=p["price_precision"],
                    size_precision=p["size_precision"],
                    price_increment=Price.from_str(p["price_increment"]),
                    size_increment=Quantity.from_str(p["size_increment"]),
                    ts_event=first["received_unix_ns"],
                    ts_init=first["received_unix_ns"],
                    outcome=outcome,
                    description=str(terms.get("question", terms.get("title", key))),
                    event_id=event_id,
                    info=info,
                )
                mapping[key, token] = instrument
                by_market[key].append(instrument)
                instruments.append(instrument)
            report["instrument_inputs"].append(
                dict(
                    market_key=key,
                    parameters=p,
                    metadata_revision=first["revision"],
                    observed_at_ns=first["received_unix_ns"],
                    parent_event_id=event_id,
                    inferred_precision_or_currency=False,
                    fees_missing_are_zero=False,
                )
            )

        def append(value):
            nonlocal serial, delta_count
            require(serial < self.max_records, "EXPORT_RECORD_CAP_REACHED")
            data.append((int(value.ts_init), serial, value))
            serial += 1
            if type(value).__name__ == "OrderBookDeltas":
                delta_count += len(value.deltas)
                require(delta_count <= self.max_deltas, "EXPORT_DELTA_CAP_REACHED")

        flags = int(RecordFlag.F_SNAPSHOT) | int(RecordFlag.F_LAST)
        for row in group.iter_rows("orderbook.parquet"):
            key = row["venue"] + ":" + row["market_id"]
            received = row["received_unix_ns"]
            require(
                isinstance(received, int) and received > 0, "BOOK_RECEIVE_TIME_REQUIRED"
            )
            if row["kind"] == "book":
                instrument = mapping[key, row["asset_id"]]
                p = specs[key]
                require(
                    parameters(
                        group.terms_at(key, received, row["metadata_ref"]), row["venue"]
                    )
                    == p,
                    "BOOK_TERMS_MISMATCH",
                )
                require(
                    isinstance(row["source_time_ms"], int)
                    and row["source_time_ms"] > 0,
                    "BOOK_SOURCE_TIME_REQUIRED",
                )
                event = row["source_time_ms"] * 1_000_000
                book = row["book"]
                require(
                    not book.get("derived"),
                    "DERIVED_BOOK_REQUIRES_EXPLICIT_LIQUIDITY_MODEL",
                )
                deltas = [OrderBookDelta.clear(instrument.id, 0, event, received)]
                seen = set()
                for side, order_side in [
                    ("bids", OrderSide.BUY),
                    ("asks", OrderSide.SELL),
                ]:
                    for price, size in book[side]:
                        require(
                            delta_count + len(deltas) < self.max_deltas,
                            "EXPORT_DELTA_CAP_REACHED",
                        )
                        px, q = decimal(price), decimal(size)
                        require(
                            0 <= px <= 1 and q > 0 and (side, px) not in seen,
                            "INVALID_L2_LEVEL",
                        )
                        seen.add((side, px))
                        require(
                            px % decimal(p["price_increment"]) == 0,
                            "LEVEL_OFF_PRICE_TICK",
                        )
                        order = BookOrder(
                            order_side,
                            Price.from_str(fixed(px, p["price_precision"])),
                            Quantity.from_str(fixed(q, p["size_precision"])),
                            0,
                        )
                        deltas.append(
                            OrderBookDelta(
                                instrument.id,
                                BookAction.ADD,
                                order,
                                int(RecordFlag.F_SNAPSHOT),
                                0,
                                event,
                                received,
                            )
                        )
                last = deltas[-1]
                deltas[-1] = OrderBookDelta(
                    instrument.id,
                    last.action,
                    last.order,
                    int(last.flags) | flags,
                    0,
                    event,
                    received,
                )
                append(OrderBookDeltas(instrument.id, deltas))
            elif row.get("book_reset"):
                report["gaps"].append(
                    dict(
                        market_key=key,
                        received_unix_ns=received,
                        source_kind=row["kind"],
                        context=row.get("context_json"),
                    )
                )
                # These CLEARs are synthetic gap fences, not observed empty books.
                for instrument in by_market[key]:
                    clear = OrderBookDelta.clear(instrument.id, 0, received, received)
                    clear = OrderBookDelta(
                        instrument.id,
                        clear.action,
                        clear.order,
                        flags,
                        0,
                        received,
                        received,
                    )
                    append(OrderBookDeltas(instrument.id, [clear]))
        counts = Counter()
        seen = {}
        for row in group.iter_rows("trades.parquet"):
            if row["kind"] != "trade":
                continue
            counts["observations"] += 1
            if not row["within_requested_window"]:
                counts["outside_window"] += 1
                continue
            if self.trade_mode == "exclude":
                counts["excluded_by_policy"] += 1
                continue
            if row["identity_kind"] == "ambiguous_fingerprint":
                counts["ambiguous_quarantined"] += 1
                continue
            require(
                row["identity_kind"] == "settlement_id"
                and row["venue"] == "predictfun",
                "UNSUPPORTED_TRADE_IDENTITY",
            )
            key = row["venue"] + ":" + row["market_id"]
            identity = (key, row["asset_id"], row["event_key"])
            if identity in seen:
                require(
                    seen[identity] == row["fingerprint"], "CONFLICTING_STABLE_TRADE_ID"
                )
                counts["duplicate_observations"] += 1
                continue
            seen[identity] = row["fingerprint"]
            instrument = mapping[key, row["asset_id"]]
            p = specs[key]
            side = {
                "BUY": AggressorSide.BUY,
                "SELL": AggressorSide.SELL,
                "Bid": AggressorSide.BUY,
                "Ask": AggressorSide.SELL,
            }.get(row["side_raw"])
            require(side is not None, "UNKNOWN_AGGRESSOR_SIDE")
            require(
                row["executed_unix_ns"] <= row["received_unix_ns"],
                "TRADE_EXECUTION_AFTER_RECEIPT",
            )
            require(
                0 <= decimal(row["price"]) <= 1 and decimal(row["quantity"]) > 0,
                "INVALID_TRADE_VALUE",
            )
            tick = TradeTick(
                instrument.id,
                Price.from_str(fixed(row["price"], p["price_precision"])),
                Quantity.from_str(fixed(row["quantity"], p["size_precision"])),
                side,
                TradeId("pms-" + digest(identity)[:32]),
                row["executed_unix_ns"],
                row["received_unix_ns"],
            )
            append(tick)
            counts["canonical_observed_trades"] += 1
        data.sort(key=lambda x: (x[0], x[1]))
        report["trade_counts"] = dict(counts)
        report.update(
            instruments=len(instruments),
            data_records=len(data),
            book_batches=sum(type(x[2]).__name__ == "OrderBookDeltas" for x in data),
            trade_ticks=sum(type(x[2]).__name__ == "TradeTick" for x in data),
            source_verification="manifest_SHA_page_checksums_and_full_logical_semantic_replay",
            prediction_market_protocol_defaults_used=False,
            predict_complement_generated=False,
            same_timestamp_order="source_receipt_order_then_stable_export_order",
            policy_limitations=[
                "No verified Polymarket canonical fill identity in the base API rows: ambiguous rows are quarantined",
                "Historical observed trades retain late availability and are not live execution-time signals",
                "Instrument precision/currency/fee changes currently fail closed; dynamic instrument updates are not silently ignored",
                "Synthetic gap clears must be interpreted with the saved gap controls, not as real empty liquidity",
                "No expiry/settlement, guaranteed passive fills, complementary liquidity duplication or atomic multi-venue execution modeled",
            ],
        )
        return ExportedGroup(instruments, [x[2] for x in data], report)

    def export(self, source, target):
        from nautilus_trader.persistence import ParquetDataCatalog

        target = Path(target).resolve()
        source = Path(source).resolve()
        require(
            not target.exists() and not target.is_relative_to(source),
            "OUTPUT_MUST_BE_NEW_AND_INDEPENDENT",
        )
        result = self.convert(source)
        target.mkdir(parents=True)
        (target / "catalog").mkdir()
        catalog = ParquetDataCatalog(str(target / "catalog"))
        catalog.write_instruments(result.instruments)
        # Flatten batches only for catalog storage; F_LAST preserves boundaries.
        deltas = [
            delta
            for value in result.data
            if type(value).__name__ == "OrderBookDeltas"
            for delta in value.deltas
        ]
        trades = [value for value in result.data if type(value).__name__ == "TradeTick"]
        # Nautilus 2.x catalogs require one instrument identity per write.
        for values, write in [
            (deltas, catalog.write_order_book_deltas),
            (trades, catalog.write_trade_ticks),
        ]:
            grouped = defaultdict(list)
            for value in values:
                grouped[str(value.instrument_id)].append(value)
            for rows in grouped.values():
                write(rows)
        result.report["catalog_delta_rows"] = len(deltas)
        result.report["event_order"] = [event_hash(value) for value in result.data]
        result.report["instrument_hashes"] = {
            str(i.id): instrument_hash(i) for i in result.instruments
        }
        result.report["catalog_files"] = {
            p.relative_to(target).as_posix(): dict(
                bytes=p.stat().st_size, sha256=file_hash(p)
            )
            for p in sorted((target / "catalog").rglob("*"))
            if p.is_file()
        }
        (target / "export-report.json").write_text(
            json.dumps(result.report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        # The engine probe consumes a new catalog reader, not conversion memory.
        verified = self.read_export(target)
        (target / "export-report.json").write_text(
            json.dumps(verified.report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return verified

    @staticmethod
    def read_export(target):
        from nautilus_trader.persistence import ParquetDataCatalog
        from nautilus_trader.model import OrderBookDeltas, RecordFlag

        target = Path(target).resolve()
        report = json.loads((target / "export-report.json").read_text(encoding="utf-8"))
        require(
            {
                p.relative_to(target).as_posix()
                for p in (target / "catalog").rglob("*")
                if p.is_file()
            }
            == set(report["catalog_files"]),
            "CATALOG_FILE_SET_CHANGED",
        )
        for name, entry in report["catalog_files"].items():
            p = target / name
            require(
                p.resolve().is_relative_to(target)
                and not p.is_symlink()
                and p.is_file()
                and p.stat().st_size == entry["bytes"]
                and file_hash(p) == entry["sha256"],
                "CATALOG_FILE_CHANGED",
            )
        catalog = ParquetDataCatalog(str(target / "catalog"))
        # Nautilus stores currency codes, then resolves them through its global
        # registry on read. Explicitly bind saved definitions for this read;
        # restore prior registry entries immediately, never change the source.
        from nautilus_trader.model import Currency, CurrencyType

        currency_specs = {}
        previous = {}
        for row in report["instrument_inputs"]:
            p = row["parameters"]
            old = currency_specs.setdefault(p["currency"], p["currency_precision"])
            require(old == p["currency_precision"], "CURRENCY_CODE_PRECISION_CONFLICT")
        with _currency_read_lock:
            try:
                for code, places in currency_specs.items():
                    try:
                        previous[code] = Currency.from_str(code)
                    except ValueError:
                        pass
                    Currency.register(
                        Currency(code, places, 0, code, CurrencyType.CRYPTO),
                        overwrite=True,
                    )
                instruments = catalog.instruments()
            finally:
                for value in previous.values():
                    Currency.register(value, overwrite=True)
        require(
            {str(i.id): instrument_hash(i) for i in instruments}
            == report["instrument_hashes"],
            "CATALOG_INSTRUMENT_MISMATCH",
        )
        pending = defaultdict(list)
        events = []
        for row in catalog.query_order_book_deltas():
            ident = str(row.instrument_id)
            pending[ident].append(row)
            if int(row.flags) & int(RecordFlag.F_LAST):
                batch = pending.pop(ident)
                require(
                    all(
                        d.ts_init == row.ts_init and d.ts_event == row.ts_event
                        for d in batch
                    ),
                    "CATALOG_BATCH_CLOCK",
                )
                events.append(OrderBookDeltas(row.instrument_id, batch))
        require(not pending, "CATALOG_PARTIAL_BOOK_BATCH")
        events.extend(catalog.query_trade_ticks())
        by_hash = defaultdict(deque)
        for value in events:
            by_hash[event_hash(value)].append(value)
        restored = []
        for hashed in report["event_order"]:
            require(by_hash[hashed], "CATALOG_EVENT_MISMATCH")
            restored.append(by_hash[hashed].popleft())
        require(
            not any(by_hash.values()) and len(restored) == report["data_records"],
            "CATALOG_EVENT_COUNT",
        )
        report["catalog_cold_readback_verified"] = True
        return ExportedGroup(instruments, restored, report)
