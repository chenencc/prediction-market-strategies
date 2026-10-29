"""Offline nautilus-source/1 reader; no collector, credentials or network imports.

Venue executions, API observations and local content IDs are distinct concepts.
Unsafe books are cleared at gaps and stay unusable until a fresh snapshot.
"""

from collections import Counter, defaultdict
from decimal import Decimal, localcontext
import json
from pathlib import Path

from .recording_group import require, file_hash, digest
from .nautilus_exporter import ExportedGroup
from ..adapters.common import binary_instrument, book_snapshot

D = Decimal
NS = 1_000_000_000


def scaled(value, unit):
    require(unit in {"native_wei", "probability", "shares"}, "UNKNOWN_SOURCE_UNIT")
    require(not isinstance(value, (float, bool)), "EXACT_SOURCE_NUMBER_REQUIRED")
    with localcontext() as ctx:
        ctx.prec = 100
        result = D(str(value))
        require(result.is_finite() and result >= 0, "INVALID_SOURCE_NUMBER")
        if unit == "native_wei":
            require(result == result.to_integral_value(), "NATIVE_INTEGER_REQUIRED")
            result = result.scaleb(-18)
        return result


def canonical_economics(row):
    """Fees can be amended independently. Do not use payload_id as execution ID."""
    return tuple(
        str(row[k])
        for k in (
            "market_id",
            "asset_id",
            "side",
            "execution_ns",
            "price_native",
            "size_native",
            "price_unit",
            "size_unit",
            "transaction_hash",
        )
    )


def trade_identity(venue, row):
    if venue == "predictfun":
        require(row.get("settlement_id"), "MISSING_SETTLEMENT_ID")
        # This is an observed aggregate taker match, not every maker fill.
        return (venue, row["market_id"], row["settlement_id"])
    require(row.get("transaction_hash"), "MISSING_TRADE_TRANSACTION")
    return (venue,) + canonical_economics(row)


def side_for_outcome(side):
    require(side in {"BUY", "SELL", "Bid", "Ask"}, "UNKNOWN_AGGRESSOR")
    # Bid/Ask is on the taker's own outcome, including indexSet=2.
    return "BUY" if side in {"BUY", "Bid"} else "SELL"


def gap_mask(events, gaps):
    """Input (received_ns, sequence, kind, token, payload); output plus audit.

    No scope evidence => venue-wide invalidation. An update cannot heal a gap.
    Clear synthetic control events are visibly identified in the returned audit.
    """
    pending, ready, output, notices = set(), set(), [], []
    starts = sorted(gaps, key=lambda g: g["start_ns"])
    cursor, suppressed = 0, Counter()
    for at, seq, kind, token, row in sorted(events, key=lambda x: (x[0], x[1])):
        while cursor < len(starts) and starts[cursor]["start_ns"] <= at:
            g = starts[cursor]
            for active in sorted(ready):
                output.append((g["start_ns"], -1, "gap_clear", active, g))
            pending.update(ready)
            ready.clear()
            notices.append(
                dict(
                    **g, scope="whole_venue_conservative", healing="fresh_snapshot_only"
                )
            )
            cursor += 1
        in_gap = any(g["start_ns"] <= at <= g["end_ns"] for g in starts)
        if in_gap:
            pending.add(token)
            suppressed[kind] += 1
            continue
        if kind == "book":
            ready.add(token)
            pending.discard(token)
        elif kind == "price_change" and token not in ready:
            suppressed["update_without_valid_snapshot"] += 1
            continue
        output.append((at, seq, kind, token, row))
    return output, dict(
        gaps=notices, suppressed=dict(suppressed), awaiting_snapshot=sorted(pending)
    )


class CompactSource:
    def __init__(self, root):
        import pyarrow.parquet as pq

        self.root = Path(root).resolve()
        self.manifest = json.loads((self.root / "manifest.json").read_text("utf-8"))
        require(self.manifest["schema"] == "nautilus-source/1", "COMPACT_SOURCE_SCHEMA")
        self.entries = {}
        for item in self.manifest["files"]:
            name = item["file"]
            require(name not in self.entries, "DUPLICATE_MANIFEST_FILE")
            p = (self.root / name).resolve()
            require(
                p.is_relative_to(self.root) and p.is_file() and not p.is_symlink(),
                "UNSAFE_SOURCE_PATH",
            )
            require(
                p.stat().st_size == item["bytes"] and file_hash(p) == item["sha256"],
                "SOURCE_HASH_MISMATCH",
            )
            require(
                pq.ParquetFile(p).metadata.num_rows == item["rows"], "SOURCE_ROW_COUNT"
            )
            self.entries[name] = item
        require(
            set(self.entries)
            == {
                p.relative_to(self.root).as_posix()
                for p in self.root.rglob("*.parquet")
            },
            "SOURCE_FILE_SET",
        )
        auxiliary = {}
        for item in self.manifest["auxiliary_files"]:
            p = (self.root / item["file"]).resolve()
            require(
                item["file"] not in auxiliary
                and p.is_relative_to(self.root)
                and not p.is_symlink()
                and p.is_file(),
                "UNSAFE_AUXILIARY_PATH",
            )
            require(file_hash(p) == item["sha256"], "AUXILIARY_HASH_MISMATCH")
            auxiliary[item["file"]] = item
        require(
            {
                p.relative_to(self.root).as_posix()
                for p in self.root.glob("v3/pairs/**/*.json")
            }
            <= set(auxiliary),
            "UNBOUND_PAIR_RELATION",
        )
        self.start = self.manifest["window"]["start_ns"]
        self.end = self.manifest["window"]["end_ns"]
        self.pairs = [
            json.loads(p.read_text("utf-8"))
            for p in sorted(self.root.glob("v3/pairs/**/*.json"))
        ]
        require(
            len(self.pairs) == self.manifest["pair_relations"]
            and len({p["pair_id"] for p in self.pairs}) == len(self.pairs),
            "PAIR_RELATION_COUNT",
        )

    def rows(self, kind, venue, filename=None):
        import pyarrow as pa
        import pyarrow.parquet as pq

        prefix = f"v3/{kind}/venue={venue}/"
        result = []
        for name in sorted(self.entries):
            if not name.startswith(prefix) or (
                filename and Path(name).name != filename
            ):
                continue
            t = pq.ParquetFile(self.root / name, page_checksum_verification=True).read()
            # Avoid datetime microsecond rounding (and Windows timezone package dependency).
            for idx, f in enumerate(t.schema):
                if pa.types.is_timestamp(f.type):
                    t = t.set_column(idx, f.name, t.column(idx).cast(pa.int64()))
            result.extend(t.to_pylist())
        return result

    def api_trades(self, venue, selected):
        payloads = {r["payload_id"]: r for r in self.rows("trade_content", venue)}
        contents = {
            r["content_id"]: r["occurrences"]
            for r in self.rows("trade_page_content", venue)
        }
        canonical, economics, multiplicity = {}, {}, Counter()
        counts, revisions = Counter(), []
        for page in sorted(
            self.rows("trade_pages", venue),
            key=lambda r: (r["available_at_ns"], r["sequence"]),
        ):
            if page["market_id"] not in selected:
                continue
            occurrences = contents[page["content_id"]]
            require(len(occurrences) == page["returned_rows"], "PAGE_MULTIPLICITY")
            require(
                [r["row_index"] for r in occurrences] == list(range(len(occurrences))),
                "PAGE_ORDER",
            )
            page_counts = Counter()
            for occurrence in occurrences:
                row = payloads[occurrence["payload_id"]]
                require(row["market_id"] == page["market_id"], "PAGE_MARKET_BINDING")
                counts["observations"] += 1
                if not self.start <= row["execution_ns"] < self.end:
                    counts["outside_window"] += 1
                    continue
                try:
                    key = trade_identity(venue, row)
                except ValueError:
                    counts["quarantined_missing_identity"] += 1
                    continue
                fingerprint = canonical_economics(row)
                require(
                    key not in economics or economics[key] == fingerprint,
                    "CONFLICTING_STABLE_TRADE_ID",
                )
                economics[key] = fingerprint
                if venue == "polymarket":
                    # Preserve same-page identical-row multiplicity, but flag ambiguity.
                    page_counts[key] += 1
                    ordinal = page_counts[key]
                    multiplicity[key] = max(multiplicity[key], ordinal)
                else:
                    ordinal = 1
                identity = key + (ordinal,)
                if identity in canonical:
                    counts["repeat_observations"] += 1
                    if (
                        canonical[identity]["row"]["fee_bundle_id"]
                        != row["fee_bundle_id"]
                    ):
                        revisions.append(
                            dict(
                                identity=digest(identity),
                                available_at_ns=page["available_at_ns"],
                                fee_bundle_id=row["fee_bundle_id"],
                            )
                        )
                    continue
                require(
                    row["execution_ns"] <= page["available_at_ns"],
                    "TRADE_EXECUTION_AFTER_RECEIPT",
                )
                canonical[identity] = dict(
                    row=row,
                    available_at_ns=page["available_at_ns"],
                    identity=digest(identity),
                    identity_kind="settlement_aggregate"
                    if venue == "predictfun"
                    else "transaction_economics_multiplicity",
                )
        counts["canonical_observed"] = len(canonical)
        counts["ambiguous_multiplicity_groups"] = sum(
            v > 1 for v in multiplicity.values()
        )
        return list(canonical.values()), dict(
            counts=dict(counts), fee_revisions=revisions
        )

    def convert(self, *, pair_ids=None):
        from nautilus_trader.model import (
            BookOrder,
            OrderBookDelta,
            OrderBookDeltas,
            BookAction,
            OrderSide,
            RecordFlag,
            Price,
            Quantity,
            TradeTick,
            TradeId,
            AggressorSide,
        )

        pairs = (
            self.pairs
            if pair_ids is None
            else [r for r in self.pairs if r["pair_id"] in pair_ids]
        )
        require(
            pairs and (pair_ids is None or len(pairs) == len(set(pair_ids))),
            "PAIR_SELECTION",
        )
        selected = {
            v: {
                m.split(":", 1)[1]
                for r in pairs
                for m in r["members"]
                if m.startswith(v + ":")
            }
            for v in ("polymarket", "predictfun")
        }
        instruments, data, inputs, reports = [], [], [], {}
        for venue, mids in selected.items():
            definitions = {
                r["market_version_id"]: r for r in self.rows("instruments", venue)
            }
            fees = {r["fee_version_id"]: r for r in self.rows("fee_policies", venue)}
            versions = sorted(
                [
                    r
                    for r in self.rows("instrument_versions", venue)
                    if r["market_id"] in mids
                ],
                key=lambda r: (r["available_at_ns"], r["sequence"]),
            )
            initial = {}
            for r in versions:
                initial.setdefault(r["market_id"], r)
            require(set(initial) == mids, "MISSING_MARKET_DEFINITION")
            tokens, specs = {}, {}
            for mid, r in initial.items():
                meta = definitions[r["market_version_id"]]
                fee = fees[r["fee_version_id"]]
                contract = json.loads(meta["contract_metadata_json"])
                if venue == "polymarket":
                    require(
                        contract.get("version") == "v1", "UNSUPPORTED_POLY_PROTOCOL"
                    )
                    require(
                        meta["price_increment"] and meta["minimum_order_size"],
                        "POLY_SPEC_REQUIRED",
                    )
                    tick, minimum, currency, places = (
                        D(meta["price_increment"]),
                        meta["minimum_order_size"],
                        "pUSD",
                        6,
                    )
                    definition_source = "nautechsystems/nautilus_trader@698d5cc8:http/parse.rs (pUSD,6 decimals)"
                else:
                    require(
                        type(meta["price_decimals"]) is int,
                        "PREDICT_QUOTE_GRID_REQUIRED",
                    )
                    tick = D(1).scaleb(-meta["price_decimals"])
                    minimum, currency, places = "0.01", "USDT", 18
                    definition_source = "PredictDotFun/sdk@5ff2b4a1:OrderBuilder.ts,Constants.ts (SDK helper,not exchange minimum certification)"
                require(fee["configuration_status"] == "reported", "UNKNOWN_FEE_POLICY")
                if venue == "polymarket":
                    disabled = fee["fees_enabled"] is False
                    require(
                        disabled
                        or (
                            fee["schedule_rate"] is not None
                            and fee["schedule_exponent"] == "1"
                            and fee["schedule_taker_only"] is True
                        ),
                        "UNKNOWN_POLY_FEE_FORMULA",
                    )
                    schedule = dict(
                        rate="0" if disabled else fee["schedule_rate"],
                        exponent="1",
                        takerOnly=True,
                        rebateRate="0",
                    )
                else:
                    require(
                        fee["taker_fee_bps"] is not None
                        and fee["maker_fee_bps"] is not None,
                        "UNKNOWN_PREDICT_FEE_RATE",
                    )
                    schedule = None
                spec = dict(
                    price_precision=18,
                    size_precision=18,
                    price_increment=format(tick, ".18f"),
                    size_increment="0.000000000000000001",
                    currency=currency,
                    currency_precision=places,
                )
                specs[mid] = spec
                # Explicit as-of timelines are retained. A material revision is blocked rather than silently using opening terms.
                material = []
                for vr in versions:
                    if vr["market_id"] != mid:
                        continue
                    md = definitions[vr["market_version_id"]]
                    if vr["fee_version_id"] != r["fee_version_id"] or (
                        md["price_increment"],
                        md["price_decimals"],
                    ) != (meta["price_increment"], meta["price_decimals"]):
                        material.append(vr["available_at_ns"])
                for outcome in json.loads(meta["instruments_json"]):
                    token = outcome["asset_id"]
                    info = dict(
                        min_order_size=minimum,
                        fee_schedule=schedule,
                        observed_fee_policy=fee,
                        spec_provenance=definition_source,
                        native_quote_grid=str(tick),
                        condition_id=meta["condition_id"],
                        is_neg_risk=meta["is_neg_risk"],
                        source_contract_metadata=contract,
                        quantity_increment_kind="model_exact_representation_not_exchange_lot",
                        sdk_minimum_not_exchange_certification=venue == "predictfun",
                        native_index_set=outcome["index_set"],
                        book_origin=outcome["book_origin"],
                        shared_liquidity_group=venue + ":" + mid,
                        independent_complement_execution=False,
                        material_revision_at_ns=material,
                        order_execution_enabled=False,
                        market_version=contract.get("version"),
                    )
                    inst = binary_instrument(
                        venue=venue,
                        market_id=mid,
                        condition_id=meta["condition_id"],
                        token=token,
                        outcome=outcome["label"],
                        terms=dict(question=meta["question"]),
                        spec=spec,
                        observed_ns=r["available_at_ns"],
                        revision=r["market_version_id"],
                        extra_info=info,
                    )
                    tokens[token] = inst
                    instruments.append(inst)
                    inputs.append(
                        dict(
                            instrument_id=str(inst.id),
                            parameters=spec,
                            market_version_id=r["market_version_id"],
                            fee_version_id=r["fee_version_id"],
                            observed_ns=r["available_at_ns"],
                        )
                    )
            event_rows = self.rows("events", venue)
            lifecycle = self.rows("events", venue, "market_status.parquet")
            terminal = {}
            for row in lifecycle:
                if row["market_id"] in mids and (
                    row["market_status"] in {"RESOLVED", "CANCELLED"}
                    or row["trading_status"] in {"CLOSED", "RESOLVED"}
                ):
                    terminal.setdefault(row["market_id"], row["timestamp_received"])
            # Load event kinds independently; a BBO or trade message is not a book update.
            ordered = []
            for kind in ("book", "price_change"):
                for row in self.rows("events", venue, kind + ".parquet"):
                    token = str(int.from_bytes(row["asset_id"], "big"))
                    if token in tokens:
                        ordered.append(
                            (
                                row["timestamp_received"],
                                row["sequence"],
                                kind,
                                token,
                                row,
                            )
                        )
            gaps = self.rows("gaps", venue)
            safe, gap_report = gap_mask(ordered, gaps)
            books = Counter()
            for inst in tokens.values():
                mid = inst.info["source_market_key"].split(":", 1)[1]
                if mid in terminal and terminal[mid] < self.end:
                    at = terminal[mid]
                    data.append(
                        OrderBookDeltas(
                            inst.id,
                            [
                                OrderBookDelta(
                                    inst.id,
                                    BookAction.CLEAR,
                                    BookOrder(
                                        OrderSide.NO_ORDER_SIDE,
                                        Price.from_str("0"),
                                        Quantity.from_str("0"),
                                        0,
                                    ),
                                    int(RecordFlag.F_LAST),
                                    0,
                                    at,
                                    at,
                                )
                            ],
                        )
                    )
                    books["terminal_clear_no_assumed_payout"] += 1
            for at, seq, kind, token, row in safe:
                inst = tokens[token]
                if at >= self.end:
                    continue
                mid = inst.info["source_market_key"].split(":", 1)[1]
                if mid in terminal and at >= terminal[mid]:
                    books["blocked_after_observed_terminal"] += 1
                    continue
                require(at >= inst.ts_init, "BOOK_BEFORE_INSTRUMENT_DEFINITION")
                if any(at >= x for x in inst.info["material_revision_at_ns"]):
                    books["blocked_material_spec_revision"] += 1
                    continue
                if kind == "book":
                    if row["timestamp"] == 0:
                        require(
                            not row["bids"] and not row["asks"],
                            "UNKNOWN_CLOCK_NONEMPTY_BOOK",
                        )
                        # Native empty state with no source clock. Keep zero;
                        # receipt time must never impersonate exchange time.
                        batch = OrderBookDeltas(
                            inst.id,
                            [
                                OrderBookDelta(
                                    inst.id,
                                    BookAction.CLEAR,
                                    BookOrder(
                                        OrderSide.NO_ORDER_SIDE,
                                        Price.from_str("0"),
                                        Quantity.from_str("0"),
                                        0,
                                    ),
                                    int(RecordFlag.F_SNAPSHOT) | int(RecordFlag.F_LAST),
                                    0,
                                    0,
                                    at,
                                )
                            ],
                        )
                        books["empty_snapshot_unknown_source_clock"] += 1
                    else:
                        batch = book_snapshot(
                            inst,
                            [(str(r["price"]), str(r["size"])) for r in row["bids"]],
                            [(str(r["price"]), str(r["size"])) for r in row["asks"]],
                            ts_event=row["timestamp"] * 1_000_000,
                            ts_init=at,
                        )
                elif kind == "gap_clear":
                    batch = OrderBookDeltas(
                        inst.id,
                        [
                            OrderBookDelta(
                                inst.id,
                                BookAction.CLEAR,
                                BookOrder(
                                    OrderSide.NO_ORDER_SIDE,
                                    Price.from_str("0"),
                                    Quantity.from_str("0"),
                                    0,
                                ),
                                int(RecordFlag.F_LAST),
                                0,
                                at,
                                at,
                            )
                        ],
                    )
                else:
                    side = OrderSide.BUY if row["side"] == "BUY" else OrderSide.SELL
                    require(row["side"] in {"BUY", "SELL"}, "UNKNOWN_BOOK_SIDE")
                    require(
                        D(str(row["price"])) % inst.price_increment.as_decimal() == 0,
                        "UPDATE_OFF_TICK",
                    )
                    batch = OrderBookDeltas(
                        inst.id,
                        [
                            OrderBookDelta(
                                inst.id,
                                BookAction.UPDATE if row["size"] else BookAction.DELETE,
                                BookOrder(
                                    side,
                                    Price.from_str(
                                        format(D(str(row["price"])), ".18f")
                                    ),
                                    Quantity.from_str(
                                        format(D(str(row["size"])), ".18f")
                                    ),
                                    0,
                                ),
                                int(RecordFlag.F_LAST),
                                0,
                                row["timestamp"] * 1_000_000,
                                at,
                            )
                        ],
                    )
                data.append(batch)
                books[kind] += 1
            api, trade_report = self.api_trades(venue, mids)
            # Native Poly WS fills are preferred; API supplies only missing observed executions.
            ws = defaultdict(list)
            if venue == "polymarket":
                for row in self.rows("events", venue, "last_trade_price.parquet"):
                    token = str(int.from_bytes(row["asset_id"], "big"))
                    if (
                        token not in tokens
                        or not self.start <= row["timestamp_received"] < self.end
                    ):
                        continue
                    key = (
                        "0x" + row["transaction_hash"].hex(),
                        token,
                        row["side"],
                        D(str(row["price"])),
                        D(str(row["size"])),
                        row["timestamp"] // 1000,
                    )
                    ws[key].append(row)
            consumed = Counter()
            ticks = []
            for value in api:
                row = value["row"]
                inst = tokens[row["asset_id"]]
                price, size = (
                    scaled(row["price_native"], row["price_unit"]),
                    scaled(row["size_native"], row["size_unit"]),
                )
                require(0 <= price <= 1 and size > 0, "INVALID_TRADE_ECONOMICS")
                at, ts = value["available_at_ns"], row["execution_ns"]
                # API execution clocks can be settlement clocks seconds after
                # WS, and the API may report an aggregate effective price.
                # Join only a UNIQUE same-tx/token/side/quantity group within
                # the recorded quote grid and a bounded 10-second clock skew.
                candidates = [
                    (key, native)
                    for key, values in ws.items()
                    if key[:3]
                    == (row["transaction_hash"], row["asset_id"], row["side"])
                    and key[4] == size
                    and abs(key[3] - price) <= inst.price_increment.as_decimal()
                    for native in values[consumed[key] :]
                    if abs(native["timestamp"] * 1_000_000 - ts) <= 10 * NS
                ]
                if len(candidates) == 1:
                    key, native = candidates[0]
                    trade_report.setdefault("ws_api_groups", [])
                    trade_report["ws_api_groups"].append(
                        dict(
                            identity=value["identity"],
                            api_execution_ns=ts,
                            ws_event_ns=native["timestamp"] * 1_000_000,
                            api_available_ns=at,
                            ws_available_ns=native["timestamp_received"],
                            api_effective_price=str(price),
                            ws_price=str(key[3]),
                            meaning="unique_same_transaction_outcome_quantity_group_not_atomic_fill_proof",
                        )
                    )
                    at, ts = (
                        native["timestamp_received"],
                        native["timestamp"] * 1_000_000,
                    )
                    price = key[3]
                    consumed[key] += 1
                    trade_report.setdefault("ws_api_matched", 0)
                    trade_report["ws_api_matched"] += 1
                elif any(
                    key[:3] == (row["transaction_hash"], row["asset_id"], row["side"])
                    for key in ws
                ):
                    trade_report.setdefault("ambiguous_api_groups_quarantined", 0)
                    trade_report["ambiguous_api_groups_quarantined"] += 1
                    continue
                ticks.append(
                    TradeTick(
                        inst.id,
                        Price.from_str(format(price, ".18f")),
                        Quantity.from_str(format(size, ".18f")),
                        AggressorSide.BUY
                        if side_for_outcome(row["side"]) == "BUY"
                        else AggressorSide.SELL,
                        TradeId("cmp-" + value["identity"][:32]),
                        ts,
                        at,
                    )
                )
            # Keep observed WS-only executions too, explicitly reported as unmatched.
            for key, values in ws.items():
                for native in values[consumed[key] :]:
                    inst = tokens[key[1]]
                    ticks.append(
                        TradeTick(
                            inst.id,
                            Price.from_str(format(key[3], ".18f")),
                            Quantity.from_str(format(key[4], ".18f")),
                            AggressorSide.BUY
                            if key[2] == "BUY"
                            else AggressorSide.SELL,
                            TradeId(
                                "ws-"
                                + digest([str(x) for x in key] + [native["sequence"]])[
                                    :32
                                ]
                            ),
                            native["timestamp"] * 1_000_000,
                            native["timestamp_received"],
                        )
                    )
                    trade_report.setdefault("ws_only", 0)
                    trade_report["ws_only"] += 1
            data.extend(ticks)
            reports[venue] = dict(
                books=dict(books),
                trades=trade_report,
                gap_policy=gap_report,
                trade_ticks=len(ticks),
                derived_no_book_emitted=False,
                source_event_rows=len(event_rows),
                observed_aggregate_matches=api,
                fee_bundles=self.rows("trade_fees", venue),
                lifecycle=lifecycle,
                terminal_markets_no_assumed_payout=terminal,
            )
        data.sort(key=lambda r: r.ts_init)
        return ExportedGroup(
            instruments,
            data,
            dict(
                schema="compact-nautilus-import/1",
                source_manifest_sha256=file_hash(self.root / "manifest.json"),
                source_schema="nautilus-source/1",
                source_window=self.manifest["window"],
                pair_ids=[p["pair_id"] for p in pairs],
                instrument_inputs=inputs,
                data_records=len(data),
                venues=reports,
                clocks="source_event_and_actual_available_ns",
                strict_queue_position_certified=False,
                complete_trade_coverage=False,
                network_requests=0,
            ),
        )
