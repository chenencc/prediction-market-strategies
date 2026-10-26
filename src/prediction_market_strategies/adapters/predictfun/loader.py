"""Offline Predict.fun native JSON -> public Nautilus data objects.

No HTTP, credentials, accounts or order submissions. Native response clocks are
always supplied by the caller. Extra instrument definitions are explicit,
market-bound, time-bound inputs rather than hidden venue defaults.
"""

from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import json
import re

from ...data.recording_group import InputError, require, digest
from ..common import (
    parameters,
    binary_instrument,
    book_snapshot,
    predict_trade_tick,
    decimal,
)

PREDICTFUN = "PREDICTFUN"
ADAPTER_SCHEMA = "predictfun-offline/1"
DEFINITION_FIELDS = frozenset(
    (
        "pricePrecision",
        "sizePrecision",
        "quantityPrecision",
        "sizeIncrement",
        "quantityIncrement",
        "priceIncrement",
        "priceTick",
        "collateralCurrency",
        "collateralDecimals",
        "collateralTokenAddress",
        "chainId",
        "exchangeAddress",
    )
)


def native_json(value, *, max_bytes=16 * 1024 * 1024):
    if isinstance(value, (str, bytes)):
        require(
            len(value.encode() if isinstance(value, str) else value) <= max_bytes,
            "PREDICT_PAYLOAD_TOO_LARGE",
        )
        value = json.loads(
            value,
            parse_float=str,
            parse_constant=lambda _: (_ for _ in ()).throw(
                InputError("NONFINITE_JSON")
            ),
        )
    else:
        value = deepcopy(value)
    require(isinstance(value, dict), "PREDICT_OBJECT_REQUIRED")
    return value


def clock(value):
    require(type(value) is int and 0 < value < 2**64, "PREDICT_RECEIPT_CLOCK_REQUIRED")
    return value


def native_id(value):
    require(
        type(value) in (str, int) and re.fullmatch(r"(0|[1-9][0-9]*)", str(value)),
        "PREDICT_NATIVE_ID_REQUIRED",
    )
    return str(value)


def atoms(value):
    """Protocol integer units, exactly scaled by 10**18; never use float."""
    raw = native_id(value)
    require(len(raw) <= 78 and int(raw) < 2**256, "PREDICT_UINT256_RANGE")
    with localcontext() as ctx:
        ctx.prec = 100
        return format(Decimal(raw).scaleb(-18), "f")


def execution_time(value):
    require(isinstance(value, str), "PREDICT_EXECUTION_TIME_REQUIRED")
    match = re.fullmatch(
        r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)", value
    )
    require(match is not None, "PREDICT_EXACT_TIMEZONE_TIME_REQUIRED")
    base = datetime.fromisoformat(match[1] + match[3].replace("Z", "+00:00"))
    delta = base.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 10**9 + int(
        (match[2] or "").ljust(9, "0")
    )


def definition_terms(raw, definition, received_ns):
    terms = deepcopy(raw)
    if definition is None:
        return terms, None
    require(
        isinstance(definition, dict)
        and definition.get("schema") == "predictfun-instrument-definition/1",
        "PREDICT_DEFINITION_SCHEMA",
    )
    require(
        native_id(definition["market_id"]) == native_id(raw["id"]),
        "PREDICT_DEFINITION_MARKET",
    )
    require(
        clock(definition["known_at_ns"]) <= received_ns, "PREDICT_FUTURE_DEFINITION"
    )
    require(
        definition.get("kind") in {"source_observation", "research_model"}
        and isinstance(definition.get("source_ref"), str)
        and definition["source_ref"].strip(),
        "PREDICT_DEFINITION_PROVENANCE",
    )
    fields = definition["fields"]
    require(
        isinstance(fields, dict) and set(fields) <= DEFINITION_FIELDS,
        "PREDICT_DEFINITION_FIELDS",
    )
    for key, value in fields.items():
        require(
            key not in terms or terms[key] == value,
            "PREDICT_DEFINITION_CONFLICT:" + key,
        )
        terms[key] = deepcopy(value)
    return terms, dict(
        definition=deepcopy(definition),
        sha256=digest(definition),
        independently_verified=False,
    )


def fee_observations(row):
    result = []
    for role, parties in [("taker", [row["taker"]]), ("maker", row.get("makers", []))]:
        for index, party in enumerate(parties):
            raw = party.get("fee")
            unit = (
                {"COLLATERAL": "collateral", "SHARES": "shares"}.get(
                    (raw or {}).get("type")
                )
                if isinstance(raw, dict)
                else None
            )
            amount = None
            status = "not_reported" if raw is None else "unknown_type"
            if unit:
                try:
                    amount = atoms(raw["amount"])
                    status = "reported"
                except (InputError, KeyError):
                    status = "invalid_amount"
            result.append(
                dict(
                    party=role,
                    party_index=index,
                    native=deepcopy(raw),
                    amount=amount,
                    unit=unit,
                    asset_id=str((party.get("outcome") or {}).get("onChainId"))
                    if unit == "shares"
                    and (party.get("outcome") or {}).get("onChainId") is not None
                    else None,
                    status=status,
                    actual_chain_debit_verified=False,
                )
            )
    return result


@dataclass
class PredictTradeBatch:
    ticks: list
    observations: list
    cursor: str | None
    counts: dict


class PredictDataLoader:
    """Independent Predict namespace, native quote side and bounded trade identity state."""

    def __init__(
        self,
        *,
        raw,
        terms,
        received_ns,
        definition=None,
        max_trade_ids=100_000,
        max_receipts=100_000,
    ):
        require(
            type(max_trade_ids) is int and 1 <= max_trade_ids <= 1_000_000,
            "PREDICT_IDENTITY_BOUND",
        )
        require(
            type(max_receipts) is int and 1 <= max_receipts <= 1_000_000,
            "PREDICT_RECEIPT_BOUND",
        )
        self.market_id = native_id(raw["id"])
        self.terms = deepcopy(terms)
        self.spec = parameters(terms, "predictfun")
        self.max_trade_ids = max_trade_ids
        self.max_receipts = max_receipts
        self._trades = {}
        self._last_trade_receipt = 0
        self.instruments = {}
        self.index_sets = {}
        outcomes = raw.get("outcomes")
        require(
            isinstance(outcomes, list) and len(outcomes) == 2,
            "PREDICT_BINARY_OUTCOMES_REQUIRED",
        )
        names = set()
        for outcome in outcomes:
            slot = outcome["indexSet"]
            require(
                type(slot) is int and slot in (1, 2) and slot not in self.index_sets,
                "PREDICT_BINARY_INDEX_SETS_REQUIRED",
            )
            token = native_id(outcome["onChainId"])
            name = outcome["name"]
            require(
                isinstance(name, str)
                and name.strip()
                and name.casefold() not in names
                and token not in self.instruments,
                "PREDICT_OUTCOME_IDENTITY",
            )
            names.add(name.casefold())
            self.index_sets[slot] = token
            instrument = binary_instrument(
                venue="predictfun",
                market_id=self.market_id,
                condition_id=str(raw.get("conditionId", "")),
                token=token,
                outcome=name,
                terms=terms,
                spec=self.spec,
                observed_ns=received_ns,
                revision=digest(terms),
                extra_info=dict(
                    adapter_schema=ADAPTER_SCHEMA,
                    native_index_set=slot,
                    native_quote_decimal_precision=terms.get("decimalPrecision"),
                    definition_input=definition,
                    instrument_constraints_independently_verified=False,
                    order_execution_enabled=False,
                ),
            )
            self.instruments[token] = instrument
        self.quote_token_id = self.index_sets[1]
        self.report = dict(
            adapter_schema=ADAPTER_SCHEMA,
            market_id=self.market_id,
            market_received_ns=received_ns,
            metadata_sha256=digest(raw),
            parameters=self.spec,
            definition_input=definition,
            native_receipts=[],
            native_quote_token_id=self.quote_token_id,
            complement_generated=False,
            complete_trade_coverage=False,
            execution_simulation_certified=False,
            network_requests=0,
        )

    @classmethod
    def from_market_json(
        cls,
        payload,
        *,
        received_ns,
        definition=None,
        max_trade_ids=100_000,
        max_receipts=100_000,
    ):
        received_ns = clock(received_ns)
        body = native_json(payload)
        require(
            body.get("success") is True and isinstance(body.get("data"), dict),
            "PREDICT_METADATA_ENVELOPE",
        )
        raw = body["data"]
        terms, binding = definition_terms(raw, definition, received_ns)
        return cls(
            raw=raw,
            terms=terms,
            received_ns=received_ns,
            definition=binding,
            max_trade_ids=max_trade_ids,
            max_receipts=max_receipts,
        )

    def parse_orderbook(self, payload, *, received_ns, max_deltas=2_000_000):
        received_ns = clock(received_ns)
        body = native_json(payload)
        require(
            received_ns >= self.report["market_received_ns"],
            "PREDICT_BOOK_BEFORE_METADATA",
        )
        require(
            body.get("success") is True and isinstance(body.get("data"), dict),
            "PREDICT_BOOK_ENVELOPE",
        )
        data = body["data"]
        require(native_id(data["marketId"]) == self.market_id, "PREDICT_BOOK_MARKET")
        require(
            len(self.report["native_receipts"]) < self.max_receipts,
            "PREDICT_RECEIPT_CAP_REACHED",
        )
        times = [
            data[k] for k in ("updateTimestampMs", "tsMs") if data.get(k) is not None
        ]
        require(
            times
            and all(type(t) is int and t > 0 for t in times)
            and len(set(times)) == 1,
            "PREDICT_SOURCE_CLOCK",
        )
        require(
            isinstance(data["bids"], list) and isinstance(data["asks"], list),
            "PREDICT_BOOK_LEVELS",
        )
        # The native book quotes indexSet=1. An instrument definition for slot 2
        # does not create an independent second pool of executable liquidity.
        snapshot = book_snapshot(
            self.instruments[self.quote_token_id],
            data["bids"],
            data["asks"],
            ts_event=times[0] * 1_000_000,
            ts_init=received_ns,
            max_deltas=max_deltas,
        )
        self.report["native_receipts"].append(
            dict(
                kind="orderbook",
                received_ns=received_ns,
                payload_semantic_sha256=digest(body),
            )
        )
        return snapshot

    def parse_matches(self, payload, *, received_ns, max_rows=1000):
        received_ns = clock(received_ns)
        body = native_json(payload)
        require(
            type(max_rows) is int and 1 <= max_rows <= 100_000, "PREDICT_PAGE_BOUND"
        )
        require(
            received_ns
            >= max(self.report["market_received_ns"], self._last_trade_receipt),
            "PREDICT_NONMONOTONIC_TRADE_RECEIPT",
        )
        require(
            body.get("success") is True and isinstance(body.get("data"), list),
            "PREDICT_MATCH_ENVELOPE",
        )
        require(len(body["data"]) <= max_rows, "PREDICT_PAGE_TOO_LARGE")
        require(
            len(self.report["native_receipts"]) < self.max_receipts,
            "PREDICT_RECEIPT_CAP_REACHED",
        )
        cursor = body.get("cursor")
        require(cursor is None or isinstance(cursor, str), "PREDICT_CURSOR_TYPE")
        ticks = []
        observations = []
        counts = Counter()
        pending = {}
        for row in body["data"]:
            require(
                native_id(row["market"]["id"]) == self.market_id, "PREDICT_TRADE_MARKET"
            )
            token = native_id(row["taker"]["outcome"]["onChainId"])
            require(token in self.instruments, "PREDICT_TRADE_TOKEN")
            slot = row["taker"]["outcome"].get("indexSet")
            require(
                slot is None
                or (type(slot) is int and self.index_sets.get(slot) == token),
                "PREDICT_TRADE_INDEX_SET",
            )
            price, quantity = atoms(row["priceExecuted"]), atoms(row["amountFilled"])
            executed = execution_time(row["executedAt"])
            require(executed <= received_ns, "TRADE_EXECUTION_AFTER_RECEIPT")
            require(
                0 <= decimal(price) <= 1 and decimal(quantity) > 0,
                "INVALID_TRADE_VALUE",
            )
            side = row["taker"]["quoteType"]
            require(side in {"Bid", "Ask"}, "UNKNOWN_AGGRESSOR_SIDE")
            sid = row.get("settlementId")
            require(
                sid is None or type(sid) in (str, int), "PREDICT_SETTLEMENT_ID_TYPE"
            )
            stable = sid is not None and str(sid).strip() != ""
            event_key = "settlement:" + str(sid) if stable else None
            identity = (token, event_key)
            economics = dict(
                price=price,
                quantity=quantity,
                executed_at=row["executedAt"],
                side=side,
                token=token,
                transaction_hash=row.get("transactionHash"),
                taker_hash=row["taker"].get("hash"),
                taker_signer=row["taker"].get("signer"),
                makers=[
                    {
                        k: m.get(k)
                        for k in ("hash", "signer", "quoteType", "amount", "price")
                    }
                    for m in row.get("makers", [])
                ],
            )
            fingerprint = digest(economics)
            fees = fee_observations(row)
            observation = dict(
                market_id=self.market_id,
                token_id=token,
                event_key=event_key,
                fingerprint=fingerprint,
                received_ns=received_ns,
                executed_ns=executed,
                executed_at_raw=row["executedAt"],
                price=price,
                quantity=quantity,
                side_raw=side,
                transaction_hash=row.get("transactionHash"),
                fees=fees,
                status="quarantined_missing_settlement_id"
                if not stable
                else "observed",
                chain_verified=False,
            )
            counts["observations"] += 1
            if not stable:
                counts["ambiguous_quarantined"] += 1
                observations.append(observation)
                continue
            previous = pending.get(identity, self._trades.get(identity))
            if previous is not None:
                require(
                    previous["fingerprint"] == fingerprint,
                    "CONFLICTING_STABLE_TRADE_ID",
                )
                observation["status"] = "duplicate_observation"
                observation["first_received_ns"] = previous["first_received_ns"]
                counts["duplicate_observations"] += 1
            else:
                require(
                    len(self._trades) + len(pending) < self.max_trade_ids,
                    "PREDICT_TRADE_IDENTITY_CAP_REACHED",
                )
                tick = predict_trade_tick(
                    self.instruments[token],
                    market_id=self.market_id,
                    event_key=event_key,
                    price=price,
                    quantity=quantity,
                    side_raw=side,
                    ts_event=executed,
                    ts_init=received_ns,
                )
                ticks.append(tick)
                pending[identity] = dict(
                    fingerprint=fingerprint, first_received_ns=received_ns
                )
                observation["status"] = "canonical_observed_trade"
                counts["canonical_observed_trades"] += 1
            observations.append(observation)
        # Commit identity state only after the whole page succeeds. A malformed
        # final row must not suppress valid ticks when the page is retried.
        self._trades.update(pending)
        self._last_trade_receipt = received_ns
        self.report["native_receipts"].append(
            dict(
                kind="matches",
                received_ns=received_ns,
                payload_semantic_sha256=digest(body),
                cursor=cursor,
                counts=dict(counts),
            )
        )
        return PredictTradeBatch(ticks, observations, cursor or None, dict(counts))

    def as_exported_group(self, data):
        """Wrap explicit parsed objects for the shared catalog writer, not a source manifest audit."""
        from ...data.nautilus_exporter import ExportedGroup

        data = list(data)
        require(len(data) <= 250_000, "EXPORT_RECORD_CAP_REACHED")
        require(
            sum(len(v.deltas) for v in data if type(v).__name__ == "OrderBookDeltas")
            <= 2_000_000,
            "EXPORT_DELTA_CAP_REACHED",
        )
        ids = {str(i.id) for i in self.instruments.values()}
        require(
            all(
                type(v).__name__ in {"OrderBookDeltas", "TradeTick"}
                and str(v.instrument_id) in ids
                and v.ts_init >= self.report["market_received_ns"]
                for v in data
            ),
            "PREDICT_FOREIGN_DATA",
        )
        report = deepcopy(self.report)
        report.update(
            schema="nautilus-native-predict-export/1",
            source_format="Predict.fun native response JSON",
            source_verification="native schema and identity checks; caller definition provenance bound but not independently certified",
            source_metadata_policy="native JSON plus explicit optional definition",
            instruments=len(self.instruments),
            data_records=len(data),
            book_batches=sum(type(v).__name__ == "OrderBookDeltas" for v in data),
            trade_ticks=sum(type(v).__name__ == "TradeTick" for v in data),
            ts_init_policy="caller supplied receipt availability",
            synthetic_timestamp_offsets=False,
            instrument_inputs=[
                dict(
                    market_key="predictfun:" + self.market_id,
                    parameters=self.spec,
                    metadata_revision=digest(self.terms),
                    observed_at_ns=self.report["market_received_ns"],
                )
            ],
        )
        return ExportedGroup(
            list(self.instruments.values()),
            sorted(data, key=lambda v: v.ts_init),
            report,
        )
