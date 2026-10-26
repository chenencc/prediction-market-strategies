# Predict.fun offline compatibility

`adapters.predictfun.PredictDataLoader` maps native market, orderbook and matches
responses into Nautilus public data types. The existing `NautilusExporter` and
this native loader share instrument, snapshot, numeric and trade builders in
`adapters/common.py`. Neither path imports or modifies the running recorder.
There are no network clients, credentials, order submissions or engine patches.

## Native schemas and important differences

| Native Predict input | Mapping |
| --- | --- |
| `id`, `outcomes[].onChainId` | `<market_id>-<onChainId>.PREDICTFUN`, one BinaryOption per outcome |
| `outcomes[].indexSet` | Exact binary mapping 1/2, independent of human outcome labels |
| `decimalPrecision` | Native quote-grid precision, **not quantity precision** |
| Optional explicit `pricePrecision` | Nautilus representation precision; may exceed quote-grid precision |
| Orderbook `data.marketId`, `bids`, `asks` | One complete native indexSet=1 snapshot, CLEAR + ADD; empty snapshot is CLEAR |
| `updateTimestampMs` / `tsMs` | Source event clock; conflicting aliases are rejected |
| Actual caller receipt | `ts_init`; no invented nanosecond offsets |
| `taker.outcome.onChainId`, `taker.quoteType` | The actual traded outcome; Bid -> BUY, Ask -> SELL |
| `priceExecuted`, `amountFilled` | Exact integer protocol values scaled by 10**18, without floats |
| `settlementId` | Stable API trade identity scoped to market/outcome; repeated page observations collapse |
| `executedAt` | Exact ISO8601 execution time, including up to 9 fractional digits and explicit timezone |
| Taker/maker `fee.amount`, `fee.type` | Separate fee observations, 18-decimal units; COLLATERAL and SHARES remain distinct |
| Missing stable identity / unknown fee unit | Quarantine trade / preserve unknown fee; no unique fill or zero-fee guess |

The native orderbook describes one quote side. Defining both instruments does
not create a second independent pool. This adapter does not manufacture a No
book or duplicate liquidity. No-token matches nevertheless remain No-token
TradeTicks at their reported price; they are not relabeled as Yes trades.

### Quote grid versus aggregate trade price

The earlier exporter incorrectly treated Predict's `decimalPrecision` as a
quantity precision alias. That alias is removed for Predict only. Its documented
meaning is the quote price grid; when no explicit tick field exists, grid
`10**(-decimalPrecision)` is used and its origin is recorded. Explicit conflicting
increments are rejected. Price representation cannot be coarser than that grid.

Saved real market 517145 has `decimalPrecision=3`, while one match reports
`priceExecuted="121369800000000000"`, meaning **0.1213698**. Aggregate match prices
can have more digits than an individual resting quote. The loader never rounds
that price to 0.121 or fabricates multiple executions. A declared representation
with sufficient precision is required; otherwise conversion fails. Quantities
and collateral definitions remain separate explicit inputs. The currently
tested Nautilus representation supports price/size precision up to 16, so an
unrepresentable finer value is refused rather than truncated.

## Explicit definitions for absent metadata

Native responses do not necessarily contain every field needed by BinaryOption.
The loader accepts a separate **market-bound, time-bound definition**. It is
never automatically selected from today's venue conventions. Example for an
offline display/data-ingestion experiment:

```python
from prediction_market_strategies.adapters.predictfun import PredictDataLoader
from prediction_market_strategies.data.nautilus_exporter import NautilusExporter

definition = {
    "schema": "predictfun-instrument-definition/1",
    "market_id": "517145",
    "known_at_ns": definition_available_ns,
    "kind": "research_model",
    "source_ref": "my-reviewed-offline-model-definition",
    "fields": {
        "pricePrecision": 16,
        "sizePrecision": 16,
        "sizeIncrement": "0.0000000000000001",
        "collateralCurrency": "USDT",
        "collateralDecimals": 18,
        "chainId": 56,
    },
}

loader = PredictDataLoader.from_market_json(
    saved_market_response_json,
    received_ns=market_available_ns,
    definition=definition,
)
snapshot = loader.parse_orderbook(saved_book_response_json, received_ns=book_available_ns)
batch = loader.parse_matches(saved_matches_response_json, received_ns=matches_available_ns)
objects = loader.as_exported_group([snapshot, *batch.ticks])
verified = NautilusExporter.write_catalog(objects, new_independent_output_directory)
```

The example size increment is a **declared research representation quantum**,
not a statement about venue lot constraints. The source and hash of the supplied
definition are retained, and `independently_verified=False` stays explicit.
`source_observation` is also an allowed caller label, but that label is not an
independent audit of its truth. Future-known definitions, mismatched market IDs,
conflicting native fields and attempts to replace outcomes/resolution through
this definition mechanism are refused. Without a definition, all missing
required terms still fail closed. This does not repair the 67 old Polymarket
control snapshots or silently establish historical constraints.

Supply JSON text/bytes when reading native floating-point JSON literals. It is
parsed as exact decimal strings. Python floats in book levels are rejected
rather than rounded back into supposed original values.

`parse_matches` returns ticks, all observations, counts and the native cursor.
It does not fetch the next page. Reuse the same loader for cursor pages and
overlaps; identity state is bounded. Different settlements within one transaction
stay different. Conflicting stable identities fail. Fee amendments remain new
observations without creating another TradeTick. A failed page commits no
partial identity state, so a corrected page can be retried without dropping its
earlier valid ticks. Receipt, page-size, identity and catalog data bounds apply.

`TradeTick` has no fee field. Native per-party fees are returned in
`batch.observations`; retain the source pages or persist these observations when
building fee research. Native catalog control metadata binds receipt hashes,
but does not copy every fee observation into each TradeTick. The existing
three-Parquet path already retains them in source `trades.parquet`/`fees_json`.

## Clock and execution boundaries

Use original recorded availability clocks when they are available. If inspecting
old files whose HTTP receipt times are unknown, use the actual local read clock
and label that role explicitly. Do not assign execution time to `ts_init` or
pretend a newly supplied research definition was known at historical execution.

The no-order engine probe selects L2_MBP only when each active instrument has
book input. For pure-trade or partially book-covered data it uses L1_MBP for
**data ingestion only**, recording `l2_execution_inputs_complete=False` and the
selected mode. This does not certify L2 execution or fix absent complementary
depth. The initial saved-trade probe failed because it requested L2 without any
book input; that failure and its output directory are retained in the audit.

Account/P&L aggregation of 18-decimal collateral remains unsupported by the
inspected Windows compatibility wheel. The probe skips that known abort path;
it does not change currency precision. Fees are retained, not simulated by a
certified Predict commission model. No orders, profitable strategy, chain-final
settlement, complete trade history or passive fills are certified.

## Validation

The suite now has **34 passing tests**, including the previous exporter cases.
All four recorder-format fixtures also pass catalog cold read and no-order
engine replay after the shared-builder refactor.

Two already-saved real public match pages from market 517145 contain 10 matches,
both outcomes, Ask/Bid directions and 22 party fee observations. All 10 converted
to TradeTicks, survived exact catalog cold read and were consumed in 10 engine
iterations. Re-reading page 1 added zero ticks. This used an explicit research
representation and **local file-read availability**, not reconstructed original
HTTP clocks or certified historical contract definitions. Both page cursors
remained present: this sample does not establish complete history/pagination.
A separate synthetic native book plus trade passed L2 input replay. Original
files and running services remained unchanged; no market/RPC/model/HF APIs ran.

The tested wheel and unverified newer engine source remain separately pinned in
`runtime.compatibility.lock.json` and `engine.lock.json`.

## Reviewed primary references

* [Predict orderbook semantics](https://dev.predict.fun/doc-685654)
* [Market schema](https://dev.predict.fun/market-14037477d0)
* [Match schema](https://dev.predict.fun/matchdata-14037487d0)
* [Orderbook schema](https://dev.predict.fun/orderbookdata-14037511d0)
* [Fee schema](https://dev.predict.fun/fee-14037472d0)
* Predict TS SDK `5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486`,
  [OrderBuilder.ts](https://github.com/PredictDotFun/sdk/blob/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486/src/OrderBuilder.ts)
* Predict Python SDK `6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1`,
  [constants.py](https://github.com/PredictDotFun/sdk-python/blob/6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1/src/predict_sdk/constants.py)
* Nautilus source `698d5cc8f7012bdc111b060e97de3210ee0a6002`:
  [Polymarket instrument parsing](https://github.com/nautechsystems/nautilus_trader/blob/698d5cc8f7012bdc111b060e97de3210ee0a6002/crates/adapters/polymarket/src/http/parse.rs),
  [snapshot parsing](https://github.com/nautechsystems/nautilus_trader/blob/698d5cc8f7012bdc111b060e97de3210ee0a6002/crates/adapters/polymarket/src/websocket/parse.rs),
  [fee model](https://github.com/nautechsystems/nautilus_trader/blob/698d5cc8f7012bdc111b060e97de3210ee0a6002/crates/adapters/polymarket/src/models.rs)
