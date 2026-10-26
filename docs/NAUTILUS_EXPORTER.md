# NautilusExporter

The exporter is an offline consumer of `recording-group-parquet/1` with the
`instrument-metadata/1` retained terms added by recorder commit
`57f391918de5b95062e5532ca0200e9786188e1b`. It does not import that repository,
connect to providers, modify source data, or contain trading clients.

## Input and output

One **recording group**, including every venue/market/outcome member, is the
input. The group can be Type1 intelligence/opening, Type2 Pair, Type3 combination
or Type4 cross-sectional pool:

```text
source-group/
  context.parquet       # Observed instrument versions, clocks, intelligence if required
  orderbook.parquet     # Bound snapshots/deltas, individual stream receipts and gaps
  trades.parquet        # Observations, scan/page identity, source/receipt time and fees
  publication.json     # Existing control manifest binding those three files

new-independent-export/
  catalog/             # Nautilus instruments, OrderBookDelta and TradeTick datasets
  export-report.json   # Source bindings, conversion policies, event order and catalog hashes
  replay-report.json   # Optional no-order engine probe
```

The final recorder/HF format remains exactly three Parquet products. Nautilus
catalog layout is a separate derived format, with files split by instrument/type
as its native API requires. The exporter does not upload, retire, merge or delete
the originals. An existing output path or output inside the input is refused;
failed outputs are retained for diagnosis and are never silently overwritten.

## Explicit runtime

The reviewed desired engine source remains pinned in `engine.lock.json`:
`698d5cc8f7012bdc111b060e97de3210ee0a6002`. Its upstream CI run
[40309612595](https://github.com/nautechsystems/nautilus_trader/actions/runs/40309612595)
finished with failure and no downloadable artifacts at the 2026-10-26 check.
That exact source has **not** been built or runtime-verified here.

For this bounded compatibility experiment, use a separate Windows CPython 3.13
environment. `requirements-exporter-runtime.txt` selects an official, SHA-bound
wheel, version `2.0.0rc8.dev20261025`, self-reporting source
`b23139911fd9fed82aed2811c5a8a373202b9480`. `runtime.compatibility.lock.json`
records this difference. It is not an automatic replacement for the reviewed
engine baseline. No engine source patch is needed for this exporter.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-exporter-runtime.txt
.\.venv\Scripts\python.exe -m pip install --no-deps -e .
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe tools/validate_exporter_fixtures.py --output D:/scratch/new-validation
```

Run exporters in isolated processes. Native currency deserialization consults a
process-global registry; the reader temporarily binds the recorded definitions
and restores prior entries. Unknown new codes can remain registered. The Python
lock protects calls through this exporter, not unrelated native code threads.
Incompatible precisions sharing a currency code in one group are rejected.

## Export a saved group

```powershell
.\.venv\Scripts\python.exe -m prediction_market_strategies.data.export_cli `
  --source D:/datasets/group `
  --output D:/research/new-catalog `
  --max-records 250000 --max-deltas 2000000 `
  --trade-mode observed --replay
```

Python API: `NautilusExporter(...).export(source, target)` returns instruments,
ordered data and a verification report **from a fresh catalog reader**.
`NautilusExporter.read_export(target)` can verify/read an existing export.
`convert(source)` performs source verification and returns objects without
writing a catalog. Both event and expanded-delta memory budgets are bounded.
Large groups should be exported as independently sealed recording segments.

Input validation includes physical SHA/size, page checksums, membership,
context/metadata hashes, checkpoint/delta bases, full logical and materialized
semantic replay, and as-of metadata receipt clocks. Final catalog validation
checks its complete file inventory, hashes, instrument definitions and every
event's semantic hash, restoring saved cross-instrument receipt ordering.

## Mapping and clock meaning

| Recorded value | Nautilus mapping |
| --- | --- |
| Each native outcome/token | Separate `BinaryOption` |
| Polymarket condition + token | `<condition>-<token>.POLYMARKET` |
| Predict.fun market + native token | `<market>-<token>.PREDICTFUN` |
| One fully reconstructed official book observation | Independent L2 snapshot: CLEAR + ADD, F_SNAPSHOT and final F_LAST |
| Transport-invalid/reset control | Explicit synthetic gap CLEAR fence; separately recorded in the export report |
| Book source clock | `ts_event` in nanoseconds, retaining recorded millisecond precision |
| Actual book response receipt | `ts_init`; no synthetic time offsets |
| Stable, supported Predict settlement identity | One `TradeTick` across repeated scan/page observations |
| Trade execution time / first observed availability | `ts_event` / `ts_init`, including late post-window availability |

Storage compression deltas are not exchange event deltas. Independent samples
do not establish subsecond order flow, queue position or fills between polls.
Gap fences invalidate liquidity and are not observed empty books. Missing
Predict complementary books stay missing; no extra liquidity is generated.
Same-timestamp events retain source/export order, not an inferred exchange order.

Explicit metadata is mandatory: price and quantity precision; price and quantity
increments; collateral code and decimals. Conflicting aliases fail. Polymarket
currently requires an explicit supported `v1` protocol. Missing fields are not
inferred from the number of digits in an observed price, minimum order size,
current venue conventions, or a default USDC/pUSD definition. Quantity minimum
and quantity increment have different meanings.

Predict's native `decimalPrecision` describes the quote grid. The shared adapter
now handles it separately from quantity and aggregate-trade representation;
see [Predict native compatibility](PREDICTFUN.md). The native JSON path also
accepts explicit time/market-bound definition inputs, which do not silently
override missing historical fields in archived three-Parquet groups.

All retained terms, fee schedules, constraints and source metadata revision are
kept in `BinaryOption.info` and the report. A scheduled closing date does not
prove observed trading closure: activation/expiration constraints are not
invented from that date. The original context retains lifecycle/settlement
observations. Changes to precision, currency, increments or execution fee fields
currently fail closed; historical dynamic-instrument simulation is not provided.

The Type1 intelligence snapshot remains in the original `context.parquet`, bound
by its source manifest/hash. This exporter does not schedule intelligence as
custom Nautilus data or claim a point-in-time intelligence strategy evaluation.

## Trades, fees and execution limits

Base Polymarket API rows marked `ambiguous_fingerprint` are counted and
quarantined rather than guessed to be unique fills. Predict rows require a
supported stable `settlement_id`; duplicates must agree on fingerprint. An
unsupported identity or conflicting stable ID fails. `--trade-mode exclude`
skips tick conversion but still exhausts and verifies the original trade file.
Trades outside the recording window are excluded from ticks. Late acquisition
keeps its true availability, rather than retroactively exposing information to
a strategy at execution time. Chain verification descriptors are not imported
automatically and complete trade coverage is not certified.

`TradeTick` has no per-fill fee field; the original `fees_json` remains in the
bound source file. Instrument fee terms are retained, but this change does not
implement dynamic fee/settlement accounting, passive-fill probability or atomic
cross-venue execution. Never interpret preserved fees as verified fee simulation.

The engine probe attaches **no strategy**, creates zero-funded simulated venues,
disables trade-based fills, and uses an explicit zero fee model only because it
submits no orders. It checks input-consumption iterations and independently
replays L2 books. This is data plumbing, not a fee, strategy or P&L acceptance.
Native `total_events` is an order/account event count, not the data input count.
For trade-only or partially book-covered venues it uses L1 for data ingestion,
reporting incomplete L2 inputs; it does not invent missing books. Full native
snapshot coverage continues to use L2.

The inspected Windows wheel aborts when aggregating an account with 18-decimal
collateral into floating-point result statistics. Exact currencies survive
catalog write/read and data replay. The probe therefore skips `get_result()`
for currencies above 16 decimals and records that limitation explicitly. It
does not reduce precision, rename currency or claim to fix this upstream path.

## Validation and remaining real-data gate

Eighteen offline tests cover all four shapes, changed payloads, missing/conflicting
metadata, missing delta bases, as-of metadata, bounded memory, duplicate and
ambiguous trades, precise native cold reads, preserved fees, gap fences and
read-only output boundaries. Four synthetic groups from the **actual frozen
recorder**, including its publication/readback/retirement path, passed native
catalog write/cold-read and no-order engine replay. They contain fake official
transport responses and a private-HF stub. They are not live-market data tests.

An additional offline check of 67 saved real market control records found zero
with every required explicit export input: all lacked price/quantity precision,
quantity increment, collateral currency/token/decimals and chain identity. These
records were saved before the new metadata retention path; retaining fields
does not create values absent from a response. They were refused rather than
filled with assumptions. No retired archives were downloaded. A full real
three-Parquet sample with complete, provenance-bound instrument definitions is
still required before declaring real-data end-to-end acceptance. The exact
reviewed engine revision and 18-decimal account-result path remain separate
runtime gates before financial strategy backtests.
