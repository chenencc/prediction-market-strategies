# Incremental HF V3 download and backtest preparation

This private repository now provides `tools/hf_v3_sync.py` and the local
`BacktestDataset` reader. Market data, download cache, conversion work, indexes,
receipts and referenced quality certificates live on **E:**. Code and Python
environments can remain on D:. No collector deployment is changed.

## Fees: what the real data contains

The first production chunk was downloaded from
`chchen13/forecastagent-data`, prefix `production/markets1000-20261110-r2`.
Its 1,192 metadata versions contain:

| Venue / state | Metadata versions | Retained fields |
| --- | ---: | --- |
| Polymarket reported schedule | 894 | `feeSchedule` (rate/exponent/takerOnly/rebateRate), `feesEnabled`, `feeType`, `makerBaseFee`, `takerBaseFee` |
| Polymarket explicitly disabled | 5 | `feesEnabled=false` |
| Predict reported basis points | 293 | `feeRateBps`, `makerFeeBps` |

These are metadata-version counts, not physical-market counts. Examples in this
chunk include Poly schedule rate `0.04`, exponent `1`, and Predict taker `200`
bps / maker `0` bps. Do not apply either example to every market or time interval.
Fee formulas, rounding, rebate eligibility and whether a fee is charged in cash
or shares still belong in the execution model. Legacy Poly `makerBaseFee` and
`takerBaseFee` are not automatically interpreted as basis points.

V3 trade fee legs retain `fee_rate_bps`, `fee_amount_native`, `fee_unit` and
`fee_status` when reported. This first chunk has 49 Poly trade rate observations
marked `not_reported`, plus one reported Predict maker fee leg and one taker leg.
Missing amounts are **unknown**, not zero. The prior minimal strategy's zero-fee
simulation was an explicit smoke-test assumption, not an absence of V3 fee data.

Every converted chunk retains original metadata and clocks in canonical
Parquet, and writes `fee-summary.json`. Historical fee versions must be selected
using availability time; never substitute the latest metadata for earlier events.

## Run

The fixed engine is `b23139911fd9fed82aed2811c5a8a373202b9480` on Windows Python
3.13.14. The downloader verifies the pinned wheel, installed engine files and
native commit marker. Install `requirements-hf-sync.txt` into an isolated
environment. The validated local interpreter is:
`D:/metaculus/.tmp/hf-v3-sync-env/Scripts/python.exe`.

From `D:/metaculus/prediction-market-strategies`:

```powershell
$py = 'D:/metaculus/.tmp/hf-v3-sync-env/Scripts/python.exe'
& $py -B tools/hf_v3_sync.py `
  --token-file 'C:/Users/Administrator/Desktop/hf.txt' `
  --max-chunks 3 --delete-converted-downloads
```

The default destination is:
`E:/metaculus_data/backtests/hf-v3/production1000`.
The token is read directly from its existing file, never copied to E:, persisted
in reports or printed. `--token-file` can be omitted when native HF credentials
are configured. The downloader does not write to the shared HF cache.

Re-running the same command continues after the last committed sequence. It does
not download already completed payloads. It resolves `main` to an exact revision
for each pass, lists manifest metadata and checks previously seen Git blob IDs.
There is no full-history download or replay on each pass. Conversion uses only
the current chunk and the preceding converted checkpoint/release.

For a bounded repeated session, for example:

```powershell
& $py -B tools/hf_v3_sync.py `
  --token-file 'C:/Users/Administrator/Desktop/hf.txt' `
  --max-chunks 3 --passes 20 --poll-seconds 60 --delete-converted-downloads
```

This runs at most 20 passes and finishes each in-progress chunk safely. It is not
a timer/service and no background automation is installed. Conversion time is
additional to the sleep interval. Defaults preserve at least 12 GiB free space.

Default `--first-sequence 1` imports the full available collection in order.
If intentionally starting in the middle, use an explicit first chunk boundary
and a **new root**; earlier history remains missing. A source gap stops progress,
and is never silently skipped. `--layout audited-sources` supports the newer
`v3-local-audited-source/1` manifest structure in a separately pinned root; current
real acceptance used the production `segments` / `file-spool-publication/3` path.
An upstream layout change is not silently adopted.

## Data layout and direct backtest access

```text
production1000/
  config.lock.json                 source, fixed engine and converter hashes
  index.sqlite                     durable sequence/receipt index
  cache/<sha256>.zip                only pending or explicitly retained downloads
  chunks/<first>-<last>/
    source-manifest.json            exact original source/provenance
    standard/                      canonical Parquet, full metadata/fees/clocks
    nautilus/                      native Catalog + exact overflow + event index
    quality/                       only upstream-referenced certificates, if any
    fee-summary.json
    receipt.json                   full hashes and conversion fidelity proof
  work/                            bounded current + predecessor workspace
  runs/                            individual success/failure receipts
  latest-run.json                  current invocation or last completed result
```

Set the private source directory and fixed collector services directory on
`PYTHONPATH` (the CLI does this itself), then:

```python
from prediction_market_strategies.data.hf_backtest import BacktestDataset

dataset = BacktestDataset('E:/metaculus_data/backtests/hf-v3/production1000')
print(dataset.status())
instruments = list(dataset.instruments())
events = dataset.events()           # native OrderBookDeltas / TradeTick iterator
metadata_and_fees = dataset.context('definitions')
trade_fee_context = dataset.context('trade_payloads')

# With your independently configured Nautilus BacktestEngine and venues:
# for instrument in instruments:
#     engine.add_instrument(instrument)
# engine.add_data(list(events))     # use a bounded dataset for an in-memory run
```

Use this complete reader instead of querying only native catalog files: catalog
alone omits `exact-overflow.parquet`. The converted instruments are data-replay
instruments (`XPROB`, representation increments), not certified exchange order
specifications. Configure execution specs/fees before interpreting simulated P&L.

`events()` merges cross-chunk timestamp overlaps by original `ts_init`, without
changing timestamps or dropping records. Only overlapping chunks are buffered,
with a default 500,000-event bound; increase `max_buffered_events` explicitly if
needed. `events(order='source')` preserves chunk/source order for auditing, which
can contain short timestamp reversals and should not be fed directly to a
chronological backtest. The real three-chunk sample exposed one 26.9 ms overlap.

Quality results are preserved, not recomputed on HF. Use
`dataset.quality_certificates(known_at_ns=decision_ns)` if they affect a strategy
decision. Missing embedded certificates remain unknown. Local checks validate
transport integrity and faithful conversion, not exchange-wide trade coverage.

## Local download cleanup and restart guarantees

`--delete-converted-downloads` authorizes this exact sequence:

1. Download one core ZIP at the resolved revision; verify bytes and SHA256.
2. Preserve the original V3 header and every compact record in canonical data.
3. Convert to Nautilus; cold-read native plus overflow, and compare full depth
   against V3 and the canonical checkpoint.
4. Atomically publish the local converted bundle; commit its receipt to SQLite
   with FULL synchronous durability.
5. Reverify the committed bundle and the source ZIP binding, then unlink only
   that content-addressed **local ZIP** and persist its cleanup state.

No HF deletion API exists in this implementation. Metadata, fees, quality
certificates, converted files, failure reports and sequence gaps are retained.
A failed conversion keeps its source; an interrupted cleanup is idempotent.
The source file may be removed only after the converted bundle is independently
readable without it. Dropping canonical files to save more space is not covered.

Windows long paths use an explicit Python filesystem adapter and a small scoped
Catalog-path compatibility shim for the fixed Rust wheel. Prior converted bundles
are linked on the same drive where supported, not downloaded or rewritten.
All recursive temporary cleanup is confined to the worker's own resolved `work/`
directory. Engine/converter changes fail the root's config lock; accepted data
must not be silently reused under an untested version.

Offline tests: `python -m unittest discover -s tests -p test_hf_sync.py -v`.
Real acceptance is recorded under the E: root and `inspection/`; the initial
Windows path failures and the verified path-only migration receipts were retained.
Only a bounded initial batch is acceptance-tested; the backlog is reported, not
claimed to be fully downloaded or validated.

HF interface references: [HfApi](https://huggingface.co/docs/huggingface_hub/package_reference/hf_api)
and [exact-revision downloads](https://huggingface.co/docs/huggingface_hub/guides/download).
