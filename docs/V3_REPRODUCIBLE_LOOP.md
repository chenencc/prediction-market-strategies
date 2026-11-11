# Real V3 to a minimal Nautilus strategy

Foundation inventory and local acceptance, 2026-11-11. This is the entry point
for the reproducible strategy plumbing loop; acquisition remains separate.

## Inventory

| Component | Observed state |
| --- | --- |
| Strategy repository | `chenencc/prediction-market-strategies`: independent PRIVATE repository, rechecked through authenticated GitHub CLI on 2026-11-11 |
| Original desired engine | `engine.lock.json`: `698d5cc8f7012bdc111b060e97de3210ee0a6002`; still unverified, not silently promoted |
| Actual loop engine | `runtime.compatibility.lock.json`: `b23139911fd9fed82aed2811c5a8a373202b9480`, `2.0.0rc8.dev20261025` |
| Runtime verification | Fresh Windows CPython 3.13.14 environment; hash-pinned official wheel; 106 installed RECORD hashes; full source commit embedded in native binary |
| Engine wheel SHA256 | `ef2391d1b03b96c8231cc8ff20490a21e195cdbb2fad6ffcfd9a51e87379e7c7` |
| Native binary SHA256 | `3c71b03d869b9a4570b3be2c2b726f5887a37bc849ba400e2a080a36987cd05b` |
| Converter | Separate `market-data-collector`, commit `0272ed7bae3e06c062fd98cf4635fcd9fb5b1fed`; source hashes recorded per run |
| Historical V3 input | Local retained HF sample, revision `6cdf6341737ad138d97365dbaa04c16e3f3c5252`, collection `pair40-resident-20261101-r1` |
| Source interval | Three contiguous chunks, source sequences 4232826 through 4240880; 11,089 compact rows |
| Existing adapters | Legacy recording/compact adapters already exist; this loop uses the actual V3 codec and the complete `iter_events` reader |
| Isolation | No collector/server configuration changes, source mutation, HF upload, venue requests or real orders |

This baseline is deliberately fixed, not a claim of latest-upstream compatibility.
The engine has not been built from source here. The fresh installed wheel, its
complete embedded commit and installed files were verified. The old desired pin
and its historical build failure remain separate.

## What runs

`tools/run_v3_loop.py` validates and bundles a small retained real sample, rebuilds
standard and Nautilus outputs in new directories, checks conversion fidelity,
and launches two separate Python processes running `backtest/v3_minimal.py`.
Each run verifies its installed engine before replay. It requires identical
semantic JSON results across processes and zero new conversion batches on resume.

The strategy receives the complete sequence through native book/trade callbacks.
It places exactly one BUY IOC for one share on the first eligible Polymarket book
update, using the ask and size visible in that callback. It has no future price
scan. Historical header baselines warm the books but **cannot trigger orders**.

The real sample contains 11,165 replay events: 11,152 book batches (including 120
baseline seeds) and 13 observed trade ticks, across 121 instrument/outcome IDs.
These instrument counts are not physical-market counts. Three exact-overflow
events are retained through the combined reader. A trade-only Predict outcome
has no L2 book; one separately counted EMPTY-book control satisfies the engine's
L2 registration requirement without inventing liquidity or a mirrored NO book.
Therefore the engine consumes 11,166 events, not 11,165 alleged source events.

Conversion checks cover 360 book checkpoints and 25,186 depth levels. Replay
checks cover every callback's fingerprint and order, final book tops, one IOC
fill at the visible ask, and independent-process reproducibility. The full-depth
conversion test and callback/strategy test are separate evidence.

## Reproduce locally

The accepted output is kept outside tracked Git files:
`D:/metaculus/prediction-market-strategies/results/v3-loop-20261111/accepted`.
Earlier failed development attempts remain under
`D:/metaculus/.tmp/nautilus-v3-loop-20261111/run-01` and `run-02`.

Use Windows CPython **3.13.14**. Create a fresh environment and install
`requirements-v3-loop.txt`; it pins the engine wheel by hash and all runtime
dependency versions. This bootstrap can download packages. The loop itself
does not download data. The local environment used for acceptance is:
`D:/metaculus/.tmp/nautilus-v3-loop-20261111/env/Scripts/python.exe`.

From this strategy repository:

```powershell
$py = 'D:/metaculus/.tmp/nautilus-v3-loop-20261111/env/Scripts/python.exe'
$collector = 'D:/metaculus/market-data-collector'
$accepted = 'D:/metaculus/prediction-market-strategies/results/v3-loop-20261111/accepted'

# Rebuild from the bundled ORIGINAL V3 files, not from an old success report.
# The new output directory must not exist. Keep both repositories at the
# commits/file hashes recorded in accepted/code.json.
& $py -B tools/run_v3_loop.py --collector $collector `
  --source-receipt "$accepted/source-index.json" `
  --engine-lock "$accepted/engine.lock.json" `
  --output 'D:/metaculus/prediction-market-strategies/results/v3-loop-20261111/reproduction-01'

& $py -B -m unittest discover -s tests -p test_v3_loop.py -v
```

For an initial retained-cache run, `--source-receipt` can be
`D:/metaculus/.coordination/v3-standard-source-20261102.json`.
The local source fails closed on missing files, changed hashes/manifests or source
sequence gaps. It never falls back to downloading HF or contacting an exchange.

Each output contains `acceptance.json`, `source-index.json`, original small V3
files under `input/`, fixed engine lock, installed runtime inventory, code hashes,
conversion report, independent `replay-1.json`/`replay-2.json` and logs, plus a
resume report. Wall clocks/log timestamps are not expected to match; all fields
of the two replay JSON receipts must match. No random ID is used as a signal or
discarded to hide a business-result difference.

## Findings and scope

The first attempt exposed a trade-only outcome without L2; the explicit empty
control resolves the engine import requirement while retaining unavailable
liquidity. The next exposed the pinned v2 API names `subscribe_trades/on_trade`;
the private strategy now uses those names. A subsequent review prevented orders
on warm-up baselines. These failures and earlier run receipts were retained.

This sample predates the new upload-before-quality certificate format: quality
certificate availability is **unknown**, never treated as passed. Conversion
fidelity is checked locally for strategy integration, not as renewed HF quality
auditing. HF private-source availability today is not inferred from the cache.

The IOC uses artificial `SIMUSD` collateral, zero fees, a fixture expiry one day
after the last event and representation increments. It deliberately does not
certify exchange tick/minimum rules, Predict share-fee accounting, queue position,
settlement, complete trade history, profitable execution or P&L. Native account
analysis is disabled. Historical event times remain unchanged; baseline warm-up
times and actual observed-window start are separately recorded.

The next stage is to pin a newer candidate engine separately and run this same
fixture against it. Then introduce observed execution specifications and venue
fees, and a recent V3 sample with upload-time quality certificates. Do not replace
the accepted runtime or silently reuse derived catalogs across an engine upgrade.
