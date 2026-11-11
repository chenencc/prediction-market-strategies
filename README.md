# Prediction Market Strategies

Private research workspace for Polymarket and Predict.fun strategies built on
NautilusTrader. This repository contains our strategy, adapter and backtest code;
the engine fork contains only changes which require engine internals.

## Repository boundaries

| Repository | Role |
| --- | --- |
| `nautechsystems/nautilus_trader` | Official upstream engine |
| `chenencc/nautilus_trader` | Public fork, clean upstream branch and minimal engine patches |
| `chenencc/prediction-market-strategies` | Private strategy and research workspace |

The engine baseline is pinned to the exact commit in `engine.lock.json`. Creating
these repositories does not install/build the engine or certify an integration.
There are no trading clients, order submissions, deployments, scheduled runs or
data-provider calls in the bootstrap.

## Layout

```text
engine.lock.json               # Reviewed exact engine source baseline
src/prediction_market_strategies/
  adapters/polymarket/          # Project-specific Polymarket integration
  adapters/predictfun/          # Independent Predict.fun integration
  data/                        # Read-only dataset/metadata integration
  strategies/                  # Strategies, separate from venue adapters
  risk/                        # Cross-strategy risk controls
  backtest/                    # Backtest configuration and execution
configs/                      # Non-secret examples
tests/                        # Offline regression cases
docs/                         # Architecture and upgrade decisions
```

The package has no engine dependency installed by default. The standalone
exporter now reads the three-Parquet recording contract, verifies source hashes
and snapshot/delta replay, then writes a Nautilus catalog and verifies cold
readback. It supports all four task/group shapes without importing the recorder.

See [NautilusExporter usage, validation and limitations](docs/NAUTILUS_EXPORTER.md).
The [Predict.fun native-format loader](docs/PREDICTFUN.md) also reads saved market,
orderbook and matches JSON through the same conversion primitives.
The optional [local binary-pair paper experiment](docs/PAPER_TRADING.md) uses
public Polymarket L2 data and a Sandbox execution client with virtual funds.
It never deploys or sends real exchange orders.
The [local Paper Observatory](docs/PAPER_MONITOR.md) adds a read-only browser
terminal and a bounded single-market buy/sell strategy, with native orders,
simulated fills, depth, P&L, data-gap indicators and historical frame replay.
The [depth-tier maker](docs/DEPTH_MAKER.md), independently based on
`ccjingeth/predict-fun-marketmaker`, shares that local paper infrastructure and
adds passive quoting, acknowledgement-gated replacement and bounded fill hedges.
Run `python -m unittest discover -s tests -v` after installing its explicit runtime.
The inspected compatibility runtime is pinned separately in
`runtime.compatibility.lock.json`; it **does not verify** the newer source in
`engine.lock.json`. No floating engine dependency is installed.

## Initial work sequence

1. Establish the pinned engine build and runtime environment.
2. Agree the recorded-data contract and its metadata completeness requirements.
3. Implement data/venue integration with deterministic offline replay.
4. Add a small strategy and reproducible backtest before considering live execution.

The acquisition system remains in its existing repository. The legacy
three-Parquet recording contract remains supported. The new native collector
uses a shared, compressed `nautilus-source/1` archive with native events, API page
occurrences, market/fee versions and Pair relationships. Its
[private compact-source adapter](docs/COMPACT_ARCHIVE_IMPORT.md) verifies hashes,
preserves 18-place values, deduplicates observations and masks unsafe book gaps.
Exported Nautilus catalogs are independent research artifacts;
they never replace or modify source products. No real dataset, private key
or provider token belongs in Git. The small checked-in fixture contains only
synthetic transport responses processed by the actual recorder.

See [engine upgrade workflow](docs/ENGINE_UPGRADES.md) and
[architecture boundaries](docs/ARCHITECTURE.md).

For the retained real HF V3 sample, fixed engine verification, minimal native
strategy and independent-process reproduction, start with
[Real V3 reproducible loop](docs/V3_REPRODUCIBLE_LOOP.md).

For incremental HF downloads, E: storage, local-cache retirement after verified
conversion, and direct backtest/fee-context readers, see
[HF V3 incremental backtest sync](docs/HF_V3_SYNC.md).
