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

Run bootstrap checks with `python -m unittest discover -s tests -v`.
The package has no engine dependency installed by default. Before using any
Nautilus APIs, build or otherwise verify the exact engine revision in the lock;
do not substitute the latest package or a floating Git branch.

## Initial work sequence

1. Establish the pinned engine build and runtime environment.
2. Agree the recorded-data contract and its metadata completeness requirements.
3. Implement data/venue integration with deterministic offline replay.
4. Add a small strategy and reproducible backtest before considering live execution.

The acquisition system remains in its existing repository. Final recorded data
continues to use `context.parquet`, `orderbook.parquet` and `trades.parquet` per
recording group. No dataset, private key or provider token belongs in Git.

See [engine upgrade workflow](docs/ENGINE_UPGRADES.md) and
[architecture boundaries](docs/ARCHITECTURE.md).
