# Local complementary-token paper trading

The explicit runner connects to public Polymarket market data and registers only
Nautilus `SandboxExecutionClientFactory` for execution. It has no wallet/private
key argument, real execution factory, SSH client, server deployment or scheduler.
Only local files are created. Imports do not connect to any provider.

```powershell
.venv/Scripts/python.exe -m prediction_market_strategies.paper.polymarket_pair `
  --slug france-declares-state-of-emergency-by-december-31-2026 `
  --seconds 120 --output results/local-pair-paper-new
```

The output directory must be new. Runs are limited to one binary market,
30–600 seconds, 1,000 virtual units of the observed collateral, at most one
two-leg entry of ten shares and 20 collateral units worst-case reserved cost.
The selected condition is a historical, active non-crypto market, not a new
opening test. Market metadata and native tokens are fetched again at launch.
System HTTP(S) proxy configuration is passed explicitly to the Rust market-data
transport; its URL and credentials are not saved. No proxy is installed or changed.

## Strategy

`BinaryPairPaperStrategy` adapts the pinned open-source reference described in
`THIRD_PARTY_NOTICES.md`, retaining LGPL-3.0-or-later for that strategy file.
The reference buys complementary tokens when their combined executable cost
is below aggregate binary payout. Our first version is confined to one v1
Polymarket condition with native Yes/No outcomes and common collateral. It is
not a cross-venue Pair or a multi-market payoff-certificate strategy.

Entry requires two complete independent L2 snapshots, valid observed fee terms,
venue order minimums and quantity grids, sufficient depth/cash, spread <=0.08,
local receipt age <=2 seconds and receipt skew <=500ms. These are conservative
experiment settings, not exchange specifications. A quiet unchanged book can be
rejected as stale; watchdog calls never refresh its receipt timestamp. Socket
transitions clear both views; incrementals cannot establish readiness.

The net-edge threshold is 0.02 per pair after conservatively rounded fee
estimates. A 250ms decision delay is followed by another complete eligibility
check. Unlike the reference's market IOC orders, this runner uses price-capped
IOC limits, sizes both legs on a common quantity grid and makes no atomic-fill
assumption. Entry permits only the observed collateral and explicit native fee
schedule; missing fees never become zero. The selected test market explicitly
reports a zero-rate schedule even though the native adapter logs a generic
fee-enabled/zero-rate warning.

Two terminal order-ID sets prevent duplicate completion accounting; native fill
identities prevent duplicate inventory. Partial/rejected legs cause up to two
fresh-book IOC attempts to sell the excess leg. Residuals below the venue order
minimum, pending-order timeouts and exhausted unwind attempts are retained and
reported as blocked. Cancellation/flattening is never assumed successful.
Matched inventory is held; no synthetic resolution or redemption is injected.

## Simulator and outputs

The Sandbox uses native L2 depth, explicit `PolymarketFeeModel`, liquidity
consumption, cash-account risk checks and a configured 100ms insert/cancel
latency. Trade/bar fills and queue-position inference are disabled. Model
latency is a configured assumption, not proof of actual exchange latency.

* `report.json`: runtime/source hashes, clocks, constraints, real-feed counts,
  virtual orders, positions, cash, commissions, unresolved/residual state,
  local artifact hashes and explicit acceptance limitations.
* `initial-instruments.json` / `final-instruments.json`: native outcome IDs,
  condition, observed tick/quantity/currency/fee terms and settlement description.
* `events.json`: candidate, submission, fill, terminal, recovery and socket events.
* `observations.json`: bounded once-per-second diagnostics containing original
  source/receipt clocks and top-of-book state. These rows are **not** official
  sampling requests, nor reconstructed continuous exchange events.
* `catalog/`: native Parquet instruments and full received book deltas, grouped
  by native identity. A new catalog reader verifies complete F_LAST batches,
  source/receipt clocks, prices/sizes/flags and instrument metadata. There is
  no SQL database. This research catalog is separate from the acquisition
  system's context/orderbook/trades recording-group archive contract.

Windows artifact enumeration uses extended-length paths, so long native
condition/token identities cannot silently disappear from the hash manifest.
No host registry or long-path policy is changed. The original live report and
a later offline storage-only validation remain distinct receipts.

The process exits after the finite window and saves all failed attempts too.
No opportunity means no orders, not an excuse to relax the threshold. A real
feed run with no eligible entry validates connectivity/evaluation/readback only;
it does not validate real-feed Sandbox fills or strategy profitability.

## Offline validation and remaining boundaries

`tests/test_binary_pair_paper.py` uses artificial books with an explicit synthetic
future expiry. It exercises real Nautilus BacktestEngine matching: fee-bearing
two-leg IOC entry, no-edge/no-order, one-leg price movement and unwind, partial
second-leg fill and matched residual inventory, duplicate fill/terminal events,
stale/future/skewed/empty books, unknown fees and catalog semantic readback.
These are synthetic execution tests, separate from real public data.

The runtime remains the separately pinned compatibility wheel in
`runtime.compatibility.lock.json`, not a certified build of `engine.lock.json`.
An 18-decimal collateral account path is refused because that installed wheel
has a known aggregation limitation. There is no Predict.fun live data client,
real execution client, queue calibration, chain settlement, exchange-actual fee
validation or guaranteed fill/profit claim. Historical 0.25Hz sampled books
remain insufficient to reproduce all short-lived execution opportunities.
