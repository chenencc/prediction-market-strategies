# Depth-tier market maker: local paper implementation

Behavioral reference: [ccjingeth/predict-fun-marketmaker](https://github.com/ccjingeth/predict-fun-marketmaker),
exact reviewed main commit `9346c7abefaff73758586e6cb8d7193fcbc79afb`.
This implements the README's depth-tier passive maker, not that repository's
entire desktop application or every experimental strategy class. No downloaded
TypeScript, package install hook, wallet code or execution runtime is run.

## Components

```text
Public Polymarket L2 and observed market/reward terms
  -> binary_market.py: shared native identity, snapshot, clock and event handling
  -> depth_maker.py: pure Decimal quote policy
  -> depth_maker_nautilus.py: cancel/ack/requote, inventory, hedge and protection state
  -> Sandbox only: virtual cash, bounded native L2 matching
  -> same local Parquet/readback/hash pipeline as binary-pair paper
```

The previous binary-pair arbitrage strategy now shares the native binary-market
lifecycle. The shared callback boundary halts and cancels on an exception;
native callback error logging alone is not accepted as a successful quiet run.
The acquisition recorder, servers, Metaculus and the public engine fork are
outside this change. Nothing starts when these modules are imported.

## Implemented behavior and deliberate corrections

| Setting | Conservative | Aggressive |
| --- | --- | --- |
| Native tier reference | Fourth | Third |
| Retreat beyond tier | 0.015 probability units | 0.015 |
| Minimum distance behind same-side BBO | 0.035 | 0.030 |
| Minimum strictly-ahead depth, per side | 6,000 shares | 4,000 shares |
| Relative front-depth drop that cancels | 5% | 8% |
| Depth balance check | Yes | No |
| Cooldown after a maker order first fills | 6 hours | 4 hours |
| Two distinct filled maker orders within one hour | Blacklist 7 days | Blacklist 48 hours |

These are research policy settings, not claims that a tier is safe or that
orders cannot be filled. A tight native ladder can require retreating farther
than the named tier to maintain the hard distance. Prices round away from the
touch on the native grid; all final prices are rechecked. Smart gap insertion
is allowed only when the rounded candidate passes the same distance, depth and
reward checks. The implementation never advances an otherwise unsafe quote to
make it fit a reward band, and never creates missing native book tiers.

Only held inventory can be sold. BUY orders are capped by per-outcome inventory,
worst-case inventory/open-order cost and available cash, with a separate hedge
reserve. Reducing SELL orders remain permitted when the inventory cap is hit.
The finite default is at most five quote cycles and 30 submitted orders; slots
are reserved for up to two hedge attempts. The live entry point has one market,
1,000 virtual collateral units and a 30–600 second window. There is no real
execution-client factory or credential argument.

Changed targets, stale/skewed books, diminished depth and socket transitions
cancel resting makers. Old IDs must receive terminal acknowledgements before
replacement; a missing cancel acknowledgement blocks rather than granting a
new quote slot. A partial fill cancels remaining makers before recalculating
the actual residual, including fills which arrive while cancellation is pending.
This is a deliberate safety change from the reference's optional uninterrupted
continuous-hedging branch.

`FLATTEN` sells only the excess held outcome using a price-capped IOC.
`BUY_OPPOSITE` buys only the missing opposite quantity using its own native book,
a cash limit and a heavy-leg cost upper bound. It rejects excessive locked loss;
complementary inventory is not labeled cash profit. Both routes use a configured
2.5% relative limit around current touch, native quantity minimums, a bounded
hedge-attempt count and an unhedged deadline. Tiny residuals are retained as a
blockage, not rounded up into unwanted new exposure. Duplicate partial-fill
events do not add inventory or count as new cooldown/blacklist incidents.

## Rewards and fees

Polymarket [liquidity rewards](https://docs.polymarket.com/programs/liquidity-rewards)
define size and spread settings per market. Settings alone do not prove an
active allocation. The local preflight binds the exact Gamma market/condition,
positive allocation, start/end dates, minimum size and spread units. Its JSON
receipt and local HTTP request/receipt clocks are saved separately.

Polymarket `rewardsMaxSpread` is interpreted as cents from the size-adjusted
midpoint, not the quote's full bid/ask width. A reward constraint does not prove
that the exchange scored an order, nor promise any payment. No points, daily
rewards or maker rebate is credited to virtual cash. Observed `takerOnly=True`
is required to give post-only makers zero simulated commission; hedges use the
pinned native Polymarket taker model. Exchange-actual fee/rebate accounting is
still a separate validation task.

Predict.fun is a distinct namespace. The pure policy supports an explicitly
defined venue-specific reward basis, but no Predict live strategy client or
Predict fee/18-decimal account simulation is certified by this implementation.
The existing Predict native-format loader remains offline-only. A Predict book
for one quote outcome is never silently duplicated into an opposite book.

## Local entry point

```powershell
.venv/Scripts/python.exe -m prediction_market_strategies.paper.polymarket_maker `
  --slug will-benjamin-netanyahu-be-the-next-prime-minister-of-israel `
  --seconds 120 --maker-mode conservative --maker-quantity 150 `
  --hedge-mode FLATTEN --output results/depth-maker-new
```

The directory must be new. The quantity is explicit virtual research sizing;
the runner fetches current terms and can refuse to quote. Do not lower guards
to force a fill. Public metadata GETs have at most three transient retries;
permanent metadata errors and all order commands do not use that retry path.
The system proxy is reused without saving its URL or credentials. It is not
installed or changed by the runner.

Outputs extend the [existing paper artifacts](PAPER_TRADING.md) with
`reward-preflight.json`, quote/cancel/hedge/protection events and maker statistics.
The matcher does not assume a passive fill from a mere touch. In the inspected
2.x runtime, L2 touch paths can bypass the fill probability, so the model also
returns an explicit empty executable book at exact touch. Native tests verify
both no fill at equality and fills after price-through. This still cannot
reconstruct queue priority or certify real exchange fills.

## Validation status

The complete offline regression has 71 passes, including actual native matching
for passive price-through fills, zero reward credit, partial fills, flattening,
opposite-token hedges, cancellation races, acknowledgement-gated replacement,
tiny residual retention, quote budgets, clock/risk bounds and callback failures.
The existing exporter, Predict loader and binary-pair tests remain included.

A new real-time trial was attempted locally but failed during public metadata
TLS/timeout before receiving any new books or submitting simulated orders.
This is not a successful live maker test. Failed receipts are retained.

An independent offline replay of 136 previously saved real public book batches
passed. For the checked source books, tier quotes were 0.055 from the adjusted
midpoint, beyond the retained market reward band of 0.045. The strategy correctly
did not quote. The retained reward file's last-write clock is an availability
upper bound, not a measured HTTP clock or current-exchange-allocation proof.
That replay validates saved-input behavior, not current connectivity or profit.

Use the separately pinned compatibility runtime, not an assumed build of
`engine.lock.json`. The known 18-decimal account limitation remains. There is
no new production listener, server deployment, account access or automated run.
