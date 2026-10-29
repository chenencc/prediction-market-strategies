# Compact source to the private Nautilus runtime

All conversion, strategy safety and fee accounting stay in the **private**
`chenencc/prediction-market-strategies` repository. The upstream collector is not
an engine dependency. Engine upgrades stay explicit in `engine.lock.json`;
the installed compatibility wheel has a different commit and is never silently
described as a build of that lock.

## Accepted input and identity

`CompactSource` reads `nautilus-source/1`, verifies every Parquet SHA/row count,
page checksums, auxiliary file hashes and Pair relations. It retains independent
source and availability clocks, full L2 levels, market/fee versions, native
contract metadata, fees, lifecycle and gaps. It never changes the source.

API page/content IDs are local references. Repeated API observations and fee
amendments do not create additional TradeTicks. Predict settlements identify
aggregate taker matches, rather than every maker execution. For Polymarket,
transaction/outcome/side/economics and maximum same-page multiplicity form a
bounded identity with explicit ambiguity reporting.

The known Polymarket WS/API records in the retained trial use different clocks
and sometimes different prices: an API settlement time can be seconds after the
WS event, and the API can expose an effective aggregate price. A unique same
transaction/outcome/side/quantity group, within one source quote tick and ten
seconds, is reported as a reconciliation group. Both economic representations
remain in the evidence. The live WS tick appears once; an ambiguous API group
is quarantined. This is not proof of atomic fill identity or complete coverage.

## Specifications and units

Polymarket uses recorded tick/minimum quantities and fee schedules. The reviewed
engine's `http/parse.rs` supplies the explicit pUSD/6-decimal contract reference.
Legacy `base_fee` is not substituted for the schedule rate.

Predict price grid comes from market `decimalPrecision`; quote grid and quantity
precision are distinct. Native integer executions and charged fees scale exactly
by 10^18. Model objects preserve 18 places without floats. Their 1e-18 quantity
increment is an exact **representation** unit, not an asserted venue order lot.
The pinned Predict SDK helper rejects quantities below 0.01 and truncates new
limit-order price/quantity to three/five significant digits. These SDK checks
are not independent certification of a market's exchange minimum. The source
does not report such a minimum; it remains explicitly unverified.

Predict taker Bid/Ask maps to BUY/SELL of the taker's own outcome, including NO.
Only the native YES book is exported as executable depth. NO is a complementary
view of the same pool, never independent additional liquidity. The data-only L2
probe creates visibly synthetic **empty/unavailable** book controls for any
trades-only NO instruments required by engine validation. These have no snapshot
flag and permit no execution. NO execution requires routing/consumption against
the shared YES pool; this probe does not claim to implement that router.

References are pinned in the previous Predict adapter audit:

- [Predict SDK OrderBuilder](https://github.com/PredictDotFun/sdk/blob/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486/src/OrderBuilder.ts)
- [Predict SDK constants](https://github.com/PredictDotFun/sdk/blob/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486/src/Constants.ts)
- [Predict native book semantics](https://dev.predict.fun/doc-685654)
- [Pinned Polymarket instrument parser](https://github.com/nautechsystems/nautilus_trader/blob/698d5cc8f7012bdc111b060e97de3210ee0a6002/crates/adapters/polymarket/src/http/parse.rs)

## Fees and gaps

`PolymarketNoRewardFeeModel` uses explicit fill liquidity side, zero maker
commission and taker `quantity * rate * price * (1-price)`, floored to five
decimal places. This ports the reviewed engine rounding fix into the private
adapter for the older installed wheel. It excludes uncertain daily rewards.
It never treats missing fee terms as free trading. Multi-execution settlement
rounding is separate from a per-fill simulated commission.

Predict reported BUY share fees reduce received inventory; SELL collateral fees
reduce cash proceeds. The exact ledger verifies conservation and compares the
native integer fee curve with reported amounts. Cash-equivalent fees do not
replace share debits. The matching probe deliberately labels the native engine's
Predict position as gross and records its estimated net-share overlay separately.
**Unmodified Money-only engine P&L is not certified for Predict share fees.**

At a recorded gap, all possibly affected books are cleared. Unknown socket scope
means conservative venue-wide invalidation. Updates cannot re-arm an invalid
book; only a fresh full snapshot can. The shared paper lifecycle cancels resting
orders and drops readiness on a control CLEAR. Observed terminal markets are
cleared without guessing terminal payouts from unknown-scale integer messages.
Material tick or fee revisions block use of stale initial specifications.

## Exact cold storage and reproduction

The installed wheel accepts 18-place Price/Quantity objects but its native
catalog encoder caps decimals at scale 16. `exact_spill=True` writes exactly
representable values at scale 16, with no rounding. A whole event needing more
precision goes into hash-bound `exact-overflow.parquet`. The **private**
`NautilusExporter.read_export` restores both streams and original instrument
precision before engine ingestion. Hashes/counts/event ordering must match.
A standalone native catalog query omits overflow events and is not complete;
use this reader until the explicitly pinned engine/catalog is upgraded.

Run from this private project with its existing runtime:

```powershell
.venv/Scripts/python.exe -B tools/accept_compact_archive.py SOURCE_ARCHIVE NEW_LOCAL_TARGET
```

The command verifies the entire input, writes a new independent catalog, cold
reads it, runs L2 data ingestion and one simulated taker IOC per venue against
retained native depth. It writes English `acceptance.json` with timings, counts,
rounding/precision contracts, fee accounting, controls and limitations. There
are no provider calls, new collection, live orders or source deletion.

The 2026-10-29 acceptance reused the saved 200-Pair five-minute trial. Acceptance
of data and matching plumbing does not certify venue minimums, queue priority,
full trade coverage, terminal payouts, Predict net-account integration or the
newer desired engine build. See `docs/validation/compact-archive-20261029.json`.
