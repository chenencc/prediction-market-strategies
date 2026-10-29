# Third-party strategy reference

`src/prediction_market_strategies/strategies/binary_pair_arbitrage.py` adapts the
binary-pair strategy concept and entry checks from:

* Repository: https://github.com/evan-kolberg/prediction-market-backtesting
* Commit: `c76e77af00ef53472a9da8f66dae7fdd2d3e5928`
* File: `strategies/binary_pair_arbitrage.py`
* Original header: Copyright (C) 2015-2026 Nautech Systems Pty Ltd.
* License: GNU Lesser General Public License, version 3.0 or later.
* License text: https://www.gnu.org/licenses/lgpl-3.0.html

Our adapted strategy file and its extracted `binary_market.py` lifecycle retain
this license. It was rewritten for the
installed 2.x API, exact Decimal arithmetic, observed fees, stale-book checks,
delayed decisions, price-capped IOC orders and explicit residual handling.
This notice does not relicense other dependencies or declare all upstream
repository files MIT. See the dependency license files for NautilusTrader.

## Depth-maker behavior reference

The independently implemented `strategies/depth_maker.py` policy was informed
by the public README and depth-tier behavior in
https://github.com/ccjingeth/predict-fun-marketmaker at commit
`9346c7abefaff73758586e6cb8d7193fcbc79afb`. That repository's `package.json`
declares MIT; the inspected root has no standalone LICENSE file. No TypeScript,
wallet, desktop or provider runtime from that project is copied or executed.
Our native adapter uses the LGPL shared lifecycle and retains its SPDX notice.

## Polymarket simulated fee rounding

`paper/fees.py` follows the exact taker-fee flooring rule reviewed in
`nautechsystems/nautilus_trader` commit
`698d5cc8f7012bdc111b060e97de3210ee0a6002`,
`crates/adapters/polymarket/src/models.rs` (LGPL-3.0-or-later).
It supplies the same five-place floor for the older compatibility wheel,
validates the fill liquidity side and excludes daily rewards from commissions.
It does not change the engine source pin or certify actual venue-paid fees.
