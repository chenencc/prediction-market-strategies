# Third-party strategy reference

`src/prediction_market_strategies/strategies/binary_pair_arbitrage.py` adapts the
binary-pair strategy concept and entry checks from:

* Repository: https://github.com/evan-kolberg/prediction-market-backtesting
* Commit: `c76e77af00ef53472a9da8f66dae7fdd2d3e5928`
* File: `strategies/binary_pair_arbitrage.py`
* Original header: Copyright (C) 2015-2026 Nautech Systems Pty Ltd.
* License: GNU Lesser General Public License, version 3.0 or later.
* License text: https://www.gnu.org/licenses/lgpl-3.0.html

Our adapted strategy file retains this license. It was rewritten for the
installed 2.x API, exact Decimal arithmetic, observed fees, stale-book checks,
delayed decisions, price-capped IOC orders and explicit residual handling.
This notice does not relicense other dependencies or declare all upstream
repository files MIT. See the dependency license files for NautilusTrader.
