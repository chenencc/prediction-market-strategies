# Cross-venue pair convergence research

Current scope, 2026-11-12 Asia/Shanghai: download the complete available V3
snapshot to E:, retain verified Nautilus replay, then research short holding
periods on Polymarket × Predict.fun pairs. This is local offline research. No
exchange orders, collector changes, HF uploads or remote deletions are involved.

## Frozen data import

Repository: `chchen13/forecastagent-data`.
Prefix: `production/markets1000-20261110-r2`.
Snapshot: `e0a7982b34e4b66c3254e58024e2f9efdd7497cf`.
Inventory: 1,174 immutable core ZIPs, 2,027,619,354 compressed bytes, source
sequences 1 through 67,326,978. This is all history present at the frozen
revision, not future uploads. HF currently exposes only this production prefix.

Core ZIPs mix paired and independent markets. Downloading all core ZIPs is a
superset needed to preserve source sequence and conversion checkpoints. Pair
selection happens after conversion, using original identity and outcome maps.
Single-market groups are excluded. A historical union of pair definitions is
not a simultaneous market count.

Destination: `E:/metaculus_data/backtests/hf-v3/production1000`.
Already verified initial chunks are reused. The snapshot job records inventory,
source pins, process identity, progress and per-chunk conversion receipts. It
downloads in bounded parallel workers, retries failed requests without skipping
sequences, then converts sequentially with the existing fixed converter and
engine. On failure, rerun the identical command; completed payload hashes and
committed conversions are reused. An exclusive worker lock prevents duplicate
jobs. The engine remains `b23139911fd9fed82aed2811c5a8a373202b9480`.

```powershell
$py = 'D:/metaculus/.tmp/hf-v3-sync-env/Scripts/python.exe'
& $py -B tools/pair_history_sync.py `
  --token-file 'C:/Users/Administrator/Desktop/hf.txt' `
  --revision e0a7982b34e4b66c3254e58024e2f9efdd7497cf `
  --workers 8 `
  --research-output 'E:/metaculus_data/backtests/pair-research/snapshot-e0a7982-full-v1'
```

Do not start this while its worker is alive. Read
`snapshots/<revision>/status.json`, then verify its PID/command. A quiet log does
not establish failure. Status separately records download and conversion
completion. Local ZIPs are retired only after faithful conversion, cold readback
and durable ACK through the existing `hf_sync` implementation. HF is untouched.
References to quality certificates are preserved; source quality is not
re-audited after upload. See [HF_V3_SYNC.md](HF_V3_SYNC.md).

Full research is chained after conversion, with an explicit ending sequence.
A changed research-code hash stops that stage. A failed research run preserves
its output; inspect it and use a new output directory for a corrected run.

## Strategy and simulation contract

The first policy buys one outcome on Polymarket and the complementary outcome
on Predict.fun. Both orientations are evaluated. It tries to sell both legs
after net profit, stop-loss or timeout. It never books hypothetical settlement
proceeds, including when the data ends. Still-open positions, pending legs and
failed exits remain explicit.

`strategies/pair_convergence.py` is an independent research matching model.
`backtest/pair_research.py` reads actual Nautilus events and applies them to
native `OrderBook` objects, including exact overflow. It does **not** yet submit
orders through Nautilus `BacktestEngine`; that integration and venue execution
certification remain later gates. Do not label this a native-engine P&L test.

The controlled baseline compares 300/900/3,600-second holding limits with:

- five shares, separate 1,000-unit cash budgets per venue, reserved before orders;
- one-second decision interval, five-second book-age ceiling, two-second quote
  skew ceiling, 250ms Poly and 500ms Predict independent arrival delays;
- two-cent entry surplus after estimated entry/exit fees and execution buffer;
- 0.5-cent per-share take-profit, three-cent stop-loss, price-capped IOC legs;
- partial/zero leg fills preserved; incomplete entry requests liquidation;
- no future fee metadata, no rewards, no default zero fee, no settlement credit;
- conservative shared visible-depth consumption across all pairs. Predict's
  derived complement reads and consumes the opposite side of its SAME native
  book; it is not extra liquidity. An unchanged public quantity does not refill
  simulated consumed size.

Parameters are research assumptions, not measured venue latency or optimized
trading settings. Timeout starts an exit attempt; stale/missing liquidity can
leave inventory open longer than the holding limit. Four executions and their
fees matter for a complete round trip.

Fee formulas use historical market fields and the explicitly selected current
formula family, with conservative cash-equivalent rounding plus a buffer:
[Polymarket fees](https://docs.polymarket.com/trading/fees) and
[Predict fees](https://dev.predict.fun/trading/fees). Actual share deductions,
per-match rounding, discounts, collateral parity, executable precision and full
venue minimum rules are not certified. Missing fee formulas reject entries.
Poly minimum shares use metadata; Predict's 1-USDT minimum is an explicit
research assumption from its [limits](https://docs.predict.fun/the-basics/predict-fees-and-limits).

Pair mapping is not proof that exceptional settlement rules are equivalent.
All pair records are tagged `unverified_mapping_only`; simulated results remain
conditional research, not executable arbitrage or certified profitability.
Source quality gaps/unknowns are not promoted to a pass.

## Running and reading results

```powershell
& $py -B tools/run_pair_research.py `
  --max-chunks 3 `
  --output 'E:/metaculus_data/backtests/pair-research/new-run-directory'
```

Without `--max-chunks`, read the entire committed local prefix. The snapshot
worker additionally passes `--through-sequence` to prevent silently including
later collection. Outputs contain pair maps, source pins, code hashes, actual
coverage, 5/15/60-minute scenario results, fills, closed trades, outstanding
inventory, cash per venue and both committed/prefunded capital-hours. Input
events are not sampled; policy decisions have a declared interval.

First real run: `20261112-first3-v1`, sequences 1..238,778, 234.678003852 seconds,
148 distinct historical cross-platform pair definitions, 296 orientations,
391,974 native events (391,704 book batches and 270 trades), 20,654 paired-book
batches. All scenarios produced no entries. Gates rejected quote skew/missing
books, insufficient net edge or Predict minimum notional. This short plumbing
run does not assess the 5/15/60-minute holding-window performance.

## Verification and next gates

Offline tests cover delayed independent legs, partial entry, forced losing exit,
missing exit liquidity, reserved funds, unchanged-depth reuse, complement depth
sharing, as-of metadata, invalid source sequences and single-market exclusion.
The complete existing suite passed 139 tests after setting the repository
PYTHONPATH and installing its declared `fastapi==0.121.0` test dependency.
Evidence: `E:/metaculus_data/backtests/pair-research/regression-20261112.log`.

Before profitability claims: finish full snapshot conversion; verify rule
equivalence and actual fee/collateral mechanics; inspect retained data-quality
gaps; run longer histories with separate training/holdout dates; measure capital
usage, hold-time tails, forced losses and actual exit capacity. Do not relax
freshness or fee gates merely to manufacture trades.
