# Local Paper Observatory

This local terminal observes finite or rolling Nautilus Paper runs. It shows native YES/NO or Up/Down depth,
orders, simulated fills, inventory, cash, fees, decision reasons and historical
frames. It has no execution endpoints and cannot send real exchange orders.

## Start the terminal

Use the existing, explicitly verified compatibility engine environment described
in `PAPER_TRADING.md` and `runtime.compatibility.lock.json`. This work does not
validate the newer engine source pin or install a floating engine dependency.

From the repository root, in PowerShell:

```powershell
.venv/Scripts/python.exe -m pip install -e '.[monitor]'
npm ci --prefix web
npm run build --prefix web
.venv/Scripts/python.exe -m prediction_market_strategies.paper.server
```

Open <http://127.0.0.1:5190>. The interface reads `results/` and `web/dist/` by
default. It listens on loopback only; cross-origin browser requests and foreign
Host headers are rejected. Closing the page does not stop a Paper process.

The interface is read-only. Start experiments from a separate terminal, with a
new output directory for each run. Runs appear automatically. By default it
follows the newest run; selecting a historical run disables following until
the user chooses the follow button again.

## Continuous BTC observation

```powershell
.venv/Scripts/python.exe -m prediction_market_strategies.paper.continuous `
  --root results --session-id my-btc-observation

# Graceful stop: finish outstanding virtual orders and attempt to close inventory.
.venv/Scripts/python.exe -m prediction_market_strategies.paper.continuous `
  --root results --stop-session my-btc-observation
```

This session uses the currently open official BTC 15-minute Up/Down market. It
validates the returned slug, condition ID, outcomes, close time and orderability
before starting one child at a time. The underlying native WebSocket feed is
live; only execution is simulated. No credentials or real execution client are
configured. Keep the local process and computer running for ongoing observation.

Each cycle buys 10 Up shares with a native IOC limit order, then requests a sell
after a 0.02 price move in either direction or 10 seconds of holding. After a
confirmed flat cycle it waits 5 seconds. Decisions are limited to every 100 ms;
all arriving native book updates still reach the engine. Freshness, spread,
depth, fee, minimum-size and order-confirmation checks remain active. This is an
activity demonstration: repeated crossing of the spread and fees can steadily
lose virtual money. It is not designed as a profitable trading signal.

New entries stop 60 seconds before expiry; remaining inventory is requested to
exit 45 seconds before expiry. After verified flat native shutdown, the next
current window is discovered automatically. Cash, losses and cumulative fees
carry forward; no new 1,000-unit reset occurs. Unknown order state, residual
inventory, collateral changes or a failed child stop continuation with the
original reports retained. Discovery failures retry without inventing a market.
A root-level OS lock prevents a second session owner. Free disk below 2 GiB
prevents another child from starting.

An explicitly stopped session can be resumed into a new session ID with
`--carry-from-session <old-session-id>`; the prior native closing report and
account balance must agree. Use the graceful stop command above, rather than
terminating the processes. Session control, discovery receipts and child logs
are in `results/_sessions/<session-id>/`; UI runs are in
`results/<session-id>-0001/`, `-0002/`, and so on.

**Continuous observation storage is deliberately different from finite replay
capture.** It retains instrument metadata, monitor frames, decisions, native
order/fill events and final account reports. It does not retain every high-rate
raw WebSocket book batch in memory or export a full raw-input replay catalog.
Reports explicitly mark `raw_book_inputs_preserved=false` and
`full_input_replay_available=false`. Finite capture/replay behavior is unchanged.

The interface consumes replacement snapshots approximately every 0.5 seconds,
refreshes historical chart points every 5 seconds and appends the newest live
point immediately. UI connection, process heartbeat and official-book freshness
are shown separately. A connected page alone does not prove fresh market data.

## Simple single-market strategy

The strategy buys YES at most once. Defaults are 10 shares, maximum buy price
0.50, maximum spread 0.04 and maximum entry notional including fees 20. It uses
a native, depth-bounded IOC limit order. It never sells more than its recorded
holdings. Missing fees, stale books, insufficient depth and uncertain order
state prevent further execution.

After an actual simulated fill, it requests an exit if the current bid is 0.02
above or below average entry, or 15 seconds have elapsed. These are price offsets,
not percentages or guaranteed net profit. Exit still requires a fresh complete
book and sufficient bid depth; 15 seconds is not a guaranteed liquidation time.
Partial residuals below the venue minimum are retained and reported as a failed
trial. The runner does not fabricate closing fills at its deadline.

Example using a market slug (the market must still be open and compatible):

```powershell
.venv/Scripts/python.exe -m prediction_market_strategies.paper.polymarket_pair `
  --strategy single-market `
  --slug france-declares-state-of-emergency-by-december-31-2026 `
  --seconds 60 --quantity 10 --buy-below 0.50 `
  --take-profit 0.02 --stop-loss 0.02 --hold-seconds 15 `
  --output results/my-new-paper-run
```

Only official public metadata and market data are requested. Nautilus Sandbox
provides virtual 1,000 units of the observed collateral currency. The finite
live window is 30–600 seconds. Initialization and bounded shutdown are additional.
There is no signing key, account login, live execution adapter or server deploy.

## Replay without external calls

```powershell
# Explicitly labelled synthetic matching scenario
.venv/Scripts/python.exe -m prediction_market_strategies.paper.replay_monitor `
  --output results/new-synthetic-ui-run

# Reuse an existing native Paper archive without modifying it
.venv/Scripts/python.exe -m prediction_market_strategies.paper.replay_monitor `
  --source results/local-pair-paper-20261027-r5 `
  --output results/new-real-data-ui-replay
```

Replay executes the same strategy through the native BacktestEngine. The source
directory needs `initial-instruments.json` and a native `catalog/`. Snapshot/delta
batch boundaries and receive clocks are preserved. It does not reconnect to an
exchange. The terminal distinguishes live public data, saved real replay and
synthetic validation; none of these produces real trades.

## Observation boundary

- The engine thread reads its own native cache. Every 250 ms it builds a plain
  observation; the background writer coalesces display frames independently.
- Critical order/fill events have a bounded queue and monotonic sequence. Queue
  overflow or writer failure stops further strategy actions and is reported.
  Display frames may be coalesced; they are not the raw market data archive.
- `monitor-state.json` is atomically replaced. `monitor-events.jsonl` and
  `monitor-history.jsonl` are small UI/audit journals, not a SQL database.
- In finite capture mode, native instruments and book events stay in the existing Parquet catalog.
  `report.json` records final results, archive checks and artifact hashes.
- The local API serves GET-only run/state/history/frame/SSE endpoints. SSE
  reconnects replace full state, avoiding duplicate fill accumulation.
- Financial values and nanosecond clocks cross the API as strings. UI formatting
  does not change native values. Prices use actual observation time on the chart;
  stale/missing sections are blank, with separate simulated fill markers.
- Inventory is based on deduplicated native fills. Realized P&L includes recorded
  commissions and allocated cost basis. Remaining inventory is valued against
  native bid depth before hypothetical exit fees. Stale or insufficient depth
  makes valuation unavailable rather than substituting a mid-price.

## Validation and limits

On 2026-10-30, 94 offline tests passed, including a native round trip, partial
fills below the minimum, stale held inventory, exact monetary/clock serialization
and shutdown draining 1,000 critical events. Frontend type checking and production
build passed. Saved real data replay completed with 382 display frames.

The finite public-data trial completed two simulated fills: buy 10 YES at 0.08,
sell 10 at 0.07, with recorded zero fees, ending virtual cash 999.90 pUSD and no
inventory. Only four native book batches arrived, and the socket had a heartbeat
timeout/disconnection. The exit waited for fresh data. This verifies a bounded
Paper round trip, not stable feed operation, fill realism or profitable strategy.

The initial UI does not launch jobs, edit strategy parameters, estimate queue
position, simulate exchange incentives, settle on-chain assets, or support live
trading. The matcher uses configured 100 ms order latency; latency and queue
assumptions are not measurements of real exchange execution.
