# Architecture boundaries

The engine is an exact-version dependency. Our default extension point is an
independent adapter/package; edit engine internals only when the public API is
insufficient. Record each necessary engine patch separately for easier upgrades.

## Project components

* `data`: load recorded inputs, preserve source/availability clocks, gaps,
  instrument versions and provenance. It must not silently download current
  market state to repair a historical backtest.
* `adapters/polymarket`: project-specific mapping around the pinned official
  Polymarket adapter and observed venue metadata.
* `adapters/predictfun`: independent venue/instrument, precision, fees and event
  semantics. Never label Predict data as Polymarket.
* `strategies`: signal generation and order intent using venue-neutral typed inputs.
* `risk`: budgets and exposure controls shared across strategies and venues.
* `backtest`: deterministic setup, simulation assumptions, run identity and reports.

Strategies and node configuration remain outside adapters. Recording, upload,
cloud verification and deletion stay in the existing acquisition system. This
repository consumes its final data contract; it does not own the running recorder
or Metaculus services.

## Data and simulation boundaries

The three stored Parquets are our archival format, not Nautilus catalog files.
`NautilusExporter` verifies and converts them into a separate native catalog.
Sampled depth does not establish
continuous changes, queue position, atomic multi-venue execution or guaranteed
fills. Historical post-capture trades and delayed chain verification are not
live-available strategy signals. Missing fees, precision and settlement evidence
remain explicit unknowns.

## Current integration boundary

The offline exporter has synthetic recorder-format catalog/readback/engine
validation. Real metadata completeness and exact engine-source validation remain
separate gates; see [exporter validation](NAUTILUS_EXPORTER.md).
An explicit [local paper runner](PAPER_TRADING.md) now attaches a complementary
binary strategy to public Polymarket data and a simulated execution client.
There is no real execution client. Predict.fun remains offline-only.
No execution or network work is enabled by import.
