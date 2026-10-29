"""Bounded offline acceptance using the private, explicitly pinned runtime.

Raw archives stay local and unchanged. Writes a derived Nautilus catalog and
English evidence; no venue clients, live orders, secrets or external calls.
"""

import argparse
from collections import Counter
import json
from pathlib import Path
import platform
import time

from prediction_market_strategies.data.compact_source import CompactSource
from prediction_market_strategies.data.nautilus_exporter import NautilusExporter
from prediction_market_strategies.backtest.replay_probe import replay_probe
from prediction_market_strategies.backtest.source_matching import matching_probe
from prediction_market_strategies.adapters.predictfun.accounting import (
    reconcile_reported_fees,
)


def main():
    import nautilus_trader as nt

    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("target", type=Path)
    parser.add_argument("--pair-id", action="append")
    args = parser.parse_args()
    times = {}
    started = time.monotonic()
    src = CompactSource(args.source)
    times["source_integrity_seconds"] = time.monotonic() - started
    before = time.monotonic()
    export = src.convert(pair_ids=args.pair_id)
    times["conversion_seconds"] = time.monotonic() - before
    x = export.report["venues"]["predictfun"]
    fees = reconcile_reported_fees(
        x["observed_aggregate_matches"], x["fee_bundles"], 200
    )
    if fees["errors"]:
        raise ValueError("REPORTED_FEE_RECONCILIATION_FAILED")
    before = time.monotonic()
    cold = NautilusExporter.write_catalog(export, args.target, exact_spill=True)
    times["catalog_write_and_cold_read_seconds"] = time.monotonic() - before
    before = time.monotonic()
    engine = replay_probe(cold, force_l2=True)
    times["engine_replay_seconds"] = time.monotonic() - before
    before = time.monotonic()
    matching = {v: matching_probe(cold, v) for v in ("POLYMARKET", "PREDICTFUN")}
    times["bounded_matching_seconds"] = time.monotonic() - before
    report = dict(
        schema="compact-archive-acceptance/1",
        status="exact_import_and_bounded_matching_passed_with_explicit_runtime_limits",
        source=str(args.source.resolve()),
        catalog=str(args.target.resolve()),
        runtime=dict(
            version=nt.__version__,
            python=platform.python_version(),
            private_strategy_repository="chenencc/prediction-market-strategies",
            desired_engine_lock_verified=False,
        ),
        pair_groups=len(cold.report["pair_ids"]),
        instruments=len(cold.instruments),
        event_counts=dict(Counter(type(r).__name__ for r in cold.data)),
        data_records=len(cold.data),
        cold_readback_verified=cold.report["catalog_cold_readback_verified"],
        catalog_precision_contract=cold.report["native_catalog_precision_contract"],
        source_manifest_sha256=cold.report["source_manifest_sha256"],
        venues={
            v: {
                k: x[k]
                for k in (
                    "books",
                    "trades",
                    "trade_ticks",
                    "gap_policy",
                    "derived_no_book_emitted",
                    "terminal_markets_no_assumed_payout",
                )
            }
            for v, x in cold.report["venues"].items()
        },
        predict_reported_fee_accounting=fees,
        engine=engine,
        matching=matching,
        timings=times,
        source_unchanged=CompactSource(args.source).manifest == src.manifest,
        limits=[
            "API observations/settlement aggregates are not certified atomic fills or complete history",
            "Predict SDK minimum is not an independently observed exchange order minimum",
            "Derived NO shares native YES liquidity; never execute two independent books against it",
            "Predict BUY share fees require inventory accounting, not Money-only commission",
            "No assumed terminal payouts from unconfirmed native integer resolution scales",
            "Exact desired engine.lock source not available in installed older Windows compatibility wheel",
        ],
        network_requests=0,
        live_orders=0,
    )
    (args.target / "acceptance.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                k: report[k]
                for k in (
                    "status",
                    "pair_groups",
                    "instruments",
                    "event_counts",
                    "data_records",
                    "cold_readback_verified",
                    "source_unchanged",
                    "timings",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
