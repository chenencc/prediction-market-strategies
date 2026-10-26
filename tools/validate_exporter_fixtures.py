"""Exercise actual native catalogs and the engine with four synthetic recorder groups.

No provider calls. Output must be a new directory; existing runs are preserved.
Run after installing the explicitly pinned compatibility runtime and this package.
"""

import argparse
import json
from pathlib import Path
import platform
import sys

from prediction_market_strategies.data.nautilus_exporter import NautilusExporter
from prediction_market_strategies.data.recording_group import file_hash, require
from prediction_market_strategies.backtest.replay_probe import replay_probe


def main():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
    from fixture_support import samples, write_group

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), "VALIDATION_OUTPUT_ALREADY_EXISTS")
    args.output.mkdir(parents=True)
    fixture = samples()
    results = []
    for group in fixture["groups"]:
        task = group["task_type"]
        source = write_group(args.output / "sources" / task, group)
        before = {p.name: file_hash(p) for p in source.iterdir()}
        exported = NautilusExporter().export(source, args.output / "exports" / task)
        probe = replay_probe(exported)
        after = {p.name: file_hash(p) for p in source.iterdir()}
        require(before == after, "SOURCE_MODIFIED")
        row = dict(
            task_type=task,
            instruments=exported.report["instruments"],
            book_batches=exported.report["book_batches"],
            trade_ticks=exported.report["trade_ticks"],
            trade_counts=exported.report["trade_counts"],
            catalog_cold_readback_verified=exported.report[
                "catalog_cold_readback_verified"
            ],
            source_files_unchanged=before == after,
            source_hashes=before,
            replay=probe,
        )
        results.append(row)
        (args.output / "exports" / task / "replay-report.json").write_text(
            json.dumps(probe, indent=2), encoding="utf-8"
        )
    report = dict(
        schema="nautilus-exporter-fixture-validation/1",
        python=platform.python_version(),
        fixture_recorder_commit=fixture.get("recorder_commit"),
        fixture_provenance=fixture.get("provenance"),
        real_market_data=False,
        provider_calls=0,
        live_trading_enabled=False,
        engine_source_lock_runtime_verified=False,
        results=results,
        interpretation="Four synthetic recorder-format inputs through native catalog write, cold read and no-order engine run; not a live-data, strategy or P&L acceptance",
    )
    (args.output / "validation.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(json.dumps(report))


if __name__ == "__main__":
    main()
