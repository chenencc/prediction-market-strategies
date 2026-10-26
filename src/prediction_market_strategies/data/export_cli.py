"""Run with python -m prediction_market_strategies.data.export_cli."""

import argparse
import json
from pathlib import Path
from .nautilus_exporter import NautilusExporter


def main():
    p = argparse.ArgumentParser(
        description="Verify and export one three-Parquet recording group offline"
    )
    p.add_argument("--source", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--max-records", type=int, default=250_000)
    p.add_argument("--max-deltas", type=int, default=2_000_000)
    p.add_argument("--trade-mode", choices=["observed", "exclude"], default="observed")
    p.add_argument(
        "--replay",
        action="store_true",
        help="Run a no-order local engine replay after export",
    )
    args = p.parse_args()
    result = NautilusExporter(
        max_records=args.max_records,
        max_deltas=args.max_deltas,
        trade_mode=args.trade_mode,
    ).export(args.source, args.output)
    if args.replay:
        from ..backtest.replay_probe import replay_probe

        report = replay_probe(result)
        Path(args.output, "replay-report.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
    print(
        json.dumps(
            dict(output=str(Path(args.output).resolve()), **result.report),
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
