"""Download new immutable HF V3 chunks and prepare local backtest data on E:."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("E:/metaculus_data/backtests/hf-v3/production1000"))
    parser.add_argument("--collector", type=Path, default=REPO.parent / "market-data-collector")
    parser.add_argument("--repo", default="chchen13/forecastagent-data")
    parser.add_argument("--prefix", default="production/markets1000-20261110-r2")
    parser.add_argument("--collection", default="markets1000-20261110-r2")
    parser.add_argument("--ref", default="main", help="Resolved to an exact revision on each pass")
    parser.add_argument("--layout", choices=("segments", "audited-sources"), default="segments")
    parser.add_argument("--first-sequence", type=int, default=1)
    parser.add_argument("--token-file", type=Path, help="Read locally; never copied to E: or saved in receipts")
    parser.add_argument("--max-chunks", type=int, default=3)
    parser.add_argument("--delete-converted-downloads", action="store_true", help="Delete only verified local ZIP cache after durable ACK")
    parser.add_argument("--minimum-free-gib", type=float, default=12)
    parser.add_argument("--passes", type=int, default=1)
    parser.add_argument("--poll-seconds", type=int, default=60)
    args = parser.parse_args()
    if not 1 <= args.passes <= 1440 or args.poll_seconds < 5 or args.first_sequence < 1 or args.minimum_free_gib < 1:
        parser.error("bounded passes, positive sequence, >=5s polling and >=1GiB reserve required")
    sys.path.insert(0, str(args.collector.resolve() / "services"))
    from prediction_market_strategies.data.hf_sync import HFRemote, sha, sync
    spec = importlib.util.spec_from_file_location("loop_runtime", REPO / "tools/run_v3_loop.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    runtime = module.verify_runtime(REPO / "runtime.compatibility.lock.json")
    token = args.token_file.read_text(encoding="utf-8-sig").strip() if args.token_file else None
    remote = HFRemote(args.repo, token)
    sources = ["services/local_postprocess/compact_standard.py", "services/local_postprocess/nautilus.py",
               "services/local_postprocess/instruments.py", "services/local_postprocess/standard.py",
               "services/mixed_recorder/backtest_spool.py", "services/mixed_recorder/backtest_fields.py"]
    private_sources = ["src/prediction_market_strategies/data/hf_sync.py",
                       "src/prediction_market_strategies/data/v3_incremental.py",
                       "src/prediction_market_strategies/data/nautilus_windows.py"]
    config = dict(schema="hf-v3-local-config/1", repo=args.repo, prefix=args.prefix.rstrip("/"), collection=args.collection,
                  ref=args.ref, layout=args.layout, first_sequence=args.first_sequence,
                  minimum_free_bytes=int(args.minimum_free_gib * 1024**3), engine_commit=runtime["source_commit"],
                  engine_binary_sha256=runtime["binary_sha256"],
                  converter_files={p: sha(args.collector / p) for p in sources},
                  private_files={p: sha(REPO / p) for p in private_sources})
    for index in range(args.passes):
        result = sync(args.root, remote, config, max_chunks=args.max_chunks,
                      delete_cache=args.delete_converted_downloads)
        print(json.dumps(result), flush=True)
        if index + 1 < args.passes:
            time.sleep(args.poll_seconds)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps(dict(status="failed", error_type=type(error).__name__,
                              reason=str(error) if isinstance(error, ValueError) else "see local receipt; credentials and URLs suppressed")))
        raise SystemExit(1)
