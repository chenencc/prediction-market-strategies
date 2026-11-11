"""Offline short-hold cross-venue pair research on locally converted native V3."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO/"src"))
sys.path.insert(0, str(REPO.parent/"market-data-collector/services"))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, default=Path("E:/metaculus_data/backtests/hf-v3/production1000"))
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--max-chunks", type=int)
    p.add_argument("--through-sequence", type=int)
    args = p.parse_args()
    spec = importlib.util.spec_from_file_location("runtime", REPO/"tools/run_v3_loop.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    runtime = module.verify_runtime(REPO/"runtime.compatibility.lock.json")
    from prediction_market_strategies.backtest.pair_research import run
    result = run(args.root, args.output, max_chunks=args.max_chunks, through_sequence=args.through_sequence)
    from prediction_market_strategies.data.hf_sync import atomic_json, sha
    atomic_json(args.output/"code-receipt.json", dict(runtime=runtime, files={str(path.relative_to(REPO)):sha(path)
        for path in [REPO/"src/prediction_market_strategies/backtest/pair_research.py",
                     REPO/"src/prediction_market_strategies/strategies/pair_convergence.py"]}))
    print(json.dumps({k:v for k,v in result.items() if k != "source_pins"}), flush=True)


if __name__ == "__main__":
    main()
