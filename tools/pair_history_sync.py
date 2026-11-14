"""Freeze and download ALL V3 history, then resume verified native conversion.

Core chunks mix paired and unpaired markets. Preserve the complete immutable
source; pair membership is a time-qualified research filter, not a ZIP filter.
No HF writes, uploads, remote cleanup or exchange clients exist here.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


def validate_entries(entries, first_sequence):
    if not entries:
        raise ValueError("empty_snapshot")
    expected = first_sequence
    for entry in entries:
        name = Path(entry["path"]).stem
        if not re.fullmatch(r"[0-9]{20}-[0-9]{20}", name):
            raise ValueError("invalid_chunk_identity")
        first, last = map(int, name.split("-"))
        if first != expected or last < first:
            raise ValueError("snapshot_sequence_gap_expected_" + str(expected))
        expected = last+1
    return expected-1


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, default=Path("E:/metaculus_data/backtests/hf-v3/production1000"))
    p.add_argument("--collector", type=Path, help="Explicit alternate checkout; defaults to bundled readers")
    p.add_argument("--runtime-lock", type=Path)
    p.add_argument("--token-file", type=Path, required=True)
    p.add_argument("--revision", required=True)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--research-output", type=Path, help="Run full converted snapshot research after conversion completes")
    args = p.parse_args()
    if not 1 <= args.workers <= 8:
        p.error("1..8 download workers required")
    if not re.fullmatch("[0-9a-f]{40}", args.revision):
        p.error("exact 40-character revision required")
    from prediction_market_strategies.data.postprocess_runtime import configure_postprocess, runtime_lock_path
    collector = configure_postprocess(args.collector)
    from pair_recorder.supervisor import OwnerLock
    from prediction_market_strategies.data.hf_sync import (
        HFRemote, atomic_json, blob, core_manifest, read, sha, sync, connect,
    )
    from contextlib import closing
    import shutil

    root = args.root.resolve()
    job = root / "snapshots" / args.revision
    job.mkdir(parents=True, exist_ok=True)
    owner = OwnerLock(root / "history-worker.lock")
    status = dict(schema="pair-history-import/1", revision=args.revision,
                  phase="inventory", started_ns=time.time_ns(), pid=__import__("os").getpid(),
                  scope="all_core_chunks_pair_membership_preserved", remote_mutation=False)
    code_paths = [REPO / "tools/pair_history_sync.py", REPO / "tools/run_pair_research.py",
                  REPO / "src/prediction_market_strategies/backtest/pair_research.py",
                  REPO / "src/prediction_market_strategies/strategies/pair_convergence.py"]
    code_hashes = {str(path.relative_to(REPO)): sha(path) for path in code_paths}
    def save(**values):
        status.update(values, updated_ns=time.time_ns())
        atomic_json(job / "status.json", status)
        print(json.dumps(status), flush=True)
    try:
        atomic_json(job / ("worker-code-" + str(status["started_ns"]) + ".json"), code_hashes)
        cfg = read(root / "config.lock.json")
        for path, digest in cfg["converter_files"].items():
            if sha(collector / path) != digest:
                raise ValueError("collector_lock_changed:" + path)
        if cfg.get('bundled_source_manifest_sha256'):
            if sha(collector/'SOURCE.json') != cfg['bundled_source_manifest_sha256']:
                raise ValueError('bundled_source_manifest_changed')
        for path, digest in cfg["private_files"].items():
            if sha(REPO / path) != digest:
                raise ValueError("private_converter_lock_changed:" + path)
        spec = importlib.util.spec_from_file_location("loop_runtime", REPO / "tools/run_v3_loop.py")
        runtime = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runtime)
        lock_path = runtime_lock_path(REPO,args.runtime_lock)
        verified = runtime.verify_runtime(lock_path)
        if verified["source_commit"] != cfg["engine_commit"] or verified["binary_sha256"] != cfg["engine_binary_sha256"]:
            raise ValueError("runtime_lock_changed")
        token = args.token_file.read_text(encoding="utf-8-sig").strip()
        remote = HFRemote(cfg["repo"], token)
        if remote.head(args.revision) != args.revision:
            raise ValueError("snapshot_revision_changed")
        entries = remote.entries(args.revision, cfg["prefix"], cfg["layout"])
        expected = validate_entries(entries, cfg["first_sequence"]) + 1
        atomic_json(job / "inventory.json", dict(revision=args.revision, config=cfg, entries=entries,
                                                through_sequence=expected-1, generated_ns=time.time_ns()))
        with closing(connect(root)) as db:
            done = {r["identity"]: dict(r) for r in db.execute("SELECT * FROM chunks")}
        for entry in entries:
            ident = Path(entry["path"]).stem
            if ident in done:
                if done[ident]["blob"] != entry["blob"]:
                    raise ValueError("completed_manifest_changed")
                # Prior commits already have durable full verification. Bind
                # the committed receipt here; sync verifies the predecessor
                # and research verifies all file contents as it reads them.
                receipt = root / "chunks" / ident / "receipt.json"
                if sha(receipt) != done[ident]["receipt_sha"]:
                    raise ValueError("completed_receipt_changed:" + ident)
        save(total_chunks=len(entries), reused_converted_chunks=len(done),
             through_sequence_target=expected-1, phase="download", downloaded_pending_chunks=0)

        def fetch(entry):
            ident = Path(entry["path"]).stem
            if ident in done:
                return 0
            if shutil.disk_usage(root).free < cfg["minimum_free_bytes"] + 8 * 512 * 1024**2:
                raise ValueError("download_disk_reserve_reached")
            client = HFRemote(cfg["repo"], token)
            target = job / "manifests" / (ident + ".json")
            if not target.exists() or blob(target.read_bytes()) != entry["blob"]:
                client.fetch(args.revision, entry["path"], target)
            if blob(target.read_bytes()) != entry["blob"]:
                raise ValueError("manifest_blob_mismatch")
            core = core_manifest(read(target), cfg["prefix"])
            client.fetch(args.revision, core["remote_path"], root / "cache" / (core["sha256"] + ".zip"),
                         core, max_bytes=512 * 1024**2)
            return core["bytes"]

        count, size = 0, 0
        pending = [e for e in entries if Path(e["path"]).stem not in done]
        for round_index in range(4):
            failed = []
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {pool.submit(fetch, e):e for e in pending}
                for future in as_completed(futures):
                    try:
                        size += future.result()
                        count += 1
                    except Exception as error:
                        failed.append(futures[future])
                        save(last_failed_manifest=futures[future]["path"],
                             last_error_type=type(error).__name__,
                             last_error=str(error) if isinstance(error, ValueError) or re.fullmatch(
                                 r"HF_read_failed_status_(None|[0-9]{3})", str(error)) else "transport_or_provider_failure")
                    if count % 20 == 0 or len(failed)+count >= len(entries)-len(done):
                        save(downloaded_pending_chunks=count, verified_cache_bytes=size,
                             retry_round=round_index, failures_this_round=len(failed))
            if not failed:
                break
            pending = failed
            save(retry_pending=[e["path"] for e in failed], retry_round=round_index+1)
            time.sleep(min(30, 5*(round_index+1)))
        else:
            raise ValueError("download_retry_exhausted_" + str(len(pending)))
        save(phase="convert", download_complete=True, download_completed_ns=time.time_ns(),
             catalog_workers=cfg.get("catalog_workers", 1))

        class FrozenRemote(HFRemote):
            def head(self, ref):
                return args.revision
            def entries(self, revision, prefix, layout):
                if revision != args.revision or prefix != cfg["prefix"] or layout != cfg["layout"]:
                    raise ValueError("frozen_snapshot_changed")
                return entries
            def fetch(self, revision, name, target, expected=None, max_bytes=8*1024**2):
                local = job / "manifests" / Path(name).name
                if name.startswith(cfg["prefix"] + "/segments/") and local.exists():
                    Path(target).parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(local, target)
                    return Path(target)
                return super().fetch(revision, name, target, expected, max_bytes)

        frozen = FrozenRemote(cfg["repo"], token)
        for _ in range(len(entries) + 1):
            result = sync(root, frozen, cfg, max_chunks=1, delete_cache=True)
            save(converted_chunks=result["completed_chunks"], pending_conversion=result["pending_chunks"],
                 converted_through_sequence=result["through_sequence"])
            if result["pending_chunks"] == 0:
                if result["through_sequence"] != expected-1:
                    raise ValueError("snapshot_end_not_reached")
                save(phase="complete", conversion_complete=True, completed_ns=time.time_ns())
                break
        else:
            raise ValueError("conversion_iteration_bound")
        if args.research_output:
            if any(sha(REPO/path) != digest for path,digest in code_hashes.items()):
                raise ValueError("research_code_changed_since_job_start")
            save(phase="research", research_output=str(args.research_output))
            with (job/"research.stdout.log").open("ab") as stdout, (job/"research.stderr.log").open("ab") as stderr:
                command = [sys.executable, "-B", str(REPO/"tools/run_pair_research.py"),
                           "--root", str(root), "--output", str(args.research_output),
                           "--through-sequence", str(expected-1), "--runtime-lock", str(lock_path)]
                result = subprocess.run(command, stdout=stdout, stderr=stderr, cwd=REPO)
            if result.returncode:
                raise ValueError("research_subprocess_failed_"+str(result.returncode))
            save(phase="complete", research_complete=True, completed_ns=time.time_ns())
    except Exception as error:
        save(phase="failed", error_type=type(error).__name__,
             reason=str(error) if isinstance(error, ValueError) else "details_suppressed_no_credentials_in_receipt")
        raise SystemExit(1) from None
    finally:
        owner.close()


if __name__ == "__main__":
    main()
