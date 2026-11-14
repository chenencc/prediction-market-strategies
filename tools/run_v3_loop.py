"""Rebuild a retained real V3 sample and run the same strategy in fresh processes.

No HF requests or exchange clients. Only conversion fidelity is checked here;
this does not repeat the collector's post-publication quality audit.
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import importlib.metadata as metadata
import json
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def block_network():
    def guard(event, args):
        if event in {"socket.connect", "socket.getaddrinfo", "socket.bind"}:
            raise RuntimeError("offline_validation_network_forbidden")
    sys.addaudithook(guard)


def verify_runtime(lock_path):
    import nautilus_trader as nt
    lock = json.loads(Path(lock_path).read_bytes())
    if nt.__version__ != lock["nautilus_trader"] or platform.python_version() != lock["python"]:
        raise ValueError("runtime_version_differs_from_lock")
    if lock.get('system') and (platform.system()!=lock['system'] or platform.machine()!=lock['machine']):
        raise ValueError('runtime_platform_differs_from_lock')
    dependencies = {}
    for name in lock.get('requirements_files', ("requirements-v3-loop.txt", "requirements-exporter-runtime.txt")):
        for line in (REPO / name).read_text(encoding="utf-8").splitlines():
            match = re.fullmatch(r"([A-Za-z0-9_-]+)==([^\s]+)", line.strip())
            if match:
                package, expected = match.groups()
                if metadata.version(package) != expected:
                    raise ValueError("dependency_version_differs_from_lock:" + package)
                dependencies[package] = expected
    dist = metadata.distribution("nautilus-trader")
    direct = json.loads(dist.read_text("direct_url.json"))
    if direct["archive_info"]["hashes"]["sha256"] != lock["wheel_sha256"] or direct["url"] != lock["wheel_url"]:
        raise ValueError("installed_wheel_provenance_changed")
    checked = 0
    for entry in dist.files:
        if entry.hash:
            expected = base64.urlsafe_b64encode(bytes.fromhex(sha(dist.locate_file(entry)))).decode().rstrip("=")
            if entry.hash.mode != "sha256" or expected != entry.hash.value:
                raise ValueError("installed_engine_file_changed:" + str(entry))
            checked += 1
    extension = '*.pyd' if platform.system() == 'Windows' else '*.so'
    candidates = sorted(Path(nt.__file__).parent.glob(extension))
    binary = next((p for p in candidates if lock['engine_commit'].encode() in p.read_bytes()), None)
    if binary is None:
        raise ValueError('native_binary_commit_marker_mismatch')
    if lock["engine_commit"].encode() not in binary.read_bytes():
        raise ValueError("native_binary_commit_marker_mismatch")
    return dict(version=nt.__version__, source_commit=lock["engine_commit"],
                wheel_sha256=lock["wheel_sha256"], binary_sha256=sha(binary),
                installed_files_verified=checked, python=platform.python_version(), platform=platform.platform(),
                pinned_dependencies=dependencies,
                source_build_performed=False, verification="hash_pinned_wheel_RECORD_and_native_full_commit_marker",
                packages={d.metadata["Name"]: d.version for d in metadata.distributions()})


def git_state(root):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()
    paths = git("ls-files").splitlines()
    return dict(commit=git("rev-parse", "HEAD"), status=git("status", "--porcelain"),
                tracked_files={p: sha(root / p) for p in paths if (root / p).is_file()})


def validate_source(index, base):
    batches = index["batches"]
    if not 1 <= len(batches) <= 10:
        raise ValueError("bounded_1_to_10_source_batches_required")
    previous = None
    files = []
    for batch in batches:
        manifest_path = (base / batch["local_manifest"]).resolve()
        archive_path = (base / batch["local_archive"]).resolve()
        if sha(manifest_path) != batch["manifest_sha256"]:
            raise ValueError("source_manifest_changed")
        manifest = json.loads(manifest_path.read_bytes())
        if manifest != batch["manifest"] or manifest["schema"] != "file-spool-publication/3":
            raise ValueError("v3_manifest_binding_changed")
        if previous is not None and manifest["first"] != previous + 1:
            raise ValueError("source_sequence_gap")
        previous = manifest["last"]
        expected = manifest["files"][0]
        if sha(archive_path) != expected["sha256"] or archive_path.stat().st_size != expected["bytes"]:
            raise ValueError("source_archive_changed")
        files.extend([manifest_path, archive_path])
    return {str(p): sha(p) for p in files}


def source_from(index, base, cache):
    from local_postprocess.compact_standard import CompactSource

    class OfflineSource(CompactSource):
        def manifest_paths(self):
            return [b["path"] for b in index["batches"]]

        def download(self, name, expected=None):
            for batch in index["batches"]:
                if name == batch["path"]:
                    return base / batch["local_manifest"]
                if name == batch["manifest"]["files"][0]["remote_path"]:
                    return base / batch["local_archive"]
            raise ValueError("offline_cache_miss_no_download")

    return OfflineSource(index["repo"], index["revision"], index["collection"], cache,
                         first_sequence=index["batches"][0]["manifest"]["first"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--collector", type=Path, required=True)
    parser.add_argument("--source-receipt", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--engine-lock", type=Path, default=REPO / "runtime.compatibility.lock.json")
    parser.add_argument("--replay-only", type=Path, help="Existing derived root; output is a new JSON report")
    args = parser.parse_args()
    sys.path.insert(0, str(args.collector.resolve() / "services"))
    block_network()
    runtime = verify_runtime(args.engine_lock)
    if args.replay_only:
        from prediction_market_strategies.backtest.v3_minimal import run_minimal
        if args.output.exists():
            raise ValueError("fresh_report_path_required")
        result = run_minimal(args.replay_only)
        write(args.output, dict(runtime=runtime, result=result))
        print(json.dumps(dict(status=result["status"], events=result["input_events"], fills=len(result["fills"]))))
        return
    if args.source_receipt is None:
        parser.error("--source-receipt required")
    started = time.perf_counter()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    index = json.loads(args.source_receipt.read_bytes())
    original_hashes = validate_source(index, args.source_receipt.resolve().parent)
    (out / "input").mkdir()
    # Bundle the exact small retained sample so reproduction does not depend on HF retention.
    for n, batch in enumerate(index["batches"]):
        for field, suffix in (("local_manifest", "json"), ("local_archive", "zip")):
            source = (args.source_receipt.resolve().parent / batch[field]).resolve()
            name = f"input/{n:02d}.{suffix}"
            shutil.copyfile(source, out / name)
            batch[field] = name
    write(out / "source-index.json", index)
    validate_source(index, out)
    write(out / "runtime.json", runtime)
    shutil.copyfile(args.engine_lock, out / "engine.lock.json")
    code = dict(strategy=git_state(REPO), collector=git_state(args.collector.resolve()),
                loop_files={str(p.relative_to(REPO)): sha(p) for p in
                            [Path(__file__), REPO / "src/prediction_market_strategies/backtest/v3_minimal.py",
                             REPO / "requirements-v3-loop.txt", REPO / "requirements-exporter-runtime.txt"]})
    write(out / "code.json", code)
    from local_postprocess.compact_standard import run
    from local_postprocess.nautilus import export
    from local_postprocess.compact_audit import audit
    source = source_from(index, out, out / "cache")
    standard, native = out / "standard", out / "nautilus"
    conversion = dict(standard=run(source, standard, max_batches=len(index["batches"])))
    conversion["native"] = export(standard, native, max_batches=len(index["batches"]))
    conversion["fidelity"] = audit(source, standard, native)
    write(out / "conversion.json", conversion)
    print("Fresh V3 conversion and exact cold read completed", flush=True)
    results = []
    for attempt in (1, 2):
        report = out / f"replay-{attempt}.json"
        command = [sys.executable, "-B", str(Path(__file__).resolve()), "--collector", str(args.collector.resolve()),
                   "--engine-lock", str((out / "engine.lock.json").resolve()),
                   "--replay-only", str(native), "--output", str(report)]
        with (out / f"replay-{attempt}.log").open("w", encoding="utf-8") as log:
            completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=180)
        if completed.returncode:
            raise RuntimeError(f"replay_{attempt}_failed_see_log")
        results.append(json.loads(report.read_bytes()))
    if results[0] != results[1]:
        raise ValueError("fresh_process_results_not_identical")
    resume = dict(standard=run(source, standard, max_batches=len(index["batches"])),
                  native=export(standard, native, max_batches=len(index["batches"])))
    if resume["standard"]["new_batches"] or resume["native"]["new_batches"]:
        raise ValueError("resume_reprocessed_completed_sample")
    validate_source(index, out)
    if any(sha(p) != value for p, value in original_hashes.items()):
        raise ValueError("original_source_mutated")
    write(out / "resume.json", resume)
    result = results[0]["result"]
    report = dict(schema="v3-reproducible-loop/1", status="passed", at_utc=datetime.now(timezone.utc).isoformat(),
                  engine_commit=runtime["source_commit"], hf_revision=index["revision"], collection=index["collection"],
                  source_first=index["batches"][0]["manifest"]["first"], source_last=index["batches"][-1]["manifest"]["last"],
                  batches=len(index["batches"]), independent_replay_processes=2, results_exactly_equal=True,
                  source_unchanged=True, resume_new_batches=0, wall_seconds=time.perf_counter()-started,
                  strategy=result, conversion_fidelity=conversion["fidelity"],
                  upload_performed=False, hf_download_performed=False, production_mutated=False,
                  quality_certificate="not_present_in_this_historical_sample_unknown_not_pass",
                  network_policy="Python socket guard; offline file source; no native live clients constructed",
                  files={p.relative_to(out).as_posix(): sha(p) for p in sorted(out.rglob("*")) if p.is_file()})
    write(out / "acceptance.json", report)
    print(json.dumps({k: report[k] for k in ("status", "engine_commit", "batches", "results_exactly_equal", "wall_seconds")}))


if __name__ == "__main__":
    main()
