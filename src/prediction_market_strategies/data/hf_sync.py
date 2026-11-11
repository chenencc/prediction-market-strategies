"""Incremental, read-only HF ingestion with local verified-cache retirement.

Only immutable core payloads and referenced quality certificates are downloaded.
No upload, remote deletion, exchange calls or post-publication quality audit.
"""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import time
import traceback


def sha(path):
    with extended_path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def blob(body):
    return hashlib.sha1(b"blob " + str(len(body)).encode() + b"\0" + body).hexdigest()


def atomic_json(path, value):
    path = extended_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    with part.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(part, path)


def read(path):
    return json.loads(extended_path(path).read_bytes())


def extended_path(path):
    text = str(Path(path).resolve())
    if os.name == "nt" and not text.startswith("\\\\?\\"):
        text = "\\\\?\\UNC\\" + text[2:] if text.startswith("\\\\") else "\\\\?\\" + text
    return Path(text)


def bounded(root, relative):
    root = Path(root).resolve()
    path = PurePosixPath(relative)
    if path.is_absolute() or ".." in path.parts or "\\" in relative or ":" in relative:
        raise ValueError("unsafe_relative_path")
    target = root.joinpath(*path.parts)
    if target.is_symlink() or not target.resolve().is_relative_to(root) or target.resolve() == root:
        raise ValueError("path_outside_owned_root")
    return target


def remove_work(root, work):
    target = bounded(root, Path(work).relative_to(root).as_posix())
    if target.parent != Path(root).resolve() / "work":
        raise ValueError("only_owned_work_directory_removal_allowed")
    if target.exists():
        shutil.rmtree(extended_path(target))


def inventory(folder):
    folder = extended_path(folder)
    return {p.relative_to(folder).as_posix(): {"sha256": sha(p), "bytes": p.stat().st_size}
            for p in sorted(Path(folder).rglob("*")) if p.is_file() and p.name != "receipt.json"}


def verify_bundle(folder, expected_receipt=None):
    folder = extended_path(folder)
    if expected_receipt and sha(folder / "receipt.json") != expected_receipt:
        raise ValueError("bundle_receipt_changed")
    receipt = read(folder / "receipt.json")
    for name, expected in receipt["files"].items():
        path = bounded(folder, name)
        if not path.is_file() or path.stat().st_size != expected["bytes"] or sha(path) != expected["sha256"]:
            raise ValueError("converted_file_changed:" + name)
    if receipt["conversion_verified"] is not True:
        raise ValueError("unverified_conversion")
    return receipt


def core_manifest(manifest, prefix):
    first, last = manifest["first"], manifest["last"]
    if type(first) is not int or type(last) is not int or not 0 < first <= last:
        raise ValueError("invalid_source_sequence")
    identity = f"{first:020d}-{last:020d}"
    if manifest["identity"] != identity:
        raise ValueError("source_identity_changed")
    schema = manifest.get("schema")
    if schema == "file-spool-publication/3":
        from mixed_recorder.file_backtest_publication import validate
        validate(manifest, prefix)
        core = manifest["files"][0]
    elif schema == "v3-local-audited-source/1":
        value = dict(manifest)
        digest = value.pop("sha256")
        encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
        if hashlib.sha256(encoded).hexdigest() != digest:
            raise ValueError("audited_manifest_hash_changed")
        core = manifest["core_file"]
    else:
        raise ValueError("unsupported_source_schema")
    archive = manifest["archive"]
    if (core != dict(remote_path=prefix + "/v3/events/" + identity + ".zip",
                     bytes=archive["bytes"], sha256=archive["sha256"])
            or archive["identity"] != identity or archive["first"] != first or archive["last"] != last
            or archive["encoding"] != "market-backtest/2-delta-jsonl-zstd6"
            or not re.fullmatch("[0-9a-f]{64}", core["sha256"])):
        raise ValueError("core_binding_changed")
    references = manifest.get("quality_certificates", [])
    if not isinstance(references, list) or len(references) > 256:
        raise ValueError("quality_reference_count_bound")
    for reference in references:
        name = reference.get("remote_path", "")
        if (not re.fullmatch("[0-9a-f]{64}", reference.get("sha256", ""))
                or type(reference.get("bytes")) is not int or not 0 < reference["bytes"] <= 8 * 1024**2
                or not name.startswith(prefix + "/v3/quality/") or ".." in PurePosixPath(name).parts
                or "\\" in name):
            raise ValueError("unsafe_quality_reference")
    return core


class HFRemote:
    """Stream straight to E:; no hidden HF payload cache on C: or D:."""
    def __init__(self, repo, token=None):
        from huggingface_hub import HfApi
        self.repo, self.token = repo, token
        self.api = HfApi(token=token)
        self.downloads = 0
        self.downloaded_bytes = 0

    def head(self, ref):
        return self.api.repo_info(self.repo, repo_type="dataset", revision=ref).sha

    def entries(self, revision, prefix, layout):
        directory = prefix + ("/segments" if layout == "segments" else "/v3/audited-sources")
        result = []
        for entry in self.api.list_repo_tree(self.repo, repo_type="dataset", revision=revision, path_in_repo=directory):
            if re.fullmatch(re.escape(directory) + r"/[0-9]{20}-[0-9]{20}\.json", entry.path):
                result.append(dict(path=entry.path, blob=getattr(entry, "blob_id", None)))
            if len(result) > 100000:
                raise ValueError("manifest_listing_bound")
        return sorted(result, key=lambda r: r["path"])

    def fetch(self, revision, name, target, expected=None, max_bytes=8 * 1024**2):
        from huggingface_hub import hf_hub_url
        from huggingface_hub.utils import build_hf_headers
        import httpx
        target = Path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_file() and expected and sha(target) == expected["sha256"] and target.stat().st_size == expected["bytes"]:
            return target
        limit = expected["bytes"] if expected else max_bytes
        if not 0 < limit <= max_bytes:
            raise ValueError("download_size_bound")
        part = target.with_suffix(target.suffix + ".part")
        url = hf_hub_url(self.repo, name, repo_type="dataset", revision=revision)
        for attempt in range(3):
            try:
                with httpx.Client(follow_redirects=True, timeout=60) as client:
                    with client.stream("GET", url, headers=build_hf_headers(token=self.token)) as response:
                        response.raise_for_status()
                        count = 0
                        with part.open("wb") as stream:
                            for data in response.iter_bytes(1024 * 1024):
                                count += len(data)
                                if count > limit:
                                    raise ValueError("remote_file_exceeds_bound")
                                stream.write(data)
                            stream.flush()
                            os.fsync(stream.fileno())
                if expected and (count != expected["bytes"] or sha(part) != expected["sha256"]):
                    raise ValueError("download_hash_or_size_changed")
                os.replace(part, target)
                self.downloads += 1
                self.downloaded_bytes += count
                return target
            except (httpx.TransportError, httpx.HTTPStatusError) as error:
                status = getattr(getattr(error, "response", None), "status_code", None)
                if attempt == 2 or (status is not None and status != 429 and status < 500):
                    raise RuntimeError("HF_read_failed_status_" + str(status)) from None
                time.sleep(2**attempt)


def connect(root):
    db = sqlite3.connect(Path(root) / "index.sqlite")
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("CREATE TABLE IF NOT EXISTS chunks (identity TEXT PRIMARY KEY, first_seq INTEGER UNIQUE, last_seq INTEGER, manifest_path TEXT, blob TEXT, receipt_sha TEXT, core_sha TEXT, cache_deleted INTEGER DEFAULT 0)")
    db.commit()
    return db


def retire_cache(root, db, row, failpoint=None):
    folder = Path(root) / "chunks" / row["identity"]
    receipt = verify_bundle(folder, row["receipt_sha"])
    expected = receipt["core"]
    if expected["sha256"] != row["core_sha"]:
        raise ValueError("cleanup_source_binding_changed")
    path = bounded(root, "cache/" + row["core_sha"] + ".zip")
    if path.exists():
        if sha(path) != expected["sha256"] or path.stat().st_size != expected["bytes"]:
            raise ValueError("cleanup_payload_changed")
        path.unlink()
    if failpoint == "after_unlink":
        raise RuntimeError("injected_after_unlink")
    with db:
        db.execute("UPDATE chunks SET cache_deleted=1 WHERE identity=?", (row["identity"],))


def sync(root, remote, config, *, max_chunks=3, delete_cache=False, failpoint=None):
    from pair_recorder.supervisor import OwnerLock
    from .v3_incremental import convert_chunk
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if not 1 <= max_chunks <= 100:
        raise ValueError("bounded_chunk_count_required")
    for name in ("cache", "chunks", "work", "runs"):
        (root / name).mkdir(exist_ok=True)
    owner = OwnerLock(root / "writer.lock")
    run_id = str(time.time_ns())
    report = dict(schema="hf-v3-incremental-run/1", status="running", new_chunks=0,
                  deleted_cache_chunks=0, root=str(root), remote_deletion=False,
                  source_quality_reaudited=False, started_ns=time.time_ns())
    try:
        config_path = root / "config.lock.json"
        if config_path.exists() and read(config_path) != config:
            raise ValueError("source_or_converter_lock_changed_use_new_root")
        if not config_path.exists():
            atomic_json(config_path, config)
        with closing(connect(root)) as db:
            # ACK-before-delete makes an interruption after unlink idempotent.
            if delete_cache:
                for row in db.execute("SELECT * FROM chunks WHERE cache_deleted=0 ORDER BY first_seq").fetchall():
                    retire_cache(root, db, row)
                    report["deleted_cache_chunks"] += 1
            revision = remote.head(config["ref"])
            if not re.fullmatch("[0-9a-f]{40}", revision):
                raise ValueError("exact_revision_required")
            entries = remote.entries(revision, config["prefix"], config["layout"])
            done = {r["identity"]: r for r in db.execute("SELECT * FROM chunks")}
            for entry in entries:
                old = done.get(PurePosixPath(entry["path"]).stem)
                if old and old["blob"] and entry["blob"] != old["blob"]:
                    raise ValueError("published_manifest_rewritten")
            candidates = [e for e in entries if PurePosixPath(e["path"]).stem not in done
                          and int(PurePosixPath(e["path"]).stem.split('-')[0]) >= config["first_sequence"]]
            report.update(revision=revision, discovered_manifests=len(entries), pending_chunks=len(candidates))
            atomic_json(root / "latest-run.json", report)
            for entry in candidates[:max_chunks]:
                if shutil.disk_usage(root).free < config["minimum_free_bytes"]:
                    raise ValueError("disk_reserve_reached")
                identity = PurePosixPath(entry["path"]).stem
                report["current_chunk"] = identity
                atomic_json(root / "latest-run.json", report)
                previous = db.execute("SELECT * FROM chunks ORDER BY first_seq DESC LIMIT 1").fetchone()
                expected_first = previous["last_seq"] + 1 if previous else config["first_sequence"]
                if int(identity.split('-')[0]) != expected_first:
                    raise ValueError("source_sequence_gap_expected_" + str(expected_first))
                target = root / "chunks" / identity
                if target.exists():
                    # A prior process finished the bundle but died before SQLite commit.
                    receipt = verify_bundle(target)
                    if receipt["manifest_blob"] != entry["blob"] or receipt["first"] != expected_first:
                        raise ValueError("orphan_bundle_source_changed")
                else:
                    work = root / "work" / identity
                    if work.exists():
                        remove_work(root, work)
                    work.mkdir()
                    manifest_path = remote.fetch(revision, entry["path"], work / "source-manifest.json")
                    if not entry["blob"] or blob(manifest_path.read_bytes()) != entry["blob"]:
                        raise ValueError("manifest_git_blob_changed")
                    manifest = read(manifest_path)
                    core = core_manifest(manifest, config["prefix"])
                    if manifest["identity"] != identity:
                        raise ValueError("manifest_filename_changed")
                    archive = remote.fetch(revision, core["remote_path"], root / "cache" / (core["sha256"] + ".zip"),
                                           core, max_bytes=512 * 1024**2)
                    if sha(archive) != core["sha256"] or archive.stat().st_size != core["bytes"]:
                        raise ValueError("download_hash_or_size_changed")
                    quality = []
                    for ref in manifest.get("quality_certificates", []):
                        if not ref["remote_path"].startswith(config["prefix"] + "/v3/quality/"):
                            raise ValueError("quality_reference_outside_collection")
                        path = remote.fetch(revision, ref["remote_path"], work / "quality" / (ref["sha256"] + ".json"), ref)
                        quality.append(path)
                    prev_folder = root / "chunks" / previous["identity"] if previous else None
                    if previous:
                        verify_bundle(prev_folder, previous["receipt_sha"])
                    bundle = convert_chunk(extended_path(work), archive, manifest, manifest_path, revision, entry,
                                           config, extended_path(prev_folder) if prev_folder else None, quality)
                    receipt = verify_bundle(bundle)
                    os.replace(bundle, target)
                    if failpoint == "after_bundle":
                        raise RuntimeError("injected_after_bundle")
                with db:
                    db.execute("INSERT INTO chunks(identity,first_seq,last_seq,manifest_path,blob,receipt_sha,core_sha) VALUES (?,?,?,?,?,?,?)",
                               (identity, receipt["first"], receipt["last"], entry["path"], entry["blob"],
                                sha(target / "receipt.json"), receipt["core"]["sha256"]))
                if failpoint == "after_commit":
                    raise RuntimeError("injected_after_commit")
                report["new_chunks"] += 1
                if delete_cache:
                    row = db.execute("SELECT * FROM chunks WHERE identity=?", (identity,)).fetchone()
                    retire_cache(root, db, row, failpoint)
                    report["deleted_cache_chunks"] += 1
                remove_work(root, root / "work" / identity)
            report.update(status="passed", completed_chunks=db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0],
                          through_sequence=db.execute("SELECT MAX(last_seq) FROM chunks").fetchone()[0],
                          pending_chunks=max(0, len(candidates)-report["new_chunks"]))
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__)
        report["stack"] = [dict(file=Path(frame.filename).name, line=frame.lineno, function=frame.name)
                           for frame in traceback.extract_tb(error.__traceback__)[-8:]]
        # Error strings from transport providers may contain private signed URLs.
        if isinstance(error, ValueError):
            report["reason"] = str(error)
        raise
    finally:
        report.update(finished_ns=time.time_ns(), downloaded_files=getattr(remote, "downloads", None),
                      downloaded_bytes=getattr(remote, "downloaded_bytes", None))
        atomic_json(root / "runs" / (run_id + ".json"), report)
        atomic_json(root / "latest-run.json", report)
        owner.close()
    return report


def iter_events(root):
    """Complete ordered Nautilus data including overflow; raw ZIPs are unnecessary."""
    from local_postprocess.nautilus import read_release
    root = Path(root)
    with closing(sqlite3.connect(f"file:{(root/'index.sqlite').as_posix()}?mode=ro", uri=True)) as db:
        previous = None
        for identity, first, last, receipt_sha in db.execute("SELECT identity,first_seq,last_seq,receipt_sha FROM chunks ORDER BY first_seq"):
            if previous is not None and first != previous + 1:
                raise ValueError("local_index_sequence_gap")
            folder = root / "chunks" / identity
            verify_bundle(folder, receipt_sha)
            yield from read_release(folder / "nautilus")
            previous = last


def iter_context(root, table="definitions"):
    """Read fee/market/as-of context, kept separately from native event objects."""
    from local_postprocess.nautilus import rows
    if table not in {"definitions", "observations", "trade_payloads", "trade_occurrences", "quotes"}:
        raise ValueError("unsupported_context_table")
    root = Path(root)
    with closing(sqlite3.connect(f"file:{(root/'index.sqlite').as_posix()}?mode=ro", uri=True)) as db:
        for identity, receipt_sha in db.execute("SELECT identity,receipt_sha FROM chunks ORDER BY first_seq"):
            folder = root / "chunks" / identity
            verify_bundle(folder, receipt_sha)
            yield from rows(folder / "standard", table)
