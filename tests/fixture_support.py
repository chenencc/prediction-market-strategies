"""Restore small synthetic recorder outputs; never downloads market data."""

from copy import deepcopy
import gzip
import json
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import pyarrow as pa
import pyarrow.parquet as pq
from prediction_market_strategies.data.recording_group import (
    canonical,
    digest,
    file_hash,
    RecordingGroup,
)


def samples():
    path = Path(__file__).parent / "fixtures/recording-groups.json.gz"
    return json.loads(gzip.decompress(path.read_bytes()))


def with_initial_gap(value):
    """Add a bound synthetic transport failure before the first saved book."""
    value = deepcopy(value)
    with TemporaryDirectory() as folder:
        original = list(
            RecordingGroup(write_group(Path(folder) / "source", value)).iter_rows(
                "orderbook.parquet"
            )
        )
    first = next(r for r in original if r["kind"] == "book")
    gap = {k: v for k, v in first.items() if k != "book"}
    gap.update(
        kind="rest_gap",
        asset_id=None,
        outcome=None,
        source_time_ms=None,
        metadata_ref=None,
        book_reset=True,
        operation_json=None,
        received_unix_ns=first["received_unix_ns"] - 1,
        context_json=canonical({"reason": "synthetic_test_timeout"}),
    )
    original.insert(original.index(first), gap)
    logical = hashlib.sha256()
    semantic = hashlib.sha256()
    rows = []
    for sequence, item in enumerate(original):
        row = {k: v for k, v in item.items() if k != "book"}
        row["sequence"] = sequence
        semantic_row = (
            {
                **{k: v for k, v in row.items() if k != "operation_json"},
                "book": item["book"],
            }
            if row["kind"] == "book"
            else row
        )
        logical.update((canonical(row) + "\n").encode())
        semantic.update((canonical(semantic_row) + "\n").encode())
        rows.append(row)
    body = value["files"]["orderbook.parquet"]
    body["rows"] = rows
    meta = body["metadata"]
    meta.update(
        rows=len(rows),
        logical_sha256=logical.hexdigest(),
        semantic_sha256=semantic.hexdigest(),
    )
    meta["kinds"]["rest_gap"] = meta["kinds"].get("rest_gap", 0) + 1
    return value, gap


def write_group(root, value):
    value = deepcopy(value)
    root = Path(root)
    root.mkdir(parents=True)
    ctx = value["context"]
    pq.write_table(
        pa.table({"payload": [canonical(ctx)]}),
        root / "context.parquet",
        write_page_checksum=True,
    )
    entries = [
        dict(
            path="context.parquet",
            kind="context",
            rows=1,
            group_id=ctx["group_id"],
            logical_sha256=digest(ctx),
            semantic_sha256=digest(ctx),
        )
    ]
    for name, body in value["files"].items():
        meta = body["metadata"]
        meta["header"]["context_sha256"] = file_hash(root / "context.parquet")
        table = pa.Table.from_pylist(body["rows"]).replace_schema_metadata(
            {b"group_parquet": canonical(meta).encode()}
        )
        pq.write_table(table, root / name, write_page_checksum=True)
        entries.append(
            dict(
                path=name,
                kind=meta["kind"],
                rows=meta["rows"],
                group_id=ctx["group_id"],
                logical_sha256=meta["logical_sha256"],
                semantic_sha256=meta["semantic_sha256"],
                kinds=meta["kinds"],
                stream_observations=meta["stream_observations"],
            )
        )
    for entry in entries:
        p = root / entry["path"]
        entry.update(bytes=p.stat().st_size, sha256=file_hash(p))
    m = dict(
        schema="recording-group-parquet/1",
        plan_id=ctx["plan_id"],
        group_id=ctx["group_id"],
        members=ctx["members"],
        native_audit_proof_hash=ctx["native_audit_proof_hash"],
        files=entries,
    )
    m["manifest_hash"] = digest(m)
    (root / "publication.json").write_text(canonical(m), encoding="utf-8")
    return root


def modify_terms(value, func):
    value = deepcopy(value)
    for market in value["context"]["markets"]:
        remap = {}
        for v in market["versions"]:
            old = v["revision"]
            func(v["terms"])
            v["revision"] = digest(v["terms"])
            remap[old] = v["revision"]
        for obs in market["observations"]:
            obs["revision"] = remap[obs["revision"]]
    return value
