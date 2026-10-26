"""Read the frozen recording-group-parquet/1 contract without recorder imports.

The manifest, page checksums, logical hashes and decoded snapshot hashes are
verified. Compressed file deltas never become synthetic venue event deltas.
"""

from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path


class InputError(ValueError):
    """A source contract or required observed input cannot be established."""


def require(condition, code):
    if not condition:
        raise InputError(code)


def canonical(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _side(old, patch):
    if "replace" in patch:
        return deepcopy(patch["replace"])
    values = {canonical(r["price"] if isinstance(r, dict) else r[0]): r for r in old}
    require(len(values) == len(old), "DUPLICATE_PRICE_LEVEL")
    for key in patch.get("delete", []):
        require(key in values, "DELTA_DELETE_ABSENT_LEVEL")
        del values[key]
    values.update(patch.get("put", {}))
    order = patch.get("order", list(values))
    require(
        len(order) == len(set(order)) and set(order) == set(values), "DELTA_LEVEL_ORDER"
    )
    return [values[key] for key in order]


class RecordingGroup:
    def __init__(self, root):
        import pyarrow.parquet as pq

        self.root = Path(root).resolve()
        self.manifest = json.loads(
            (self.root / "publication.json").read_text(encoding="utf-8")
        )
        m = self.manifest
        require(
            m["schema"] == "recording-group-parquet/1"
            and m["manifest_hash"]
            == digest({k: v for k, v in m.items() if k != "manifest_hash"}),
            "MANIFEST_HASH_OR_SCHEMA",
        )
        require(
            {r["path"] for r in m["files"]}
            == {"context.parquet", "orderbook.parquet", "trades.parquet"}
            and len(m["files"]) == 3,
            "THREE_SOURCE_FILES_REQUIRED",
        )
        self.files = {r["path"]: r for r in m["files"]}
        for name, entry in self.files.items():
            p = self.root / name
            require(
                not p.is_symlink()
                and p.is_file()
                and p.stat().st_size == entry["bytes"]
                and file_hash(p) == entry["sha256"],
                "SOURCE_FILE_HASH:" + name,
            )
        with pq.ParquetFile(
            self.root / "context.parquet", page_checksum_verification=True
        ) as file:
            rows = file.read(use_threads=False).to_pylist()
        require(
            len(rows) == 1 and isinstance(rows[0].get("payload"), str),
            "CONTEXT_PAYLOAD",
        )
        self.context = json.loads(rows[0]["payload"])
        ctx = self.context
        require(
            ctx["schema"] == "recording-group-context/1"
            and ctx["plan_id"] == m["plan_id"]
            and ctx["group_id"] == m["group_id"]
            and sorted(ctx["members"]) == sorted(m["members"])
            and ctx["native_audit_proof_hash"] == m["native_audit_proof_hash"],
            "CONTEXT_BINDING",
        )
        require(
            digest(ctx) == self.files["context.parquet"]["logical_sha256"],
            "CONTEXT_LOGICAL_HASH",
        )
        self.markets = {market["market_key"]: market for market in ctx["markets"]}
        require(
            set(self.markets) == set(ctx["members"])
            and len(self.markets) == len(ctx["markets"]),
            "MARKET_MEMBERS",
        )
        self.versions = {}
        for key, market in self.markets.items():
            for v in market["versions"]:
                require(
                    hashlib.sha256(canonical(v["terms"]).encode()).hexdigest()
                    == v["revision"],
                    "METADATA_VERSION_HASH",
                )
                self.versions[key + ":" + v["revision"]] = v
            for obs in market["observations"]:
                require(
                    key + ":" + obs["revision"] in self.versions,
                    "METADATA_OBSERVATION_REFERENCE",
                )

    def iter_rows(self, name):
        import pyarrow.parquet as pq

        entry = self.files[name]
        count = 0
        logical = hashlib.sha256()
        semantic = hashlib.sha256()
        kinds = Counter()
        streams = Counter()
        states = {}
        with pq.ParquetFile(self.root / name, page_checksum_verification=True) as file:
            meta = json.loads(file.metadata.metadata[b"group_parquet"])
            header = meta["header"]
            require(
                meta["layout"] == "recording-group-parquet/1"
                and header["plan_id"] == self.manifest["plan_id"]
                and header["group_id"] == self.manifest["group_id"]
                and header["context_sha256"] == self.files["context.parquet"]["sha256"]
                and header["native_audit_proof_hash"]
                == self.manifest["native_audit_proof_hash"]
                and sorted(header["members"]) == sorted(self.markets),
                "ARTIFACT_BINDING",
            )
            for batch in file.iter_batches(batch_size=256, use_threads=False):
                for row in batch.to_pylist():
                    require(row["sequence"] == count, "ROW_SEQUENCE")
                    key = row["venue"] + ":" + row["market_id"]
                    require(key in self.markets, "FOREIGN_MARKET")
                    logical.update((canonical(row) + "\n").encode())
                    value = None
                    if name == "orderbook.parquet":
                        if row["kind"] == "book":
                            asset = row["asset_id"]
                            stream = (key, asset)
                            require(
                                asset
                                in self.markets[key]["instrument_mapping"].values()
                                and row["metadata_ref"] in self.versions
                                and row["metadata_ref"].startswith(key + ":"),
                                "BOOK_MAPPING_OR_METADATA",
                            )
                            if row["book_reset"]:
                                states.pop(stream, None)
                            op = json.loads(row["operation_json"])
                            require(
                                op["key"]
                                == row["venue"] + "/" + row["market_id"] + ":" + asset,
                                "OPERATION_STREAM",
                            )
                            if op["type"] == "snapshot":
                                require(
                                    stream not in states and op["version"] == 1,
                                    "SNAPSHOT_BASE",
                                )
                                value = deepcopy(op["book"])
                            else:
                                require(stream in states, "MISSING_CHECKPOINT")
                                old, version = states[stream]
                                value = deepcopy(old)
                                if op["type"] == "unchanged":
                                    require(
                                        op["version"] == version, "UNCHANGED_VERSION"
                                    )
                                else:
                                    require(
                                        op["type"] == "delta"
                                        and op["base"] == version
                                        and op["version"] == version + 1,
                                        "DELTA_VERSION",
                                    )
                                    patch = op["patch"]
                                    for side in ("bids", "asks"):
                                        if side in patch:
                                            value[side] = _side(
                                                value[side], patch[side]
                                            )
                                    for field in patch.get("metadata_delete", []):
                                        del value[field]
                                    value.update(patch.get("metadata_put", {}))
                            require(
                                value["venue"] == row["venue"]
                                and value["market_id"] == row["market_id"]
                                and value["asset_id"] == asset,
                                "BOOK_IDENTITY",
                            )
                            states[stream] = (value, op["version"])
                            streams[row["venue"] + ":" + asset] += 1
                        elif row["book_reset"]:
                            states = {s: v for s, v in states.items() if s[0] != key}
                    raw = (
                        canonical(
                            {
                                **{
                                    k: v
                                    for k, v in row.items()
                                    if k != "operation_json"
                                },
                                "book": value,
                            }
                        )
                        if row["kind"] == "book"
                        else canonical(row)
                    ) + "\n"
                    semantic.update(raw.encode())
                    count += 1
                    kinds[row["kind"]] += 1
                    yield dict(row, book=value) if row["kind"] == "book" else row
        require(
            count == meta["rows"] == entry["rows"]
            and logical.hexdigest() == meta["logical_sha256"] == entry["logical_sha256"]
            and semantic.hexdigest()
            == meta["semantic_sha256"]
            == entry["semantic_sha256"]
            and dict(kinds) == meta["kinds"]
            and dict(streams) == meta["stream_observations"],
            "SOURCE_SEMANTIC_REPLAY:" + name,
        )

    def terms_at(self, key, received_ns, reference=None):
        market = self.markets[key]
        prior = [
            o
            for o in market["observations"]
            if o["received_unix_ns"] <= received_ns
            and (reference is None or key + ":" + o["revision"] == reference)
        ]
        require(prior, "NO_PRIOR_OBSERVED_METADATA:" + key)
        obs = max(prior, key=lambda o: (o["received_unix_ns"], o["receipt_sequence"]))
        return self.versions[key + ":" + obs["revision"]]["terms"]
