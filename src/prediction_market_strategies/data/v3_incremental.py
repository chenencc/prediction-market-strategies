"""One-chunk adaptation over the fixed collector codec and Nautilus exporter.

Only the predecessor checkpoint/release is needed. Canonical rows, original
header, metadata/fees and quality sidecars remain after local ZIP retirement.
"""
from collections import Counter
from copy import deepcopy
from decimal import Decimal
import gzip
import json
import os
from pathlib import Path
import shutil

from .hf_sync import sha, read, atomic_json, inventory, core_manifest, extended_path
from .nautilus_windows import catalog_paths


def link_tree(source, target):
    # CreateHardLinkW/copy2 need extended paths for long catalog filenames even
    # on machines where ordinary Python open() already supports long paths.
    def link(a, b):
        try:
            os.link(a, b)
        except OSError:
            shutil.copy2(a, b)
    shutil.copytree(extended_path(source), extended_path(target), copy_function=link)


def checkpoint(path):
    return json.loads(gzip.decompress(Path(path).read_bytes()))


def verify_conversion(archive, manifest, header, standard, native, previous):
    from mixed_recorder.backtest_spool import iter_archive, BookReplay
    from local_postprocess.nautilus import rows, read_release, make_instrument, book_batch, native_id
    from local_postprocess.compact_standard import baseline_key, asset_alias
    from nautilus_trader.model import OrderBook, BookType

    native_books = {}
    if previous:
        prior = checkpoint(previous / "standard/checkpoint.json.gz")
        prior_native = checkpoint(previous / "nautilus/checkpoint.json.gz")
        for ident in prior_native["levels"]:
            spec = prior_native["instruments"][ident]
            venue, mid, asset, at = spec
            key = baseline_key(venue, asset_alias(venue, mid, asset, 1 if venue == "predictfun" else None))
            book = prior["books"].get(key)
            if book is None or not book["valid"]:
                continue
            instrument = make_instrument(*spec)
            nb = OrderBook(instrument.id, BookType.L2_MBP)
            row = dict(bids=book["bids"], asks=book["asks"], source_ns=at, received_ns=at)
            event = book_batch(row, instrument, True, set())
            nb.apply_deltas(event)
            native_books[ident] = nb
    event_counts = Counter()
    for event in read_release(native):
        event_counts[type(event).__name__] += 1
        if type(event).__name__ == "OrderBookDeltas":
            book = native_books.setdefault(str(event.instrument_id), OrderBook(event.instrument_id, BookType.L2_MBP))
            book.apply_deltas(event)
    saved = list(rows(standard, "source_events"))
    saved.sort(key=lambda r: json.loads(r["envelope_json"])["record_ordinal"])
    iterator = iter_archive(archive, manifest["archive"])
    if next(iterator) != header:
        raise ValueError("header_changed_during_conversion")
    book_replay = BookReplay(header)
    count = 0
    for ordinal, record in enumerate(iterator):
        if ordinal >= len(saved) or json.loads(saved[ordinal]["envelope_json"])["compact_record"] != record:
            raise ValueError("canonical_record_loss")
        book_replay.apply(record)
        count += 1
    if count != len(saved):
        raise ValueError("canonical_record_count_changed")
    headers = [json.loads(r["detail_json"]) for r in rows(standard, "observations") if r["kind"] == "compact_header"]
    if headers != [header]:
        raise ValueError("canonical_header_loss")
    end = checkpoint(standard / "checkpoint.json.gz")
    markets = {r[0]: r for r in header["markets"]}
    instruments = {r[0]: r for r in header["instruments"]}
    levels = 0
    for ident, book in book_replay.books.items():
        item = instruments[ident]
        market = markets[item[1]]
        key = baseline_key(market[1], asset_alias(market[1], market[2], item[2], item[3]))
        actual = end["books"][key]
        if actual["valid"] != book["valid"]:
            raise ValueError("canonical_book_validity_changed")
        nb = native_books.get(native_id(market[1], market[2], item[2]))
        for side in ("bids", "asks"):
            expected = {Decimal(p): Decimal(q) for p, q in book[side].items()}
            if {Decimal(r["price"]): Decimal(r["size"]) for r in actual[side]} != expected:
                raise ValueError("canonical_book_depth_changed")
            observed = getattr(nb, side + "_to_dict")() if nb else {}
            if observed != (expected if book["valid"] else {}):
                raise ValueError("native_book_depth_changed")
            levels += len(expected)
    return dict(compact_rows=count, native_events=dict(event_counts), book_checkpoints=len(book_replay.books),
                depth_levels=levels, canonical_header_and_rows_exact=True, native_full_depth_equal=True)


def fee_summary(header, records):
    from mixed_recorder.backtest_spool import COLUMNS, FEE_COLUMNS
    from local_postprocess.instruments import fee_policy
    markets = {r[0]: r for r in header["markets"]}
    statuses, fields, examples, trade_statuses = Counter(), {}, {}, Counter()
    for version, mid, at, raw in header["metadata_versions"]:
        venue = markets[mid][1]
        policy = fee_policy(venue, raw)
        statuses[venue + ":" + policy["status"]] += 1
        fields.setdefault(venue, set()).update(k for k in raw if "fee" in k.lower())
        examples.setdefault(venue + ":" + policy["status"], policy)
    for record in records:
        if record[0] in {"t", "f"}:
            row = dict(zip(COLUMNS[record[0]], record[1:]))
            for values in row["fees"]:
                fee = dict(zip(FEE_COLUMNS, values))
                trade_statuses[str(fee["leg"]) + ":" + str(fee["fee_status"])] += 1
    return dict(metadata_version_statuses=dict(statuses), metadata_fee_fields={k: sorted(v) for k, v in fields.items()},
                examples=examples, reported_trade_fee_leg_statuses=dict(trade_statuses),
                actual_paid_fee_complete=False, missing_fee_is_not_zero=True)


def convert_chunk(work, archive, manifest, manifest_path, revision, entry, config, previous, quality):
    from mixed_recorder.backtest_spool import iter_archive
    from local_postprocess.compact_standard import Convert, CompactTables
    from local_postprocess.nautilus import export
    import nautilus_trader as nt

    identity = manifest["identity"]
    core = core_manifest(manifest, config["prefix"])
    iterator = iter_archive(archive, manifest["archive"])
    header = next(iterator)
    if header["collection"] != config["prefix"] + "/v3":
        raise ValueError("downloaded_collection_changed")
    records = list(iterator)
    # The old converter's constructor requires the legacy namespace. Adapt only
    # that guard, then immediately restore the ORIGINAL header before conversion.
    # No source bytes or saved header fields are rewritten.
    class ScopedConvert(Convert):
        def __init__(self, *args):
            collection, m, original, state, tables = args
            super().__init__(collection, m, dict(original, collection="raw-collections/" + collection + "/v3"), state, tables)
            self.header = original

    standard_root, native_root = work / "standard-stage", work / "native-stage"
    standard = standard_root / "releases" / identity
    standard.mkdir(parents=True)
    prior = read(previous / "receipt.json") if previous else None
    semantic = checkpoint(previous / "standard/checkpoint.json.gz") if previous else dict(definitions={}, trades={}, books={})
    tables = CompactTables(standard)
    converter = ScopedConvert(config["collection"], manifest, header, deepcopy(semantic), tables)
    try:
        converter.seed()
        for ordinal, record in enumerate(records):
            converter.event(record, ordinal)
        converter.finish()
    finally:
        tables.close()
    raw_checkpoint = json.dumps(converter.state, separators=(",", ":")).encode()
    with (standard / "checkpoint.json.gz").open("wb") as stream:
        stream.write(gzip.compress(raw_checkpoint, mtime=0))
        stream.flush()
        os.fsync(stream.fileno())
    for file in standard.rglob("*"):
        if file.is_file():
            with file.open("r+b") as stream:
                os.fsync(stream.fileno())
    standard_receipt = dict(schema="market-standard-release/2", input_profile="file-spool-publication/3",
                            identity=identity, collection=config["collection"], first=manifest["first"], last=manifest["last"],
                            source_revision=revision, source_repo=config["repo"], source_manifest=entry["path"],
                            source_sha256=sha(manifest_path), source_files=[core],
                            counts=dict(tables.counts), compact_record_counts=dict(converter.stats),
                            files=[dict(file=k, **v) for k, v in inventory(standard).items()],
                            external_prefix_requires_history=header["external_prefix_requires_history"],
                            original_header_namespace_preserved=True, full_trade_completeness_certified=False)
    atomic_json(standard / "release.json", standard_receipt)
    standard_commit = dict(identity=identity, last=manifest["last"], source_sha256=sha(manifest_path),
                           release_sha256=sha(standard / "release.json"))
    commits = []
    if prior:
        commits.append(prior["standard_commit"])
        link_tree(previous / "standard", standard_root / "releases" / prior["identity"])
        link_tree(previous / "nautilus", native_root / "releases" / prior["identity"])
        atomic_json(native_root / "state.json", dict(identity=prior["native_state_identity"],
                    commits=[prior["native_commit"]], last=prior["last"]))
    commits.append(standard_commit)
    atomic_json(standard_root / "state.json", dict(schema="market-standard-state/2", repo=config["repo"],
                collection=config["collection"], first=config["first_sequence"], last=manifest["last"], commits=commits))
    native = native_root / "releases" / identity
    with catalog_paths():
        exported = export(standard_root, native_root, max_batches=1)
        if exported["new_batches"] != 1 or exported["through_sequence"] != manifest["last"]:
            raise ValueError("native_increment_not_completed")
        proof = verify_conversion(archive, manifest, header, standard, native, previous)
    native_state = read(native_root / "state.json")
    bundle = work / "bundle"
    bundle.mkdir()
    os.replace(standard, bundle / "standard")
    os.replace(native, bundle / "nautilus")
    shutil.copyfile(manifest_path, bundle / "source-manifest.json")
    for file in quality:
        target = bundle / "quality" / file.name
        target.parent.mkdir(exist_ok=True)
        shutil.copyfile(file, target)
    atomic_json(bundle / "fee-summary.json", fee_summary(header, records))
    receipt = dict(schema="strategy-backtest-chunk/1", identity=identity, first=manifest["first"], last=manifest["last"],
                   revision=revision, manifest_path=entry["path"], manifest_blob=entry["blob"], core=core,
                   standard_commit=standard_commit, native_commit=native_state["commits"][-1],
                   native_state_identity=native_state["identity"], runtime=nt.__version__,
                   quality="preserved_source_certificates" if quality else "unknown_no_embedded_certificate",
                   quality_reaudited=False, conversion_verified=True, fidelity=proof,
                   execution_ready=False, files=inventory(bundle))
    atomic_json(bundle / "receipt.json", receipt)
    return bundle
