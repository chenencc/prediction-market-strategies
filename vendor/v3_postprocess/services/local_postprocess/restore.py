"""Pinned HF downloads and lossless, bounded Parquet source datasets."""
import hashlib
import json
import re
import os
import zipfile
from collections import Counter
from pathlib import Path, PurePosixPath

from mixed_recorder.raw_metadata_codec import all_files, read as read_metadata
from mixed_recorder.raw_segments import verify_saved_zip
from mixed_recorder.publication import sha_file
from mixed_recorder.store import encoded
from task_control.contracts import verified


def safe(root, relative):
    p = PurePosixPath(relative)
    if p.is_absolute() or '..' in p.parts or '\\' in relative or ':' in relative:
        raise ValueError('unsafe_path')
    target = Path(root).resolve().joinpath(*p.parts)
    if not target.resolve().is_relative_to(Path(root).resolve()):
        raise ValueError('unsafe_path')
    return target


def fetch(repo, revision, manifest, cache, *, token=None):
    """Read-only HF access. Recursively pin predecessors; never resolve main."""
    os.environ.setdefault('HF_HUB_DISABLE_XET', '1')
    from huggingface_hub import hf_hub_download
    if not re.fullmatch('[a-f0-9]{40}', revision):
        raise ValueError('exact_revision_required')
    cache = Path(cache)
    visited = set()
    result = []

    def download(rev, name):
        if not re.fullmatch('[a-f0-9]{40}', rev): raise ValueError('exact_predecessor_revision_required')
        safe(cache, name)
        cache_path = str((cache / 'hf-cache').resolve())
        if os.name == 'nt': cache_path = '\\\\?\\' + cache_path
        return Path(hf_hub_download(repo, name, repo_type='dataset', revision=rev,
                                   token=token, cache_dir=cache_path))

    def visit(rev, name, expected=None):
        key = (rev, name)
        if key in visited or len(visited) >= 1000:
            raise ValueError('predecessor_cycle_or_bound')
        visited.add(key)
        source = download(rev, name)
        if source.stat().st_size > 2 * 1024**2:
            raise ValueError('manifest_size_bound')
        if expected and sha_file(source) != expected:
            raise ValueError('predecessor_manifest_hash')
        pub = json.loads(source.read_bytes())
        if pub['schema'] != 'raw-event-publication/1':
            raise ValueError('publication_schema')
        segment = verified(pub['segment'], 'raw-event-segment/1')
        prefix = 'raw-collections/' + pub['collection_id']
        if name != prefix + '/segments/' + segment['contract_hash'] + '/manifest.json':
            raise ValueError('publication_identity')
        previous = pub['previous_publication']
        if previous:
            visit(previous['revision'], previous['manifest_remote_path'], previous['manifest_sha256'])
        root = safe(cache, 'segments/' + segment['contract_hash'])
        root.mkdir(parents=True, exist_ok=True)
        mapping = {prefix + '/segments/' + segment['contract_hash'] + '/events.zip': segment['payload']}
        mapping.update({prefix + '/' + r['file']: r for r in all_files(segment['metadata'])})
        if len(pub['files']) != len(mapping) or {r['remote_path'] for r in pub['files']} != set(mapping):
            raise ValueError('publication_inventory')
        for remote, item in mapping.items():
            declared = next(r for r in pub['files'] if r['remote_path'] == remote)
            if any(declared[k] != item[k] for k in ('sha256', 'bytes')):
                raise ValueError('publication_file_binding')
            target = safe(root, item['file'])
            target.parent.mkdir(parents=True, exist_ok=True)
            data = target if target.exists() else download(rev, remote)
            if data.stat().st_size != item['bytes'] or sha_file(data) != item['sha256']:
                raise ValueError('download_hash_mismatch')
            if not target.exists():
                import shutil
                shutil.copyfile(data, target)
        (root / 'publication.json').write_bytes(source.read_bytes())
        (root / 'source.json').write_bytes(encoded(dict(repo=repo, revision=rev, manifest=name, sha256=sha_file(source))))
        result.append(root)

    visit(revision, manifest)
    return result


def convert(roots, output):
    """Lossless event ledger; no snapshots/deltas fabricated or clocks rounded."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    roots = list(roots)
    if not roots: raise ValueError('source_shards_required')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    schema = pa.schema([('sequence', pa.uint64()), ('received_ns', pa.int64()),
                        ('venue', pa.string()), ('event_type', pa.string()),
                        ('market_id', pa.string()), ('data', pa.large_string())])
    counts = Counter()
    receipts = []
    previous = None
    prior_fences = {}
    for root in map(Path, roots):
        pub = json.loads((root / 'publication.json').read_bytes())
        r = verified(pub['segment'], 'raw-event-segment/1')
        if r['index'] != len(receipts) or (previous is None) != (r['previous'] is None):
            raise ValueError('initial_snapshot_history_required')
        if previous and r['previous']['contract_hash'] != previous['contract_hash']:
            raise ValueError('segment_chain_changed')
        for f in r['source_fences']:
            old = prior_fences.get(f['table'])
            if old and (old['through_sequence'] != f['after_sequence'] or old['generation'] != f['generation']):
                raise ValueError('sequence_fence_discontinuity')
        verify_saved_zip(root, r)
        folder = output / f"segment-{r['index']:08d}"
        folder.mkdir()
        for ref in r['metadata']:
            (folder / (ref['kind'] + '.json')).write_bytes(encoded(read_metadata(root, ref)))
        file_proofs = []
        with zipfile.ZipFile(root / r['payload']['file']) as archive:
            if len(archive.namelist()) != len(set(archive.namelist())):
                raise ValueError('duplicate_zip_member')
            envelope = json.loads(archive.read('manifest.json'))
            if any(envelope[k] != r[k] for k in ('index', 'previous', 'source_fences', 'metadata')):
                raise ValueError('zip_envelope_changed')
            for entry in r['entries']:
                table = Path(entry['file']).stem
                fence = next(f for f in r['source_fences'] if f['table'] == table)
                last = fence['after_sequence']; count = 0
                inv = hashlib.sha256(); logical = hashlib.sha256()
                target = folder / (table + '.parquet')
                batch = []
                with pq.ParquetWriter(target, schema, compression='zstd', compression_level=9,
                                      write_page_checksum=True) as writer, archive.open(entry['file']) as stream:
                    while True:
                        line = stream.readline(32 * 1024**2 + 1)
                        if not line: break
                        if len(line) > 32 * 1024**2 or not line.endswith(b'\n'):
                            raise ValueError('event_line_bound')
                        row = json.loads(line)
                        seq, at = int(row['sequence']), int(row['received_ns'])
                        if seq != last + 1: raise ValueError('native_sequence_gap')
                        last = seq; count += 1
                        digest = hashlib.sha256(row['data'].encode()).hexdigest()
                        inv.update(f'[{seq},{at},"{digest}"]\n'.encode())
                        event = json.loads(row['data'])  # index only; original lexemes remain in data
                        venue = event.get('venue', 'polymarket' if table == 'accept_poly_v3' else 'unknown')
                        kind = event.get('event_type', 'unknown')
                        mid = event.get('native_market_id', event.get('market'))
                        item = dict(sequence=seq, received_ns=at, venue=venue, event_type=kind,
                                    market_id=str(mid) if mid is not None else None, data=row['data'])
                        logical.update(encoded(item)); counts[venue + ':' + kind] += 1
                        batch.append(item)
                        if len(batch) >= 2048:
                            writer.write_table(pa.Table.from_pylist(batch, schema=schema)); batch.clear()
                    if batch: writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                if count != entry['rows'] or count != fence['row_count'] or last != fence['through_sequence'] or inv.hexdigest() != fence['source_inventory_sha256']:
                    raise ValueError('native_inventory_mismatch')
                check = hashlib.sha256(); read_count = 0
                with pq.ParquetFile(target, page_checksum_verification=True) as reader:
                    for b in reader.iter_batches(batch_size=2048):
                        for item in b.to_pylist(): check.update(encoded(item)); read_count += 1
                if read_count != count or check.digest() != logical.digest():
                    raise ValueError('parquet_roundtrip_mismatch')
                file_proofs.append(dict(file=target.relative_to(output).as_posix(), rows=count,
                                        sha256=sha_file(target), bytes=target.stat().st_size))
        receipts.append(dict(index=r['index'], segment_hash=r['contract_hash'], files=file_proofs,
                             source=json.loads((root / 'source.json').read_bytes())))
        previous = r; prior_fences = {f['table']: f for f in r['source_fences']}
    receipt = dict(schema='local-backtest-source/1', segments=receipts, event_counts=dict(counts),
                   lossless_native_roundtrip=True, directly_loadable_nautilus_catalog=False,
                   exchange_completeness_certified=False, semantic_book_replay_performed=False,
                   trades_complete=False, upload_performed=False)
    (output / 'manifest.json').write_bytes(encoded(receipt))
    return receipt
