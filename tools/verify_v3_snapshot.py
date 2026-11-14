"""Read-only comparison of a local converted dataset with the published snapshot lock."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO/'src'))
from prediction_market_strategies.data.hf_sync import read, sha, blob, verify_bundle


def verify(root, lock, allow_prefix=False, full_files=False):
    root = Path(root).resolve()
    expected = lock['chunks']
    with closing(sqlite3.connect(f'file:{(root/"index.sqlite").as_posix()}?mode=ro', uri=True)) as db:
        entries = db.execute('SELECT identity,first_seq,last_seq,blob,receipt_sha,core_sha FROM chunks ORDER BY first_seq').fetchall()
    if not entries or len(entries) > len(expected):
        raise ValueError('empty_or_oversized_snapshot')
    if not allow_prefix and len(entries) != len(expected):
        raise ValueError('snapshot_incomplete:'+str(len(entries))+'/'+str(len(expected)))
    source_revisions = set()
    for row, wanted in zip(entries, expected):
        identity, first, last, manifest_blob, receipt_sha, core_sha = row
        if (identity, first, last, manifest_blob, core_sha) != (
                wanted['identity'], wanted['first'], wanted['last'],
                wanted['manifest_git_blob'], wanted['payload']['sha256']):
            raise ValueError('snapshot_source_binding_mismatch:'+identity)
        folder = root/'chunks'/identity
        if sha(folder/'receipt.json') != receipt_sha:
            raise ValueError('receipt_changed:'+identity)
        receipt = read(folder/'receipt.json')
        raw = (folder/'source-manifest.json').read_bytes()
        if blob(raw) != manifest_blob or sha(folder/'source-manifest.json') != wanted['manifest_sha256']:
            raise ValueError('source_manifest_changed:'+identity)
        if receipt['core'] != wanted['payload'] or not receipt.get('conversion_verified'):
            raise ValueError('conversion_not_verified:'+identity)
        fidelity = receipt.get('fidelity', {})
        if not fidelity.get('canonical_header_and_rows_exact') or not fidelity.get('native_full_depth_equal'):
            raise ValueError('fidelity_receipt_not_passed:'+identity)
        if full_files:
            verify_bundle(folder, receipt_sha)
        source_revisions.add(receipt['revision'])
    return dict(schema='hf-v3-snapshot-comparison/1',
        status='complete_source_match' if len(entries)==len(expected) else 'partial_source_match',
        matched_chunks=len(entries),expected_chunks=len(expected),
        through_sequence=entries[-1][2],target_sequence=lock['last_sequence'],
        source_snapshot=lock['revision'],receipt_source_revisions=sorted(source_revisions),
        derived_files_rehashed=full_files,quality_reaudited=False,execution_ready=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--snapshot', type=Path, default=REPO/'docs/data/hf-v3-e0a7982.snapshot.json')
    parser.add_argument('--allow-prefix', action='store_true', help='Report a matching partial prefix without claiming full completion')
    parser.add_argument('--full-files', action='store_true', help='Also rehash every converted file; expensive on the full dataset')
    args = parser.parse_args()
    print(json.dumps(verify(args.root,read(args.snapshot),args.allow_prefix,args.full_files),sort_keys=True))


if __name__ == '__main__':
    main()
