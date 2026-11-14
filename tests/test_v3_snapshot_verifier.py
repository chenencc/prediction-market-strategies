import importlib.util
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from prediction_market_strategies.data.hf_sync import blob, sha

spec = importlib.util.spec_from_file_location('snapshot_verifier',Path(__file__).resolve().parents[1]/'tools/verify_v3_snapshot.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class SnapshotVerifierTests(unittest.TestCase):
    def fixture(self, root):
        chunks=[]
        with closing(sqlite3.connect(root/'index.sqlite')) as db:
            db.execute('CREATE TABLE chunks (identity,first_seq,last_seq,blob,receipt_sha,core_sha)')
            for first in (1,11):
                identity=f'{first:020d}-{first+9:020d}'
                folder=root/'chunks'/identity
                folder.mkdir(parents=True)
                manifest=folder/'source-manifest.json'
                manifest.write_bytes(json.dumps(dict(first=first,last=first+9)).encode())
                core=dict(sha256='a'*64,bytes=5,remote_path='source/'+identity+'.zip')
                receipt=folder/'receipt.json'
                receipt.write_text(json.dumps(dict(core=core,conversion_verified=True,revision='b'*40,
                    fidelity=dict(canonical_header_and_rows_exact=True,native_full_depth_equal=True))))
                chunks.append(dict(identity=identity,first=first,last=first+9,payload=core,
                    manifest_git_blob=blob(manifest.read_bytes()),manifest_sha256=sha(manifest)))
                if first==1:
                    db.execute('INSERT INTO chunks VALUES (?,?,?,?,?,?)',
                        (identity,first,first+9,chunks[-1]['manifest_git_blob'],sha(receipt),core['sha256']))
            db.commit()
        return dict(chunks=chunks,last_sequence=20,revision='b'*40)

    def test_partial_data_never_claims_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);lock=self.fixture(root)
            with self.assertRaisesRegex(ValueError,'snapshot_incomplete'):
                verifier.verify(root,lock)
            result=verifier.verify(root,lock,allow_prefix=True)
            self.assertEqual(result['status'],'partial_source_match')
            self.assertEqual(result['through_sequence'],10)

    def test_changed_source_manifest_rejected_even_when_index_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);lock=self.fixture(root)
            (root/'chunks'/lock['chunks'][0]['identity']/'source-manifest.json').write_bytes(b'{}')
            with self.assertRaisesRegex(ValueError,'source_manifest_changed'):
                verifier.verify(root,lock,allow_prefix=True)
