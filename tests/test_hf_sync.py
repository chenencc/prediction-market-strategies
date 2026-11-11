"""Transactional downloader tests. Conversion itself is covered by real-V3 runs."""
from contextlib import closing
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT.parent / "market-data-collector/services"))
from prediction_market_strategies.data import hf_sync as h


class Remote:
    def __init__(self):
        self.downloads = 0
        self.downloaded_bytes = 0
        self.files = {}
        self.listed = []
        for first in (1, 11):
            identity = f"{first:020d}-{first+9:020d}"
            data = ("payload-" + identity).encode()
            digest = h.hashlib.sha256(data).hexdigest()
            core = dict(remote_path="raw-collections/test/v3/events/" + identity + ".zip", bytes=len(data), sha256=digest)
            archive = dict(schema="market-backtest-chunk/1", encoding="market-backtest/2-delta-jsonl-zstd6",
                           identity=identity, first=first, last=first+9, rows=10, file="data.zip", bytes=len(data), sha256=digest)
            manifest = dict(schema="file-spool-publication/3", release="v3", identity=identity, first=first, last=first+9,
                            encoding=archive["encoding"], projection_policy="backtest-v3-zstd6",
                            source_payload_is_official_event_envelope=False, full_trade_completeness_certified=False,
                            blocks=[dict(file=f"{first:020d}.jsonl", first=first, last=first+9, rows=10)],
                            codec_sha256="a"*64, archive=archive, files=[core])
            path = "raw-collections/test/segments/" + identity + ".json"
            body = json.dumps(manifest).encode()
            self.files[path] = body
            self.files[core["remote_path"]] = data
            self.listed.append(dict(path=path, blob=h.blob(body)))

    def head(self, ref):
        return "a" * 40

    def entries(self, *args):
        return self.listed

    def fetch(self, revision, name, target, expected=None, **kwargs):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.files[name])
        self.downloads += 1
        return target


def fake_convert(work, archive, manifest, manifest_path, revision, entry, config, previous, quality):
    target = work / "bundle"
    target.mkdir()
    (target / "converted").write_bytes(archive.read_bytes())
    receipt = dict(identity=manifest["identity"], first=manifest["first"], last=manifest["last"],
                   manifest_blob=entry["blob"], core=manifest["files"][0], conversion_verified=True,
                   files=h.inventory(target))
    h.atomic_json(target / "receipt.json", receipt)
    return target


class TransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "store"
        self.remote = Remote()
        self.config = dict(repo="owner/data", ref="main", prefix="raw-collections/test", layout="segments",
                           first_sequence=1, minimum_free_bytes=1)
        self.convert = patch("prediction_market_strategies.data.v3_incremental.convert_chunk", fake_convert)
        self.convert.start()
        self.addCleanup(self.convert.stop)

    def run_sync(self, **kwargs):
        return h.sync(self.root, self.remote, self.config, **kwargs)

    def test_incremental_resume_does_not_download_completed_history(self):
        self.run_sync(max_chunks=1, delete_cache=True)
        self.run_sync(max_chunks=1, delete_cache=True)
        downloads = self.remote.downloads
        result = self.run_sync(delete_cache=True)
        self.assertEqual(result["new_chunks"], 0)
        self.assertEqual(self.remote.downloads, downloads)
        self.assertEqual(list((self.root / "cache").glob("*.zip")), [])

    def test_failure_before_conversion_keeps_source(self):
        with patch("prediction_market_strategies.data.v3_incremental.convert_chunk", side_effect=ValueError("conversion_failure")):
            with self.assertRaises(ValueError):
                self.run_sync(delete_cache=True)
        self.assertEqual(len(list((self.root / "cache").glob("*.zip"))), 1)
        with closing(h.connect(self.root)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0], 0)

    def test_recover_bundle_before_ack_without_redownload(self):
        with self.assertRaisesRegex(RuntimeError, "after_bundle"):
            self.run_sync(max_chunks=1, delete_cache=True, failpoint="after_bundle")
        before = self.remote.downloads
        result = self.run_sync(max_chunks=1, delete_cache=True)
        self.assertEqual(result["new_chunks"], 1)
        self.assertEqual(self.remote.downloads, before)

    def test_recover_ack_before_delete(self):
        with self.assertRaisesRegex(RuntimeError, "after_commit"):
            self.run_sync(max_chunks=1, delete_cache=True, failpoint="after_commit")
        self.assertEqual(len(list((self.root / "cache").glob("*.zip"))), 1)
        self.run_sync(max_chunks=1, delete_cache=True)
        self.assertEqual(list((self.root / "cache").glob("*.zip")), [])

    def test_recover_delete_before_state_update(self):
        with self.assertRaisesRegex(RuntimeError, "after_unlink"):
            self.run_sync(max_chunks=1, delete_cache=True, failpoint="after_unlink")
        self.run_sync(max_chunks=1, delete_cache=True)
        self.assertEqual(list((self.root / "cache").glob("*.zip")), [])

    def test_corrupt_output_prevents_cache_delete(self):
        self.run_sync(max_chunks=1)
        target = next((self.root / "chunks").glob("*/converted"))
        target.write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "converted_file_changed"):
            self.run_sync(delete_cache=True)
        self.assertEqual(len(list((self.root / "cache").glob("*.zip"))), 1)

    def test_sequence_gap_not_skipped(self):
        self.remote.listed = self.remote.listed[1:]
        with self.assertRaisesRegex(ValueError, "source_sequence_gap_expected_1"):
            self.run_sync()
        self.assertEqual(self.remote.downloads, 0)

    def test_rewritten_committed_manifest_is_rejected(self):
        self.run_sync(max_chunks=1)
        self.remote.listed[0]["blob"] = "f" * 40
        with self.assertRaisesRegex(ValueError, "published_manifest_rewritten"):
            self.run_sync()

    def test_configuration_change_not_silently_adopted(self):
        self.run_sync(max_chunks=1)
        self.config["first_sequence"] = 11
        with self.assertRaisesRegex(ValueError, "source_or_converter_lock_changed"):
            self.run_sync()

    def test_path_escape_rejected(self):
        for value in ("../outside", "C:/outside", "a/../../outside"):
            with self.assertRaises(ValueError):
                h.bounded(self.root, value)

    def test_long_catalog_paths_can_reuse_prior_release(self):
        from prediction_market_strategies.data.v3_incremental import link_tree
        source = Path(self.temp.name) / "source"
        source.mkdir()
        (source / ("long-catalog-name-" + "x"*100 + ".parquet")).write_bytes(b"test")
        owned = self.root / "work" / "long-path-test"
        self.addCleanup(h.remove_work, self.root.resolve(), owned)
        target = owned / ("a"*90) / ("b"*90) / "prior"
        link_tree(source, target)
        self.assertEqual(h.extended_path(target / next(source.iterdir()).name).read_bytes(), b"test")
        self.assertEqual(len(h.inventory(target)), 1)

    def test_extended_path_native_catalog_boundary(self):
        from prediction_market_strategies.data.nautilus_windows import catalog_paths
        path = h.extended_path(Path(self.temp.name) / "catalog")
        path.mkdir()
        import nautilus_trader.persistence as persistence
        before = persistence.ParquetDataCatalog
        with catalog_paths():
            catalog = persistence.ParquetDataCatalog(str(path))
            self.assertIsNotNone(catalog)
        self.assertIs(persistence.ParquetDataCatalog, before)

    def test_cross_chunk_overlap_keeps_original_clocks_and_all_events(self):
        from prediction_market_strategies.data.hf_backtest import merge_available
        streams = {1: [SimpleNamespace(ts_init=n) for n in (1,4,7)],
                   11: [SimpleNamespace(ts_init=n) for n in (3,4,9)],
                   21: [SimpleNamespace(ts_init=n) for n in (12,13)]}
        descriptors = [dict(first=k, min_ns=v[0].ts_init, events=len(v)) for k,v in streams.items()]
        result = list(merge_available(descriptors, lambda d: streams[d['first']], 6))
        self.assertEqual([e.ts_init for e in result], [1,3,4,4,7,9,12,13])
        self.assertEqual({id(e) for e in result}, {id(e) for v in streams.values() for e in v})

    def test_cross_chunk_overlap_is_memory_bounded(self):
        from prediction_market_strategies.data.hf_backtest import merge_available
        descriptors = [dict(first=1,min_ns=1,events=2),dict(first=11,min_ns=2,events=2)]
        with self.assertRaisesRegex(ValueError, "memory_bound"):
            list(merge_available(descriptors, lambda d: [SimpleNamespace(ts_init=d['min_ns']),SimpleNamespace(ts_init=4)], 3))

    def test_quality_filename_cannot_escape_local_work(self):
        entry = self.remote.listed[0]
        manifest = json.loads(self.remote.files[entry['path']])
        manifest['quality_certificates'] = [dict(sha256='../../outside',bytes=1,
                                                 remote_path='raw-collections/test/v3/quality/test.json')]
        body = json.dumps(manifest).encode()
        self.remote.files[entry['path']] = body
        entry['blob'] = h.blob(body)
        with self.assertRaisesRegex(ValueError, 'unsafe_quality_reference'):
            self.run_sync()
        self.assertEqual(self.remote.downloads, 1)

    def test_corrupt_download_is_rejected_before_conversion(self):
        path = next(k for k in self.remote.files if k.endswith('.zip'))
        self.remote.files[path] = b'corrupt'
        with self.assertRaisesRegex(ValueError, 'download_hash_or_size_changed'):
            self.run_sync(delete_cache=True)


if __name__ == "__main__":
    unittest.main()
