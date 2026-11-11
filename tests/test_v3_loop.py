"""Fail-closed provenance regressions; no private fixture or network required."""
import copy
import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("v3_loop", ROOT / "tools/run_v3_loop.py")
loop = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loop)


class SourceBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.index = {"batches": []}
        for n in range(2):
            archive = self.root / f"{n}.zip"
            archive.write_bytes(b"opaque payload for hash binding " + bytes([n]))
            manifest = {"schema": "file-spool-publication/3", "first": n * 10 + 1, "last": n * 10 + 10,
                        "files": [{"sha256": loop.sha(archive), "bytes": archive.stat().st_size}]}
            manifest_path = self.root / f"{n}.json"
            loop.write(manifest_path, manifest)
            self.index["batches"].append({"local_manifest": manifest_path.name, "local_archive": archive.name,
                                          "manifest": manifest, "manifest_sha256": loop.sha(manifest_path)})

    def test_contiguous_bound_files(self):
        self.assertEqual(len(loop.validate_source(self.index, self.root)), 4)

    def test_same_size_payload_tamper_rejected(self):
        archive = self.root / "0.zip"
        original = archive.read_bytes()
        archive.write_bytes(b"X" + original[1:])
        with self.assertRaisesRegex(ValueError, "source_archive_changed"):
            loop.validate_source(self.index, self.root)

    def test_manifest_tamper_rejected(self):
        with (self.root / "0.json").open("ab") as stream:
            stream.write(b" ")
        with self.assertRaisesRegex(ValueError, "source_manifest_changed"):
            loop.validate_source(self.index, self.root)

    def test_embedded_manifest_must_match_file(self):
        index = copy.deepcopy(self.index)
        index["batches"][0]["manifest"]["first"] = 2
        with self.assertRaisesRegex(ValueError, "v3_manifest_binding_changed"):
            loop.validate_source(index, self.root)

    def test_gap_rejected_even_when_manifest_rehashed(self):
        batch = self.index["batches"][1]
        batch["manifest"]["first"] += 1
        loop.write(self.root / batch["local_manifest"], batch["manifest"])
        batch["manifest_sha256"] = loop.sha(self.root / batch["local_manifest"])
        with self.assertRaisesRegex(ValueError, "source_sequence_gap"):
            loop.validate_source(self.index, self.root)

    def test_empty_sample_rejected(self):
        with self.assertRaisesRegex(ValueError, "bounded_1_to_10"):
            loop.validate_source({"batches": []}, self.root)

    def test_wrong_engine_version_rejected_before_replay(self):
        import json
        lock = json.loads((ROOT / "runtime.compatibility.lock.json").read_bytes())
        lock["nautilus_trader"] = "0.0.0-wrong-engine"
        path = self.root / "wrong-engine.json"
        loop.write(path, lock)
        with self.assertRaisesRegex(ValueError, "runtime_version_differs_from_lock"):
            loop.verify_runtime(path)


if __name__ == "__main__":
    unittest.main()
