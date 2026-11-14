import hashlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from prediction_market_strategies.data import postprocess_runtime as runtime


class PostprocessBundleTests(unittest.TestCase):
    def test_native_mac_selects_separate_lock(self):
        with patch.object(runtime.platform,'system',return_value='Darwin'), patch.object(runtime.platform,'machine',return_value='arm64'):
            self.assertEqual(runtime.runtime_lock_path(Path('repo')).name,'runtime.compatibility.macos-arm64.lock.json')

    def test_intel_or_rosetta_does_not_select_windows_runtime(self):
        with patch.object(runtime.platform,'system',return_value='Darwin'), patch.object(runtime.platform,'machine',return_value='x86_64'):
            with self.assertRaisesRegex(ValueError,'native_apple_silicon'):
                runtime.runtime_lock_path(Path('repo'))

    def test_platform_specific_locks_pin_same_engine_but_distinct_wheels(self):
        repo=Path(__file__).resolve().parents[1]
        win=json.loads((repo/'runtime.compatibility.lock.json').read_bytes())
        mac=json.loads((repo/'runtime.compatibility.macos-arm64.lock.json').read_bytes())
        self.assertEqual(win['engine_commit'],mac['engine_commit'])
        self.assertNotEqual(win['wheel_sha256'],mac['wheel_sha256'])
        self.assertEqual(mac['requirements_files'],['requirements-postprocess-common.txt'])

    def test_bundle_imports_without_private_collector_checkout(self):
        root = runtime.configure_postprocess()
        import local_postprocess.compact_standard as standard
        import local_postprocess.nautilus as native
        import mixed_recorder.backtest_spool as codec
        for module in (standard, native, codec):
            self.assertTrue(Path(module.__file__).resolve().is_relative_to(root.resolve()))
        self.assertIn('book_baselines', standard.SCHEMAS_V2)
        self.assertIn('fees', codec.COLUMNS['t'])

    def test_changed_bundle_is_rejected_before_import(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            root = repo/'vendor/v3_postprocess'
            root.mkdir(parents=True)
            (root/'reader.py').write_bytes(b'changed')
            (root/'SOURCE.json').write_text(json.dumps(dict(exact_files_sha256={
                'reader.py':hashlib.sha256(b'original').hexdigest()})))
            with patch.object(runtime,'__file__',str(repo/'src/prediction_market_strategies/data/postprocess_runtime.py')):
                with self.assertRaisesRegex(ValueError,'bundled_postprocessor_changed'):
                    runtime.configure_postprocess()

    def test_mixed_private_and_bundled_modules_are_rejected(self):
        module = types.ModuleType('marketdata.foreign')
        module.__file__ = str(Path(__file__).resolve())
        with patch.dict(sys.modules, {'marketdata.foreign':module}):
            with self.assertRaisesRegex(ValueError,'postprocessor_already_loaded_from_another_root'):
                runtime.configure_postprocess()
