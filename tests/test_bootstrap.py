"""Validate the dependency pin and safe bootstrap defaults without the engine."""
import json
from pathlib import Path
import re
import tomllib
import unittest

ROOT=Path(__file__).resolve().parents[1]


class BootstrapTests(unittest.TestCase):
    def test_engine_is_pinned_to_an_exact_source_revision(self):
        lock=json.loads((ROOT/'engine.lock.json').read_text())
        self.assertRegex(lock['commit'],r'^[0-9a-f]{40}$')
        self.assertEqual(lock['commit'],lock['upstream_base_commit'])
        self.assertEqual(lock['local_engine_patches'],[])
        self.assertFalse(lock['automatic_upgrades'])

    def test_research_configuration_does_not_enable_execution(self):
        cfg=json.loads((ROOT/'configs/research.example.json').read_text())
        self.assertEqual(cfg['mode'],'research');self.assertFalse(cfg['live_execution_enabled'])
        self.assertIsNone(cfg['dataset_revision']);self.assertIsNone(cfg['strategy'])

    def test_bootstrap_does_not_claim_engine_validation_or_install_a_floating_one(self):
        lock=json.loads((ROOT/'engine.lock.json').read_text())
        self.assertFalse(lock['build_verified']);self.assertFalse(lock['runtime_verified'])
        config=tomllib.loads((ROOT/'pyproject.toml').read_text())
        self.assertEqual(config['project']['dependencies'],[])
        self.assertEqual(config['tool']['prediction-market-strategies']['engine-lock'],'engine.lock.json')


if __name__=='__main__':unittest.main()
