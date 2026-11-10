#!/usr/bin/env python3
"""Exercise release identity checks without network access or credentials."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('publish_central', Path(__file__).with_name('publish-central.py'))
publish = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publish)


class PublicationIdentityTest(unittest.TestCase):
    def test_accepts_all_four_runtime_coordinates(self):
        expected = publish.expected_coordinates('v1.0.1')
        self.assertEqual(4, len(expected))
        self.assertIn('pkg:maven/com.aliyun.odps/flink-connector-maxcompute@1.0.1-1.20', expected)

    def test_rejects_mutable_or_malformed_versions(self):
        for tag in ('main', 'v1.0.1-SNAPSHOT', '../v1.0.1', 'v1.0'):
            with self.assertRaises(ValueError):
                publish.expected_coordinates(tag)

    def test_rejects_incomplete_or_wrong_release_bundles(self):
        expected = publish.expected_coordinates('v1.0.1')
        status = {'deploymentId': 'deployment', 'deploymentState': 'VALIDATED', 'purls': list(expected)}
        self.assertEqual('VALIDATED', publish.validate_status(status, 'deployment', expected))
        for purls in (list(expected)[:-1], list(publish.expected_coordinates('v1.0.0'))):
            with self.assertRaises(ValueError):
                publish.validate_status(dict(status, purls=purls), 'deployment', expected)
        with self.assertRaises(ValueError):
            publish.validate_status(status, 'another-deployment', expected)

    def test_rejects_unverified_or_different_source_runs(self):
        run = {'head_sha': 'commit', 'conclusion': 'success', 'event': 'workflow_dispatch',
               'path': '.github/workflows/release.yml'}
        publish.validate_run(run, 'commit')
        for change in ({'head_sha': 'other'}, {'conclusion': 'failure'},
                       {'event': 'pull_request'}, {'path': '.github/workflows/ci.yml'}):
            with self.assertRaises(ValueError):
                publish.validate_run(dict(run, **change), 'commit')


if __name__ == '__main__':
    unittest.main()
