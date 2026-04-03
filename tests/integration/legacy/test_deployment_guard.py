import os
import subprocess
import sys
import unittest
from tempfile import TemporaryDirectory

from src.deployment_guard import assess_legacy_deployment
from src.runtime_config import RuntimeConfig


class TestDeploymentGuard(unittest.TestCase):
    def test_local_deployment_supported_by_default(self):
        old_app_env = os.environ.get('APP_ENV')
        old_target = os.environ.get('LEGACY_DEPLOYMENT_TARGET')
        old_override = os.environ.get('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY')
        try:
            os.environ.pop('LEGACY_DEPLOYMENT_TARGET', None)
            os.environ.pop('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY', None)
            os.environ['APP_ENV'] = 'local'
            assessment = assess_legacy_deployment()
            self.assertTrue(assessment.supported)
            self.assertEqual(assessment.target, 'local')
        finally:
            if old_app_env is None:
                os.environ.pop('APP_ENV', None)
            else:
                os.environ['APP_ENV'] = old_app_env
            if old_target is None:
                os.environ.pop('LEGACY_DEPLOYMENT_TARGET', None)
            else:
                os.environ['LEGACY_DEPLOYMENT_TARGET'] = old_target
            if old_override is None:
                os.environ.pop('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY', None)
            else:
                os.environ['LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY'] = old_override

    def test_production_deployment_blocked_without_override(self):
        old_app_env = os.environ.get('APP_ENV')
        old_override = os.environ.get('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY')
        try:
            os.environ['APP_ENV'] = 'production'
            os.environ.pop('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY', None)
            assessment = assess_legacy_deployment(target='production')
            self.assertFalse(assessment.supported)
            self.assertTrue(assessment.requires_override)
        finally:
            if old_app_env is None:
                os.environ.pop('APP_ENV', None)
            else:
                os.environ['APP_ENV'] = old_app_env
            if old_override is None:
                os.environ.pop('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY', None)
            else:
                os.environ['LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY'] = old_override

    def test_production_deployment_override_allows_experimental_path(self):
        old_app_env = os.environ.get('APP_ENV')
        old_override = os.environ.get('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY')
        try:
            os.environ['APP_ENV'] = 'production'
            os.environ['LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY'] = '1'
            assessment = assess_legacy_deployment(target='production')
            self.assertTrue(assessment.supported)
            self.assertTrue(assessment.requires_override)
        finally:
            if old_app_env is None:
                os.environ.pop('APP_ENV', None)
            else:
                os.environ['APP_ENV'] = old_app_env
            if old_override is None:
                os.environ.pop('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY', None)
            else:
                os.environ['LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY'] = old_override

    def test_runtime_config_defaults_to_local_environment(self):
        old_app_env = os.environ.get('APP_ENV')
        try:
            os.environ.pop('APP_ENV', None)
            config = RuntimeConfig()
            self.assertEqual(config.environment, 'local')
            self.assertTrue(config.local_mode)
        finally:
            if old_app_env is None:
                os.environ.pop('APP_ENV', None)
            else:
                os.environ['APP_ENV'] = old_app_env

    def test_cli_blocks_production_without_override(self):
        env = dict(os.environ)
        env['APP_ENV'] = 'production'
        env.pop('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY', None)
        with TemporaryDirectory() as _:
            result = subprocess.run(
                [sys.executable, '-m', 'src.deployment_guard', '--target', 'production'],
                cwd=r'F:\Trading',
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn('intentionally blocked by default', result.stderr)


if __name__ == '__main__':
    unittest.main()
