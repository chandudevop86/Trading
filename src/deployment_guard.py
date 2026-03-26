from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path


LOCAL_ENVIRONMENTS = {'', 'local', 'dev', 'development', 'docker', 'test'}
PRODUCTION_ENVIRONMENTS = {'prod', 'production', 'uat', 'stage', 'staging'}


@dataclass(slots=True)
class DeploymentAssessment:
    target: str
    environment: str
    supported: bool
    message: str
    requires_override: bool = False


def normalize_environment(environment: str | None = None) -> str:
    return str(environment or os.getenv('APP_ENV', 'local') or 'local').strip().lower()


def normalize_target(target: str | None = None) -> str:
    value = str(target or os.getenv('LEGACY_DEPLOYMENT_TARGET', '') or '').strip().lower()
    if value:
        return value
    environment = normalize_environment()
    return 'production' if environment in PRODUCTION_ENVIRONMENTS else 'local'


def experimental_production_override_enabled() -> bool:
    value = str(os.getenv('LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY', '') or '').strip().lower()
    return value in {'1', 'true', 'yes', 'on'}


def runtime_db_path() -> Path:
    raw = str(os.getenv('LEGACY_RUNTIME_DB_PATH', 'data/legacy_runtime.db') or 'data/legacy_runtime.db').strip()
    return Path(raw)


def assess_legacy_deployment(target: str | None = None, environment: str | None = None) -> DeploymentAssessment:
    normalized_environment = normalize_environment(environment)
    normalized_target = normalize_target(target)

    if normalized_target == 'local':
        return DeploymentAssessment(
            target='local',
            environment=normalized_environment,
            supported=True,
            message='Legacy local deployment is the supported profile. Use the canonical Streamlit entrypoint and local runtime storage.',
        )

    db_path = runtime_db_path()
    override_enabled = experimental_production_override_enabled()
    if not override_enabled:
        return DeploymentAssessment(
            target='production',
            environment=normalized_environment,
            supported=False,
            requires_override=True,
            message=(
                'Legacy production deployment is intentionally blocked by default. '
                'This repo currently supports local/operator deployment; production rollout remains experimental until the runtime architecture is fully hardened. '
                'Set LEGACY_ALLOW_EXPERIMENTAL_PROD_DEPLOY=1 only if you intentionally want the legacy experimental deploy path.'
            ),
        )

    return DeploymentAssessment(
        target='production',
        environment=normalized_environment,
        supported=True,
        requires_override=True,
        message=(
            'Experimental legacy production deployment override accepted. '
            f'Runtime DB path: {db_path}. Proceed with caution and treat this as a non-default deploy profile.'
        ),
    )


def require_supported_legacy_deployment(target: str | None = None, environment: str | None = None) -> DeploymentAssessment:
    assessment = assess_legacy_deployment(target=target, environment=environment)
    if not assessment.supported:
        raise RuntimeError(assessment.message)
    return assessment


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Validate whether the requested legacy deployment profile is supported.')
    parser.add_argument('--target', choices=['local', 'production'], default=None)
    parser.add_argument('--environment', default=None)
    args = parser.parse_args(argv)

    assessment = assess_legacy_deployment(target=args.target, environment=args.environment)
    stream = sys.stderr if not assessment.supported else sys.stdout
    stream.write(f'{assessment.message}\n')
    return 0 if assessment.supported else 2


if __name__ == '__main__':
    raise SystemExit(main())
