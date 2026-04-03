from pathlib import Path


def test_expected_architecture_paths_exist() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    app_root = repo_root / 'app' / 'vinayak'
    expected_repo = [
        repo_root / 'app',
        repo_root / 'web',
        repo_root / 'infra',
        repo_root / 'docs',
        repo_root / 'tests',
    ]
    expected_app = [
        app_root / 'api',
        app_root / 'web',
        app_root / 'strategies',
        app_root / 'execution',
        app_root / 'notifications',
        app_root / 'db',
        app_root / 'cache',
        app_root / 'queue',
        app_root / 'catalog',
        app_root / 'messaging',
        app_root / 'workers',
        app_root / 'core',
        app_root / 'workers' / 'outbox_worker.py',
        app_root / 'messaging' / 'outbox.py',
        app_root / 'db' / 'repositories' / 'outbox_repository.py',
        app_root / 'api' / 'routes' / 'outbox.py',
        app_root / 'api' / 'schemas' / 'outbox.py',
    ]
    for path in expected_repo + expected_app:
        assert path.exists(), f'Missing required path: {path}'


def test_sprint_one_files_exist() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    app_root = repo_root / 'app' / 'vinayak'
    required = [
        app_root / 'strategies' / 'breakout' / 'service.py',
        app_root / 'api' / 'routes' / 'strategies.py',
        app_root / 'db' / 'models' / 'signal.py',
        app_root / 'db' / 'models' / 'execution.py',
        app_root / 'api' / 'routes' / 'catalog.py',
        app_root / 'messaging' / 'bus.py',
        app_root / 'api' / 'routes' / 'outbox.py',
    ]
    for path in required:
        assert path.exists(), f'Missing Sprint 1 file: {path}'
