from pathlib import Path


def test_expected_architecture_paths_exist() -> None:
    root = Path(__file__).resolve().parents[2]
    expected = [
        root / 'api',
        root / 'web',
        root / 'strategies',
        root / 'execution',
        root / 'notifications',
        root / 'db',
        root / 'cache',
        root / 'queue',
        root / 'deploy',
        root / 'catalog',
        root / 'messaging',
        root / 'workers',
        root / 'core',
        root / 'workers' / 'outbox_worker.py',
        root / 'messaging' / 'outbox.py',
        root / 'db' / 'repositories' / 'outbox_repository.py',
        root / 'api' / 'routes' / 'outbox.py',
        root / 'api' / 'schemas' / 'outbox.py',
    ]
    for path in expected:
        assert path.exists(), f'Missing required path: {path}'


def test_sprint_one_files_exist() -> None:
    root = Path(__file__).resolve().parents[2]
    required = [
        root / 'strategies' / 'breakout' / 'service.py',
        root / 'api' / 'routes' / 'strategies.py',
        root / 'db' / 'models' / 'signal.py',
        root / 'db' / 'models' / 'execution.py',
        root / 'api' / 'routes' / 'catalog.py',
        root / 'messaging' / 'bus.py',
        root / 'api' / 'routes' / 'outbox.py',
    ]
    for path in required:
        assert path.exists(), f'Missing Sprint 1 file: {path}'


