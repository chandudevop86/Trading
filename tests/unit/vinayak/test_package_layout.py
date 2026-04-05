from pathlib import Path

_ALLOWED_SRC_VINAYAK_BRIDGES = {
    'observability/logging.py',
    'observability/metrics.py',
}

_ALLOWED_APP_SRC_BRIDGES = {
    'legacy/data_processing.py',
    'legacy/market_data.py',
    'legacy/options.py',
}


def test_root_vinayak_package_is_only_a_shim() -> None:
    package_init = Path(r'F:/Trading/vinayak/__init__.py').read_text(encoding='utf-8')

    assert 'app' in package_init
    assert 'vinayak' in package_init
    assert '__path__' in package_init


def test_src_tree_does_not_import_vinayak_directly_outside_approved_bridges() -> None:
    src_root = Path(r'F:/Trading/src')
    offenders: list[str] = []

    for path in src_root.rglob('*.py'):
        relative_path = str(path.relative_to(src_root)).replace('\\', '/')
        text = path.read_text(encoding='utf-8')
        if ('from vinayak' in text or 'import vinayak' in text) and relative_path not in _ALLOWED_SRC_VINAYAK_BRIDGES:
            offenders.append(relative_path)

    assert offenders == []


def test_app_tree_does_not_import_src_directly_outside_approved_legacy_bridges() -> None:
    app_root = Path(r'F:/Trading/app/vinayak')
    offenders: list[str] = []

    for path in app_root.rglob('*.py'):
        relative_path = str(path.relative_to(app_root)).replace('\\', '/')
        text = path.read_text(encoding='utf-8')
        if ('from src' in text or 'import src' in text) and relative_path not in _ALLOWED_APP_SRC_BRIDGES:
            offenders.append(relative_path)

    assert offenders == []
