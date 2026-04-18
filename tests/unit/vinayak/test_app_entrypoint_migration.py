from __future__ import annotations

from pathlib import Path


def test_app_main_uses_package_relative_import_without_sys_path_hack() -> None:
    source = Path(r'D:\Trading\app\main.py').read_text(encoding='utf-8')

    assert 'sys.path.insert' not in source
    assert 'sys.path.append' not in source
    assert 'from .vinayak.api.main import app' in source
