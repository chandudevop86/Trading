from pathlib import Path

from src.legacy_scope import (
    ACTIVE_LEGACY_SURFACE,
    COMPATIBILITY_LEGACY_SURFACE,
    DEPRECATED_LEGACY_SURFACE,
    SUPPORTED_LEGACY_ENTRYPOINTS,
    active_legacy_targets,
    compatibility_entrypoint_message,
    compatibility_legacy_targets,
    deprecated_entrypoint_message,
    deprecated_legacy_targets,
    is_supported_legacy_entrypoint,
)


def test_supported_legacy_entrypoints_include_primary_runtime_targets() -> None:
    assert 'src/Trading.py' in SUPPORTED_LEGACY_ENTRYPOINTS
    assert 'src.auto_run' in SUPPORTED_LEGACY_ENTRYPOINTS
    assert 'src.auto_backtest' in SUPPORTED_LEGACY_ENTRYPOINTS
    assert is_supported_legacy_entrypoint('src.breakout_bot') is True
    assert is_supported_legacy_entrypoint('src.main') is False
    assert is_supported_legacy_entrypoint('src.reconcile_live') is False
    assert is_supported_legacy_entrypoint('src.reconcile_positions') is False
    assert is_supported_legacy_entrypoint('src/breakout_app.py') is False


def test_active_code_surface_doc_mentions_all_active_targets() -> None:
    doc = Path(r'F:/Trading/docs/active_code_surface.md').read_text(encoding='utf-8')

    for target in active_legacy_targets():
        assert target in doc


def test_active_code_surface_doc_mentions_all_compatibility_targets() -> None:
    doc = Path(r'F:/Trading/docs/active_code_surface.md').read_text(encoding='utf-8')

    for target in compatibility_legacy_targets():
        assert target in doc


def test_active_code_surface_doc_mentions_all_deprecated_targets() -> None:
    doc = Path(r'F:/Trading/docs/active_code_surface.md').read_text(encoding='utf-8')

    for target in deprecated_legacy_targets():
        assert target in doc


def test_launcher_scripts_point_to_supported_legacy_targets() -> None:
    run_app = Path(r'F:/Trading/tools/run_app.ps1').read_text(encoding='utf-8')
    run_backtest = Path(r'F:/Trading/tools/run_auto_backtest.ps1').read_text(encoding='utf-8')
    legacy_suite = Path(r'F:/Trading/tools/start_legacy_paper_suite.ps1').read_text(encoding='utf-8')

    assert 'streamlit run src\\Trading.py' in run_app
    assert '-m src.auto_backtest' in run_backtest
    assert "'-m', 'src.auto_run'" in legacy_suite


def test_active_legacy_surface_is_machine_readable() -> None:
    names = {entry.name for entry in ACTIVE_LEGACY_SURFACE}
    commands = {entry.canonical_command for entry in ACTIVE_LEGACY_SURFACE}

    assert 'legacy_ui' in names
    assert 'legacy_auto_run' in names
    assert 'streamlit run src/Trading.py' in commands
    assert 'py -3 -m src.auto_run' in commands


def test_compatibility_entrypoints_are_machine_readable() -> None:
    names = {entry.name for entry in COMPATIBILITY_LEGACY_SURFACE}
    messages = {compatibility_entrypoint_message(entry.target) for entry in COMPATIBILITY_LEGACY_SURFACE}

    assert 'legacy_btst_bot' in names
    assert any('Prefer py -3 -m src.auto_backtest.' in message for message in messages)


def test_deprecated_entrypoints_are_machine_readable() -> None:
    names = {entry.name for entry in DEPRECATED_LEGACY_SURFACE}
    messages = {deprecated_entrypoint_message(entry.target) for entry in DEPRECATED_LEGACY_SURFACE}

    assert 'legacy_main' in names
    assert 'legacy_reconcile_live' in names
    assert 'legacy_reconcile_positions' in names
    assert any('src.main is deprecated' in message for message in messages)
    assert any('src.reconcile_live is deprecated' in message for message in messages)
    assert any('src.reconcile_positions is deprecated' in message for message in messages)
    assert any('Use py -3 -m src.auto_run instead.' in message for message in messages)
