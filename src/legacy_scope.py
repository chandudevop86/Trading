from __future__ import annotations

import sys
from dataclasses import dataclass


LEGACY_PRODUCT_ROOT = "src"


@dataclass(frozen=True, slots=True)
class LegacySurfaceEntry:
    name: str
    target: str
    surface_type: str
    canonical_command: str
    status: str = "active"
    recommended_replacement: str = ""


LEGACY_SURFACE: tuple[LegacySurfaceEntry, ...] = (
    LegacySurfaceEntry("legacy_ui", "src/Trading.py", "ui", "streamlit run src/Trading.py"),
    LegacySurfaceEntry("legacy_local_launcher", "tools/run_app.ps1", "launcher", "powershell -File tools/run_app.ps1"),
    LegacySurfaceEntry("legacy_auto_run", "src.auto_run", "cli", "py -3 -m src.auto_run"),
    LegacySurfaceEntry("legacy_auto_backtest", "src.auto_backtest", "cli", "py -3 -m src.auto_backtest"),
    LegacySurfaceEntry("legacy_breakout_bot", "src.breakout_bot", "cli", "py -3 -m src.breakout_bot"),
    LegacySurfaceEntry("legacy_dhan_example", "src.dhan_example", "cli", "py -3 -m src.dhan_example"),
    LegacySurfaceEntry("legacy_dhan_account", "src.dhan_account", "cli", "py -3 -m src.dhan_account"),
    LegacySurfaceEntry("legacy_docker_local", "deploy/docker/Dockerfile", "docker", "docker compose -f deploy/docker/docker-compose.yml up"),
    LegacySurfaceEntry("legacy_docker_compose", "deploy/docker/docker-compose.yml", "docker", "docker compose -f deploy/docker/docker-compose.yml up"),
    LegacySurfaceEntry("legacy_main", "src.main", "cli", "py -3 -m src.main", status="deprecated", recommended_replacement="py -3 -m src.nifty50"),
    LegacySurfaceEntry("legacy_nifty50", "src.nifty50", "cli", "py -3 -m src.nifty50", status="compatibility", recommended_replacement="tools/run_auto_backtest.ps1"),
    LegacySurfaceEntry("legacy_nifty_options", "src.nifty_options", "cli", "py -3 -m src.nifty_options", status="compatibility", recommended_replacement="streamlit run src/Trading.py"),
    LegacySurfaceEntry("legacy_nifty_futures", "src.nifty_futures", "cli", "py -3 -m src.nifty_futures", status="compatibility", recommended_replacement="streamlit run src/Trading.py"),
    LegacySurfaceEntry("legacy_btst_bot", "src.btst_bot", "cli", "py -3 -m src.btst_bot", status="compatibility", recommended_replacement="py -3 -m src.auto_backtest"),
    LegacySurfaceEntry("legacy_reconcile_live", "src.reconcile_live", "cli", "py -3 -m src.reconcile_live", status="deprecated", recommended_replacement="py -3 -m src.auto_run"),
    LegacySurfaceEntry("legacy_reconcile_positions", "src.reconcile_positions", "cli", "py -3 -m src.reconcile_positions", status="deprecated", recommended_replacement="py -3 -m src.auto_run"),
)

SUPPORTED_LEGACY_ENTRYPOINTS: tuple[str, ...] = tuple(
    entry.target for entry in LEGACY_SURFACE if entry.surface_type in {"ui", "cli"} and entry.status != "deprecated"
)

ACTIVE_LEGACY_SURFACE: tuple[LegacySurfaceEntry, ...] = tuple(
    entry for entry in LEGACY_SURFACE if entry.status == "active"
)

COMPATIBILITY_LEGACY_SURFACE: tuple[LegacySurfaceEntry, ...] = tuple(
    entry for entry in LEGACY_SURFACE if entry.status == "compatibility"
)

DEPRECATED_LEGACY_SURFACE: tuple[LegacySurfaceEntry, ...] = tuple(
    entry for entry in LEGACY_SURFACE if entry.status == "deprecated"
)


def active_legacy_targets() -> tuple[str, ...]:
    return tuple(entry.target for entry in ACTIVE_LEGACY_SURFACE)


def compatibility_legacy_targets() -> tuple[str, ...]:
    return tuple(entry.target for entry in COMPATIBILITY_LEGACY_SURFACE)


def deprecated_legacy_targets() -> tuple[str, ...]:
    return tuple(entry.target for entry in DEPRECATED_LEGACY_SURFACE)


def get_legacy_surface_entry(entrypoint: str) -> LegacySurfaceEntry | None:
    target = str(entrypoint or "")
    for entry in LEGACY_SURFACE:
        if entry.target == target:
            return entry
    return None


def is_supported_legacy_entrypoint(entrypoint: str) -> bool:
    entry = get_legacy_surface_entry(entrypoint)
    return entry is not None and entry.status != "deprecated"


def noncanonical_entrypoint_message(entrypoint: str, *, canonical: str = "src/Trading.py") -> str:
    return (
        f"[legacy-scope] {entrypoint} is not a supported legacy entrypoint. "
        f"Use {canonical} for the supported legacy runtime surface."
    )


def compatibility_entrypoint_message(entrypoint: str) -> str:
    entry = get_legacy_surface_entry(entrypoint)
    if entry is None or entry.status != "compatibility":
        return ""
    replacement = f" Prefer {entry.recommended_replacement}." if entry.recommended_replacement else ""
    return (
        f"[legacy-scope] {entrypoint} is compatibility-supported, not a primary operator surface."
        f" Keep existing workflows working, but prefer an active surface for new automation.{replacement}"
    )


def deprecated_entrypoint_message(entrypoint: str) -> str:
    entry = get_legacy_surface_entry(entrypoint)
    if entry is None or entry.status != "deprecated":
        return ""
    replacement = f" Use {entry.recommended_replacement} instead." if entry.recommended_replacement else ""
    return f"[legacy-scope] {entrypoint} is deprecated and should not be used for new or existing operator flows.{replacement}"


def warn_noncanonical_entrypoint(entrypoint: str, *, canonical: str = "src/Trading.py") -> None:
    print(noncanonical_entrypoint_message(entrypoint, canonical=canonical), file=sys.stderr)


def warn_compatibility_entrypoint(entrypoint: str) -> None:
    message = compatibility_entrypoint_message(entrypoint)
    if message:
        print(message, file=sys.stderr)


def fail_deprecated_entrypoint(entrypoint: str, exit_code: int = 2) -> None:
    message = deprecated_entrypoint_message(entrypoint)
    raise SystemExit(message or noncanonical_entrypoint_message(entrypoint))


def fail_noncanonical_entrypoint(entrypoint: str, *, canonical: str = "src/Trading.py", exit_code: int = 2) -> None:
    raise SystemExit(noncanonical_entrypoint_message(entrypoint, canonical=canonical))
