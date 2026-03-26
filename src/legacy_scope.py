from __future__ import annotations

import sys


LEGACY_PRODUCT_ROOT = "src"

SUPPORTED_LEGACY_ENTRYPOINTS: tuple[str, ...] = (
    "src/Trading.py",
    "src.main",
    "src.nifty50",
    "src.nifty_options",
    "src.nifty_futures",
    "src.breakout_bot",
    "src.btst_bot",
    "src.auto_run",
    "src.auto_backtest",
    "src.dhan_example",
    "src.dhan_account",
    "src.reconcile_live",
    "src.reconcile_positions",
)


def noncanonical_entrypoint_message(entrypoint: str, *, canonical: str = "src/Trading.py") -> str:
    return (
        f"[legacy-scope] {entrypoint} is not a supported legacy entrypoint. "
        f"Use {canonical} for the supported legacy runtime surface."
    )


def warn_noncanonical_entrypoint(entrypoint: str, *, canonical: str = "src/Trading.py") -> None:
    print(noncanonical_entrypoint_message(entrypoint, canonical=canonical), file=sys.stderr)


def fail_noncanonical_entrypoint(entrypoint: str, *, canonical: str = "src/Trading.py", exit_code: int = 2) -> None:
    raise SystemExit(noncanonical_entrypoint_message(entrypoint, canonical=canonical))

