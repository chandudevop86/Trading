from __future__ import annotations

import argparse
from pathlib import Path

from src.csv_io import write_csv_rows
from src.nse_client import fetch_nifty50_rows
from src.pipeline import apply_rules, load_rules
from src.legacy_scope import fail_deprecated_entrypoint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch NIFTY 50 and optionally apply rules")
    parser.add_argument(
        "--snapshot-output",
        default=Path("data/nifty50_snapshot.csv"),
        type=Path,
        help="CSV path for raw NIFTY 50 snapshot",
    )
    parser.add_argument(
        "--rules",
        default=Path("data/nifty50_rules.yaml"),
        type=Path,
        help="Rules YAML path",
    )
    parser.add_argument(
        "--scored-output",
        default=Path("data/nifty50_scored.csv"),
        type=Path,
        help="CSV path for scored NIFTY 50 snapshot",
    )
    return parser.parse_args()


def run(snapshot_output: Path, rules_path: Path, scored_output: Path) -> None:
    rows = fetch_nifty50_rows()
    write_csv_rows(snapshot_output, rows)

    if rules_path.exists():
        rules = load_rules(rules_path)
        scored = apply_rules(rows, rules)
        write_csv_rows(scored_output, scored)


if __name__ == "__main__":
    fail_deprecated_entrypoint('src.nifty50')
    args = parse_args()
    run(args.snapshot_output, args.rules, args.scored_output)
