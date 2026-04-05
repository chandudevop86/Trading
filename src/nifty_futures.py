from __future__ import annotations

import argparse
from pathlib import Path

from src.csv_io import write_csv_rows
from src.nse_futures import extract_futures_records, fetch_futures_chain
from src.pipeline import apply_rules, load_rules
from src.legacy_scope import fail_deprecated_entrypoint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch NSE index futures data and optionally apply rules")
    parser.add_argument("--symbol", default="NIFTY", help="Underlying index symbol, e.g. NIFTY or BANKNIFTY")
    parser.add_argument(
        "--snapshot-output",
        default=Path("data/nifty_futures_snapshot.csv"),
        type=Path,
        help="CSV path for raw futures snapshot",
    )
    parser.add_argument(
        "--rules",
        default=Path("data/nifty_futures_rules.yaml"),
        type=Path,
        help="Rules YAML path",
    )
    parser.add_argument(
        "--scored-output",
        default=Path("data/nifty_futures_scored.csv"),
        type=Path,
        help="CSV path for scored futures snapshot",
    )
    return parser.parse_args()


def run(symbol: str, snapshot_output: Path, rules_path: Path, scored_output: Path) -> None:
    payload = fetch_futures_chain(symbol)
    rows = extract_futures_records(payload)
    write_csv_rows(snapshot_output, rows)

    if rules_path.exists():
        rules = load_rules(rules_path)
        scored = apply_rules(rows, rules)
        write_csv_rows(scored_output, scored)


if __name__ == "__main__":
    fail_deprecated_entrypoint('src.nifty_futures')
    args = parse_args()
    run(args.symbol, args.snapshot_output, args.rules, args.scored_output)
