from __future__ import annotations

import argparse
from pathlib import Path

from src.pipeline import run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply rules to NIFTY 50 option-chain CSV")
    parser.add_argument(
        "--input",
        default=Path("data/nifty_options_chain_sample.csv"),
        type=Path,
        help="Input option-chain CSV path",
    )
    parser.add_argument(
        "--rules",
        default=Path("data/nifty_options_rules.yaml"),
        type=Path,
        help="Rules YAML path",
    )
    parser.add_argument(
        "--output",
        default=Path("data/nifty_options_scored.csv"),
        type=Path,
        help="Output scored CSV path",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.rules, args.output)
