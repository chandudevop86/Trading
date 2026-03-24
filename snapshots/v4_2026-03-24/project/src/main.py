from __future__ import annotations

import argparse
from pathlib import Path

from src.pipeline import run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply YAML rules to CSV data")
    parser.add_argument("--input", required=True, type=Path, help="Input CSV path")
    parser.add_argument("--rules", required=True, type=Path, help="Rules YAML path")
    parser.add_argument("--output", required=True, type=Path, help="Output CSV path")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.rules, args.output)
