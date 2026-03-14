from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable


def read_csv_rows(input_path: Path) -> list[dict[str, str]]:
    with input_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv_rows(output_path: Path, rows: Iterable[dict[str, object]]) -> None:
    rows_list = list(rows)
    headers = list(rows_list[0].keys()) if rows_list else []
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows_list)
