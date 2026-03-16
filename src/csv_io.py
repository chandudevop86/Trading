from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable


def read_csv_rows(input_path: Path) -> list[dict[str, str]]:
    """Read CSV rows with a few encoding fallbacks.

    Many CSVs (especially from Excel/Windows) are not UTF-8 and contain bytes
    like 0x95 that fail strict UTF-8 decoding.
    """

    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

    for enc in encodings:
        try:
            with input_path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue

    # Last resort: keep the file readable, even if some characters are replaced.
    with input_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f))


def write_csv_rows(output_path: Path, rows: Iterable[dict[str, object]]) -> None:
    rows_list = list(rows)
    headers = list(rows_list[0].keys()) if rows_list else []
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows_list)