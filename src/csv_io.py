from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import increment_metric, set_metric


def _duplicate_count(rows: list[dict[str, str]]) -> int:
    seen: set[tuple[tuple[str, str], ...]] = set()
    duplicates = 0
    for row in rows:
        key = tuple(sorted((str(k), str(v)) for k, v in row.items()))
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
    return duplicates


def _null_count(rows: list[dict[str, str]]) -> int:
    total = 0
    for row in rows:
        for value in row.values():
            if value in (None, ''):
                total += 1
    return total


def read_csv_rows(input_path: Path) -> list[dict[str, str]]:
    """Read CSV rows with a few encoding fallbacks."""
    encodings = ['utf-8-sig', 'utf-8', 'cp1252', 'latin-1']

    for enc in encodings:
        try:
            with input_path.open('r', encoding=enc, newline='') as f:
                rows = list(csv.DictReader(f))
            increment_metric('market_data_rows_loaded_total', len(rows))
            increment_metric('market_data_duplicates_total', _duplicate_count(rows))
            increment_metric('market_data_nulls_total', _null_count(rows))
            set_metric('csv_last_read_timestamp', __import__('datetime').datetime.utcnow().isoformat() + 'Z')
            log_event(
                component='csv_io',
                event_name='csv_read',
                severity='INFO',
                message=f'Read {len(rows)} row(s) from {input_path}',
                context_json={'path': str(input_path), 'encoding': enc, 'rows': len(rows)},
            )
            return rows
        except UnicodeDecodeError:
            continue
        except Exception as exc:
            increment_metric('csv_read_failures_total', 1)
            log_exception(
                component='csv_io',
                event_name='csv_read_failed',
                exc=exc,
                message=f'Failed reading {input_path}',
                context_json={'path': str(input_path)},
            )
            raise

    with input_path.open('r', encoding='utf-8', errors='replace', newline='') as f:
        rows = list(csv.DictReader(f))
    increment_metric('market_data_rows_loaded_total', len(rows))
    increment_metric('market_data_duplicates_total', _duplicate_count(rows))
    increment_metric('market_data_nulls_total', _null_count(rows))
    set_metric('csv_last_read_timestamp', __import__('datetime').datetime.utcnow().isoformat() + 'Z')
    log_event(
        component='csv_io',
        event_name='csv_read_with_replace',
        severity='WARNING',
        message=f'Read {len(rows)} row(s) from {input_path} using replacement decoding',
        context_json={'path': str(input_path), 'rows': len(rows)},
    )
    return rows


def write_csv_rows(output_path: Path, rows: Iterable[dict[str, object]]) -> None:
    rows_list = list(rows)
    headers = list(rows_list[0].keys()) if rows_list else []
    try:
        with output_path.open('w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows_list)
        increment_metric('csv_rows_written_total', len(rows_list))
        set_metric('csv_last_write_timestamp', __import__('datetime').datetime.utcnow().isoformat() + 'Z')
        log_event(
            component='csv_io',
            event_name='csv_write',
            severity='INFO',
            message=f'Wrote {len(rows_list)} row(s) to {output_path}',
            context_json={'path': str(output_path), 'rows': len(rows_list), 'headers': headers},
        )
    except Exception as exc:
        increment_metric('csv_write_failures_total', 1)
        log_exception(
            component='csv_io',
            event_name='csv_write_failed',
            exc=exc,
            message=f'Failed writing {output_path}',
            context_json={'path': str(output_path), 'rows': len(rows_list)},
        )
        raise
