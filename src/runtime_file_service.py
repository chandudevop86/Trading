from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.trading_core import write_rows


def append_text_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with path.open('a', encoding='utf-8') as handle:
        handle.write(f'[{stamp}] {message}\n')


def ensure_output_files(paths: list[Path], log_paths: list[Path]) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            pd.DataFrame().to_csv(path, index=False)
    for path in log_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)


def save_runtime_outputs(
    candles: pd.DataFrame,
    trades: list[dict[str, object]],
    *,
    ohlcv_output: Path,
    live_ohlcv_output: Path,
    trades_output: Path,
    signal_output: Path,
    executed_trades_output: Path,
    paper_log_output: Path,
    live_log_output: Path,
) -> None:
    candle_rows = candles.to_dict(orient='records')
    write_rows(ohlcv_output, candle_rows)
    write_rows(live_ohlcv_output, candle_rows)
    write_rows(trades_output, trades)
    write_rows(signal_output, trades)
    for path in (executed_trades_output, paper_log_output, live_log_output):
        if not path.exists() or path.stat().st_size == 0:
            pd.DataFrame().to_csv(path, index=False)


def mirror_output_file(source: Path, *destinations: Path) -> None:
    if not source.exists():
        return
    for destination in destinations:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
