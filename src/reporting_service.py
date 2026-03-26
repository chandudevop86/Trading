from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.backtest_engine import summarize_trade_log
from src.strategy_tuning import normalize_strategy_key


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == '':
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def metric_value(rows: list[dict[str, object]], key: str, default: float = 0.0) -> float:
    if not rows:
        return default
    return safe_float(rows[-1].get(key, default), default)


def recent_trade_summary(trades: list[dict[str, object]]) -> str:
    if not trades:
        return 'No recent trade generated.'
    last = dict(trades[-1])
    return (
        f"{last.get('side', 'NA')} {last.get('strategy', 'TRADE')} | "
        f"Entry {float(last.get('entry', last.get('entry_price', 0.0)) or 0.0):.2f} | "
        f"SL {float(last.get('stop_loss', 0.0) or 0.0):.2f} | "
        f"Target {float(last.get('target', last.get('target_price', 0.0)) or 0.0):.2f} | "
        f"Score {float(last.get('score', 0.0) or 0.0):.2f}"
    )


def status_message(run_clicked: bool, backtest_clicked: bool) -> str:
    if backtest_clicked:
        return 'Backtest completed'
    if run_clicked:
        return 'Run completed'
    return 'Ready'


def load_csv_summary(path: Path) -> dict[str, object]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        frame = pd.read_csv(path)
    except Exception:
        return {}
    if frame.empty:
        return {}
    return frame.iloc[-1].to_dict()


def load_csv_rows(path: Path) -> list[dict[str, object]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        frame = pd.read_csv(path)
    except Exception:
        return []
    if frame.empty:
        return []
    return frame.to_dict(orient='records')


def current_execution_rows(path: Path, strategy: str, symbol: str, execution_type: str = 'PAPER') -> list[dict[str, object]]:
    normalized_strategy = normalize_strategy_key(strategy)
    normalized_symbol = (symbol or '').strip().upper()
    rows = load_csv_rows(path)
    filtered: list[dict[str, object]] = []
    for row in rows:
        if str(row.get('execution_type', '') or '').strip().upper() != execution_type:
            continue
        row_strategy = normalize_strategy_key(str(row.get('strategy', '') or ''))
        row_symbol = str(row.get('symbol', '') or '').strip().upper()
        if normalized_strategy and row_strategy and row_strategy != normalized_strategy:
            continue
        if normalized_symbol and row_symbol and row_symbol != normalized_symbol:
            continue
        filtered.append(dict(row))
    if filtered or not normalized_symbol:
        return filtered
    return [
        dict(row)
        for row in rows
        if str(row.get('execution_type', '') or '').strip().upper() == execution_type
        and normalize_strategy_key(str(row.get('strategy', '') or '')) == normalized_strategy
    ]


def paper_execution_summary(path: Path, strategy: str, symbol: str, capital: float) -> dict[str, object]:
    rows = current_execution_rows(path, strategy, symbol, execution_type='PAPER')
    if not rows:
        return {}
    return summarize_trade_log(rows, capital=float(capital), strategy_name=normalize_strategy_key(strategy) or 'PAPER_EXECUTION')


def todays_trade_count(path: Path, strategy: str, symbol: str, execution_type: str = 'PAPER') -> int:
    rows = current_execution_rows(path, strategy, symbol, execution_type=execution_type)
    if not rows:
        return 0
    today_key = datetime.now().strftime('%Y-%m-%d')
    count = 0
    for row in rows:
        status = str(row.get('execution_status', '') or '').strip().upper()
        if status not in {'EXECUTED', 'FILLED', 'CLOSED', 'EXITED'}:
            continue
        for column in ('executed_at_utc', 'exit_time', 'entry_time', 'signal_time', 'timestamp'):
            value = str(row.get(column, '') or '').strip()
            if value[:10] == today_key:
                count += 1
                break
    return count


def active_summary(backtest_summary: dict[str, object], paper_summary: dict[str, object]) -> dict[str, object]:
    return dict(backtest_summary or paper_summary or {})


def short_broker_status(broker_choice: str, broker_status: str) -> str:
    if broker_choice == 'Dhan Live':
        return 'Dhan live active' if 'armed' in broker_status.lower() else broker_status
    return 'Paper broker active'
