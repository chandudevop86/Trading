from __future__ import annotations

from typing import Any

import pandas as pd

from vinayak.metrics.utils import coerce_trade_records, safe_divide


_FAILURE_STATUSES = {'FAILED', 'REJECTED', 'ERROR', 'BLOCKED'}
_SUCCESS_STATUSES = {'FILLED', 'EXECUTED', 'SENT', 'ACCEPTED', 'CLOSED', 'OPEN'}


def calculate_execution_metrics(trades: Any) -> dict[str, Any]:
    frame = coerce_trade_records(trades, deduplicate=False)
    if frame.empty:
        return {
            'signals_generated': 0,
            'signals_executed': 0,
            'signal_to_execution_rate': 0.0,
            'missed_trade_count': 0,
            'missed_trade_rate': 0.0,
            'duplicate_trade_attempts': 0,
            'duplicate_trade_rate': 0.0,
            'blocked_duplicates_count': 0,
            'average_signal_to_execution_latency_sec': 0.0,
            'median_signal_to_execution_latency_sec': 0.0,
            'order_failure_count': 0,
            'order_failure_rate': 0.0,
            'execution_success_rate': 0.0,
            'average_slippage': 0.0,
            'worst_slippage': 0.0,
            'invalid_trade_block_count': 0,
        }

    statuses = frame.get('status', pd.Series([''] * len(frame), index=frame.index)).astype(str).str.upper()
    executed_mask = statuses.isin(_SUCCESS_STATUSES) | frame.get('execution_time', pd.Series([pd.NaT] * len(frame), index=frame.index)).notna()
    failed_mask = statuses.isin(_FAILURE_STATUSES)
    signal_mask = frame.get('signal_time', pd.Series([pd.NaT] * len(frame), index=frame.index)).notna() | frame.get('trade_id', pd.Series([''] * len(frame), index=frame.index)).astype(str).ne('')

    dedupe_by_contract = frame[['symbol', 'side', 'entry_time']].astype(str).agg('|'.join, axis=1)
    duplicate_trade_attempts = int(dedupe_by_contract.duplicated(keep=False).sum())
    blocked_duplicates = int(frame.get('duplicate_blocked', pd.Series([False] * len(frame), index=frame.index)).fillna(False).astype(bool).sum())
    if 'rejection_reason' in frame.columns:
        blocked_duplicates += int(frame['rejection_reason'].fillna('').astype(str).str.contains('duplicate', case=False).sum())

    latency = None
    if 'signal_time' in frame.columns and 'execution_time' in frame.columns:
        latency = (frame['execution_time'] - frame['signal_time']).dt.total_seconds().dropna()

    slippage = pd.to_numeric(frame.get('slippage', pd.Series(dtype=float)), errors='coerce').dropna()
    invalid_blocks = 0
    if 'rejection_reason' in frame.columns:
        invalid_blocks = int(frame['rejection_reason'].fillna('').astype(str).str.contains('invalid|missing', case=False).sum())

    signals_generated = int(signal_mask.sum())
    signals_executed = int(executed_mask.sum())
    return {
        'signals_generated': signals_generated,
        'signals_executed': signals_executed,
        'signal_to_execution_rate': round(safe_divide(signals_executed, signals_generated), 4),
        'missed_trade_count': max(0, signals_generated - signals_executed),
        'missed_trade_rate': round(safe_divide(max(0, signals_generated - signals_executed), signals_generated), 4),
        'duplicate_trade_attempts': duplicate_trade_attempts,
        'duplicate_trade_rate': round(safe_divide(duplicate_trade_attempts, max(len(frame), 1)), 4),
        'blocked_duplicates_count': blocked_duplicates,
        'average_signal_to_execution_latency_sec': round(float(latency.mean()) if latency is not None and not latency.empty else 0.0, 4),
        'median_signal_to_execution_latency_sec': round(float(latency.median()) if latency is not None and not latency.empty else 0.0, 4),
        'order_failure_count': int(failed_mask.sum()),
        'order_failure_rate': round(safe_divide(int(failed_mask.sum()), max(signals_generated, 1)), 4),
        'execution_success_rate': round(safe_divide(signals_executed, max(signals_executed + int(failed_mask.sum()), 1)), 4),
        'average_slippage': round(float(slippage.mean()) if not slippage.empty else 0.0, 4),
        'worst_slippage': round(float(slippage.max()) if not slippage.empty else 0.0, 4),
        'invalid_trade_block_count': invalid_blocks,
    }

