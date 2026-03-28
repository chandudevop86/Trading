from __future__ import annotations

from typing import Any


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def rank_strategy_summaries(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked_input: list[dict[str, Any]] = []
    for row in summary_rows:
        item = dict(row)
        expectancy = _safe_float(item.get('expectancy_per_trade'))
        profit_factor_raw = item.get('profit_factor')
        profit_factor = float('inf') if str(profit_factor_raw).strip().lower() == 'inf' else _safe_float(profit_factor_raw)
        drawdown_pct = _safe_float(item.get('max_drawdown_pct', item.get('max_drawdown')))
        win_rate = _safe_float(item.get('win_rate', item.get('win_rate_pct')))
        trades = int(_safe_float(item.get('total_trades', item.get('trades'))))
        total_pnl = _safe_float(item.get('total_pnl'))
        positive_expectancy = str(item.get('positive_expectancy', 'NO')).strip().upper() == 'YES' or expectancy > 0
        validation_status = str(item.get('validation_status', 'FAIL') or 'FAIL').strip().upper()
        ranked_input.append(
            {
                **item,
                'expectancy_per_trade': round(expectancy, 2),
                'profit_factor_normalized': profit_factor,
                'drawdown_pct_normalized': round(drawdown_pct, 2),
                'win_rate_normalized': round(win_rate, 2),
                'trade_count_normalized': trades,
                'total_pnl_normalized': round(total_pnl, 2),
                'positive_expectancy': 'YES' if positive_expectancy else 'NO',
                'validation_status': validation_status,
            }
        )

    ranked = sorted(
        ranked_input,
        key=lambda row: (
            0 if str(row.get('validation_status', 'FAIL')).upper() == 'PASS' else 1,
            -float(row.get('profit_factor_normalized', 0.0)),
            -_safe_float(row.get('total_pnl_normalized')),
            _safe_float(row.get('drawdown_pct_normalized')),
            -_safe_float(row.get('expectancy_per_trade')),
            -_safe_float(row.get('win_rate_normalized')),
            -_safe_float(row.get('trade_count_normalized')),
            0 if str(row.get('positive_expectancy', 'NO')).upper() == 'YES' else 1,
        ),
    )

    output: list[dict[str, Any]] = []
    for idx, row in enumerate(ranked, start=1):
        result = dict(row)
        result['rank'] = idx
        result['selection_priority'] = 'VALIDATION_FIRST'
        result.pop('profit_factor_normalized', None)
        result.pop('drawdown_pct_normalized', None)
        result.pop('win_rate_normalized', None)
        result.pop('trade_count_normalized', None)
        result.pop('total_pnl_normalized', None)
        output.append(result)
    return output
