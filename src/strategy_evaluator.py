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
        drawdown = _safe_float(item.get('max_drawdown'))
        win_rate = _safe_float(item.get('win_rate', item.get('win_rate_pct')))
        trades = int(_safe_float(item.get('total_trades', item.get('trades'))))
        positive_expectancy = str(item.get('positive_expectancy', 'NO')).strip().upper() == 'YES' or expectancy > 0
        ranked_input.append(
            {
                **item,
                'expectancy_per_trade': round(expectancy, 2),
                'profit_factor_normalized': profit_factor,
                'max_drawdown': round(drawdown, 2),
                'win_rate_normalized': round(win_rate, 2),
                'trade_count_normalized': trades,
                'positive_expectancy': 'YES' if positive_expectancy else 'NO',
            }
        )

    ranked = sorted(
        ranked_input,
        key=lambda row: (
            0 if str(row.get('positive_expectancy', 'NO')).upper() == 'YES' else 1,
            -_safe_float(row.get('expectancy_per_trade')),
            -float(row.get('profit_factor_normalized', 0.0)),
            _safe_float(row.get('max_drawdown')),
            -_safe_float(row.get('win_rate_normalized')),
            -_safe_float(row.get('trade_count_normalized')),
        ),
    )

    output: list[dict[str, Any]] = []
    for idx, row in enumerate(ranked, start=1):
        result = dict(row)
        result['rank'] = idx
        result['selection_priority'] = 'EXPECTANCY_FIRST'
        result.pop('profit_factor_normalized', None)
        result.pop('win_rate_normalized', None)
        result.pop('trade_count_normalized', None)
        output.append(result)
    return output
