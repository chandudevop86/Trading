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
        second_half_expectancy = _safe_float(item.get('second_half_expectancy_per_trade'))
        expectancy_stability_gap_ratio = _safe_float(item.get('expectancy_stability_gap_ratio'))
        drawdown_proven = str(item.get('drawdown_proven', 'NO') or 'NO').strip().upper() == 'YES'
        retest_only_trade_pct = _safe_float(item.get('retest_only_trade_pct'))
        vwap_pass_pct = _safe_float(item.get('vwap_pass_pct'))
        session_pass_pct = _safe_float(item.get('session_pass_pct'))
        positive_expectancy = str(item.get('positive_expectancy', 'NO')).strip().upper() == 'YES' or expectancy > 0
        validation_status = str(item.get('validation_status', 'FAIL') or 'FAIL').strip().upper()
        real_backtest_score = (
            (1500.0 if validation_status == 'PASS' else 0.0)
            + (250.0 if drawdown_proven else 0.0)
            + (min(profit_factor, 5.0) * 90.0)
            + (expectancy * 45.0)
            + (second_half_expectancy * 35.0)
            + (min(retest_only_trade_pct, 100.0) * 1.0)
            + (min(vwap_pass_pct, 100.0) * 0.8)
            + (min(session_pass_pct, 100.0) * 0.8)
            + (min(win_rate, 100.0) * 0.5)
            - (drawdown_pct * 16.0)
            - (expectancy_stability_gap_ratio * 180.0)
        )
        ranked_input.append(
            {
                **item,
                'expectancy_per_trade': round(expectancy, 2),
                'second_half_expectancy_per_trade': round(second_half_expectancy, 2),
                'profit_factor_normalized': profit_factor,
                'drawdown_pct_normalized': round(drawdown_pct, 2),
                'win_rate_normalized': round(win_rate, 2),
                'trade_count_normalized': trades,
                'total_pnl_normalized': round(total_pnl, 2),
                'expectancy_stability_gap_ratio_normalized': round(expectancy_stability_gap_ratio, 4),
                'drawdown_proven_normalized': 'YES' if drawdown_proven else 'NO',
                'retest_only_trade_pct_normalized': round(retest_only_trade_pct, 2),
                'vwap_pass_pct_normalized': round(vwap_pass_pct, 2),
                'session_pass_pct_normalized': round(session_pass_pct, 2),
                'real_backtest_score': round(real_backtest_score, 2),
                'positive_expectancy': 'YES' if positive_expectancy else 'NO',
                'validation_status': validation_status,
            }
        )

    ranked = sorted(
        ranked_input,
        key=lambda row: (
            0 if str(row.get('validation_status', 'FAIL')).upper() == 'PASS' else 1,
            0 if str(row.get('drawdown_proven_normalized', 'NO')).upper() == 'YES' else 1,
            -_safe_float(row.get('real_backtest_score')),
            -float(row.get('profit_factor_normalized', 0.0)),
            -_safe_float(row.get('second_half_expectancy_per_trade')),
            _safe_float(row.get('expectancy_stability_gap_ratio_normalized')),
            -_safe_float(row.get('total_pnl_normalized')),
            _safe_float(row.get('drawdown_pct_normalized')),
            -_safe_float(row.get('expectancy_per_trade')),
            -_safe_float(row.get('vwap_pass_pct_normalized')),
            -_safe_float(row.get('session_pass_pct_normalized')),
            -_safe_float(row.get('retest_only_trade_pct_normalized')),
            -_safe_float(row.get('win_rate_normalized')),
            -_safe_float(row.get('trade_count_normalized')),
            0 if str(row.get('positive_expectancy', 'NO')).upper() == 'YES' else 1,
        ),
    )

    output: list[dict[str, Any]] = []
    for idx, row in enumerate(ranked, start=1):
        result = dict(row)
        result['rank'] = idx
        result['selection_priority'] = 'REAL_BACKTEST_EXPECTANCY_DRAWDOWN_VALIDATION'
        result.pop('profit_factor_normalized', None)
        result.pop('drawdown_pct_normalized', None)
        result.pop('win_rate_normalized', None)
        result.pop('trade_count_normalized', None)
        result.pop('total_pnl_normalized', None)
        result.pop('expectancy_stability_gap_ratio_normalized', None)
        result.pop('drawdown_proven_normalized', None)
        result.pop('retest_only_trade_pct_normalized', None)
        result.pop('vwap_pass_pct_normalized', None)
        result.pop('session_pass_pct_normalized', None)
        output.append(result)
    return output
