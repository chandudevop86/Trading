from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _score_bucket(score: float) -> str:
    if score >= 13:
        return '13+'
    if score >= 10:
        return '10-12'
    if score >= 7:
        return '7-9'
    return '0-6'


def _trade_score(record: dict[str, Any]) -> float:
    for key in ('score', 'total_score', 'zone_score', 'zone_strength_score', 'zone_gate_score'):
        raw = str(record.get(key, '') or '').strip()
        if raw:
            return _safe_float(record.get(key))
    return 0.0


def _equity_stats(trades: list[dict[str, Any]], starting_equity: float) -> tuple[float, float]:
    equity = float(starting_equity)
    peak = equity
    max_drawdown = 0.0
    max_drawdown_pct = 0.0
    for trade in trades:
        equity += _safe_float(trade.get('pnl'))
        peak = max(peak, equity)
        drawdown = peak - equity
        max_drawdown = max(max_drawdown, drawdown)
        if peak > 0:
            max_drawdown_pct = max(max_drawdown_pct, (drawdown / peak) * 100.0)
    return round(max_drawdown, 2), round(max_drawdown_pct, 2)


@dataclass(slots=True)
class ScoreBucketMetrics:
    bucket: str
    trades: int
    wins: int
    losses: int
    win_rate: float
    avg_pnl: float
    expectancy: float
    profit_factor: float | str
    max_drawdown_pct: float

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class ThresholdMetrics:
    threshold_label: str
    trades: int
    win_rate: float
    expectancy: float
    total_pnl: float
    max_drawdown_pct: float
    profit_factor: float | str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def analyze_score_buckets(trades: list[dict[str, Any]], *, starting_equity: float = 100000.0) -> list[ScoreBucketMetrics]:
    ordered_buckets = ['0-6', '7-9', '10-12', '13+']
    bucket_rows: list[ScoreBucketMetrics] = []
    for bucket in ordered_buckets:
        filtered = [trade for trade in trades if _score_bucket(_trade_score(trade)) == bucket]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in filtered]
        wins = sum(1 for value in pnl_values if value > 0)
        losses = sum(1 for value in pnl_values if value < 0)
        total = len(filtered)
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss_abs = abs(sum(value for value in pnl_values if value < 0))
        profit_factor: float | str = round(gross_profit / gross_loss_abs, 2) if gross_loss_abs > 0 else 'inf' if gross_profit > 0 else 0.0
        expectancy = round(sum(pnl_values) / total, 2) if total else 0.0
        _, max_drawdown_pct = _equity_stats(filtered, starting_equity)
        bucket_rows.append(
            ScoreBucketMetrics(
                bucket=bucket,
                trades=total,
                wins=wins,
                losses=losses,
                win_rate=round((wins / total) * 100.0, 2) if total else 0.0,
                avg_pnl=expectancy,
                expectancy=expectancy,
                profit_factor=profit_factor,
                max_drawdown_pct=max_drawdown_pct,
            )
        )
    return bucket_rows


def compare_thresholds(
    trades: list[dict[str, Any]],
    *,
    thresholds: tuple[int, ...] = (0, 8, 10, 12),
    starting_equity: float = 100000.0,
) -> list[ThresholdMetrics]:
    rows: list[ThresholdMetrics] = []
    for threshold in thresholds:
        label = 'ALL' if threshold <= 0 else f'{threshold}+'
        filtered = trades if threshold <= 0 else [trade for trade in trades if _trade_score(trade) >= float(threshold)]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in filtered]
        wins = sum(1 for value in pnl_values if value > 0)
        total = len(filtered)
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss_abs = abs(sum(value for value in pnl_values if value < 0))
        profit_factor: float | str = round(gross_profit / gross_loss_abs, 2) if gross_loss_abs > 0 else 'inf' if gross_profit > 0 else 0.0
        _, max_drawdown_pct = _equity_stats(filtered, starting_equity)
        rows.append(
            ThresholdMetrics(
                threshold_label=label,
                trades=total,
                win_rate=round((wins / total) * 100.0, 2) if total else 0.0,
                expectancy=round(sum(pnl_values) / total, 2) if total else 0.0,
                total_pnl=round(sum(pnl_values), 2),
                max_drawdown_pct=max_drawdown_pct,
                profit_factor=profit_factor,
            )
        )
    return rows


def best_minimum_score_threshold(rows: list[ThresholdMetrics]) -> str:
    best_label = 'ALL'
    best_key = (-10**9, -10**9, 10**9, -10**9)
    for row in rows:
        pf = 999.0 if row.profit_factor == 'inf' else float(row.profit_factor)
        candidate = (row.expectancy, pf, -row.max_drawdown_pct, -row.trades)
        if row.trades > 0 and candidate > best_key:
            best_key = candidate
            best_label = row.threshold_label
    return best_label


def render_score_backtest_summary(trades: list[dict[str, Any]], *, starting_equity: float = 100000.0) -> dict[str, object]:
    bucket_rows = analyze_score_buckets(trades, starting_equity=starting_equity)
    threshold_rows = compare_thresholds(trades, starting_equity=starting_equity)
    return {
        'score_bucket_rows': [row.to_dict() for row in bucket_rows],
        'threshold_filter_rows': [row.to_dict() for row in threshold_rows],
        'best_min_score_threshold': best_minimum_score_threshold(threshold_rows),
    }
