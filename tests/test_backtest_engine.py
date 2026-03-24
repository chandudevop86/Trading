import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from src.backtest_engine import BacktestConfig, BacktestValidationConfig, run_backtest, summarize_trade_log


def _build_market_frame(total_trades: int = 120) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    base_time = pd.Timestamp('2026-03-01 09:15:00')
    winners = 0
    for idx in range(total_trades * 2 + 4):
        timestamp = base_time + pd.Timedelta(minutes=idx)
        close = 100.0 + (idx * 0.1)
        row = {
            'timestamp': timestamp,
            'open': close,
            'high': close + 0.2,
            'low': close - 0.2,
            'close': close,
            'volume': 1000 + idx,
        }
        rows.append(row)

    for trade_index in range(total_trades):
        entry_idx = trade_index * 2
        follow_idx = entry_idx + 1
        entry_close = float(rows[entry_idx]['close'])
        if trade_index % 5 == 0:
            rows[follow_idx]['low'] = round(entry_close - 0.9, 4)
            rows[follow_idx]['high'] = round(entry_close + 0.25, 4)
        else:
            winners += 1
            rows[follow_idx]['low'] = round(entry_close - 0.1, 4)
            rows[follow_idx]['high'] = round(entry_close + 1.1, 4)
    return pd.DataFrame(rows)


def _positive_expectancy_strategy(df, capital: float, risk_pct: float, rr_ratio: float, config=None):
    del capital, risk_pct, rr_ratio, config
    trades: list[dict[str, object]] = []
    for trade_index in range(120):
        row = df.iloc[trade_index * 2]
        entry = float(row['close'])
        trades.append(
            {
                'timestamp': row['timestamp'],
                'side': 'BUY',
                'entry': entry,
                'stop_loss': round(entry - 0.6, 4),
                'target': round(entry + 0.8, 4),
                'strategy': 'POSITIVE_EDGE',
                'reason': 'structured_test_setup',
                'score': 6.0,
                'quantity': 1,
            }
        )
    return trades


def _risk_rule_strategy(df, capital: float, risk_pct: float, rr_ratio: float, config=None):
    del capital, risk_pct, rr_ratio, config
    entry_one = float(df.iloc[0]['close'])
    entry_two = float(df.iloc[1]['close'])
    return [
        {
            'timestamp': df.iloc[0]['timestamp'],
            'side': 'BUY',
            'entry': entry_one,
            'stop_loss': round(entry_one - 0.6, 4),
            'target': round(entry_one + 0.8, 4),
            'strategy': 'RISK_RULES',
            'reason': 'first',
            'score': 5.0,
            'quantity': 1,
        },
        {
            'timestamp': df.iloc[0]['timestamp'],
            'side': 'BUY',
            'entry': entry_one,
            'stop_loss': round(entry_one - 0.6, 4),
            'target': round(entry_one + 0.8, 4),
            'strategy': 'RISK_RULES',
            'reason': 'duplicate_same_bar',
            'score': 5.0,
            'quantity': 1,
        },
        {
            'timestamp': df.iloc[1]['timestamp'],
            'side': 'BUY',
            'entry': entry_two,
            'stop_loss': round(entry_two - 0.6, 4),
            'target': round(entry_two + 0.8, 4),
            'strategy': 'RISK_RULES',
            'reason': 'same_day_limit',
            'score': 5.0,
            'quantity': 1,
        },
    ]


class TestBacktestEngine(unittest.TestCase):
    def test_run_backtest_reports_readiness_for_positive_sample(self):
        df = _build_market_frame(120)
        with TemporaryDirectory() as td:
            summary = run_backtest(
                df,
                _positive_expectancy_strategy,
                BacktestConfig(
                    capital=100000.0,
                    risk_pct=0.01,
                    rr_ratio=2.0,
                    trades_output=Path(td) / 'trades.csv',
                    summary_output=Path(td) / 'summary.csv',
                    validation_output=Path(td) / 'validation.csv',
                    strategy_name='POSITIVE_EDGE',
                    validation=BacktestValidationConfig(min_trades=100, target_trades=150, max_trades=200, min_profit_factor=1.1),
                ),
            )

        self.assertEqual(summary['total_trades'], 120)
        self.assertEqual(summary['positive_expectancy'], 'YES')
        self.assertEqual(summary['deployment_ready'], 'YES')
        self.assertGreater(float(summary['expectancy_per_trade']), 0.0)
        self.assertGreater(float(summary['win_rate']), 0.0)

    def test_run_backtest_enforces_duplicate_and_daily_trade_limits(self):
        df = _build_market_frame(3)
        with TemporaryDirectory() as td:
            summary = run_backtest(
                df,
                _risk_rule_strategy,
                BacktestConfig(
                    capital=100000.0,
                    risk_pct=0.01,
                    rr_ratio=2.0,
                    trades_output=Path(td) / 'trades.csv',
                    summary_output=Path(td) / 'summary.csv',
                    validation_output=Path(td) / 'validation.csv',
                    strategy_name='RISK_RULES',
                    max_trades_per_day=1,
                    duplicate_cooldown_minutes=5,
                ),
            )

        self.assertEqual(summary['closed_trades'], 1)
        self.assertEqual(summary['duplicate_rejections'], 1)
        self.assertEqual(summary['risk_rule_rejections'], 1)

    def test_summarize_trade_log_blocks_deployment_when_sample_too_small(self):
        rows = [
            {
                'strategy': 'PAPER_SAMPLE',
                'trade_status': 'CLOSED',
                'execution_status': 'CLOSED',
                'entry_time': '2026-03-01 09:15:00',
                'exit_time': '2026-03-01 09:20:00',
                'pnl': 50.0,
                'gross_pnl': 55.0,
                'trading_cost': 5.0,
                'rr_achieved': 1.2,
                'score': 6.0,
            }
            for _ in range(20)
        ]

        with TemporaryDirectory() as td:
            summary = summarize_trade_log(
                rows,
                capital=100000.0,
                strategy_name='PAPER_SAMPLE',
                summary_output=Path(td) / 'summary.csv',
                validation_output=Path(td) / 'validation.csv',
                validation=BacktestValidationConfig(min_trades=100, target_trades=150, max_trades=200),
            )

        self.assertEqual(summary['deployment_ready'], 'NO')
        self.assertIn('MIN_TRADES<100', summary['deployment_blockers'])


if __name__ == '__main__':
    unittest.main()
