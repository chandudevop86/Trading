import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from src.backtest_engine import BacktestConfig, BacktestValidationConfig, nifty_intraday_backtest_config, nifty_intraday_validation_config, run_backtest, summarize_trade_log


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


def _overtrade_strategy(df, capital: float, risk_pct: float, rr_ratio: float, config=None):
    del capital, risk_pct, rr_ratio, config
    trades: list[dict[str, object]] = []
    for trade_index in range(205):
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
                'reason': 'overtrade_sample',
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


def _single_trade_strategy(entry_idx: int = 0):
    def _strategy(df, capital: float, risk_pct: float, rr_ratio: float, config=None):
        del capital, risk_pct, rr_ratio, config
        row = df.iloc[entry_idx]
        entry = float(row['close'])
        return [
            {
                'timestamp': row['timestamp'],
                'side': 'BUY',
                'entry': entry,
                'stop_loss': round(entry - 0.6, 4),
                'target': round(entry + 0.8, 4),
                'strategy': 'LIFECYCLE_TEST',
                'reason': 'single_trade',
                'score': 5.0,
                'quantity': 1,
            }
        ]
    return _strategy


class TestBacktestEngine(unittest.TestCase):
    def test_nifty_intraday_preset_matches_expected_thresholds(self):
        validation = nifty_intraday_validation_config()
        config = nifty_intraday_backtest_config(capital=200000.0, strategy_name='NIFTY_TEST')

        self.assertEqual(validation.min_trades, 100)
        self.assertEqual(validation.target_trades, 150)
        self.assertEqual(validation.max_trades, 200)
        self.assertEqual(validation.min_profit_factor, 1.2)
        self.assertEqual(validation.min_win_rate, 38.0)
        self.assertEqual(validation.max_drawdown_pct, 15.0)
        self.assertEqual(config.max_trades_per_day, 3)
        self.assertEqual(config.duplicate_cooldown_minutes, 15)
        self.assertEqual(config.commission_per_trade, 20.0)
        self.assertEqual(config.slippage_bps, 3.0)
        self.assertEqual(config.validation.min_avg_rr, 1.0)

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

    def test_run_backtest_closes_trade_at_target_and_computes_pnl(self):
        df = _build_market_frame(3)
        with TemporaryDirectory() as td:
            trades_output = Path(td) / 'trades.csv'
            run_backtest(
                df,
                _single_trade_strategy(2),
                BacktestConfig(
                    capital=100000.0,
                    risk_pct=0.01,
                    rr_ratio=2.0,
                    trades_output=trades_output,
                    summary_output=Path(td) / 'summary.csv',
                    validation_output=Path(td) / 'validation.csv',
                    strategy_name='LIFECYCLE_TARGET',
                ),
            )
            rows = pd.read_csv(trades_output).to_dict(orient='records')

        self.assertEqual(rows[0]['trade_status'], 'closed')
        self.assertEqual(rows[0]['exit_reason'], 'TARGET')
        self.assertGreater(float(rows[0]['exit_price']), float(rows[0]['entry_price']))
        self.assertGreater(float(rows[0]['pnl']), 0.0)
        self.assertGreater(float(rows[0]['rr_achieved']), 0.0)

    def test_run_backtest_closes_trade_at_stop_loss_and_computes_negative_pnl(self):
        df = _build_market_frame(1)
        with TemporaryDirectory() as td:
            trades_output = Path(td) / 'trades.csv'
            run_backtest(
                df,
                _single_trade_strategy(0),
                BacktestConfig(
                    capital=100000.0,
                    risk_pct=0.01,
                    rr_ratio=2.0,
                    trades_output=trades_output,
                    summary_output=Path(td) / 'summary.csv',
                    validation_output=Path(td) / 'validation.csv',
                    strategy_name='LIFECYCLE_STOP',
                ),
            )
            rows = pd.read_csv(trades_output).to_dict(orient='records')

        self.assertEqual(rows[0]['trade_status'], 'closed')
        self.assertEqual(rows[0]['exit_reason'], 'STOP_LOSS')
        self.assertLess(float(rows[0]['exit_price']), float(rows[0]['entry_price']))
        self.assertLess(float(rows[0]['pnl']), 0.0)
        self.assertLess(float(rows[0]['rr_achieved']), 0.0)

    def test_run_backtest_closes_open_trade_at_end_of_data_when_enabled(self):
        df = pd.DataFrame(
            [
                {'timestamp': '2026-03-01 09:15:00', 'open': 100.0, 'high': 100.2, 'low': 99.8, 'close': 100.0, 'volume': 1000},
                {'timestamp': '2026-03-01 09:16:00', 'open': 100.0, 'high': 100.3, 'low': 99.9, 'close': 100.1, 'volume': 1001},
            ]
        )
        with TemporaryDirectory() as td:
            trades_output = Path(td) / 'trades.csv'
            run_backtest(
                df,
                _single_trade_strategy(0),
                BacktestConfig(
                    capital=100000.0,
                    risk_pct=0.01,
                    rr_ratio=2.0,
                    trades_output=trades_output,
                    summary_output=Path(td) / 'summary.csv',
                    validation_output=Path(td) / 'validation.csv',
                    strategy_name='LIFECYCLE_EOD',
                    close_open_positions_at_end=True,
                ),
            )
            rows = pd.read_csv(trades_output).to_dict(orient='records')

        self.assertEqual(rows[0]['trade_status'], 'closed')
        self.assertEqual(rows[0]['exit_reason'], 'END_OF_DATA')
        self.assertEqual(rows[0]['exit_time'], '2026-03-01 09:16:00')
        self.assertIn('pnl', rows[0])

    def test_run_backtest_blocks_deployment_when_trade_sample_exceeds_cap(self):
        df = _build_market_frame(205)
        with TemporaryDirectory() as td:
            summary = run_backtest(
                df,
                _overtrade_strategy,
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

        self.assertEqual(summary['sample_window_passed'], 'NO')
        self.assertEqual(summary['deployment_ready'], 'NO')
        self.assertIn('MAX_TRADES>200', summary['deployment_blockers'])
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




    def test_summarize_trade_log_exposes_phase3_clean_trade_fields(self):
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
                'score': 8.0,
                'validation_status': 'PASS',
                'rejection_reason': '',
                'strict_validation_score': 8,
            }
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

        self.assertEqual(summary['clean_trade_count'], 1)
        self.assertEqual(summary['clean_trade_metrics_only'], 'YES')
        self.assertIn('readiness_summary', summary)
        self.assertIn('edge_proof_status', summary)
        self.assertIn('promotion_status', summary)
