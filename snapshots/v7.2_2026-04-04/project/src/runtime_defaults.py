from __future__ import annotations

import os
from pathlib import Path

from src.runtime_config import RuntimeConfig

RUNTIME_CONFIG = RuntimeConfig.load()
APP_PATHS = RUNTIME_CONFIG.paths

OHLCV_OUTPUT = APP_PATHS.ohlcv_csv
LIVE_OHLCV_OUTPUT = APP_PATHS.live_ohlcv_csv
TRADES_OUTPUT = APP_PATHS.trades_csv
SIGNAL_OUTPUT = APP_PATHS.signal_output_csv
EXECUTED_TRADES_OUTPUT = APP_PATHS.executed_trades_csv
PAPER_LOG_OUTPUT = APP_PATHS.paper_trading_log_csv
LIVE_LOG_OUTPUT = APP_PATHS.live_trading_log_csv
PAPER_SUMMARY_OUTPUT = APP_PATHS.paper_trade_summary_csv
BACKTEST_TRADES_OUTPUT = APP_PATHS.backtest_trades_csv
BACKTEST_SUMMARY_OUTPUT = APP_PATHS.backtest_summary_csv
BACKTEST_RESULTS_OUTPUT = APP_PATHS.backtest_results_csv
BACKTEST_VALIDATION_OUTPUT = APP_PATHS.backtest_validation_csv
OPTIMIZER_OUTPUT = APP_PATHS.optimizer_report_csv
ORDER_HISTORY_OUTPUT = APP_PATHS.order_history_csv
PAPER_ORDER_HISTORY_OUTPUT = APP_PATHS.paper_order_history_csv
APP_LOG = APP_PATHS.app_log
BROKER_LOG = APP_PATHS.broker_log
EXECUTION_LOG = APP_PATHS.execution_log
REJECTIONS_LOG = APP_PATHS.rejections_log
ERRORS_LOG = APP_PATHS.errors_log

DEFAULT_SYMBOL = os.getenv('TRADING_SYMBOL', '^NSEI').strip() or '^NSEI'
DEFAULT_INTERVAL = os.getenv('TRADING_INTERVAL', '5m').strip() or '5m'
DEFAULT_PERIOD = os.getenv('TRADING_PERIOD', '5d').strip() or '5d'

TIMEFRAME_OPTIONS = ['1m', '5m', '15m', '30m', '1h', '1d']
STRATEGY_OPTIONS = ['Breakout', 'Demand Supply (Retest)', 'Indicator', 'One Trade/Day', 'MTF 5m', 'AMD + FVG + Supply/Demand']
BROKER_OPTIONS = ['Paper', 'Dhan Live']
MODE_OPTIONS = ['Conservative', 'Balanced', 'Aggressive']


def runtime_output_paths() -> list[Path]:
    return [
        OHLCV_OUTPUT,
        LIVE_OHLCV_OUTPUT,
        TRADES_OUTPUT,
        SIGNAL_OUTPUT,
        EXECUTED_TRADES_OUTPUT,
        PAPER_LOG_OUTPUT,
        LIVE_LOG_OUTPUT,
        BACKTEST_TRADES_OUTPUT,
        BACKTEST_SUMMARY_OUTPUT,
        BACKTEST_RESULTS_OUTPUT,
        ORDER_HISTORY_OUTPUT,
        PAPER_ORDER_HISTORY_OUTPUT,
    ]


def runtime_log_paths() -> list[Path]:
    return [APP_LOG, BROKER_LOG, EXECUTION_LOG, REJECTIONS_LOG, ERRORS_LOG]

