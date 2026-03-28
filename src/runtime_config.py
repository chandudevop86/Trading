from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from src.env_loader import load_aws_runtime_secrets, load_env_file


def _bool_env(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, '') or '').strip().lower()
    if not value:
        return default
    return value in {'1', 'true', 'yes', 'on'}


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or str(default))
    except Exception:
        return default


@dataclass(slots=True)
class AppPaths:
    data_dir: Path = Path(os.getenv('TRADING_DATA_DIR', 'data'))
    logs_dir: Path = Path(os.getenv('TRADING_LOG_DIR', 'logs'))

    @property
    def ohlcv_csv(self) -> Path:
        return self.data_dir / 'ohlcv.csv'

    @property
    def live_ohlcv_csv(self) -> Path:
        return self.data_dir / 'live_ohlcv.csv'

    @property
    def trades_csv(self) -> Path:
        return self.data_dir / 'trades.csv'

    @property
    def signal_output_csv(self) -> Path:
        return self.data_dir / 'output.csv'

    @property
    def executed_trades_csv(self) -> Path:
        return self.data_dir / 'executed_trades.csv'

    @property
    def paper_trading_log_csv(self) -> Path:
        return self.data_dir / 'paper_trading_logs_all.csv'

    @property
    def live_trading_log_csv(self) -> Path:
        return self.data_dir / 'live_trading_logs_all.csv'

    @property
    def paper_trade_summary_csv(self) -> Path:
        return self.data_dir / 'paper_trade_summary.csv'

    @property
    def order_history_csv(self) -> Path:
        return self.data_dir / 'order_history.csv'

    @property
    def paper_order_history_csv(self) -> Path:
        return self.data_dir / 'paper_order_history.csv'

    @property
    def backtest_trades_csv(self) -> Path:
        return self.data_dir / 'backtest_trades.csv'

    @property
    def backtest_summary_csv(self) -> Path:
        return self.data_dir / 'backtest_summary.csv'

    @property
    def backtest_results_csv(self) -> Path:
        return self.data_dir / 'backtest_results_all.csv'

    @property
    def backtest_validation_csv(self) -> Path:
        return self.data_dir / 'backtest_validation.csv'

    @property
    def strategy_ranking_csv(self) -> Path:
        return self.data_dir / 'strategy_expectancy_report.csv'

    @property
    def optimizer_report_csv(self) -> Path:
        return self.data_dir / 'strategy_optimizer_report.csv'

    @property
    def app_log(self) -> Path:
        return self.logs_dir / 'app.log'

    @property
    def execution_log(self) -> Path:
        return self.logs_dir / 'execution.log'

    @property
    def broker_log(self) -> Path:
        return self.logs_dir / 'broker.log'

    @property
    def errors_log(self) -> Path:
        return self.logs_dir / 'errors.log'

    @property
    def rejections_log(self) -> Path:
        return self.logs_dir / 'rejections.log'


@dataclass(slots=True)
class AwsConfig:
    region: str = os.getenv('AWS_REGION', 'ap-south-1')
    bucket: str = os.getenv('AWS_S3_BUCKET', '')
    prefix: str = os.getenv('AWS_S3_PREFIX', 'trading')
    enabled: bool = _bool_env('AWS_S3_ENABLED') or bool(os.getenv('AWS_S3_BUCKET', '').strip())


@dataclass(slots=True)
class BrokerRuntimeConfig:
    mode: str = os.getenv('TRADING_BROKER_MODE', 'PAPER').strip().upper() or 'PAPER'
    live_enabled: bool = _bool_env('LIVE_TRADING_ENABLED')
    max_trades_per_day: int = _int_env('MAX_TRADES_PER_DAY', 3)
    max_daily_loss: float = _float_env('MAX_DAILY_LOSS', 0.0)
    max_order_quantity: int = _int_env('MAX_ORDER_QUANTITY', 0)
    max_order_value: float = _float_env('MAX_ORDER_VALUE', 0.0)
    symbol_allowlist: tuple[str, ...] = tuple(part.strip().upper() for part in os.getenv('LIVE_SYMBOL_ALLOWLIST', '').split(',') if part.strip())


@dataclass(slots=True)
class TradingDaemonConfig:
    strategy: str = os.getenv('TRADING_STRATEGY', 'Breakout')
    symbol: str = os.getenv('TRADING_SYMBOL', '^NSEI')
    timeframe: str = os.getenv('TRADING_INTERVAL', '5m')
    period: str = os.getenv('TRADING_PERIOD', '5d').strip() or '5d'
    capital: float = _float_env('TRADING_CAPITAL', 100000.0)
    risk_pct: float = _float_env('TRADING_RISK_PCT', 1.0)
    rr_ratio: float = _float_env('TRADING_RR_RATIO', 2.0)
    mode: str = os.getenv('TRADING_MODE', 'Balanced')
    poll_interval_seconds: int = _int_env('TRADING_POLL_INTERVAL_SECONDS', 300)
    run_backtest: bool = _bool_env('TRADING_RUN_BACKTEST')


@dataclass(slots=True)
class TelegramRuntimeConfig:
    enabled: bool = _bool_env('TELEGRAM_NOTIFICATIONS_ENABLED')
    token: str = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
    chat_id: str = os.getenv('TELEGRAM_CHAT_ID', '').strip()
    notify_on_success: bool = _bool_env('TELEGRAM_NOTIFY_SUCCESS', True)
    notify_on_error: bool = _bool_env('TELEGRAM_NOTIFY_ERRORS', True)

    @property
    def configured(self) -> bool:
        return bool(self.enabled and self.token and self.chat_id)


@dataclass(slots=True)
class RuntimeConfig:
    app_name: str = os.getenv('APP_NAME', 'trading-system')
    environment: str = os.getenv('APP_ENV', 'local')
    paths: AppPaths = field(default_factory=AppPaths)
    aws: AwsConfig = field(default_factory=AwsConfig)
    broker: BrokerRuntimeConfig = field(default_factory=BrokerRuntimeConfig)
    daemon: TradingDaemonConfig = field(default_factory=TradingDaemonConfig)
    telegram: TelegramRuntimeConfig = field(default_factory=TelegramRuntimeConfig)

    @property
    def local_mode(self) -> bool:
        return str(self.environment or 'local').strip().lower() in {'local', 'dev', 'development', 'docker', 'test'}

    @classmethod
    def load(cls, env_path: str | Path = '.env') -> 'RuntimeConfig':
        load_env_file(env_path)
        load_aws_runtime_secrets()
        return cls()
