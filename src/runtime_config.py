from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from src.env_loader import load_aws_runtime_secrets, load_env_file


@dataclass(slots=True)
class AppPaths:
    data_dir: Path = Path(os.getenv('TRADING_DATA_DIR', 'data'))
    logs_dir: Path = Path(os.getenv('TRADING_LOG_DIR', 'logs'))

    @property
    def ohlcv_csv(self) -> Path:
        return self.data_dir / 'ohlcv.csv'

    @property
    def trades_csv(self) -> Path:
        return self.data_dir / 'trades.csv'

    @property
    def executed_trades_csv(self) -> Path:
        return self.data_dir / 'executed_trades.csv'

    @property
    def order_history_csv(self) -> Path:
        return self.data_dir / 'order_history.csv'

    @property
    def backtest_trades_csv(self) -> Path:
        return self.data_dir / 'backtest_trades.csv'

    @property
    def backtest_summary_csv(self) -> Path:
        return self.data_dir / 'backtest_summary.csv'

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
    enabled: bool = os.getenv('AWS_S3_ENABLED', '').strip().lower() in {'1', 'true', 'yes', 'on'} or bool(os.getenv('AWS_S3_BUCKET', '').strip())


@dataclass(slots=True)
class BrokerRuntimeConfig:
    mode: str = os.getenv('TRADING_BROKER_MODE', 'PAPER').strip().upper() or 'PAPER'
    live_enabled: bool = os.getenv('LIVE_TRADING_ENABLED', '').strip().lower() in {'1', 'true', 'yes', 'on'}
    max_trades_per_day: int = int(os.getenv('MAX_TRADES_PER_DAY', '3') or '3')
    max_daily_loss: float = float(os.getenv('MAX_DAILY_LOSS', '0') or '0')
    max_order_quantity: int = int(os.getenv('MAX_ORDER_QUANTITY', '0') or '0')
    max_order_value: float = float(os.getenv('MAX_ORDER_VALUE', '0') or '0')
    symbol_allowlist: tuple[str, ...] = tuple(part.strip().upper() for part in os.getenv('LIVE_SYMBOL_ALLOWLIST', '').split(',') if part.strip())


@dataclass(slots=True)
class TradingDaemonConfig:
    strategy: str = os.getenv('TRADING_STRATEGY', 'Breakout')
    symbol: str = os.getenv('TRADING_SYMBOL', '^NSEI')
    timeframe: str = os.getenv('TRADING_INTERVAL', '5m')
    capital: float = float(os.getenv('TRADING_CAPITAL', '100000') or '100000')
    risk_pct: float = float(os.getenv('TRADING_RISK_PCT', '1.0') or '1.0')
    rr_ratio: float = float(os.getenv('TRADING_RR_RATIO', '2.0') or '2.0')
    mode: str = os.getenv('TRADING_MODE', 'Balanced')
    poll_interval_seconds: int = int(os.getenv('TRADING_POLL_INTERVAL_SECONDS', '300') or '300')
    run_backtest: bool = os.getenv('TRADING_RUN_BACKTEST', '').strip().lower() in {'1', 'true', 'yes', 'on'}


@dataclass(slots=True)
class RuntimeConfig:
    app_name: str = os.getenv('APP_NAME', 'trading-system')
    environment: str = os.getenv('APP_ENV', 'production')
    paths: AppPaths = field(default_factory=AppPaths)
    aws: AwsConfig = field(default_factory=AwsConfig)
    broker: BrokerRuntimeConfig = field(default_factory=BrokerRuntimeConfig)
    daemon: TradingDaemonConfig = field(default_factory=TradingDaemonConfig)

    @classmethod
    def load(cls, env_path: str | Path = '.env') -> 'RuntimeConfig':
        load_env_file(env_path)
        return cls()


