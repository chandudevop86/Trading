from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PACKAGE_ROOT / 'data'
CACHE_DIR = DATA_DIR / 'cache'
REPORTS_DIR = DATA_DIR / 'reports'
CANDLES_CACHE_DIR = CACHE_DIR / 'candles'
PAPER_LOG_PATH = DATA_DIR / 'paper_trading_logs_all.csv'
LIVE_LOG_PATH = DATA_DIR / 'live_trading_logs_all.csv'
SECURITY_MAP_PATH = DATA_DIR / 'dhan_security_map.csv'
LIVE_OHLCV_PATH = DATA_DIR / 'live_ohlcv.csv'
DATABASE_PATH = DATA_DIR / 'vinayak.db'

WORKSPACE_DATA_DIR = Path('app') / 'vinayak' / 'data'
WORKSPACE_CACHE_DIR = WORKSPACE_DATA_DIR / 'cache'
WORKSPACE_REPORTS_DIR = WORKSPACE_DATA_DIR / 'reports'
WORKSPACE_CANDLES_CACHE_DIR = WORKSPACE_CACHE_DIR / 'candles'
WORKSPACE_PAPER_LOG_PATH = WORKSPACE_DATA_DIR / 'paper_trading_logs_all.csv'
WORKSPACE_LIVE_LOG_PATH = WORKSPACE_DATA_DIR / 'live_trading_logs_all.csv'
WORKSPACE_SECURITY_MAP_PATH = WORKSPACE_DATA_DIR / 'dhan_security_map.csv'
WORKSPACE_LIVE_OHLCV_PATH = WORKSPACE_DATA_DIR / 'live_ohlcv.csv'
WORKSPACE_DATABASE_PATH = WORKSPACE_DATA_DIR / 'vinayak.db'


def workspace_path_text(path: Path) -> str:
    return path.as_posix()
