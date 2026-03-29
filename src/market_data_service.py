from __future__ import annotations

from typing import Any

import pandas as pd

from src.breakout_bot import Candle
from src.nifty_data_integration import fetch_nifty_ohlcv_frame
from src.trading_core import prepare_trading_data


def period_for_interval(interval: str, default_period: str = '5d') -> str:
    mapping = {
        '1m': '7d',
        '5m': '60d',
        '15m': '60d',
        '30m': '60d',
        '1h': '730d',
        '1d': '1y',
    }
    return mapping.get(str(interval or '').strip(), default_period)


def dataframe_to_candles(df: pd.DataFrame) -> list[Candle]:
    prepared = prepare_trading_data(df)
    candles: list[Candle] = []
    for row in prepared.itertuples(index=False):
        candles.append(
            Candle(
                timestamp=pd.Timestamp(row.timestamp).to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
            )
        )
    return candles


def fetch_ohlcv_data(symbol: str, interval: str = '5m', period: str = '5d') -> pd.DataFrame:
    return fetch_nifty_ohlcv_frame(symbol, interval=interval, period=period)


def paper_candle_rows(candles: pd.DataFrame | list[dict[str, Any]]) -> list[dict[str, object]]:
    prepared = prepare_trading_data(candles)
    return prepared.to_dict(orient='records')
