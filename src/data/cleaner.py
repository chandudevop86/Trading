from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

REQUIRED_OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
COLUMN_ALIASES = {
    "datetime": "datetime",
    "date_time": "datetime",
    "date": "date",
    "time": "time",
    "ts": "timestamp",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
    "vol": "volume",
    "adj close": "close",
    "adj_close": "close",
    "adjclose": "close",
}


class OHLCVValidationError(ValueError):
    """Raised when market data cannot be normalized into a safe OHLCV frame."""


@dataclass(slots=True)
class CleanerConfig:
    expected_interval_minutes: int | None = None
    stale_after_minutes: int | None = None
    timezone_name: str = "Asia/Kolkata"
    require_vwap: bool = True
    allow_vwap_compute: bool = True
    duplicate_policy: str = "drop_last"


def _normalize_column_name(column: object) -> str:
    return str(column).strip().lower().replace("-", "_").replace(" ", "_")


def _rename_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = [_normalize_column_name(column) for column in frame.columns]
    mapped = [COLUMN_ALIASES.get(column, column) for column in renamed]
    out = frame.copy()
    out.columns = mapped
    return out


def _build_timestamp_series(frame: pd.DataFrame) -> pd.Series | None:
    timestamp_series: pd.Series | None = None
    if "timestamp" in frame.columns:
        column = frame.loc[:, "timestamp"]
        timestamp_series = column.iloc[:, -1] if isinstance(column, pd.DataFrame) else column.copy()
    if "datetime" in frame.columns:
        datetime_series = frame["datetime"]
        timestamp_series = datetime_series if timestamp_series is None else timestamp_series.fillna(datetime_series)
    if "date" in frame.columns and "time" in frame.columns:
        combined = frame["date"].astype(str).str.strip() + " " + frame["time"].astype(str).str.strip()
        timestamp_series = combined if timestamp_series is None else timestamp_series.fillna(combined)
    elif "date" in frame.columns:
        date_series = frame["date"]
        timestamp_series = date_series if timestamp_series is None else timestamp_series.fillna(date_series)
    elif "time" in frame.columns:
        time_series = frame["time"]
        timestamp_series = time_series if timestamp_series is None else timestamp_series.fillna(time_series)
    return timestamp_series


def _parse_timestamp(value: object, timezone_name: str) -> pd.Timestamp:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        parsed = value
    elif isinstance(value, datetime):
        parsed = pd.Timestamp(value)
    else:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "nat", "none"}:
            return pd.NaT
        if text.isdigit():
            raw = int(text)
            unit = "ms" if len(text) >= 13 else "s"
            parsed = pd.to_datetime(raw, unit=unit, errors="coerce", utc=True)
        else:
            parsed = pd.to_datetime(text, errors="coerce", dayfirst=False, utc=False)
            if pd.isna(parsed):
                for fmt in (
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%d-%m-%Y %H:%M:%S",
                    "%d/%m/%Y %H:%M:%S",
                    "%Y/%m/%d %H:%M:%S",
                    "%Y-%m-%d",
                    "%d-%m-%Y",
                    "%d/%m/%Y",
                ):
                    try:
                        parsed = pd.Timestamp(datetime.strptime(text, fmt))
                        break
                    except ValueError:
                        continue
    if pd.isna(parsed):
        return pd.NaT
    if getattr(parsed, "tzinfo", None) is not None:
        return pd.Timestamp(parsed).tz_convert(ZoneInfo(timezone_name)).tz_localize(None)
    return pd.Timestamp(parsed)


def _compute_vwap(frame: pd.DataFrame) -> pd.Series:
    typical_price = (frame["high"] + frame["low"] + frame["close"]) / 3.0
    session_key = frame["timestamp"].dt.date
    cumulative_pv = (typical_price * frame["volume"]).groupby(session_key).cumsum()
    cumulative_volume = frame["volume"].groupby(session_key).cumsum().replace(0, pd.NA)
    return pd.to_numeric(cumulative_pv / cumulative_volume, errors="coerce").ffill().fillna(frame["close"])


def _validate_intervals(frame: pd.DataFrame, expected_interval_minutes: int | None) -> None:
    if expected_interval_minutes is None or len(frame) < 2:
        return
    expected = pd.Timedelta(minutes=int(expected_interval_minutes))
    diffs = frame["timestamp"].diff().dropna()
    if (diffs > expected).any():
        raise OHLCVValidationError(f"missing candle gap detected; expected interval {expected}")
    if (diffs != expected).any():
        raise OHLCVValidationError(f"inconsistent timestamp spacing detected; expected interval {expected}")


def _detect_stale_data(frame: pd.DataFrame, stale_after_minutes: int | None, timezone_name: str) -> None:
    if stale_after_minutes is None or frame.empty:
        return
    last_timestamp = pd.Timestamp(frame.iloc[-1]["timestamp"])
    now_local = datetime.now(ZoneInfo(timezone_name)).replace(tzinfo=None)
    if last_timestamp < now_local - timedelta(minutes=int(stale_after_minutes)):
        raise OHLCVValidationError(
            f"stale market data detected; latest candle {last_timestamp:%Y-%m-%d %H:%M:%S} older than {stale_after_minutes} minutes"
        )


def coerce_ohlcv(df: Any, config: CleanerConfig | None = None) -> pd.DataFrame:
    """Normalize raw OHLCV input into a strict, strategy-safe dataframe."""
    cfg = config or CleanerConfig()
    source = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if source.empty:
        raise OHLCVValidationError("empty dataframe")

    frame = _rename_columns(source)
    timestamp_series = _build_timestamp_series(frame)
    if timestamp_series is None:
        raise OHLCVValidationError("missing timestamp/date/time column")
    frame["timestamp"] = pd.Series(timestamp_series).apply(lambda value: _parse_timestamp(value, cfg.timezone_name))
    if frame["timestamp"].isna().any():
        raise OHLCVValidationError("timestamp parsing failed for one or more rows")

    missing = [column for column in REQUIRED_OHLCV_COLUMNS if column not in frame.columns]
    if missing:
        raise OHLCVValidationError(f"missing required columns: {missing}")

    keep_columns = [column for column in REQUIRED_OHLCV_COLUMNS if column in frame.columns]
    if "vwap" in frame.columns:
        keep_columns.append("vwap")
    frame = frame.loc[:, keep_columns].copy()

    for column in REQUIRED_OHLCV_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if frame[REQUIRED_OHLCV_COLUMNS[1:]].isna().any().any():
        raise OHLCVValidationError("numeric OHLCV coercion failed")

    frame = frame.sort_values("timestamp").reset_index(drop=True)
    duplicates_before = int(frame["timestamp"].duplicated().sum())
    if duplicates_before:
        if cfg.duplicate_policy == "drop_last":
            frame = frame.loc[~frame.duplicated(subset=["timestamp"], keep="last")].reset_index(drop=True)
        elif cfg.duplicate_policy == "drop_first":
            frame = frame.loc[~frame.duplicated(subset=["timestamp"], keep="first")].reset_index(drop=True)
        else:
            raise OHLCVValidationError("duplicate timestamps detected")

    invalid_ohlc = (
        (frame["high"] < frame["low"])
        | (frame["high"] < frame[["open", "close"]].max(axis=1))
        | (frame["low"] > frame[["open", "close"]].min(axis=1))
    )
    if invalid_ohlc.any():
        raise OHLCVValidationError("invalid OHLC structure detected")
    if ((frame[["open", "high", "low", "close"]] <= 0).any().any()) or (frame["volume"] < 0).any():
        raise OHLCVValidationError("prices must be positive and volume cannot be negative")

    _validate_intervals(frame, cfg.expected_interval_minutes)

    if "vwap" in frame.columns:
        frame["vwap"] = pd.to_numeric(frame["vwap"], errors="coerce")
    elif cfg.require_vwap and cfg.allow_vwap_compute:
        frame["vwap"] = _compute_vwap(frame)
    if cfg.require_vwap and ("vwap" not in frame.columns or frame["vwap"].isna().any()):
        raise OHLCVValidationError("VWAP is required but unavailable")

    _detect_stale_data(frame, cfg.stale_after_minutes, cfg.timezone_name)

    frame.attrs["cleaning_report"] = {
        "rows_in": int(len(source)),
        "rows_out": int(len(frame)),
        "duplicates_removed": duplicates_before,
        "latest_timestamp": str(frame.iloc[-1]["timestamp"]),
        "columns": list(frame.columns),
    }
    return frame.reset_index(drop=True)


__all__ = ["CleanerConfig", "OHLCVValidationError", "REQUIRED_OHLCV_COLUMNS", "coerce_ohlcv"]

