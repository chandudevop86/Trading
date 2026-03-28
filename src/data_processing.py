from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

REQUIRED_OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
DEFAULT_INTERNAL_TIMEZONE = "Asia/Kolkata"

_COLUMN_ALIASES = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "adj_close": "close",
    "adjclose": "close",
    "volume": "volume",
    "vol": "volume",
}


@dataclass(slots=True)
class ProcessingReport:
    rows_in: int = 0
    rows_out: int = 0
    duplicates_removed: int = 0
    invalid_rows_removed: int = 0
    missing_rows_removed: int = 0
    zero_range_rows_removed: int = 0
    interval_warnings: list[str] = field(default_factory=list)
    columns_normalized: list[str] = field(default_factory=list)
    final_columns: list[str] = field(default_factory=list)
    rejection_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _coerce_dataframe(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data.copy()
    if isinstance(data, (str, Path)):
        path = Path(data)
        if path.exists() and path.is_file():
            return pd.read_csv(path)
        raise FileNotFoundError(f"OHLCV input path not found: {path}")
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, tuple):
        return pd.DataFrame(list(data))
    if data is None:
        return pd.DataFrame()
    return pd.DataFrame(data)


def _normalize_column_name(column: object) -> str:
    if isinstance(column, tuple):
        parts = [str(part).strip() for part in column if str(part).strip()]
        text = "_".join(parts)
    else:
        text = str(column).strip()
    return text.lower().replace(" ", "_").replace("-", "_")


def _collapse_duplicate_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if not frame.columns.duplicated().any():
        return frame
    collapsed: dict[str, pd.Series] = {}
    for column in dict.fromkeys(frame.columns):
        duplicate_frame = frame.loc[:, frame.columns == column]
        collapsed[column] = duplicate_frame.apply(
            lambda row: next((value for value in row if pd.notna(value) and str(value).strip() != ""), None),
            axis=1,
        )
    return pd.DataFrame(collapsed)


def _build_timestamp_series(frame: pd.DataFrame) -> pd.Series | None:
    timestamp_series: pd.Series | None = None
    if "timestamp" in frame.columns:
        timestamp_series = frame["timestamp"]
    if "datetime" in frame.columns:
        candidate = frame["datetime"]
        timestamp_series = candidate if timestamp_series is None else timestamp_series.where(timestamp_series.notna(), candidate)
    if "date" in frame.columns and "time" in frame.columns:
        date_text = frame["date"].astype(str).str.strip()
        time_text = frame["time"].astype(str).str.strip()
        candidate = date_text + " " + time_text
        timestamp_series = candidate if timestamp_series is None else timestamp_series.where(timestamp_series.notna(), candidate)
    if "date" in frame.columns:
        candidate = frame["date"]
        timestamp_series = candidate if timestamp_series is None else timestamp_series.where(timestamp_series.notna(), candidate)
    if "time" in frame.columns:
        candidate = frame["time"]
        timestamp_series = candidate if timestamp_series is None else timestamp_series.where(timestamp_series.notna(), candidate)
    return timestamp_series


def _parse_timestamp_value(value: object, *, timezone_name: str = DEFAULT_INTERNAL_TIMEZONE) -> pd.Timestamp:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NaT
    if isinstance(value, (int, float)) and not pd.isna(value):
        numeric_value = int(value)
        unit = "ms" if abs(numeric_value) >= 10**12 else "s"
        parsed = pd.to_datetime(numeric_value, unit=unit, errors="coerce", utc=True)
    else:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "nat", "none"}:
            return pd.NaT
        if text.isdigit():
            numeric_value = int(text)
            unit = "ms" if len(text) >= 13 else "s"
            parsed = pd.to_datetime(numeric_value, unit=unit, errors="coerce", utc=True)
        else:
            parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return pd.NaT
    if getattr(parsed, "tzinfo", None) is not None:
        try:
            parsed = parsed.tz_convert(ZoneInfo(timezone_name)).tz_localize(None)
        except Exception:
            parsed = parsed.tz_localize(None)
    return pd.Timestamp(parsed)


def normalize_ohlcv_schema(data: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = _coerce_dataframe(data)
    report = ProcessingReport(rows_in=len(frame))
    if frame.empty:
        report.final_columns = list(REQUIRED_OHLCV_COLUMNS)
        return pd.DataFrame(columns=REQUIRED_OHLCV_COLUMNS), report.to_dict()

    original_columns = list(frame.columns)
    normalized_columns = [_normalize_column_name(column) for column in frame.columns]
    frame.columns = normalized_columns
    renamed_columns: list[str] = []
    for original, normalized in zip(original_columns, normalized_columns):
        if str(original) != normalized:
            renamed_columns.append(f"{original}->{normalized}")

    alias_map = {column: _COLUMN_ALIASES.get(column, column) for column in frame.columns}
    for original, renamed in alias_map.items():
        if original != renamed:
            renamed_columns.append(f"{original}->{renamed}")
    frame = frame.rename(columns=alias_map)
    frame = _collapse_duplicate_columns(frame)

    timestamp_series = _build_timestamp_series(frame)
    if timestamp_series is not None:
        frame["timestamp"] = timestamp_series

    missing_columns = [column for column in REQUIRED_OHLCV_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    frame = frame.loc[:, REQUIRED_OHLCV_COLUMNS].copy()
    frame["timestamp"] = frame["timestamp"].apply(_parse_timestamp_value)
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["volume"] = frame["volume"].fillna(0.0)

    report.columns_normalized = renamed_columns
    report.final_columns = list(frame.columns)
    return frame, report.to_dict()


def validate_ohlcv_rows(frame: pd.DataFrame, *, drop_zero_range: bool = False) -> tuple[pd.DataFrame, dict[str, Any]]:
    validated = frame.copy()
    report = ProcessingReport(rows_in=len(validated))
    if validated.empty:
        report.final_columns = list(validated.columns)
        return validated, report.to_dict()

    missing_mask = validated[["timestamp", "open", "high", "low", "close"]].isna().any(axis=1)
    invalid_timestamp_mask = validated["timestamp"].isna()
    negative_mask = (validated[["open", "high", "low", "close", "volume"]] < 0).any(axis=1)
    ordering_mask = ~(
        (validated["high"] >= validated["open"])
        & (validated["high"] >= validated["close"])
        & (validated["low"] <= validated["open"])
        & (validated["low"] <= validated["close"])
        & (validated["high"] >= validated["low"])
    )
    zero_range_mask = (validated["high"] == validated["low"]) if drop_zero_range else pd.Series(False, index=validated.index)

    duplicate_mask = validated.duplicated(subset=["timestamp"], keep="last")

    report.rejection_counts = {
        "missing_required": int(missing_mask.sum()),
        "invalid_timestamp": int(invalid_timestamp_mask.sum()),
        "invalid_price": int(ordering_mask.sum()),
        "negative_values": int(negative_mask.sum()),
        "duplicates": int(duplicate_mask.sum()),
        "zero_range": int(zero_range_mask.sum()),
    }
    report.missing_rows_removed = int(missing_mask.sum())
    report.duplicates_removed = int(duplicate_mask.sum())
    report.zero_range_rows_removed = int(zero_range_mask.sum())

    invalid_mask = ordering_mask | negative_mask | zero_range_mask
    report.invalid_rows_removed = int(invalid_mask.sum())

    removal_mask = missing_mask | invalid_mask
    validated = validated.loc[~removal_mask].copy()
    validated = validated.loc[~validated.duplicated(subset=["timestamp"], keep="last")].copy()
    validated = validated.sort_values("timestamp").reset_index(drop=True)
    report.rows_out = len(validated)
    report.final_columns = list(validated.columns)
    return validated, report.to_dict()


def classify_intraday_session(timestamp: object) -> str:
    parsed = _parse_timestamp_value(timestamp)
    if pd.isna(parsed):
        return "unknown"
    hhmm = parsed.strftime("%H:%M")
    if "09:15" <= hhmm <= "09:29":
        return "open"
    if "09:30" <= hhmm <= "10:59":
        return "morning"
    if "11:00" <= hhmm <= "13:29":
        return "midday"
    if "13:30" <= hhmm <= "14:59":
        return "afternoon"
    if "15:00" <= hhmm <= "15:30":
        return "close"
    return "offhours"


def _build_interval_warnings(frame: pd.DataFrame, expected_interval_minutes: int) -> list[str]:
    if frame.empty or len(frame) < 2:
        return []
    warnings: list[str] = []
    gap_rows = frame.loc[frame["gap_flag"]]
    for row in gap_rows.head(10).itertuples(index=False):
        warnings.append(
            f"Gap after {pd.Timestamp(row.timestamp).strftime('%Y-%m-%d %H:%M:%S')} interval={float(row.interval_minutes):.2f}m"
        )
    irregular_rows = frame.loc[~frame["interval_valid"] & ~frame["gap_flag"]]
    for row in irregular_rows.head(10).itertuples(index=False):
        warnings.append(
            f"Unexpected interval at {pd.Timestamp(row.timestamp).strftime('%Y-%m-%d %H:%M:%S')} interval={float(row.interval_minutes):.2f}m expected={expected_interval_minutes}m"
        )
    return warnings


def enrich_ohlcv_metrics(frame: pd.DataFrame, *, expected_interval_minutes: int = 5) -> pd.DataFrame:
    if frame.empty:
        extra_columns = [
            "session_date", "session_day", "session_time", "time_block",
            "range", "body", "body_ratio", "upper_wick", "lower_wick",
            "avg_range_20", "avg_volume_20", "avg_range", "avg_volume", "volume_ratio", "range_ratio",
            "vwap", "above_vwap", "day_bias", "is_bullish", "is_bearish",
            "interval_minutes", "interval_valid", "gap_flag",
            "opening_range_high", "opening_range_low", "opening_high", "opening_low", "opening_range",
            "opening_range_breakout_up", "opening_range_breakout_down",
            "intraday_high_so_far", "intraday_low_so_far",
            "previous_day_high", "previous_day_low", "previous_day_close", "pdh", "pdl",
        ]
        return pd.DataFrame(columns=list(frame.columns) + extra_columns)

    out = frame.copy()
    out["session_date"] = out["timestamp"].dt.strftime("%Y-%m-%d")
    out["session_day"] = out["session_date"]
    out["session_time"] = out["timestamp"].dt.strftime("%H:%M:%S")
    out["time_block"] = out["timestamp"].apply(classify_intraday_session)

    out["range"] = (out["high"] - out["low"]).clip(lower=0.0)
    out["body"] = (out["close"] - out["open"]).abs()
    out["body_ratio"] = (out["body"] / out["range"].replace(0, pd.NA)).fillna(0.0)
    out["upper_wick"] = (out["high"] - out[["open", "close"]].max(axis=1)).clip(lower=0.0)
    out["lower_wick"] = (out[["open", "close"]].min(axis=1) - out["low"]).clip(lower=0.0)

    out["avg_range_20"] = out.groupby("session_date")["range"].transform(lambda series: series.rolling(20, min_periods=1).mean())
    out["avg_volume_20"] = out.groupby("session_date")["volume"].transform(lambda series: series.rolling(20, min_periods=1).mean())
    out["avg_range"] = out["avg_range_20"]
    out["avg_volume"] = out["avg_volume_20"]
    safe_avg_volume = out["avg_volume_20"].where(out["avg_volume_20"] > 0)
    safe_avg_range = out["avg_range_20"].where(out["avg_range_20"] > 0)
    out["volume_ratio"] = (out["volume"] / safe_avg_volume).fillna(0.0).astype(float)
    out["range_ratio"] = (out["range"] / safe_avg_range).fillna(0.0).astype(float)

    typical_price = (out["high"] + out["low"] + out["close"]) / 3.0
    cumulative_pv = (typical_price * out["volume"]).groupby(out["session_date"]).cumsum()
    cumulative_volume = out["volume"].groupby(out["session_date"]).cumsum()
    safe_cumulative_volume = cumulative_volume.where(cumulative_volume > 0)
    out["vwap"] = (cumulative_pv / safe_cumulative_volume).ffill()
    out["vwap"] = out["vwap"].where(out["vwap"].notna(), out["close"]).astype(float)
    out["above_vwap"] = out["close"] >= out["vwap"]
    out["day_bias"] = out["above_vwap"].map({True: "bullish", False: "bearish"})

    out["is_bullish"] = out["close"] > out["open"]
    out["is_bearish"] = out["close"] < out["open"]

    out["interval_minutes"] = out.groupby("session_date")["timestamp"].diff().dt.total_seconds().div(60.0)
    out["interval_minutes"] = out["interval_minutes"].fillna(float(expected_interval_minutes)).round(2)
    out["interval_valid"] = out["interval_minutes"].between(float(expected_interval_minutes) - 0.1, float(expected_interval_minutes) + 0.1)
    first_index = out.groupby("session_date", sort=False).head(1).index
    out.loc[first_index, "interval_valid"] = True
    out["gap_flag"] = out["interval_minutes"] > float(expected_interval_minutes) * 1.5

    opening_window = out["session_time"].between("09:15:00", "09:29:59")
    opening_high = out["high"].where(opening_window).groupby(out["session_date"]).transform("max")
    opening_low = out["low"].where(opening_window).groupby(out["session_date"]).transform("min")
    out["opening_range_high"] = opening_high.fillna(out.groupby("session_date")["high"].transform("first"))
    out["opening_range_low"] = opening_low.fillna(out.groupby("session_date")["low"].transform("first"))
    out["opening_high"] = out["opening_range_high"]
    out["opening_low"] = out["opening_range_low"]
    out["opening_range"] = (out["opening_range_high"] - out["opening_range_low"]).clip(lower=0.0)
    out["opening_range_breakout_up"] = out["close"] > out["opening_range_high"]
    out["opening_range_breakout_down"] = out["close"] < out["opening_range_low"]

    out["intraday_high_so_far"] = out.groupby("session_date")["high"].cummax()
    out["intraday_low_so_far"] = out.groupby("session_date")["low"].cummin()

    daily = out.groupby("session_date", sort=True).agg(
        day_high=("high", "max"),
        day_low=("low", "min"),
        day_close=("close", "last"),
    ).reset_index()
    daily["previous_day_high"] = daily["day_high"].shift(1)
    daily["previous_day_low"] = daily["day_low"].shift(1)
    daily["previous_day_close"] = daily["day_close"].shift(1)
    out = out.merge(
        daily[["session_date", "previous_day_high", "previous_day_low", "previous_day_close"]],
        on="session_date",
        how="left",
    )
    out["pdh"] = out["previous_day_high"]
    out["pdl"] = out["previous_day_low"]
    return out


def build_processing_report(report: dict[str, Any], processed: pd.DataFrame) -> dict[str, Any]:
    out = dict(report)
    out["rows_out"] = int(len(processed))
    out["final_columns"] = list(processed.columns)
    return out


def load_and_process_ohlcv(
    data: Any,
    *,
    include_derived: bool = True,
    expected_interval_minutes: int = 5,
    drop_zero_range: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    normalized, normalize_report = normalize_ohlcv_schema(data)
    validated, validation_report = validate_ohlcv_rows(normalized, drop_zero_range=drop_zero_range)
    processed = enrich_ohlcv_metrics(validated, expected_interval_minutes=expected_interval_minutes) if include_derived else validated

    report = ProcessingReport(
        rows_in=int(normalize_report.get("rows_in", 0)),
        rows_out=int(len(processed)),
        duplicates_removed=int(validation_report.get("duplicates_removed", 0)),
        invalid_rows_removed=int(validation_report.get("invalid_rows_removed", 0)),
        missing_rows_removed=int(validation_report.get("missing_rows_removed", 0)),
        zero_range_rows_removed=int(validation_report.get("zero_range_rows_removed", 0)),
        columns_normalized=list(normalize_report.get("columns_normalized", [])),
        final_columns=list(processed.columns),
        rejection_counts=dict(validation_report.get("rejection_counts", {})),
        interval_warnings=_build_interval_warnings(processed, expected_interval_minutes) if include_derived else [],
    )
    return processed, report.to_dict()


__all__ = [
    "DEFAULT_INTERNAL_TIMEZONE",
    "REQUIRED_OHLCV_COLUMNS",
    "build_processing_report",
    "classify_intraday_session",
    "enrich_ohlcv_metrics",
    "load_and_process_ohlcv",
    "normalize_ohlcv_schema",
    "validate_ohlcv_rows",
]

