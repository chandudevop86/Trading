from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_OHLCV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")
COLUMN_ALIASES = {
    "time": "timestamp",
    "date": "timestamp",
    "datetime": "timestamp",
    "date_time": "timestamp",
    "ts": "timestamp",
    "o": "open",
    "open_price": "open",
    "h": "high",
    "high_price": "high",
    "l": "low",
    "low_price": "low",
    "c": "close",
    "close_price": "close",
    "v": "volume",
    "vol": "volume",
    "qty": "volume",
}
HARD_FAIL_REASONS = {
    "no_structure_break",
    "stale_zone",
    "weak_retest_reaction",
    "deep_zone_penetration",
    "no_vwap_alignment",
    "weak_structure_clarity",
    "bad_rr",
    "duplicate_setup_context",
    "breakout_chase_entry",
}
FAIL_REASON_TEXT = {
    "weak_zone_departure": "departure was weak and lacked displacement",
    "no_structure_break": "the zone did not break meaningful prior structure",
    "dirty_zone_base": "the zone base was noisy or too wide",
    "stale_zone": "the zone was no longer fresh",
    "late_retest": "the retest arrived too late after zone creation",
    "weak_retest_reaction": "the 5m rejection was too weak",
    "deep_zone_penetration": "price penetrated too deep into the zone before reversing",
    "no_vwap_alignment": "VWAP logic was not aligned with the trade direction",
    "bad_session_timing": "the setup appeared in a blocked intraday timing window",
    "low_intraday_volatility": "intraday volatility was too low for a clean move",
    "chop_market_fail": "the market was overlapping and directionless",
    "trend_misaligned": "trend alignment was against the trade direction",
    "bad_rr": "reward-to-risk was below the required minimum",
    "oversized_zone": "the zone was too wide for intraday execution",
    "no_liquidity_event": "the setup lacked the required liquidity sweep",
    "no_imbalance_support": "the originating zone lacked imbalance support",
    "weak_structure_clarity": "price structure around the retest was messy",
    "duplicate_setup_context": "the same zone had already been used",
    "breakout_chase_entry": "entry was a breakout chase instead of a qualified retest",
    "low_validation_score": "the weighted validation score was below threshold",
    "missing_vwap": "VWAP data was unavailable when required",
}


@dataclass(slots=True)
class StrictValidationConfig:
    min_departure_atr: float = 1.5
    min_impulsive_candles: int = 2
    max_base_candles: int = 6
    max_touch_count: int = 1
    max_penetration_pct: float = 0.70
    min_rejection_score: float = 6.0
    min_structure_score: float = 6.0
    min_rr: float = 2.0
    max_zone_width_atr: float = 1.2
    allowed_start_time: str = "09:25"
    cutoff_time: str = "14:45"
    block_lunch_window: bool = True
    lunch_start_time: str = "12:00"
    lunch_end_time: str = "13:15"
    require_vwap_alignment: bool = True
    require_sweep: bool = False
    max_retest_delay_candles: int = 24
    min_validation_score: float = 7.0
    atr_window: int = 14
    volatility_lookback: int = 20
    chop_overlap_threshold: float = 0.62
    chop_score_threshold: float = 6.0
    adx_minimum: float = 18.0
    base_wick_ratio_threshold: float = 1.4
    imbalance_threshold: float = 5.0
    retest_near_zone_buffer_pct: float = 0.10
    stop_buffer_pct_of_zone: float = 0.05
    cooldown_bars: int = 3
    zone_timeframe: str = "15m"
    entry_timeframe: str = "5m"
    zone_timeframe_minutes: int = 15
    entry_timeframe_minutes: int = 5
    allow_column_aliases: bool = True
    reject_duplicate_timestamps: bool = True
    reject_missing_candles: bool = True
    require_strict_frequency: bool = True
    allow_vwap_computation: bool = True
    logger_csv_path: str | None = None


@dataclass(slots=True)
class RejectedTradesLogger:
    rows: list[dict[str, Any]] = field(default_factory=list)

    def log(self, result: dict[str, Any]) -> None:
        if str(result.get("status", "")).upper() != "FAIL":
            return
        metrics = dict(result.get("metrics", {}) or {})
        self.rows.append(
            {
                "timestamp": str(result.get("timestamp", "")),
                "symbol": str(result.get("symbol", "")),
                "zone_id": str(result.get("zone_id", "")),
                "zone_type": str(result.get("zone_type", "")),
                "status": str(result.get("status", "")),
                "validation_score": _round(result.get("validation_score", 0.0)),
                "fail_reasons": "|".join(str(reason) for reason in result.get("fail_reasons", []) or []),
                "rr_ratio": _round(metrics.get("rr_ratio", 0.0)),
                "rejection_score": _round(metrics.get("rejection_score", 0.0)),
                "structure_score": _round(metrics.get("structure_score", 0.0)),
                "touch_count": int(_safe_float(metrics.get("touch_count", 0.0))),
                "vwap_alignment": bool(metrics.get("vwap_alignment", False)),
            }
        )

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)

    def write_csv(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self.to_frame().to_csv(target, index=False)
        return target

    def clear(self) -> None:
        self.rows.clear()


_DEFAULT_LOGGER = RejectedTradesLogger()


def get_rejected_trades_frame() -> pd.DataFrame:
    return _DEFAULT_LOGGER.to_frame()


def clear_rejected_trades_log() -> None:
    _DEFAULT_LOGGER.clear()


def write_rejected_trades_csv(path: str | Path) -> Path:
    return _DEFAULT_LOGGER.write_csv(path)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _round(value: Any, digits: int = 4) -> float:
    return round(_safe_float(value), digits)


def _clamp(value: float, low: float = 0.0, high: float = 10.0) -> float:
    return max(low, min(high, float(value)))


def _time_to_minutes(value: str) -> int:
    hour, minute = [int(part) for part in str(value).split(":", 1)]
    return hour * 60 + minute


def _normalize_frame(
    df: pd.DataFrame,
    *,
    require_vwap: bool = False,
    allow_vwap_compute: bool = True,
    expected_interval_minutes: int | None = None,
    config: StrictValidationConfig | None = None,
) -> pd.DataFrame:
    cfg = config or StrictValidationConfig()
    if not isinstance(df, pd.DataFrame):
        raise TypeError("expected pandas DataFrame")
    if df.empty:
        raise ValueError("empty dataframe")
    frame = df.copy()
    raw_columns = [str(column).strip().lower() for column in frame.columns]
    frame.columns = [COLUMN_ALIASES.get(column, column) if cfg.allow_column_aliases else column for column in raw_columns]
    missing = [column for column in REQUIRED_OHLCV_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    keep_columns = list(REQUIRED_OHLCV_COLUMNS) + [column for column in ("vwap", "symbol") if column in frame.columns]
    frame = frame.loc[:, keep_columns].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    if frame["timestamp"].isna().any():
        raise ValueError("timestamp contains invalid values")
    if cfg.reject_duplicate_timestamps and frame["timestamp"].duplicated().any():
        raise ValueError("duplicate timestamps detected")
    for column in REQUIRED_OHLCV_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if frame[list(REQUIRED_OHLCV_COLUMNS[1:])].isna().any().any():
        raise ValueError("invalid numeric OHLCV values")
    if not frame["timestamp"].is_monotonic_increasing:
        raise ValueError("timestamps must be sorted ascending")
    if ((frame["high"] < frame["low"]) | (frame["open"] <= 0) | (frame["high"] <= 0) | (frame["low"] <= 0) | (frame["close"] <= 0)).any():
        raise ValueError("invalid OHLC price relationships")
    if ((frame["high"] < frame[["open", "close"]].max(axis=1)) | (frame["low"] > frame[["open", "close"]].min(axis=1))).any():
        raise ValueError("OHLC candles are internally inconsistent")
    if (frame["volume"] < 0).any():
        raise ValueError("volume cannot be negative")
    if cfg.require_strict_frequency and len(frame) > 1:
        diffs = frame["timestamp"].diff().dropna()
        expected = pd.Timedelta(minutes=int(expected_interval_minutes)) if expected_interval_minutes else diffs.mode().iloc[0]
        if cfg.reject_missing_candles and (diffs > expected).any():
            raise ValueError(f"missing candles detected; expected fixed interval of {expected}")
        if (diffs != expected).any():
            raise ValueError(f"inconsistent candle spacing detected; expected fixed interval of {expected}")
    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].astype(str)
    if "vwap" in frame.columns:
        frame["vwap"] = pd.to_numeric(frame["vwap"], errors="coerce")
    elif require_vwap and allow_vwap_compute and cfg.allow_vwap_computation:
        typical = (frame["high"] + frame["low"] + frame["close"]) / 3.0
        session_key = frame["timestamp"].dt.date
        cumulative_pv = (typical * frame["volume"]).groupby(session_key).cumsum()
        cumulative_volume = frame["volume"].groupby(session_key).cumsum().replace(0, pd.NA)
        frame["vwap"] = pd.to_numeric(cumulative_pv / cumulative_volume, errors="coerce")
    if require_vwap and ("vwap" not in frame.columns or frame["vwap"].isna().any()):
        raise ValueError("missing VWAP if required")
    return frame.reset_index(drop=True)


def standardize_market_data(
    df: pd.DataFrame,
    *,
    expected_interval_minutes: int | None = None,
    require_vwap: bool = False,
    config: StrictValidationConfig | None = None,
) -> pd.DataFrame:
    cfg = config or StrictValidationConfig()
    return _normalize_frame(
        df,
        require_vwap=require_vwap,
        allow_vwap_compute=cfg.allow_vwap_computation,
        expected_interval_minutes=expected_interval_minutes,
        config=cfg,
    )


def _validate_zone_schema(zone: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(zone, dict):
        raise TypeError("zone must be a dictionary")
    zone_id = str(zone.get("zone_id", "") or "").strip()
    zone_type = str(zone.get("zone_type", "") or "").strip().lower()
    zone_low = _safe_float(zone.get("zone_low"))
    zone_high = _safe_float(zone.get("zone_high"))
    if not zone_id:
        raise ValueError("zone_id is required")
    if zone_type not in {"demand", "supply"}:
        raise ValueError("zone_type must be demand or supply")
    if zone_low <= 0 or zone_high <= 0 or zone_high <= zone_low:
        raise ValueError("zone boundaries are invalid")
    creation = zone.get("creation_timestamp") or zone.get("timestamp") or zone.get("created_at")
    if creation is None:
        raise ValueError("creation timestamp is required")
    normalized = dict(zone)
    normalized["zone_id"] = zone_id
    normalized["zone_type"] = zone_type
    normalized["zone_low"] = zone_low
    normalized["zone_high"] = zone_high
    normalized["creation_timestamp"] = pd.Timestamp(creation)
    normalized["symbol"] = str(zone.get("symbol", "") or "NIFTY")
    return normalized


def _failed_result(zone: dict[str, Any] | None, symbol: str, explanation: str) -> dict[str, Any]:
    result = {
        "symbol": symbol,
        "timeframe_zone": str((zone or {}).get("timeframe_zone", "15m")),
        "timeframe_entry": str((zone or {}).get("timeframe_entry", "5m")),
        "zone_id": str((zone or {}).get("zone_id", "")),
        "zone_type": str((zone or {}).get("zone_type", "")),
        "status": "FAIL",
        "validation_score": 0.0,
        "fail_reasons": ["invalid_input"],
        "metrics": {"execution_allowed": False, "validation_score": 0.0},
        "execution_allowed": False,
        "execution_blockers": ["invalid_input"],
        "explanation": explanation,
        "timestamp": str((zone or {}).get("creation_timestamp", "")),
    }
    _DEFAULT_LOGGER.log(result)
    return result


def _true_range(frame: pd.DataFrame) -> pd.Series:
    prev_close = frame["close"].shift(1)
    return pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def _atr(frame: pd.DataFrame, window: int) -> pd.Series:
    return _true_range(frame).rolling(max(int(window), 1), min_periods=1).mean().clip(lower=1e-6)


def _adx(frame: pd.DataFrame, window: int) -> pd.Series:
    high_diff = frame["high"].diff()
    low_diff = -frame["low"].diff()
    plus_dm = high_diff.where((high_diff > low_diff) & (high_diff > 0), 0.0)
    minus_dm = low_diff.where((low_diff > high_diff) & (low_diff > 0), 0.0)
    atr_series = _atr(frame, window)
    plus_di = 100.0 * (plus_dm.rolling(window, min_periods=1).mean() / atr_series)
    minus_di = 100.0 * (minus_dm.rolling(window, min_periods=1).mean() / atr_series)
    di_sum = (plus_di + minus_di).replace(0, pd.NA)
    dx = pd.to_numeric((plus_di - minus_di).abs() / di_sum, errors="coerce").fillna(0.0) * 100.0
    return dx.rolling(window, min_periods=1).mean().fillna(0.0)


def _body_pct(frame: pd.DataFrame) -> pd.Series:
    candle_range = (frame["high"] - frame["low"]).clip(lower=1e-6)
    return (frame["close"] - frame["open"]).abs() / candle_range


def _wick_ratio(frame: pd.DataFrame) -> pd.Series:
    upper = frame["high"] - frame[["open", "close"]].max(axis=1)
    lower = frame[["open", "close"]].min(axis=1) - frame["low"]
    body = (frame["close"] - frame["open"]).abs().clip(lower=1e-6)
    return ((upper + lower) / body).clip(lower=0.0)


def _range_overlap(prev_candle: pd.Series, candle: pd.Series) -> float:
    overlap = max(0.0, min(float(prev_candle["high"]), float(candle["high"])) - max(float(prev_candle["low"]), float(candle["low"])))
    width = max(float(max(prev_candle["high"], candle["high"]) - min(prev_candle["low"], candle["low"])), 1e-6)
    return overlap / width


def _swing_points(frame: pd.DataFrame, window: int = 2) -> tuple[pd.Series, pd.Series]:
    lookback = max(int(window), 1)
    swing_high = frame["high"][(frame["high"] == frame["high"].rolling(lookback * 2 + 1, center=True).max())].dropna()
    swing_low = frame["low"][(frame["low"] == frame["low"].rolling(lookback * 2 + 1, center=True).min())].dropna()
    return swing_high, swing_low


def _zone_width(zone: dict[str, Any]) -> float:
    return max(_safe_float(zone.get("zone_high")) - _safe_float(zone.get("zone_low")), 1e-6)


def _zone_timestamp(zone: dict[str, Any]) -> pd.Timestamp:
    raw = zone.get("creation_timestamp") or zone.get("timestamp") or zone.get("created_at")
    return pd.Timestamp(raw)


def _trade_direction(zone_type: str) -> str:
    return "long" if str(zone_type).lower() == "demand" else "short"


def _target_from_structure(frame: pd.DataFrame, entry_idx: int, direction: str) -> float:
    sample = frame.iloc[max(0, entry_idx - 20): min(len(frame), entry_idx + 20)]
    if sample.empty:
        return _safe_float(frame.iloc[entry_idx]["close"])
    if direction == "long":
        highs = sample["high"].iloc[entry_idx - max(0, entry_idx - 20) + 1:]
        return float(highs.max()) if not highs.empty else float(sample["high"].max())
    lows = sample["low"].iloc[entry_idx - max(0, entry_idx - 20) + 1:]
    return float(lows.min()) if not lows.empty else float(sample["low"].min())


def _reaction_metrics(candle: pd.Series, zone: dict[str, Any]) -> dict[str, float | bool]:
    zone_type = str(zone["zone_type"]).lower()
    zone_low = _safe_float(zone["zone_low"])
    zone_high = _safe_float(zone["zone_high"])
    width = _zone_width(zone)
    price_range = max(float(candle["high"]) - float(candle["low"]), 1e-6)
    body = abs(float(candle["close"]) - float(candle["open"]))
    upper_wick = float(candle["high"]) - max(float(candle["open"]), float(candle["close"]))
    lower_wick = min(float(candle["open"]), float(candle["close"])) - float(candle["low"])
    wick_rejection = lower_wick if zone_type == "demand" else upper_wick
    wick_ratio = wick_rejection / max(body, 1e-6)
    body_strength = body / price_range
    close_away = (float(candle["close"]) - zone_high) / width if zone_type == "demand" else (zone_low - float(candle["close"])) / width
    engulfing = False
    score = _clamp((wick_ratio / 1.5) * 4.0 + (body_strength / 0.6) * 2.5 + (max(close_away, 0.0) / 0.6) * 2.5 + (1.0 if engulfing else 0.0))
    return {
        "wick_rejection": _round(wick_ratio),
        "body_strength": _round(body_strength),
        "close_away_from_edge": _round(close_away),
        "engulfing": engulfing,
        "rejection_score": _round(score),
    }


def _session_allowed(timestamp: pd.Timestamp, config: StrictValidationConfig) -> bool:
    minute_value = timestamp.hour * 60 + timestamp.minute
    if minute_value < _time_to_minutes(config.allowed_start_time):
        return False
    if minute_value > _time_to_minutes(config.cutoff_time):
        return False
    if config.block_lunch_window and _time_to_minutes(config.lunch_start_time) <= minute_value <= _time_to_minutes(config.lunch_end_time):
        return False
    return True

def detect_15m_zones(df_15m: pd.DataFrame, config: StrictValidationConfig | None = None, *, symbol: str = "NIFTY") -> list[dict[str, Any]]:
    """Detect candidate 15m demand and supply zones from base-plus-displacement structure."""
    cfg = config or StrictValidationConfig()
    frame = standardize_market_data(df_15m, expected_interval_minutes=cfg.zone_timeframe_minutes, config=cfg)
    atr_series = _atr(frame, cfg.atr_window)
    zones: list[dict[str, Any]] = []
    counter = {"demand": 0, "supply": 0}
    for start_idx in range(0, max(len(frame) - 3, 0)):
        for base_count in range(2, cfg.max_base_candles + 1):
            end_idx = start_idx + base_count - 1
            departure_end = end_idx + max(cfg.min_impulsive_candles, 2)
            if departure_end >= len(frame):
                break
            base = frame.iloc[start_idx : end_idx + 1]
            departure = frame.iloc[end_idx + 1 : departure_end + 1]
            base_low = float(base["low"].min())
            base_high = float(base["high"].max())
            width = max(base_high - base_low, 1e-6)
            atr_ref = max(float(atr_series.iloc[end_idx]), 1e-6)
            move_up = float(departure["high"].max()) - base_high
            move_down = base_low - float(departure["low"].min())
            direction = "demand" if move_up >= move_down else "supply"
            departure_distance = max(move_up, move_down, 0.0)
            departure_atr = departure_distance / atr_ref
            impulsive = int(((_body_pct(departure) >= 0.55) & (((departure["high"] - departure["low"]) / atr_ref) >= 0.9)).sum())
            avg_body_pct = float(_body_pct(departure).mean()) if not departure.empty else 0.0
            close_location_strength = (
                ((departure["close"] - departure["low"]) / (departure["high"] - departure["low"]).clip(lower=1e-6)).mean()
                if direction == "demand"
                else ((departure["high"] - departure["close"]) / (departure["high"] - departure["low"]).clip(lower=1e-6)).mean()
            )
            if departure_atr < 0.8:
                continue
            counter[direction] += 1
            zone_id = f"{symbol}_{pd.Timestamp(frame.iloc[end_idx]['timestamp']).date()}_{cfg.zone_timeframe}_{direction}_{counter[direction]:02d}"
            overlap_values = [_range_overlap(base.iloc[idx - 1], base.iloc[idx]) for idx in range(1, len(base))]
            prior = frame.iloc[max(0, start_idx - 5) : start_idx]
            bos_confirmed = False
            if not prior.empty:
                bos_confirmed = float(departure["high"].max()) > float(prior["high"].max()) if direction == "demand" else float(departure["low"].min()) < float(prior["low"].min())
            zones.append(
                {
                    "symbol": symbol,
                    "zone_id": zone_id,
                    "zone_type": direction,
                    "timeframe_zone": cfg.zone_timeframe,
                    "timeframe_entry": cfg.entry_timeframe,
                    "creation_timestamp": frame.iloc[end_idx]["timestamp"],
                    "created_at": frame.iloc[end_idx]["timestamp"],
                    "zone_low": _round(base_low),
                    "zone_high": _round(base_high),
                    "zone_width_points": _round(width),
                    "base_start_idx": start_idx,
                    "base_end_idx": end_idx,
                    "departure_end_idx": departure_end,
                    "departure_candles_count": int(len(departure)),
                    "departure_atr": _round(departure_atr),
                    "impulsive_candles": impulsive,
                    "avg_body_pct": _round(avg_body_pct),
                    "close_location_strength": _round(close_location_strength),
                    "displacement_speed": int(max(1, departure_end - end_idx)),
                    "bos_confirmed": bool(bos_confirmed),
                    "base_candles": int(base_count),
                    "base_range": _round(width),
                    "base_range_atr": _round(width / atr_ref),
                    "wick_ratio": _round(float(_wick_ratio(base).mean())),
                    "overlap_ratio": _round(sum(overlap_values) / len(overlap_values)) if overlap_values else 0.0,
                    "alternating_candle_direction": int((base["close"].gt(base["open"]).astype(int).diff().abs() == 1).sum()),
                    "imbalance_score": _round(min(10.0, departure_atr * 3.0 + close_location_strength * 2.0)),
                    "setup_already_used": False,
                }
            )
            break
    return zones


def map_15m_zone_to_5m(df_5m: pd.DataFrame, zone: dict[str, Any], config: StrictValidationConfig | None = None) -> pd.DataFrame:
    """Return the 5m candles relevant to the zone retest, annotated with zone interaction flags."""
    cfg = config or StrictValidationConfig()
    frame = standardize_market_data(df_5m, expected_interval_minutes=cfg.entry_timeframe_minutes, require_vwap=cfg.require_vwap_alignment, config=cfg)
    created_at = _zone_timestamp(zone)
    width = _zone_width(zone)
    buffer_points = width * cfg.retest_near_zone_buffer_pct
    zone_low = _safe_float(zone["zone_low"])
    zone_high = _safe_float(zone["zone_high"])
    mapped = frame.loc[frame["timestamp"] > created_at].copy()
    mapped["inside_zone"] = (mapped["low"] <= zone_high) & (mapped["high"] >= zone_low)
    mapped["near_zone"] = (mapped["low"] <= zone_high + buffer_points) & (mapped["high"] >= zone_low - buffer_points)
    mapped["zone_low"] = zone_low
    mapped["zone_high"] = zone_high
    return mapped.reset_index(drop=True)


def score_zone_departure(zone: dict[str, Any], df_15m: pd.DataFrame, config: StrictValidationConfig | None = None) -> tuple[float, dict[str, Any]]:
    cfg = config or StrictValidationConfig()
    frame = standardize_market_data(df_15m, expected_interval_minutes=cfg.zone_timeframe_minutes, config=cfg)
    atr_series = _atr(frame, cfg.atr_window)
    metrics = {
        "departure_atr": _safe_float(zone.get("departure_atr")),
        "impulsive_candles": int(_safe_float(zone.get("impulsive_candles"))),
        "avg_body_pct": _safe_float(zone.get("avg_body_pct")),
        "close_location_strength": _safe_float(zone.get("close_location_strength")),
        "displacement_speed": int(_safe_float(zone.get("displacement_speed", 1)) or 1),
        "bos_confirmed": bool(zone.get("bos_confirmed", False)),
        "base_candles": int(_safe_float(zone.get("base_candles", 0))),
        "base_range_atr": _safe_float(zone.get("base_range_atr", 0.0)),
        "wick_ratio": _safe_float(zone.get("wick_ratio", 0.0)),
        "overlap_ratio": _safe_float(zone.get("overlap_ratio", 0.0)),
    }
    if metrics["departure_atr"] <= 0:
        end_idx = int(_safe_float(zone.get("base_end_idx", 0)))
        dep_end = int(_safe_float(zone.get("departure_end_idx", end_idx)))
        if dep_end > end_idx and dep_end < len(frame):
            dep = frame.iloc[end_idx + 1 : dep_end + 1]
            atr_ref = max(float(atr_series.iloc[end_idx]), 1e-6)
            if str(zone["zone_type"]).lower() == "demand":
                metrics["departure_atr"] = max(float(dep["high"].max()) - _safe_float(zone["zone_high"]), 0.0) / atr_ref
            else:
                metrics["departure_atr"] = max(_safe_float(zone["zone_low"]) - float(dep["low"].min()), 0.0) / atr_ref
            metrics["impulsive_candles"] = int(((_body_pct(dep) >= 0.55) & (((dep["high"] - dep["low"]) / atr_ref) >= 0.9)).sum())
            metrics["avg_body_pct"] = float(_body_pct(dep).mean()) if not dep.empty else 0.0
    score = 0.0
    score += min(metrics["departure_atr"] / max(cfg.min_departure_atr, 1e-6), 1.5) * 4.0
    score += min(metrics["impulsive_candles"] / max(cfg.min_impulsive_candles, 1), 1.5) * 2.5
    score += min(metrics["avg_body_pct"] / 0.55, 1.5) * 1.5
    score += min(metrics["close_location_strength"] / 0.65, 1.5) * 1.0
    score += min(3.0 / max(metrics["displacement_speed"], 1), 1.0) * 1.0
    metrics["zone_departure_score"] = _round(_clamp(score))
    return _clamp(score), {key: (_round(value) if isinstance(value, float) else value) for key, value in metrics.items()}


def score_freshness(zone: dict[str, Any], df_5m: pd.DataFrame, config: StrictValidationConfig | None = None) -> tuple[float, dict[str, Any]]:
    cfg = config or StrictValidationConfig()
    mapped = map_15m_zone_to_5m(df_5m, zone, cfg)
    interactions = mapped.index[mapped["inside_zone"] | mapped["near_zone"]].tolist()
    if "touch_count" in zone:
        touch_count = int(_safe_float(zone.get("touch_count")))
    else:
        touch_count = max(len(interactions) - 1, 0) if interactions else 0
    candles_since_created = int(len(mapped))
    if "candles_since_zone_created" in zone:
        candles_since_created = int(_safe_float(zone.get("candles_since_zone_created")))
    first_retest_already_happened = bool(touch_count >= 1)
    score = 10.0 if touch_count == 0 else 6.0 if touch_count == 1 else 0.0
    if candles_since_created > cfg.max_retest_delay_candles:
        score = max(0.0, score - 3.0)
    metrics = {
        "touch_count": touch_count,
        "first_retest_already_happened": first_retest_already_happened,
        "candles_since_zone_created": candles_since_created,
        "freshness_score": _round(_clamp(score)),
    }
    return _clamp(score), metrics


def check_vwap_alignment(df_5m: pd.DataFrame, trade_direction: str, config: StrictValidationConfig | None = None) -> tuple[bool, float, dict[str, Any]]:
    cfg = config or StrictValidationConfig()
    frame = standardize_market_data(df_5m, expected_interval_minutes=cfg.entry_timeframe_minutes, require_vwap=cfg.require_vwap_alignment, config=cfg)
    last = frame.iloc[-1]
    vwap = _safe_float(last.get("vwap"))
    close = _safe_float(last["close"])
    if trade_direction == "long":
        aligned = close >= vwap
        score = 10.0 if aligned and close > vwap else 4.0 if aligned else 0.0
    else:
        aligned = close <= vwap
        score = 10.0 if aligned and close < vwap else 4.0 if aligned else 0.0
    return aligned, _clamp(score), {"vwap_alignment": aligned, "vwap_score": _round(_clamp(score)), "vwap_value": _round(vwap)}


def score_structure_context(df_5m: pd.DataFrame, config: StrictValidationConfig | None = None) -> tuple[float, dict[str, Any]]:
    cfg = config or StrictValidationConfig()
    frame = _normalize_frame(df_5m)
    sample = frame.tail(max(cfg.volatility_lookback, 12)).copy()
    overlap_values = [_range_overlap(sample.iloc[idx - 1], sample.iloc[idx]) for idx in range(1, len(sample))]
    overlap_ratio = float(sum(overlap_values) / len(overlap_values)) if overlap_values else 0.0
    alternating = int((sample["close"].gt(sample["open"]).astype(int).diff().abs() == 1).sum())
    chop_score = min(10.0, overlap_ratio * 8.0 + (alternating / max(len(sample), 1)) * 4.0)
    adx_value = float(_adx(sample, min(14, max(len(sample) - 1, 2))).iloc[-1]) if len(sample) > 2 else 0.0
    swing_highs, swing_lows = _swing_points(sample, 2)
    swing_clarity = 10.0 if len(swing_highs) >= 2 and len(swing_lows) >= 2 else 4.0
    structure_score = _clamp(swing_clarity * 0.4 + (1.0 - min(overlap_ratio, 1.0)) * 6.0 + min(adx_value / max(cfg.adx_minimum, 1e-6), 1.2) * 2.0)
    return structure_score, {
        "structure_score": _round(structure_score),
        "overlap_ratio": _round(overlap_ratio),
        "alternating_candle_direction": alternating,
        "chop_score": _round(chop_score),
        "adx": _round(adx_value),
    }

def score_retest_quality(zone: dict[str, Any], df_5m: pd.DataFrame, config: StrictValidationConfig | None = None) -> tuple[float, dict[str, Any]]:
    cfg = config or StrictValidationConfig()
    mapped = map_15m_zone_to_5m(df_5m, zone, cfg)
    width = _zone_width(zone)
    interaction_mask = mapped["inside_zone"] | mapped["near_zone"]
    interactions = mapped.index[interaction_mask].tolist()
    if not interactions:
        return 0.0, {
            "qualified_retest_found": False,
            "breakout_chase_entry": True,
            "rejection_score": 0.0,
            "penetration_pct": 1.0,
            "entry_timestamp": "",
            "entry_price": 0.0,
            "stop_price": 0.0,
            "target_price": 0.0,
            "sweep_confirmed": False,
            "retest_score": 0.0,
        }
    first_touch_idx = interactions[0]
    entry_idx = int(_safe_float(zone.get("entry_idx", first_touch_idx)))
    if entry_idx < first_touch_idx:
        entry_idx = first_touch_idx
    if entry_idx >= len(mapped):
        entry_idx = len(mapped) - 1
    if entry_idx > first_touch_idx + 1:
        entry_idx = first_touch_idx + 1
    touch = mapped.iloc[first_touch_idx]
    entry = mapped.iloc[entry_idx]
    zone_type = str(zone["zone_type"]).lower()
    if zone_type == "demand":
        depth_inside = max(_safe_float(zone["zone_high"]) - float(touch["low"]), 0.0)
    else:
        depth_inside = max(float(touch["high"]) - _safe_float(zone["zone_low"]), 0.0)
    penetration_pct = depth_inside / width
    reaction = _reaction_metrics(entry, zone)
    prior = mapped.iloc[max(0, first_touch_idx - 4) : first_touch_idx]
    sweep_confirmed = False
    if not prior.empty:
        sweep_confirmed = float(touch["low"]) < float(prior["low"].min()) if zone_type == "demand" else float(touch["high"]) > float(prior["high"].max())
    breakout_chase_entry = not bool(interaction_mask.iloc[min(entry_idx, len(interaction_mask) - 1)])
    if "breakout_chase_entry" in zone:
        breakout_chase_entry = bool(zone.get("breakout_chase_entry"))
    entry_price = _safe_float(zone.get("entry_price", entry["close"]))
    stop_buffer = width * cfg.stop_buffer_pct_of_zone
    stop_price = _safe_float(zone.get("stop_price"))
    if stop_price <= 0:
        stop_price = _safe_float(zone["zone_low"]) - stop_buffer if zone_type == "demand" else _safe_float(zone["zone_high"]) + stop_buffer
    target_price = _safe_float(zone.get("target_price"))
    if target_price <= 0:
        target_price = _target_from_structure(mapped, entry_idx, _trade_direction(zone_type))
    retest_score = _clamp(reaction["rejection_score"] * 0.7 + max(0.0, 1.0 - penetration_pct) * 3.0)
    return retest_score, {
        "qualified_retest_found": True,
        "breakout_chase_entry": breakout_chase_entry,
        "entry_timestamp": str(entry["timestamp"]),
        "entry_price": _round(entry_price),
        "stop_price": _round(stop_price),
        "target_price": _round(target_price),
        "penetration_pct": _round(zone.get("penetration_pct", penetration_pct)),
        "sweep_confirmed": bool(zone.get("sweep_confirmed", sweep_confirmed)),
        "retest_score": _round(retest_score),
        "rejection_score": _round(zone.get("rejection_score", reaction["rejection_score"])),
        "wick_rejection": _round(zone.get("wick_rejection", reaction["wick_rejection"])),
        "body_strength": _round(zone.get("body_strength", reaction["body_strength"])),
        "close_away_from_zone_edge": _round(zone.get("close_away_from_zone_edge", reaction["close_away_from_edge"])),
        "engulfing_confirmed": bool(zone.get("engulfing_confirmed", reaction["engulfing"])),
    }


def calculate_realistic_rr(zone: dict[str, Any], entry_price: float, stop_price: float, target_price: float) -> float:
    risk = abs(float(entry_price) - float(stop_price))
    reward = abs(float(target_price) - float(entry_price))
    return 0.0 if risk <= 1e-6 else reward / risk


def validate_5m_retest(df_5m: pd.DataFrame, zone: dict[str, Any], config: StrictValidationConfig | None = None) -> dict[str, Any]:
    cfg = config or StrictValidationConfig()
    mapped = map_15m_zone_to_5m(df_5m, zone, cfg)
    retest_score, retest_metrics = score_retest_quality(zone, mapped, cfg)
    freshness_score, freshness_metrics = score_freshness(zone, mapped, cfg)
    structure_score, structure_metrics = score_structure_context(mapped, cfg)
    aligned, vwap_score, vwap_metrics = check_vwap_alignment(mapped, _trade_direction(str(zone["zone_type"]).lower()), cfg)
    entry_timestamp = pd.Timestamp(retest_metrics["entry_timestamp"]) if str(retest_metrics["entry_timestamp"]) else mapped.iloc[-1]["timestamp"]
    atr_series = _atr(mapped, cfg.atr_window)
    atr_value = float(atr_series.iloc[-1])
    atr_pct = atr_value / max(float(mapped.iloc[-1]["close"]), 1e-6)
    range_series = (mapped["high"] - mapped["low"]).rolling(min(cfg.volatility_lookback, len(mapped)), min_periods=1).mean()
    volatility_percentile = float(range_series.rank(pct=True).iloc[-1]) if not range_series.empty else 0.0
    session_range = float(mapped["high"].max() - mapped["low"].min())
    volatility_score = _clamp((atr_pct / 0.003) * 5.0 + volatility_percentile * 3.0 + min(session_range / max(atr_value, 1e-6), 3.0) * 0.7)
    session_allowed = _session_allowed(entry_timestamp, cfg)
    metrics = {
        **freshness_metrics,
        **retest_metrics,
        **structure_metrics,
        **vwap_metrics,
        "vwap_alignment": aligned,
        "atr_5m": _round(atr_value),
        "atr_pct": _round(zone.get("atr_pct", atr_pct)),
        "session_range": _round(zone.get("session_range", session_range)),
        "volatility_percentile": _round(zone.get("volatility_percentile", volatility_percentile)),
        "volatility_score": _round(zone.get("volatility_score", volatility_score)),
        "session_allowed": session_allowed,
        "entry_time": str(entry_timestamp),
        "bad_session_timing": not session_allowed,
        "chop_score": _round(zone.get("chop_score", structure_metrics["chop_score"])),
    }
    if "touch_count" in zone:
        metrics["touch_count"] = int(_safe_float(zone.get("touch_count")))
    return metrics


def _trend_score(zone: dict[str, Any], df_5m: pd.DataFrame, df_15m: pd.DataFrame, config: StrictValidationConfig) -> tuple[float, dict[str, Any]]:
    frame_5m = standardize_market_data(df_5m, expected_interval_minutes=config.entry_timeframe_minutes, config=config)
    frame_15m = standardize_market_data(df_15m, expected_interval_minutes=config.zone_timeframe_minutes, config=config)
    fast_5 = frame_5m["close"].ewm(span=5, adjust=False).mean()
    slow_5 = frame_5m["close"].ewm(span=13, adjust=False).mean()
    fast_15 = frame_15m["close"].ewm(span=5, adjust=False).mean()
    slow_15 = frame_15m["close"].ewm(span=13, adjust=False).mean()
    direction = _trade_direction(str(zone["zone_type"]).lower())
    bullish = fast_5.iloc[-1] >= slow_5.iloc[-1] and fast_15.iloc[-1] >= slow_15.iloc[-1]
    bearish = fast_5.iloc[-1] <= slow_5.iloc[-1] and fast_15.iloc[-1] <= slow_15.iloc[-1]
    aligned = bullish if direction == "long" else bearish
    reversal_score = _safe_float(zone.get("reversal_score", 0.0))
    score = 9.0 if aligned else 4.0 if reversal_score >= 7.5 else 0.0
    return _clamp(score), {
        "higher_tf_trend": "bullish" if bullish else "bearish" if bearish else "mixed",
        "trend_direction": direction,
        "trend_score": _round(_clamp(score)),
        "trend_aligned": aligned,
        "reversal_score": _round(reversal_score),
    }


def build_fail_reasons(metrics: dict[str, Any], config: StrictValidationConfig | None = None) -> list[str]:
    cfg = config or StrictValidationConfig()
    reasons: list[str] = []
    if _safe_float(metrics.get("departure_atr")) < cfg.min_departure_atr or int(_safe_float(metrics.get("impulsive_candles"))) < cfg.min_impulsive_candles or _safe_float(metrics.get("avg_body_pct")) < 0.55:
        reasons.append("weak_zone_departure")
    if not bool(metrics.get("bos_confirmed")):
        reasons.append("no_structure_break")
    if int(_safe_float(metrics.get("base_candles"))) > cfg.max_base_candles or _safe_float(metrics.get("base_range_atr")) > 0.8 or _safe_float(metrics.get("overlap_ratio")) > 0.6 or _safe_float(metrics.get("wick_ratio")) > cfg.base_wick_ratio_threshold:
        reasons.append("dirty_zone_base")
    if int(_safe_float(metrics.get("touch_count"))) >= 2:
        reasons.append("stale_zone")
    if int(_safe_float(metrics.get("candles_since_zone_created"))) > cfg.max_retest_delay_candles:
        reasons.append("late_retest")
    if _safe_float(metrics.get("rejection_score")) < cfg.min_rejection_score:
        reasons.append("weak_retest_reaction")
    if _safe_float(metrics.get("penetration_pct")) > cfg.max_penetration_pct:
        reasons.append("deep_zone_penetration")
    if cfg.require_vwap_alignment and not bool(metrics.get("vwap_alignment")):
        reasons.append("no_vwap_alignment")
    if not bool(metrics.get("session_allowed", True)):
        reasons.append("bad_session_timing")
    if _safe_float(metrics.get("atr_pct")) <= 0.0 or _safe_float(metrics.get("atr_pct")) < 0.0015 or _safe_float(metrics.get("session_range")) < max(_safe_float(metrics.get("atr_5m")), 1e-6) * 2.0 or _safe_float(metrics.get("volatility_percentile")) < 0.30:
        reasons.append("low_intraday_volatility")
    if _safe_float(metrics.get("chop_score")) > cfg.chop_score_threshold or _safe_float(metrics.get("adx")) < cfg.adx_minimum:
        reasons.append("chop_market_fail")
    if not bool(metrics.get("trend_aligned")) and _safe_float(metrics.get("reversal_score")) < 7.5:
        reasons.append("trend_misaligned")
    if _safe_float(metrics.get("rr_ratio")) < cfg.min_rr:
        reasons.append("bad_rr")
    if _safe_float(metrics.get("zone_width_atr")) > cfg.max_zone_width_atr:
        reasons.append("oversized_zone")
    if cfg.require_sweep and not bool(metrics.get("sweep_confirmed")):
        reasons.append("no_liquidity_event")
    if _safe_float(metrics.get("imbalance_score")) < cfg.imbalance_threshold:
        reasons.append("no_imbalance_support")
    if _safe_float(metrics.get("structure_score")) < cfg.min_structure_score:
        reasons.append("weak_structure_clarity")
    if bool(metrics.get("setup_already_used")):
        reasons.append("duplicate_setup_context")
    if not bool(metrics.get("qualified_retest_found")) or bool(metrics.get("breakout_chase_entry")):
        reasons.append("breakout_chase_entry")
    if _safe_float(metrics.get("validation_score")) < cfg.min_validation_score:
        reasons.append("low_validation_score")
    return reasons

def _explanation(fail_reasons: list[str], zone: dict[str, Any]) -> str:
    if not fail_reasons:
        return "15m zone and 5m retest satisfied strict validation, so execution is allowed."
    phrases = [FAIL_REASON_TEXT.get(reason, reason.replace("_", " ")) for reason in fail_reasons]
    zone_type = str(zone.get("zone_type", "zone"))
    if len(phrases) == 1:
        return f"15m {zone_type} zone failed because {phrases[0]}."
    return f"15m {zone_type} zone failed because {', '.join(phrases[:-1])}, and {phrases[-1]}."


def validate_zone_candidate(
    zone: dict[str, Any],
    df_5m: pd.DataFrame,
    df_15m: pd.DataFrame,
    config: StrictValidationConfig | None = None,
) -> dict[str, Any]:
    """Validate a 15m zone with strict 5m retest rules and return an execution-safe contract."""
    cfg = config or StrictValidationConfig()
    try:
        zone = _validate_zone_schema(zone)
        zone_frame = standardize_market_data(df_15m, expected_interval_minutes=cfg.zone_timeframe_minutes, config=cfg)
        entry_frame = standardize_market_data(df_5m, expected_interval_minutes=cfg.entry_timeframe_minutes, require_vwap=cfg.require_vwap_alignment, config=cfg)
    except (TypeError, ValueError) as exc:
        return _failed_result(zone if isinstance(zone, dict) else None, str((zone or {}).get("symbol", "NIFTY")) if isinstance(zone, dict) else "NIFTY", f"Validation failed because {exc}.")
    departure_score, departure_metrics = score_zone_departure(zone, zone_frame, cfg)
    retest_metrics = validate_5m_retest(entry_frame, zone, cfg)
    freshness_score = _safe_float(retest_metrics.get("freshness_score"))
    retest_score = _safe_float(retest_metrics.get("retest_score"))
    structure_score = _safe_float(retest_metrics.get("structure_score"))
    trend_score, trend_metrics = _trend_score(zone, entry_frame, zone_frame, cfg)
    rr_ratio = calculate_realistic_rr(zone, _safe_float(retest_metrics["entry_price"]), _safe_float(retest_metrics["stop_price"]), _safe_float(retest_metrics["target_price"]))
    rr_score = _clamp((rr_ratio / max(cfg.min_rr, 1e-6)) * 10.0)
    zone_width_atr = _zone_width(zone) / max(float(_atr(zone_frame, cfg.atr_window).iloc[-1]), 1e-6)
    metrics = {
        **departure_metrics,
        **retest_metrics,
        **trend_metrics,
        "symbol": str(zone.get("symbol", "")),
        "zone_width_points": _round(_zone_width(zone)),
        "zone_width_atr": _round(zone.get("zone_width_atr", zone_width_atr)),
        "rr_ratio": _round(zone.get("rr_ratio", rr_ratio)),
        "rr_score": _round(rr_score),
        "freshness_score": _round(freshness_score),
        "retest_score": _round(retest_score),
        "structure_score": _round(zone.get("structure_score", structure_score)),
        "volatility_score": _round(retest_metrics.get("volatility_score", 0.0)),
        "vwap_score": _round(retest_metrics.get("vwap_score", 0.0)),
        "zone_departure_score": _round(departure_score),
        "imbalance_score": _round(zone.get("imbalance_score", _safe_float(zone.get("imbalance_score", 0.0)))),
        "setup_already_used": bool(zone.get("setup_already_used", False)),
    }
    validation_score = (
        0.20 * _safe_float(metrics["zone_departure_score"])
        + 0.15 * _safe_float(metrics["freshness_score"])
        + 0.20 * _safe_float(metrics["retest_score"])
        + 0.10 * _safe_float(metrics["vwap_score"])
        + 0.10 * _safe_float(metrics["structure_score"])
        + 0.10 * _safe_float(metrics["trend_score"])
        + 0.05 * _safe_float(metrics["volatility_score"])
        + 0.10 * _safe_float(metrics["rr_score"])
    )
    metrics["validation_score"] = _round(validation_score)
    fail_reasons = build_fail_reasons(metrics, cfg)
    hard_failed = any(reason in HARD_FAIL_REASONS for reason in fail_reasons)
    status = "PASS" if not fail_reasons and not hard_failed and validation_score >= cfg.min_validation_score else "FAIL"
    execution_blockers = [] if status == "PASS" and not bool(metrics.get("setup_already_used")) else list(dict.fromkeys(list(fail_reasons) + (["duplicate_setup_context"] if bool(metrics.get("setup_already_used")) else [])))
    metrics["execution_allowed"] = status == "PASS" and not execution_blockers
    result = {
        "symbol": str(zone.get("symbol", "")),
        "timeframe_zone": str(zone.get("timeframe_zone", cfg.zone_timeframe)),
        "timeframe_entry": str(zone.get("timeframe_entry", cfg.entry_timeframe)),
        "zone_id": str(zone.get("zone_id", "")),
        "zone_type": str(zone.get("zone_type", "")),
        "status": status,
        "validation_score": _round(validation_score, 2),
        "fail_reasons": fail_reasons,
        "metrics": metrics,
        "execution_allowed": metrics["execution_allowed"],
        "execution_blockers": execution_blockers,
        "explanation": _explanation(fail_reasons, zone),
        "timestamp": str(retest_metrics.get("entry_time", zone.get("creation_timestamp", ""))),
    }
    _DEFAULT_LOGGER.log(result)
    if cfg.logger_csv_path:
        _DEFAULT_LOGGER.write_csv(cfg.logger_csv_path)
    return result


def execution_allowed(validation_result: dict[str, Any], *, setup_already_used: bool = False, cooldown_active: bool = False) -> bool:
    """Central execution safety gate for live or paper trading."""
    if str(validation_result.get("status", "")).upper() != "PASS":
        return False
    if not bool(validation_result.get("execution_allowed", False)):
        return False
    metrics = dict(validation_result.get("metrics", {}) or {})
    if bool(metrics.get("setup_already_used")) or setup_already_used:
        return False
    if cooldown_active:
        return False
    return True


def sample_usage() -> dict[str, Any]:
    """Return a minimal sample contract showing how the engine is intended to be consumed."""
    return {
        "config": asdict(StrictValidationConfig()),
        "flow": [
            "clean data",
            "detect 15m zone",
            "map zone to 5m",
            "validate retest on 5m",
            "apply fail metrics",
            "compute validation score",
            "PASS / FAIL",
            "only then allow execution",
        ],
    }


def sample_output() -> dict[str, Any]:
    return {
        "symbol": "NIFTY",
        "timeframe_zone": "15m",
        "timeframe_entry": "5m",
        "zone_id": "NIFTY_2026-03-29_15m_demand_01",
        "zone_type": "demand",
        "status": "FAIL",
        "validation_score": 5.9,
        "fail_reasons": [
            "weak_zone_departure",
            "stale_zone",
            "deep_zone_penetration",
            "bad_rr",
        ],
        "metrics": {
            "departure_atr": 1.1,
            "impulsive_candles": 1,
            "bos_confirmed": False,
            "base_candles": 7,
            "touch_count": 2,
            "candles_since_zone_created": 28,
            "rejection_score": 4.8,
            "penetration_pct": 0.81,
            "vwap_alignment": False,
            "chop_score": 7.5,
            "structure_score": 5.2,
            "rr_ratio": 1.7,
            "zone_width_atr": 1.3,
            "setup_already_used": False,
        },
        "explanation": "15m demand zone failed because departure was weak, the zone was no longer fresh, price penetrated too deep into the zone before reversing, and reward-to-risk was below the required minimum.",
    }













