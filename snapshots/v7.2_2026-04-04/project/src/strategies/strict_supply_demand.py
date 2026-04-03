
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, time, timedelta
from typing import Any, Sequence

import pandas as pd

from src.analytics.metrics import compute_trade_metrics, evaluate_production_readiness
from src.data.cleaner import OHLCVValidationError, coerce_ohlcv


REQUIRED_FIELDS = ["timestamp", "open", "high", "low", "close", "volume"]
_OPTIONAL_FIELDS = ["vwap", "ema_20", "ema_50", "session", "symbol"]
_ALIAS_MAP = {
    "datetime": "timestamp",
    "date": "date",
    "time": "time",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "vol": "volume",
}
_STRUCTURE_MAP = {
    ("rally", "rally"): "RBR",
    ("drop", "rally"): "DBR",
    ("rally", "drop"): "RBD",
    ("drop", "drop"): "DBD",
}
_REJECTION_REASON_TEXT = {
    "duplicate_zone": "zone already traded recently",
    "entry_after_cutoff": "entry was after the configured cutoff time",
    "invalid_rr": "reward to risk was below threshold",
    "max_retests_exceeded": "zone had too many retests",
    "rejection_candle_weak": "rejection candle was too weak",
    "session_filter_failed": "trade was outside the allowed session",
    "trend_alignment_failed": "trend alignment filter failed",
    "violated_zone": "zone was already violated",
    "vwap_alignment_failed": "VWAP alignment filter failed",
    "weak_departure": "departure from the base was too weak",
    "weak_zone_score": "zone score was below the minimum threshold",
}


@dataclass(slots=True)
class SupplyDemandStrategyConfig:
    min_base_candles: int = 2
    max_base_candles: int = 5
    min_departure_ratio: float = 1.8
    min_total_score: float = 65.0
    max_retests: int = 1
    stop_buffer_pct: float = 0.001
    min_rr_ratio: float = 2.0
    risk_pct: float = 0.01
    capital: float = 100000.0
    entry_cutoff_time: str = "14:45"
    cooldown_minutes: int = 30
    use_vwap_filter: bool = True
    use_trend_filter: bool = True
    use_volume_filter: bool = True
    use_session_filter: bool = False
    use_volatility_filter: bool = True
    allowed_sessions: tuple[str, ...] = ("OPENING", "MORNING")
    min_impulse_strength: float = 1.2
    min_body_fraction: float = 0.45
    max_base_range_pct: float = 0.012
    max_base_overlap_ratio: float = 0.85
    min_rejection_body_ratio: float = 0.45
    min_rejection_wick_ratio: float = 0.2
    min_volume_expansion: float = 1.1
    volatility_floor_pct: float = 0.12
    session_start: str = "09:15"
    session_end: str = "15:30"
    duplicate_bucket_minutes: int = 5
    strategy_name: str = "DEMAND_SUPPLY"
    symbol: str = ""
    mode: str = "Balanced"
    trailing_sl_pct: float = 0.0
    pivot_window: int = 2
    max_trades_per_day: int = 1
    duplicate_signal_cooldown_bars: int = 24
    require_vwap_alignment: bool = True
    require_trend_bias: bool = True
    require_market_structure: bool = True
    max_retest_bars: int = 4
    min_reaction_strength: float = 0.75
    min_zone_selection_score: float = 5.0
    min_confirmation_body_ratio: float = 0.6
    zone_departure_buffer_pct: float = 0.0006
    vwap_reclaim_buffer_pct: float = 0.0005
    allow_afternoon_session: bool = False
    avoid_midday: bool = True


@dataclass(slots=True)
class StructureRecord:
    structure_type: str
    base_start_index: int
    base_end_index: int
    base_candles_count: int
    leg_in_size: float
    leg_out_size: float
    impulse_strength: float
    base_range: float
    base_range_pct: float
    departure_ratio: float
    leg_in_direction: str
    leg_out_direction: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SupplyDemandZone:
    zone_id: str
    symbol: str
    type: str
    structure_type: str
    created_at: str
    zone_low: float
    zone_high: float
    base_start_index: int
    base_end_index: int
    strength_score: float
    freshness_score: float
    tested: bool
    test_count: int
    violated: bool
    impulse_strength: float
    departure_ratio: float
    volume_score: float
    trend_score: float
    cleanliness_score: float
    validation_score: float
    total_score: float
    structure_clarity_score: float
    move_away_score: float
    base_tightness_score: float
    higher_timeframe_alignment_score: float
    vwap_score: float
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["notes"] = list(self.notes)
        return payload


@dataclass(slots=True)
class ValidationDecision:
    is_valid: bool
    rejection_reasons: list[str]
    validation_score: float
    entry_ready: bool
    metrics: dict[str, Any]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_time(value: str) -> time:
    return datetime.strptime(value, "%H:%M").time()


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out.columns = [_ALIAS_MAP.get(str(column).strip().lower(), str(column).strip().lower()) for column in frame.columns]
    return out


def normalize_ohlcv_for_supply_demand(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise TypeError("normalize_ohlcv_for_supply_demand expects a pandas DataFrame")
    if df.empty:
        raise OHLCVValidationError("empty dataframe")
    normalized = _normalize_columns(df)
    cleaned = coerce_ohlcv(normalized)
    for column in _OPTIONAL_FIELDS:
        if column in normalized.columns and column not in cleaned.columns:
            cleaned[column] = normalized[column].reset_index(drop=True)
    if cleaned.empty:
        raise OHLCVValidationError("no valid OHLCV rows available after normalization")
    return cleaned.reset_index(drop=True)


def _session_label(text: str) -> str:
    if "09:15:00" <= text <= "09:45:00":
        return "OPENING"
    if "09:45:01" <= text <= "11:30:00":
        return "MORNING"
    if "11:30:01" <= text <= "13:30:00":
        return "MIDDAY"
    if "13:30:01" <= text <= "15:30:00":
        return "AFTERNOON"
    return "OFFHOURS"


def _enrich_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["range"] = (out["high"] - out["low"]).clip(lower=0.0)
    out["body"] = (out["close"] - out["open"]).abs()
    out["body_fraction"] = (out["body"] / out["range"].replace(0.0, pd.NA)).fillna(0.0)
    out["avg_range_20"] = out["range"].rolling(20, min_periods=1).mean()
    out["avg_volume_20"] = out["volume"].rolling(20, min_periods=1).mean().replace(0.0, pd.NA)
    out["volume_ratio"] = (out["volume"] / out["avg_volume_20"]).fillna(1.0)
    prev_close = out["close"].shift(1)
    tr = pd.concat([
        out["high"] - out["low"],
        (out["high"] - prev_close).abs(),
        (out["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    out["atr_14"] = tr.rolling(14, min_periods=1).mean()
    out["atr_pct"] = ((out["atr_14"] / out["close"].replace(0.0, pd.NA)) * 100.0).fillna(0.0)
    if "ema_20" not in out.columns:
        out["ema_20"] = out["close"].ewm(span=20, adjust=False).mean()
    if "ema_50" not in out.columns:
        out["ema_50"] = out["close"].ewm(span=50, adjust=False).mean()
    if "session" not in out.columns:
        out["session"] = out["timestamp"].dt.strftime("%H:%M:%S").map(_session_label)
    return out


def _direction_from_move(value: float) -> str:
    if value > 0:
        return "rally"
    if value < 0:
        return "drop"
    return "base"

def detect_supply_demand_structures(candles: pd.DataFrame, config: SupplyDemandStrategyConfig | None = None) -> list[StructureRecord]:
    cfg = config or SupplyDemandStrategyConfig()
    frame = _enrich_frame(normalize_ohlcv_for_supply_demand(candles))
    structures: list[StructureRecord] = []
    for base_len in range(int(cfg.min_base_candles), int(cfg.max_base_candles) + 1):
        for start in range(1, len(frame) - base_len - 1):
            end = start + base_len - 1
            base = frame.iloc[start : end + 1]
            prev_row = frame.iloc[start - 1]
            next_row = frame.iloc[end + 1]
            base_range = float(base["high"].max() - base["low"].min())
            base_mid = float((base["high"].max() + base["low"].min()) / 2.0)
            base_range_pct = base_range / max(abs(base_mid), 1e-6)
            overlap_values: list[float] = []
            for idx in range(start + 1, end + 1):
                left = frame.iloc[idx - 1]
                right = frame.iloc[idx]
                overlap = max(0.0, min(float(left["high"]), float(right["high"])) - max(float(left["low"]), float(right["low"])))
                union = max(float(max(left["high"], right["high"]) - min(left["low"], right["low"])), 1e-6)
                overlap_values.append(overlap / union)
            overlap_ratio = sum(overlap_values) / max(len(overlap_values), 1)
            if base_range_pct > float(cfg.max_base_range_pct):
                continue
            if overlap_ratio > float(cfg.max_base_overlap_ratio):
                continue
            if not bool((base["body_fraction"] <= 0.55).all()):
                continue
            leg_in_move = float(prev_row["close"] - frame.iloc[max(start - 2, 0)]["close"])
            leg_out_move = float(next_row["close"] - base.iloc[-1]["close"])
            leg_in_direction = _direction_from_move(leg_in_move)
            leg_out_direction = _direction_from_move(leg_out_move)
            structure_type = _STRUCTURE_MAP.get((leg_in_direction, leg_out_direction))
            if structure_type is None:
                continue
            leg_in_size = abs(leg_in_move)
            leg_out_size = abs(leg_out_move)
            departure_ratio = leg_out_size / max(base_range, 1e-6)
            impulse_strength = leg_out_size / max(float(next_row["atr_14"] or 0.0), 1e-6)
            if departure_ratio < float(cfg.min_departure_ratio) or impulse_strength < float(cfg.min_impulse_strength):
                continue
            structures.append(
                StructureRecord(
                    structure_type=structure_type,
                    base_start_index=start,
                    base_end_index=end,
                    base_candles_count=base_len,
                    leg_in_size=round(leg_in_size, 4),
                    leg_out_size=round(leg_out_size, 4),
                    impulse_strength=round(impulse_strength, 4),
                    base_range=round(base_range, 4),
                    base_range_pct=round(base_range_pct, 6),
                    departure_ratio=round(departure_ratio, 4),
                    leg_in_direction=leg_in_direction,
                    leg_out_direction=leg_out_direction,
                )
            )
    deduped: list[StructureRecord] = []
    seen: set[tuple[int, int, str]] = set()
    for item in structures:
        key = (item.base_start_index, item.base_end_index, item.structure_type)
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def _zone_bounds(frame: pd.DataFrame, structure: StructureRecord) -> tuple[str, float, float]:
    base = frame.iloc[structure.base_start_index : structure.base_end_index + 1]
    if structure.structure_type in {"DBR", "RBR"}:
        zone_type = "demand"
        zone_low = float(base["low"].min())
        zone_high = float(base[["open", "close", "high"]].max().min())
    else:
        zone_type = "supply"
        zone_low = float(base[["open", "close", "low"]].min().max())
        zone_high = float(base["high"].max())
    if zone_high <= zone_low:
        zone_low = float(base["low"].min())
        zone_high = float(base["high"].max())
    return zone_type, round(zone_low, 4), round(zone_high, 4)


def score_supply_demand_zone(frame: pd.DataFrame, structure: StructureRecord, config: SupplyDemandStrategyConfig | None = None) -> SupplyDemandZone:
    cfg = config or SupplyDemandStrategyConfig()
    zone_type, zone_low, zone_high = _zone_bounds(frame, structure)
    symbol = str(frame["symbol"].iloc[-1]) if "symbol" in frame.columns and str(frame["symbol"].iloc[-1]).strip() else str(cfg.symbol or "UNKNOWN")
    created_at = pd.Timestamp(frame.iloc[structure.base_end_index]["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
    future = frame.iloc[structure.base_end_index + 1 :].copy()
    touched = (future["low"] <= zone_high) & (future["high"] >= zone_low) if not future.empty else pd.Series(dtype=bool)
    test_count = int(touched.sum()) if not future.empty else 0
    if zone_type == "demand":
        violated = bool((future["close"] < zone_low).any()) if not future.empty else False
        trend_ok = bool(frame.iloc[structure.base_end_index]["ema_20"] >= frame.iloc[structure.base_end_index]["ema_50"])
        vwap_ok = bool(frame.iloc[structure.base_end_index]["close"] >= frame.iloc[structure.base_end_index].get("vwap", frame.iloc[structure.base_end_index]["close"]))
    else:
        violated = bool((future["close"] > zone_high).any()) if not future.empty else False
        trend_ok = bool(frame.iloc[structure.base_end_index]["ema_20"] <= frame.iloc[structure.base_end_index]["ema_50"])
        vwap_ok = bool(frame.iloc[structure.base_end_index]["close"] <= frame.iloc[structure.base_end_index].get("vwap", frame.iloc[structure.base_end_index]["close"]))
    move_away_score = min(100.0, (structure.departure_ratio / max(float(cfg.min_departure_ratio), 1e-6)) * 40.0)
    base_tightness_score = max(0.0, (1.0 - min(structure.base_range_pct / max(float(cfg.max_base_range_pct), 1e-6), 1.0)) * 100.0)
    freshness_score = max(0.0, 100.0 - float(test_count) * 35.0 - (30.0 if violated else 0.0))
    volume_ratio = float(frame.iloc[structure.base_end_index + 1]["volume_ratio"]) if structure.base_end_index + 1 < len(frame) else 1.0
    volume_score = min(100.0, volume_ratio / max(float(cfg.min_volume_expansion), 1e-6) * 70.0)
    structure_clarity_score = min(100.0, (structure.impulse_strength / max(float(cfg.min_impulse_strength), 1e-6)) * 55.0 + (1.0 - min(test_count, 2) / 2.0) * 45.0)
    trend_score = 100.0 if trend_ok else 35.0
    vwap_score = 100.0 if vwap_ok else 40.0
    strength_score = round((move_away_score * 0.45) + (structure_clarity_score * 0.35) + (volume_score * 0.20), 2)
    cleanliness_score = round((base_tightness_score * 0.55) + (structure_clarity_score * 0.45), 2)
    validation_score = round((strength_score * 0.35) + (freshness_score * 0.25) + (cleanliness_score * 0.20) + (trend_score * 0.10) + (vwap_score * 0.10), 2)
    total_score = round((strength_score * 0.30) + (freshness_score * 0.25) + (cleanliness_score * 0.20) + (volume_score * 0.10) + (trend_score * 0.10) + (vwap_score * 0.05), 2)
    zone_id = f"{symbol}_{structure.structure_type}_{structure.base_start_index}_{structure.base_end_index}"
    notes: list[str] = []
    if test_count > 0:
        notes.append(f"zone already tested {test_count} time(s)")
    if violated:
        notes.append("zone invalidated by close through boundary")
    return SupplyDemandZone(
        zone_id=zone_id,
        symbol=symbol,
        type=zone_type,
        structure_type=structure.structure_type,
        created_at=created_at,
        zone_low=zone_low,
        zone_high=zone_high,
        base_start_index=structure.base_start_index,
        base_end_index=structure.base_end_index,
        strength_score=round(strength_score, 2),
        freshness_score=round(freshness_score, 2),
        tested=test_count > 0,
        test_count=test_count,
        violated=violated,
        impulse_strength=structure.impulse_strength,
        departure_ratio=structure.departure_ratio,
        volume_score=round(volume_score, 2),
        trend_score=round(trend_score, 2),
        cleanliness_score=round(cleanliness_score, 2),
        validation_score=round(validation_score, 2),
        total_score=round(total_score, 2),
        structure_clarity_score=round(structure_clarity_score, 2),
        move_away_score=round(move_away_score, 2),
        base_tightness_score=round(base_tightness_score, 2),
        higher_timeframe_alignment_score=round(trend_score, 2),
        vwap_score=round(vwap_score, 2),
        notes=notes,
    )


def build_supply_demand_zones(candles: pd.DataFrame, config: SupplyDemandStrategyConfig | None = None) -> list[SupplyDemandZone]:
    cfg = config or SupplyDemandStrategyConfig()
    frame = _enrich_frame(normalize_ohlcv_for_supply_demand(candles))
    return [score_supply_demand_zone(frame, structure, cfg) for structure in detect_supply_demand_structures(frame, cfg)]

def _entry_candle(frame: pd.DataFrame, zone: SupplyDemandZone) -> pd.Series:
    for idx in range(zone.base_end_index + 1, len(frame)):
        row = frame.iloc[idx]
        if float(row["low"]) <= float(zone.zone_high) and float(row["high"]) >= float(zone.zone_low):
            return row
    return frame.iloc[min(zone.base_end_index + 1, len(frame) - 1)]


def _rejection_strength(row: pd.Series, side: str) -> float:
    candle_range = max(float(row["high"] - row["low"]), 1e-6)
    body_ratio = abs(float(row["close"] - float(row["open"]))) / candle_range
    lower_wick = max(min(float(row["open"]), float(row["close"])) - float(row["low"]), 0.0) / candle_range
    upper_wick = max(float(row["high"]) - max(float(row["open"]), float(row["close"])), 0.0) / candle_range
    if side == "BUY":
        return body_ratio * 60.0 + lower_wick * 40.0
    return body_ratio * 60.0 + upper_wick * 40.0


def _trade_side(zone: SupplyDemandZone) -> str:
    return "BUY" if zone.type == "demand" else "SELL"


def validate_supply_demand_trade(
    zone: SupplyDemandZone,
    candles: pd.DataFrame,
    config: SupplyDemandStrategyConfig | None = None,
    *,
    traded_zone_ids: set[str] | None = None,
    last_trade_at: datetime | None = None,
) -> ValidationDecision:
    cfg = config or SupplyDemandStrategyConfig()
    frame = _enrich_frame(normalize_ohlcv_for_supply_demand(candles))
    row = _entry_candle(frame, zone)
    side = _trade_side(zone)
    reasons: list[str] = []
    rejection_strength = _rejection_strength(row, side)
    entry_time = pd.Timestamp(row["timestamp"]).to_pydatetime()
    if zone.total_score < float(cfg.min_total_score):
        reasons.append("weak_zone_score")
    if zone.violated:
        reasons.append("violated_zone")
    if zone.test_count > int(cfg.max_retests):
        reasons.append("max_retests_exceeded")
    if zone.departure_ratio < float(cfg.min_departure_ratio):
        reasons.append("weak_departure")
    if entry_time.time() > _parse_time(str(cfg.entry_cutoff_time)):
        reasons.append("entry_after_cutoff")
    if rejection_strength < (float(cfg.min_rejection_body_ratio) * 100.0):
        reasons.append("rejection_candle_weak")
    if traded_zone_ids and zone.zone_id in traded_zone_ids:
        reasons.append("duplicate_zone")
    if last_trade_at is not None and (entry_time - last_trade_at) < timedelta(minutes=int(cfg.cooldown_minutes)):
        reasons.append("duplicate_zone")
    if bool(cfg.use_session_filter):
        session_value = str(row.get("session", "") or "").upper()
        allowed = {str(item).upper() for item in cfg.allowed_sessions}
        if session_value not in allowed:
            reasons.append("session_filter_failed")
    if bool(cfg.use_vwap_filter) and "vwap" in frame.columns:
        if side == "BUY" and float(row["close"]) < float(row["vwap"]):
            reasons.append("vwap_alignment_failed")
        if side == "SELL" and float(row["close"]) > float(row["vwap"]):
            reasons.append("vwap_alignment_failed")
    if bool(cfg.use_trend_filter):
        ema20 = float(row.get("ema_20", row["close"]))
        ema50 = float(row.get("ema_50", row["close"]))
        if side == "BUY" and ema20 < ema50:
            reasons.append("trend_alignment_failed")
        if side == "SELL" and ema20 > ema50:
            reasons.append("trend_alignment_failed")
    if bool(cfg.use_volatility_filter) and float(row.get("atr_pct", 0.0)) < float(cfg.volatility_floor_pct):
        reasons.append("weak_departure")
    stop_buffer = max(abs(zone.zone_high - zone.zone_low) * float(cfg.stop_buffer_pct), float(row.get("atr_14", 0.0)) * float(cfg.stop_buffer_pct), 0.01)
    entry_price = float(row["close"])
    if side == "BUY":
        stop_loss = float(zone.zone_low) - stop_buffer
        target_price = entry_price + (entry_price - stop_loss) * float(cfg.min_rr_ratio)
    else:
        stop_loss = float(zone.zone_high) + stop_buffer
        target_price = entry_price - (stop_loss - entry_price) * float(cfg.min_rr_ratio)
    risk_per_unit = abs(entry_price - stop_loss)
    rr_ratio = abs(target_price - entry_price) / max(risk_per_unit, 1e-6)
    if rr_ratio < float(cfg.min_rr_ratio):
        reasons.append("invalid_rr")
    metrics = {
        "entry_timestamp": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
        "entry_price": round(entry_price, 4),
        "stop_loss": round(stop_loss, 4),
        "target_price": round(target_price, 4),
        "risk_per_unit": round(risk_per_unit, 4),
        "rr_ratio": round(rr_ratio, 4),
        "rejection_strength": round(rejection_strength, 2),
        "zone_score": round(zone.total_score, 2),
    }
    validation_score = max(0.0, round(zone.validation_score - len(reasons) * 12.5, 2))
    return ValidationDecision(len(reasons) == 0, reasons, validation_score, len(reasons) == 0, metrics)


def _position_size(capital: float, risk_pct: float, risk_per_unit: float) -> int:
    if capital <= 0 or risk_pct <= 0 or risk_per_unit <= 0:
        return 0
    return max(int((capital * risk_pct) / risk_per_unit), 0)


def _existing_zone_ids(rows: Sequence[dict[str, Any]] | None) -> set[str]:
    if not rows:
        return set()
    return {str(row.get("zone_id", "") or "").strip() for row in rows if str(row.get("zone_id", "") or "").strip()}


def _latest_trade_timestamp(rows: Sequence[dict[str, Any]] | None) -> datetime | None:
    if not rows:
        return None
    parsed: list[datetime] = []
    for row in rows:
        raw = row.get("trade_time") or row.get("timestamp") or row.get("signal_time") or row.get("entry_time")
        stamp = pd.to_datetime(raw, errors="coerce")
        if not pd.isna(stamp):
            parsed.append(pd.Timestamp(stamp).to_pydatetime())
    return max(parsed) if parsed else None

def generate_supply_demand_trade_candidates(
    candles: pd.DataFrame,
    config: SupplyDemandStrategyConfig | None = None,
    *,
    existing_trade_rows: Sequence[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[SupplyDemandZone]]:
    cfg = config or SupplyDemandStrategyConfig()
    frame = _enrich_frame(normalize_ohlcv_for_supply_demand(candles))
    zones = build_supply_demand_zones(frame, cfg)
    traded_zone_ids = _existing_zone_ids(existing_trade_rows)
    last_trade_at = _latest_trade_timestamp(existing_trade_rows)
    trades: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    for zone in zones:
        validation = validate_supply_demand_trade(zone, frame, cfg, traded_zone_ids=traded_zone_ids, last_trade_at=last_trade_at)
        if not validation.is_valid:
            rejects.append({
                "zone_id": zone.zone_id,
                "symbol": zone.symbol,
                "zone_type": zone.type,
                "validation_score": validation.validation_score,
                "reasons": list(validation.rejection_reasons),
                "reason_text": ", ".join(_REJECTION_REASON_TEXT.get(reason, reason.replace("_", " ")) for reason in validation.rejection_reasons),
            })
            continue
        quantity = _position_size(float(cfg.capital), float(cfg.risk_pct), float(validation.metrics["risk_per_unit"]))
        if quantity <= 0:
            rejects.append({
                "zone_id": zone.zone_id,
                "symbol": zone.symbol,
                "zone_type": zone.type,
                "validation_score": validation.validation_score,
                "reasons": ["invalid_rr"],
                "reason_text": "quantity sizing produced zero quantity",
            })
            continue
        side = _trade_side(zone)
        timestamp_text = str(validation.metrics["entry_timestamp"])
        candidate = {
            "trade_id": f"{zone.zone_id}_{side}_{timestamp_text.replace(':', '').replace(' ', '_')}",
            "symbol": zone.symbol,
            "timestamp": timestamp_text,
            "trade_time": timestamp_text,
            "strategy_name": str(cfg.strategy_name),
            "strategy": str(cfg.strategy_name),
            "zone_id": zone.zone_id,
            "setup_type": str(zone.structure_type),
            "side": side,
            "entry_price": float(validation.metrics["entry_price"]),
            "entry": float(validation.metrics["entry_price"]),
            "stop_loss": float(validation.metrics["stop_loss"]),
            "stoploss": float(validation.metrics["stop_loss"]),
            "target_price": float(validation.metrics["target_price"]),
            "target": float(validation.metrics["target_price"]),
            "risk_per_unit": float(validation.metrics["risk_per_unit"]),
            "quantity": int(quantity),
            "capital": float(cfg.capital),
            "risk_pct": float(cfg.risk_pct),
            "rr_ratio": float(validation.metrics["rr_ratio"]),
            "validation_score": float(validation.validation_score),
            "validation_status": "PASS",
            "validation_reasons": [],
            "execution_allowed": True,
            "notes": f"{zone.type} zone from {zone.structure_type} scored {zone.total_score:.2f}",
            "zone_low": float(zone.zone_low),
            "zone_high": float(zone.zone_high),
            "zone_type": zone.type,
            "total_score": float(zone.total_score),
            "strength_score": float(zone.strength_score),
            "freshness_score": float(zone.freshness_score),
            "cleanliness_score": float(zone.cleanliness_score),
        }
        trades.append(candidate)
        traded_zone_ids.add(zone.zone_id)
        last_trade_at = pd.Timestamp(timestamp_text).to_pydatetime()
    return trades, rejects, zones


def evaluate_supply_demand_readiness(
    trades: Sequence[dict[str, Any]],
    rejected_rows: Sequence[dict[str, Any]],
    *,
    min_trades: int = 100,
    max_drawdown_threshold: float = 10.0,
) -> dict[str, Any]:
    metrics = compute_trade_metrics(list(trades))
    rejection_counts = Counter()
    invalid_schema_count = 0
    for row in rejected_rows:
        reasons = row.get("reasons", [])
        if isinstance(reasons, str):
            reasons = [part.strip() for part in reasons.split(",") if part.strip()]
        for reason in reasons:
            rejection_counts[str(reason)] += 1
        if "invalid_schema" in reasons:
            invalid_schema_count += 1
    readiness = evaluate_production_readiness(metrics)
    duplicate_trade_rate = 0.0 if not trades else round(sum(1 for row in trades if str(row.get("duplicate_reason", "")).strip()) / len(trades), 4)
    passed = (
        int(metrics.get("total_trades", 0)) >= int(min_trades)
        and float(metrics.get("expectancy", 0.0)) > 0.0
        and float(metrics.get("profit_factor", 0.0)) > 1.3
        and float(metrics.get("max_drawdown", 0.0)) <= float(max_drawdown_threshold)
        and duplicate_trade_rate == 0.0
        and invalid_schema_count == 0
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "readiness": readiness,
        "total_trades": int(metrics.get("total_trades", 0)),
        "win_rate": float(metrics.get("win_rate", 0.0)),
        "average_rr": float(metrics.get("avg_r_multiple", 0.0)),
        "expectancy": float(metrics.get("expectancy", 0.0)),
        "profit_factor": float(metrics.get("profit_factor", 0.0)),
        "gross_profit": round(sum(max(_safe_float(row.get("pnl", 0.0)), 0.0) for row in trades), 2),
        "gross_loss": round(abs(sum(min(_safe_float(row.get("pnl", 0.0)), 0.0) for row in trades)), 2),
        "max_drawdown": float(metrics.get("max_drawdown", 0.0)),
        "average_hold_time": 0.0,
        "rejected_trades_count": int(len(rejected_rows)),
        "validation_fail_counts": dict(rejection_counts),
        "duplicate_trade_rate": duplicate_trade_rate,
        "invalid_schema_count": invalid_schema_count,
        "summary": "PASS: supply and demand strategy meets current paper-readiness thresholds." if passed else "FAIL: supply and demand strategy is still weak or unsafe for paper promotion.",
    }


def build_supply_demand_report(
    candles: pd.DataFrame,
    config: SupplyDemandStrategyConfig | None = None,
    *,
    existing_trade_rows: Sequence[dict[str, Any]] | None = None,
    executed_trade_rows: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cfg = config or SupplyDemandStrategyConfig()
    trades, rejects, zones = generate_supply_demand_trade_candidates(candles, cfg, existing_trade_rows=existing_trade_rows)
    readiness = evaluate_supply_demand_readiness(executed_trade_rows or trades, rejects)
    return {
        "zone_rows": [zone.to_dict() for zone in zones],
        "trade_rows": trades,
        "rejection_summary": dict(Counter(reason for row in rejects for reason in row.get("reasons", []))),
        "readiness_summary": readiness,
    }


def generate_trades(df: pd.DataFrame, capital: float, risk_pct: float, rr_ratio: float, config: SupplyDemandStrategyConfig | None = None) -> list[dict[str, Any]]:
    cfg = config or SupplyDemandStrategyConfig()
    cfg.capital = float(capital)
    cfg.risk_pct = float(risk_pct)
    cfg.min_rr_ratio = float(rr_ratio)
    trades, _rejects, _zones = generate_supply_demand_trade_candidates(df, cfg)
    return trades


__all__ = [
    "REQUIRED_FIELDS",
    "SupplyDemandStrategyConfig",
    "SupplyDemandZone",
    "StructureRecord",
    "ValidationDecision",
    "build_supply_demand_report",
    "build_supply_demand_zones",
    "detect_supply_demand_structures",
    "evaluate_supply_demand_readiness",
    "generate_supply_demand_trade_candidates",
    "generate_trades",
    "normalize_ohlcv_for_supply_demand",
    "score_supply_demand_zone",
    "validate_supply_demand_trade",
]
