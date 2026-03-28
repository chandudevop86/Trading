from __future__ import annotations

import csv
import hashlib
import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Optional, Tuple

from src.brokers import (
    Broker,
    BrokerConfigurationError,
    BrokerExecutionError,
    BrokerOrderRequest,
    BrokerOrderResult,
    DhanBroker,
    DhanBrokerConfig,
    PaperBroker,
    PaperBrokerConfig,
    TradeCandidate,
)
from src.csv_io import read_csv_rows
from src.runtime_config import RuntimeConfig
from src.trading_core import append_log
from src.strategy_tuning import normalize_strategy_key, strategy_tuning_preset
from src.runtime_persistence import (
    load_current_rows,
    load_execution_risk_state,
    load_latest_batch_rows,
    persist_row,
    persist_rows,
    refresh_execution_risk_state,
)

DEFAULT_LOT_SIZES = {
    "NIFTY": 65,
    "NIFTY FUT": 65,
    "NIFTY FUTURES": 65,
    "NIFTYFUT": 65,
}

TRADE_STATUS_NEW = "NEW"
TRADE_STATUS_REVIEWED = "REVIEWED"
TRADE_STATUS_PENDING_EXECUTION = "PENDING_EXECUTION"
TRADE_STATUS_EXECUTED = "EXECUTED"
TRADE_STATUS_BLOCKED = "BLOCKED"
TRADE_STATUS_OPEN = "OPEN"
TRADE_STATUS_CLOSED = "CLOSED"
TRADE_STATUS_ERROR = "ERROR"

SKIP_REASON_DUPLICATE_ACTIVE_TRADE = "DUPLICATE_ACTIVE_TRADE"
SKIP_REASON_DUPLICATE_EXECUTED_TRADE = "DUPLICATE_EXECUTED_TRADE"
SKIP_REASON_INVALID_SIDE = "INVALID_SIDE"
SKIP_REASON_MAX_TRADES_PER_DAY = "MAX_TRADES_PER_DAY"
SKIP_REASON_MAX_DAILY_LOSS = "MAX_DAILY_LOSS"
SKIP_REASON_MISSING_PRICE = "MISSING_PRICE"
SKIP_REASON_MISSING_QUANTITY = "MISSING_QUANTITY"
SKIP_REASON_MISSING_TIMESTAMP = "MISSING_TIMESTAMP"
SKIP_REASON_MISSING_STOP_LOSS = "MISSING_STOP_LOSS"
SKIP_REASON_MISSING_TARGET = "MISSING_TARGET"
SKIP_REASON_INVALID_TRADE_LEVELS = "INVALID_TRADE_LEVELS"
SKIP_REASON_DUPLICATE_BATCH_TRADE = "DUPLICATE_BATCH_TRADE"
SKIP_REASON_DUPLICATE_SIGNAL_KEY = "DUPLICATE_SIGNAL_KEY"
SKIP_REASON_DUPLICATE_SIGNAL_COOLDOWN = "DUPLICATE_SIGNAL_COOLDOWN"
SKIP_REASON_BROKER_ERROR = "BROKER_ERROR"
SKIP_REASON_KILL_SWITCH = "KILL_SWITCH_ENABLED"
SKIP_REASON_OPTIMIZER_GATE = "OPTIMIZER_GATE_BLOCKED"
SKIP_REASON_MAX_OPEN_TRADES = "MAX_OPEN_TRADES"

ACTIVE_TRADE_STATUSES = {
    TRADE_STATUS_REVIEWED,
    TRADE_STATUS_PENDING_EXECUTION,
    TRADE_STATUS_EXECUTED,
    TRADE_STATUS_OPEN,
}
EXECUTION_SUCCESS_STATUSES = {"EXECUTED", "SENT", "FILLED"}
RUNTIME_CONFIG = RuntimeConfig.load()
ORDER_HISTORY_OUTPUT = RUNTIME_CONFIG.paths.order_history_csv
OPTIMIZER_REPORT_OUTPUT = RUNTIME_CONFIG.paths.optimizer_report_csv
BROKER_LOG_PATH = RUNTIME_CONFIG.paths.broker_log
EXECUTION_LOG_PATH = RUNTIME_CONFIG.paths.execution_log
REJECTIONS_LOG_PATH = RUNTIME_CONFIG.paths.rejections_log
ERRORS_LOG_PATH = RUNTIME_CONFIG.paths.errors_log
EXECUTION_SCHEMA = [
    "trade_id",
    "trade_key",
    "trade_status",
    "position_status",
    "duplicate_reason",
    "blocked_reason",
    "validation_error",
    "strategy",
    "symbol",
    "timeframe",
    "data_symbol",
    "trade_symbol",
    "trading_symbol",
    "security_id",
    "exchange_segment",
    "instrument_type",
    "signal_time",
    "duplicate_signal_key",
    "side",
    "price",
    "share_price",
    "strike_price",
    "option_expiry",
    "option_type",
    "option_strike",
    "quantity",
    "execution_type",
    "execution_status",
    "executed_at_utc",
    "reviewed_at_utc",
    "analyzed_at_utc",
    "broker_name",
    "broker_order_id",
    "broker_status",
    "broker_message",
    "broker_response_json",
    "risk_limit_reason",
    "pnl",
    "exit_time",
    "exit_price",
    "exit_reason",
]


@dataclass
class ExecutionResult:
    rows: list[dict[str, object]] = field(default_factory=list)
    executed_rows: list[dict[str, object]] = field(default_factory=list)
    blocked_rows: list[dict[str, object]] = field(default_factory=list)
    skipped_rows: list[dict[str, object]] = field(default_factory=list)
    error_rows: list[dict[str, object]] = field(default_factory=list)
    executed_count: int = 0
    blocked_count: int = 0
    skipped_count: int = 0
    duplicate_count: int = 0
    error_count: int = 0

    def __iter__(self):
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index: int) -> dict[str, object]:
        return self.rows[index]

    def __bool__(self) -> bool:
        return bool(self.rows)


def default_quantity_for_symbol(symbol: str) -> int:
    normalized = symbol.strip().upper()
    return int(DEFAULT_LOT_SIZES.get(normalized, 1))


def normalize_order_quantity(symbol: str, quantity: object) -> int:
    lot = default_quantity_for_symbol(symbol)
    try:
        qty = int(float(quantity))
    except (TypeError, ValueError):
        qty = 0

    if qty <= 0:
        return 0
    if lot <= 1:
        return qty

    return max(lot, (qty // lot) * lot)


def _extract_share_and_strike(row: dict[str, object]) -> tuple[object, object]:
    share_price = row.get("entry_price", row.get("entry", row.get("close", row.get("price", ""))))
    strike_price = row.get("strike_price", row.get("option_strike", row.get("strike", "")))
    return share_price, strike_price


def _parse_dt(text: object) -> Optional[datetime]:
    def _normalize(dt: datetime) -> datetime:
        # Normalize offset-aware values to naive UTC so CSV/log timestamps and
        # market-data timestamps remain directly comparable.
        if dt.tzinfo is not None:
            return dt.astimezone(UTC).replace(tzinfo=None)
        return dt

    raw = str(text or "").strip()
    if not raw:
        return None

    try:
        return _normalize(datetime.fromisoformat(raw))
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return _normalize(datetime.strptime(raw, fmt))
        except ValueError:
            continue

    return None


def _safe_float(value: object) -> float:
    try:
        if value is None or str(value).strip() == "":
            return 0.0
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _normalize_text(value: object) -> str:
    return str(value or "").strip().upper()


def _price_value(record: dict[str, object]) -> float:
    for key in ("entry_price", "entry", "price", "share_price", "close", "spot_ltp"):
        value = _safe_float(record.get(key))
        if value > 0:
            return value
    return 0.0


def _timeframe_key(record: dict[str, object]) -> str:
    return str(record.get("timeframe", record.get("interval", "")) or "").strip().lower() or "na"


def _signal_time_text(record: dict[str, object]) -> str:
    return str(record.get("signal_time", record.get("entry_time", record.get("timestamp", ""))) or "").strip()


def make_duplicate_signal_key(record: dict[str, object]) -> str:
    strategy = _normalize_text(record.get("strategy", "TRADE_BOT"))
    symbol = _normalize_text(record.get("symbol", "UNKNOWN"))
    timeframe = _timeframe_key(record)
    signal_time = _signal_time_text(record)
    side = _normalize_text(record.get("side"))
    return "|".join([strategy, symbol, timeframe, signal_time, side])


def _cooldown_group_key(record: dict[str, object]) -> str:
    strategy = _normalize_text(record.get("strategy", "TRADE_BOT"))
    symbol = _normalize_text(record.get("symbol", "UNKNOWN"))
    timeframe = _timeframe_key(record)
    side = _normalize_text(record.get("side"))
    return "|".join([strategy, symbol, timeframe, side])


def _trade_cooldown_group_key(record: dict[str, object]) -> str:
    strategy = _normalize_text(record.get("strategy", "TRADE_BOT"))
    symbol = _normalize_text(record.get("symbol", "UNKNOWN"))
    timeframe = _timeframe_key(record)
    return "|".join([strategy, symbol, timeframe])



def _infer_timeframe_minutes(value: object) -> int:
    raw = str(value or "").strip().lower()
    mapping = {
        "1m": 1,
        "3m": 3,
        "5m": 5,
        "10m": 10,
        "15m": 15,
        "30m": 30,
        "45m": 45,
        "1h": 60,
        "2h": 120,
        "4h": 240,
        "1d": 1440,
    }
    return int(mapping.get(raw, 0))


def _cooldown_window_seconds(record: dict[str, object]) -> int:
    explicit_minutes = _safe_float(record.get("duplicate_signal_cooldown_minutes"))
    if explicit_minutes > 0:
        return int(explicit_minutes * 60)
    bars = int(_safe_float(record.get("duplicate_signal_cooldown_bars")))
    timeframe_minutes = _infer_timeframe_minutes(record.get("timeframe", record.get("interval")))
    if bars > 0 and timeframe_minutes > 0:
        return int(bars * timeframe_minutes * 60)
    preset = strategy_tuning_preset(str(record.get("strategy", "") or ""))
    if int(getattr(preset, "duplicate_cooldown_minutes", 0) or 0) > 0:
        return int(preset.duplicate_cooldown_minutes) * 60
    return 0

def _effective_max_trades_per_day(record: dict[str, object], explicit_limit: int | None) -> int | None:
    if explicit_limit is not None and int(explicit_limit) > 0:
        return int(explicit_limit)
    record_limit = int(_safe_float(record.get("max_trades_per_day")))
    if record_limit > 0:
        return record_limit
    preset = strategy_tuning_preset(str(record.get("strategy", "") or ""))
    if int(getattr(preset, "max_trades_per_day", 0) or 0) > 0:
        return int(preset.max_trades_per_day)
    return None


def make_trade_key(record: dict[str, object]) -> str:
    strategy = _normalize_text(record.get("strategy", "TRADE_BOT"))
    symbol = _normalize_text(record.get("symbol", "UNKNOWN"))
    signal_time = _signal_time_text(record)
    side = _normalize_text(record.get("side"))
    entry_price = f"{_price_value(record):.6f}"
    instrument = _normalize_text(
        record.get("trading_symbol")
        or record.get("contract_symbol")
        or record.get("option_strike")
        or record.get("strike_price")
        or symbol
    )
    payload = "|".join([strategy, symbol, _timeframe_key(record), signal_time, side, entry_price, instrument])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def make_trade_id(record: dict[str, object]) -> str:
    existing = str(record.get("trade_id", "") or "").strip()
    if existing:
        return existing
    strategy = _normalize_text(record.get("strategy", "TRADE_BOT"))
    symbol = _normalize_text(record.get("symbol", "UNKNOWN"))
    signal_time = _signal_time_text(record)
    side = _normalize_text(record.get("side"))
    entry_price = f"{_price_value(record):.6f}"
    option_key = _normalize_text(record.get("option_strike") or record.get("trading_symbol") or record.get("strike_price"))
    payload = "|".join([strategy, symbol, _timeframe_key(record), signal_time, side, entry_price, option_key])
    return str(uuid.uuid5(uuid.NAMESPACE_URL, payload))


def execution_candidate_key(record: dict[str, object]) -> str:
    return make_trade_key(record)


def _ensure_trade_identity(record: dict[str, object], *, default_status: str | None = None) -> dict[str, object]:
    normalized = dict(record)
    normalized.setdefault("strategy", str(normalized.get("strategy", "TRADE_BOT") or "TRADE_BOT"))
    normalized.setdefault("symbol", str(normalized.get("symbol", "UNKNOWN") or "UNKNOWN"))
    normalized.setdefault("timeframe", str(normalized.get("timeframe", normalized.get("interval", "")) or ""))
    normalized.setdefault("signal_time", _signal_time_text(normalized))
    normalized["duplicate_signal_key"] = str(normalized.get("duplicate_signal_key", "") or make_duplicate_signal_key(normalized))
    normalized["trade_key"] = str(normalized.get("trade_key", "") or make_trade_key(normalized))
    normalized["trade_id"] = make_trade_id(normalized)
    if default_status and not str(normalized.get("trade_status", "") or "").strip():
        normalized["trade_status"] = default_status
    return normalized


def build_execution_candidates(strategy: str, output_rows: list[dict[str, object]], symbol: str) -> list[dict[str, object]]:
    symbol = symbol.strip() or "UNKNOWN"
    candidates: list[dict[str, object]] = []
    if not output_rows:
        return candidates

    if strategy == "Indicator (RSI/ADX/MACD+VWAP)":
        last = output_rows[-1]
        signal = str(last.get("market_signal", "NEUTRAL")).strip().upper()
        side = "HOLD"
        if signal in {"BULLISH_TREND", "OVERSOLD", "LONG", "BUY"}:
            side = "BUY"
        elif signal in {"BEARISH_TREND", "OVERBOUGHT", "SHORT", "SELL"}:
            side = "SELL"
        close_price = _price_value(last)
        stop_loss = _safe_float(last.get("stop_loss"))
        target_price = _safe_float(last.get("target_price", last.get("target")))
        if side == "BUY":
            stop_loss = stop_loss if stop_loss > 0 else round(close_price * 0.995, 4)
            target_price = target_price if target_price > 0 else round(close_price + ((close_price - stop_loss) * 2.0), 4)
        elif side == "SELL":
            stop_loss = stop_loss if stop_loss > 0 else round(close_price * 1.005, 4)
            target_price = target_price if target_price > 0 else round(close_price - ((stop_loss - close_price) * 2.0), 4)
        candidates.append(_ensure_trade_identity({
            "strategy": "INDICATOR",
            "symbol": symbol,
            "timestamp": str(last.get("timestamp", "")),
            "signal_time": str(last.get("timestamp", "")),
            "timeframe": str(last.get("timeframe", last.get("interval", "")) or ""),
            "side": side,
            "price": close_price,
            "entry": close_price,
            "entry_price": close_price,
            "stop_loss": stop_loss,
            "target": target_price,
            "target_price": target_price,
            "share_price": close_price,
            "strike_price": last.get("strike_price"),
            "score": _safe_float(last.get("score")) or 0.0,
            "quantity": default_quantity_for_symbol(symbol),
            "reason": signal,
            "duplicate_signal_cooldown_bars": last.get("duplicate_signal_cooldown_bars", 0),
            "duplicate_signal_cooldown_minutes": last.get("duplicate_signal_cooldown_minutes", ""),
        }, default_status=TRADE_STATUS_NEW))
        return candidates

    for row in output_rows:
        share_price, strike_price = _extract_share_and_strike(row)
        option_type = str(row.get("option_type", "")).strip().upper()
        if not option_type:
            side_val = str(row.get("side", row.get("type", ""))).strip().upper()
            if side_val == "BUY":
                option_type = "CE"
            elif side_val == "SELL":
                option_type = "PE"
        option_strike = str(row.get("option_strike", "")).strip()
        if not option_strike and option_type and strike_price not in {None, ""}:
            try:
                option_strike = f"{int(float(str(strike_price)))}{option_type}"
            except Exception:
                option_strike = f"{strike_price}{option_type}"
        entry_value = row.get("entry", row.get("entry_price", row.get("price", row.get("close", ""))))
        stop_value = row.get("stop_loss", row.get("sl", ""))
        target_value = row.get("target", row.get("target_price", row.get("tp", "")))
        candidates.append(_ensure_trade_identity({
            "strategy": str(row.get("strategy", "TRADE_BOT")),
            "symbol": symbol,
            "timestamp": str(row.get("timestamp", row.get("entry_time", row.get("time", "")))),
            "signal_time": str(row.get("entry_time", row.get("timestamp", row.get("time", "")))),
            "timeframe": str(row.get("timeframe", row.get("interval", "")) or ""),
            "side": str(row.get("side", row.get("type", "HOLD"))),
            "price": row.get("price", entry_value),
            "entry": entry_value,
            "entry_price": row.get("entry_price", entry_value),
            "share_price": share_price,
            "strike_price": strike_price,
            "option_expiry": row.get("option_expiry", row.get("expiry_date", "")),
            "option_expiry_source": row.get("option_expiry_source", ""),
            "option_type": option_type,
            "option_strike": option_strike,
            "trade_no": row.get("trade_no", ""),
            "trade_label": row.get("trade_label", ""),
            "target_1": row.get("target_1", ""),
            "target_2": row.get("target_2", ""),
            "target_3": row.get("target_3", ""),
            "spot_ltp": row.get("spot_ltp", row.get("close", row.get("share_price", ""))),
            "option_ltp": row.get("option_ltp", ""),
            "lots": row.get("lots", ""),
            "order_value": row.get("order_value", ""),
            "stop_loss": stop_value,
            "trailing_stop_loss": row.get("trailing_stop_loss", ""),
            "target": target_value,
            "target_price": row.get("target_price", target_value),
            "score": _safe_float(row.get("score", row.get("total_score"))) or 0.0,
            "quantity": row.get("quantity", default_quantity_for_symbol(symbol)),
            "duplicate_signal_cooldown_bars": row.get("duplicate_signal_cooldown_bars", 0),
            "duplicate_signal_cooldown_minutes": row.get("duplicate_signal_cooldown_minutes", ""),
            "reason": (
                f"{str(row.get('reason', '') or '').strip()} | "
                f"SL:{stop_value} TSL:{row.get('trailing_stop_loss', '')} TP:{row.get('target_price', target_value)}"
            ).strip(' |') or f"SL:{stop_value} TSL:{row.get('trailing_stop_loss', '')} TP:{row.get('target_price', target_value)}",
        }, default_status=TRADE_STATUS_NEW))
    return candidates


def build_analysis_queue(candidates: list[dict[str, object]], analyzed_at_utc: Optional[str] = None) -> list[dict[str, object]]:
    actionable = [c for c in candidates if _normalize_text(c.get("side")) in {"BUY", "SELL"}]
    if not actionable:
        return []
    stamp = analyzed_at_utc or datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    analyzed: list[dict[str, object]] = []
    for candidate in actionable:
        row = _ensure_trade_identity(candidate, default_status=TRADE_STATUS_REVIEWED)
        row["analysis_status"] = "ANALYZED"
        row["analyzed_at_utc"] = stamp
        row["reviewed_at_utc"] = stamp
        row["execution_ready"] = "YES"
        row["trade_status"] = TRADE_STATUS_REVIEWED
        analyzed.append(row)
    return analyzed


def _read_trade_rows(path: Path) -> list[dict[str, object]]:
    db_rows = load_current_rows(path)
    if db_rows:
        return [dict(row) for row in db_rows]
    if not path.exists():
        return []
    return [dict(row) for row in read_csv_rows(path)]
def _row_is_closed(row: dict[str, object]) -> bool:
    return _normalize_text(row.get("trade_status")) == TRADE_STATUS_CLOSED or _normalize_text(row.get("position_status")) == TRADE_STATUS_CLOSED or _normalize_text(row.get("execution_status")) in {"CLOSED", "EXITED"}


def _row_is_active(row: dict[str, object]) -> bool:
    if _row_is_closed(row):
        return False
    if _normalize_text(row.get("trade_status")) in ACTIVE_TRADE_STATUSES:
        return True
    return _normalize_text(row.get("execution_status")) in {"EXECUTED", "SENT", "FILLED"}


def _load_risk_state_snapshot(path: Path, execution_type: str | None = None) -> dict[str, object]:
    normalized_execution_type = _normalize_text(execution_type or '')
    if not normalized_execution_type:
        return {}
    try:
        state = load_execution_risk_state(path, normalized_execution_type)
    except Exception:
        return {}
    if not isinstance(state, dict):
        return {}
    has_state = any(
        [
            state.get('historical_trade_ids'),
            state.get('active_trade_keys'),
            state.get('active_duplicate_signal_keys'),
            state.get('recent_signal_times'),
            state.get('daily_state'),
            int(float(state.get('open_trade_count', 0) or 0)) > 0,
        ]
    )
    return state if has_state else {}


def _deserialize_recent_signal_times(raw: object) -> dict[str, datetime]:
    if not isinstance(raw, dict):
        return {}
    recent: dict[str, datetime] = {}
    for key, value in raw.items():
        parsed = _parse_dt(value)
        if parsed is not None:
            recent[str(key)] = parsed
    return recent


def _deserialize_daily_state(raw: object) -> dict[str, dict[str, float]]:
    if not isinstance(raw, dict):
        return {}
    state: dict[str, dict[str, float]] = {}
    for day_key, payload in raw.items():
        if not isinstance(payload, dict):
            continue
        state[str(day_key)] = {
            "count": float(payload.get("count", 0.0) or 0.0),
            "realized_pnl": float(payload.get("realized_pnl", 0.0) or 0.0),
        }
    return state


def load_active_trade_keys(path: Path, execution_type: str | None = None) -> set[str]:
    state = _load_risk_state_snapshot(path, execution_type)
    if state:
        return {str(value) for value in state.get("active_trade_keys", []) if str(value).strip()}

    keys: set[str] = set()
    for row in _read_trade_rows(path):
        if execution_type and _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        if not _row_is_active(row):
            continue
        keys.add(str(row.get("trade_key", "") or make_trade_key(row)))
    return keys


def load_active_duplicate_signal_keys(path: Path, execution_type: str | None = None) -> set[str]:
    state = _load_risk_state_snapshot(path, execution_type)
    if state:
        return {str(value) for value in state.get("active_duplicate_signal_keys", []) if str(value).strip()}

    keys: set[str] = set()
    for row in _read_trade_rows(path):
        if execution_type and _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        if not _row_is_active(row):
            continue
        keys.add(str(row.get("duplicate_signal_key", "") or make_duplicate_signal_key(row)))
    return keys


def _load_recent_signal_times(path: Path, execution_type: str | None = None) -> dict[str, datetime]:
    state = _load_risk_state_snapshot(path, execution_type)
    if state:
        return _deserialize_recent_signal_times(state.get("recent_signal_times", {}))

    recent: dict[str, datetime] = {}
    for row in _read_trade_rows(path):
        if execution_type and _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        signal_time = _parse_dt(row.get("signal_time")) or _parse_dt(row.get("entry_time")) or _parse_dt(row.get("timestamp"))
        if signal_time is None:
            continue
        group_key = _cooldown_group_key(row)
        current = recent.get(group_key)
        if current is None or signal_time > current:
            recent[group_key] = signal_time
    return recent



def _load_recent_trade_times(path: Path, execution_type: str | None = None) -> dict[str, datetime]:
    recent: dict[str, datetime] = {}
    for row in _read_trade_rows(path):
        if execution_type and _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        signal_time = _parse_dt(row.get("signal_time")) or _parse_dt(row.get("entry_time")) or _parse_dt(row.get("timestamp"))
        if signal_time is None:
            continue
        group_key = _trade_cooldown_group_key(row)
        current = recent.get(group_key)
        if current is None or signal_time > current:
            recent[group_key] = signal_time
    return recent

def _count_open_trades(path: Path, execution_type: str | None = None) -> int:
    state = _load_risk_state_snapshot(path, execution_type)
    if state:
        return int(float(state.get("open_trade_count", 0) or 0))

    count = 0
    for row in _read_trade_rows(path):
        if execution_type and _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        if _row_is_active(row):
            count += 1
    return count


def _load_historical_trade_ids(path: Path, execution_type: str | None = None) -> set[str]:
    state = _load_risk_state_snapshot(path, execution_type)
    if state:
        return {str(value) for value in state.get("historical_trade_ids", []) if str(value).strip()}

    ids: set[str] = set()
    for row in _read_trade_rows(path):
        if execution_type and _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        ids.add(str(row.get("trade_id", "") or make_trade_id(row)))
    return ids



def _duplicate_reason_for_candidate(
    record: dict[str, object],
    *,
    historical_trade_ids: set[str],
    active_trade_keys: set[str],
    active_duplicate_signal_keys: set[str],
    recent_signal_times: dict[str, datetime],
    batch_seen_trade_ids: set[str] | None = None,
    batch_seen_trade_keys: set[str] | None = None,
    batch_seen_signal_keys: set[str] | None = None,
    batch_recent_signal_times: dict[str, datetime] | None = None,
) -> str:
    trade_id = str(record.get("trade_id", "") or "")
    trade_key = str(record.get("trade_key", "") or make_trade_key(record))
    duplicate_signal_key = str(record.get("duplicate_signal_key", "") or make_duplicate_signal_key(record))
    signal_time = _parse_dt(record.get("signal_time"))
    cooldown_group = _cooldown_group_key(record)
    cooldown_seconds = _cooldown_window_seconds(record)

    if batch_seen_trade_ids is not None and trade_id and trade_id in batch_seen_trade_ids:
        return SKIP_REASON_DUPLICATE_BATCH_TRADE
    if batch_seen_trade_keys is not None and trade_key and trade_key in batch_seen_trade_keys:
        return SKIP_REASON_DUPLICATE_BATCH_TRADE
    if batch_seen_signal_keys is not None and duplicate_signal_key and duplicate_signal_key in batch_seen_signal_keys:
        return SKIP_REASON_DUPLICATE_SIGNAL_KEY
    if trade_id and trade_id in historical_trade_ids:
        return SKIP_REASON_DUPLICATE_EXECUTED_TRADE
    if trade_key and trade_key in active_trade_keys:
        return SKIP_REASON_DUPLICATE_ACTIVE_TRADE
    if duplicate_signal_key and duplicate_signal_key in active_duplicate_signal_keys:
        return SKIP_REASON_DUPLICATE_SIGNAL_KEY

    previous_signal_time = None
    if batch_recent_signal_times is not None:
        previous_signal_time = batch_recent_signal_times.get(cooldown_group)
    if previous_signal_time is None:
        previous_signal_time = recent_signal_times.get(cooldown_group)
    if cooldown_seconds > 0 and signal_time is not None and previous_signal_time is not None:
        if (signal_time - previous_signal_time).total_seconds() < cooldown_seconds:
            return SKIP_REASON_DUPLICATE_SIGNAL_COOLDOWN
    return ""
def filter_unlogged_candidates(candidates: list[dict[str, object]], output_path: Path) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    historical_trade_ids = _load_historical_trade_ids(output_path)
    active_trade_keys = load_active_trade_keys(output_path)
    active_duplicate_signal_keys = load_active_duplicate_signal_keys(output_path)
    recent_signal_times = _load_recent_signal_times(output_path)
    batch_seen_trade_ids: set[str] = set()
    batch_seen_trade_keys: set[str] = set()
    batch_seen_signal_keys: set[str] = set()
    batch_recent_signal_times: dict[str, datetime] = {}
    batch_recent_trade_times: dict[str, datetime] = {}
    batch_recent_trade_times: dict[str, datetime] = {}
    fresh: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    for candidate in candidates:
        row = _ensure_trade_identity(candidate)
        reason = _duplicate_reason_for_candidate(
            row,
            historical_trade_ids=historical_trade_ids,
            active_trade_keys=active_trade_keys,
            active_duplicate_signal_keys=active_duplicate_signal_keys,
            recent_signal_times=recent_signal_times,
            batch_seen_trade_ids=batch_seen_trade_ids,
            batch_seen_trade_keys=batch_seen_trade_keys,
            batch_seen_signal_keys=batch_seen_signal_keys,
            batch_recent_signal_times=batch_recent_signal_times,
        )
        if reason:
            flagged = dict(row)
            flagged['duplicate_reason'] = reason
            skipped.append(flagged)
            continue
        batch_seen_trade_ids.add(str(row.get('trade_id', '') or ''))
        batch_seen_trade_keys.add(str(row.get('trade_key', '') or ''))
        batch_seen_signal_keys.add(str(row.get('duplicate_signal_key', '') or ''))
        signal_time = _parse_dt(row.get('signal_time'))
        if signal_time is not None:
            batch_recent_signal_times[_cooldown_group_key(row)] = signal_time
        fresh.append(row)
    return fresh, skipped
def _trade_day_key(record: dict[str, object], fallback_day: str) -> str:
    ts = _parse_dt(record.get("signal_time")) or _parse_dt(record.get("entry_time")) or _parse_dt(record.get("timestamp"))
    if ts is None:
        return fallback_day
    return ts.strftime("%Y-%m-%d")


def _load_daily_execution_state(path: Path, execution_type: str) -> dict[str, dict[str, float]]:
    state = _load_risk_state_snapshot(path, execution_type)
    if state:
        return _deserialize_daily_state(state.get("daily_state", {}))

    daily_state: dict[str, dict[str, float]] = {}
    for row in _read_trade_rows(path):
        if _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        if _row_is_closed(row):
            continue
        if _normalize_text(row.get("execution_status")) not in EXECUTION_SUCCESS_STATUSES and _normalize_text(row.get("trade_status")) not in {TRADE_STATUS_EXECUTED, TRADE_STATUS_OPEN, TRADE_STATUS_PENDING_EXECUTION}:
            continue
        day_key = _trade_day_key(row, str(row.get("executed_at_utc", ""))[:10])
        bucket = daily_state.setdefault(day_key, {"count": 0.0, "realized_pnl": 0.0})
        bucket["count"] += 1.0
        bucket["realized_pnl"] += _safe_float(row.get("pnl"))
    return daily_state


def _risk_limit_message(max_trades_per_day: int | None, max_daily_loss: float | None) -> str:
    parts: list[str] = []
    if max_trades_per_day is not None and int(max_trades_per_day) > 0:
        parts.append(f"max trades/day={int(max_trades_per_day)}")
    if max_daily_loss is not None and float(max_daily_loss) > 0:
        parts.append(f"max daily loss={float(max_daily_loss):.2f}")
    return ", ".join(parts) if parts else "risk limit"


def live_kill_switch_enabled() -> bool:
    raw = str(os.getenv("LIVE_TRADING_KILL_SWITCH", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on", "enabled"}


def _default_live_broker_client() -> tuple[object | None, str]:
    broker_name = str(os.getenv("LIVE_BROKER", "DHAN") or "DHAN").strip().upper() or "DHAN"
    if broker_name != "DHAN":
        return None, broker_name
    try:
        return DhanBroker.from_env(allow_live=True), broker_name
    except Exception:
        return None, broker_name


def _default_security_map() -> dict[str, dict[str, str]]:
    return {}


def _apply_live_broker_result(record: dict[str, object], broker_name: str, result: object) -> None:
    record["broker_name"] = broker_name
    if isinstance(result, dict):
        record["broker_order_id"] = result.get("orderId", result.get("order_id", ""))
        record["broker_status"] = result.get("orderStatus", result.get("status", "SENT"))
        record["broker_message"] = result.get("message", result.get("remarks", result.get("omsErrorDescription", "")))
        try:
            record["broker_response_json"] = json.dumps(result, ensure_ascii=True)
        except Exception:
            record["broker_response_json"] = str(result)
    else:
        record["broker_status"] = "SENT"
        record["broker_message"] = ""


def _first_execution_time(path: Path, execution_type: str) -> Optional[datetime]:
    earliest: Optional[datetime] = None
    sources: list[list[dict[str, object]]] = [_read_trade_rows(path)]
    if path.exists():
        try:
            csv_rows = [dict(row) for row in read_csv_rows(path)]
        except Exception:
            csv_rows = []
        if csv_rows:
            sources.append(csv_rows)
    for rows in sources:
        for row in rows:
            if _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
                continue
            raw = str(row.get("executed_at_utc", "")).strip()
            if not raw:
                continue
            try:
                ts = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            except ValueError:
                continue
            earliest = ts if earliest is None else min(earliest, ts)
    return earliest


def live_trading_unlock_status(paper_log_path: Path, min_days: int = 30, now_utc: Optional[datetime] = None) -> Tuple[bool, int, str]:
    start = _first_execution_time(paper_log_path, "PAPER")
    if start is None:
        return False, 0, ""
    now = now_utc or datetime.now(UTC)
    days = max(0, (now - start).days)
    unlock_date = start.replace(microsecond=0) + timedelta(days=min_days)
    return days >= min_days, days, unlock_date.strftime("%Y-%m-%d %H:%M:%S UTC")


def validate_candidate(candidate: dict[str, object]) -> tuple[bool, str, dict[str, object]]:
    record = _ensure_trade_identity(candidate)
    side = _normalize_text(record.get("side"))
    if side not in {"BUY", "SELL"}:
        record["validation_error"] = SKIP_REASON_INVALID_SIDE
        return False, SKIP_REASON_INVALID_SIDE, record
    signal_time = str(record.get("signal_time", record.get("entry_time", record.get("timestamp", ""))) or "").strip()
    if not signal_time or _parse_dt(signal_time) is None:
        record["validation_error"] = SKIP_REASON_MISSING_TIMESTAMP
        return False, SKIP_REASON_MISSING_TIMESTAMP, record
    price = _price_value(record)
    if price <= 0:
        record["validation_error"] = SKIP_REASON_MISSING_PRICE
        return False, SKIP_REASON_MISSING_PRICE, record
    stop_loss = _safe_float(record.get("stop_loss"))
    target_price = _safe_float(record.get("target_price", record.get("target")))
    if side == "BUY":
        stop_loss = stop_loss if stop_loss > 0 else round(price * 0.995, 4)
        target_price = target_price if target_price > 0 else round(price + ((price - stop_loss) * 2.0), 4)
        if not (stop_loss < price < target_price):
            record["validation_error"] = SKIP_REASON_INVALID_TRADE_LEVELS
            return False, SKIP_REASON_INVALID_TRADE_LEVELS, record
    else:
        stop_loss = stop_loss if stop_loss > 0 else round(price * 1.005, 4)
        target_price = target_price if target_price > 0 else round(price - ((stop_loss - price) * 2.0), 4)
        if not (target_price < price < stop_loss):
            record["validation_error"] = SKIP_REASON_INVALID_TRADE_LEVELS
            return False, SKIP_REASON_INVALID_TRADE_LEVELS, record
    record["price"] = price
    record["entry"] = _safe_float(record.get("entry", price)) or price
    record["entry_price"] = _safe_float(record.get("entry_price", price)) or price
    record["stop_loss"] = stop_loss
    record["target_price"] = target_price
    record.setdefault("target", target_price)
    record.setdefault("share_price", price)
    record.setdefault("timeframe", str(record.get("timeframe", record.get("interval", "")) or ""))
    record["signal_time"] = signal_time
    record.setdefault("timestamp", signal_time)
    record.setdefault("reason", str(record.get("reason", record.get("strategy", "TRADE")) or "TRADE"))
    record.setdefault("strike_price", record.get("option_strike", record.get("strike", "")))
    record["score"] = _safe_float(record.get("score"))
    record["duplicate_signal_key"] = str(record.get("duplicate_signal_key", "") or make_duplicate_signal_key(record))
    raw_quantity = record.get("quantity")
    if raw_quantity is None or str(raw_quantity).strip() == "":
        record["validation_error"] = SKIP_REASON_MISSING_QUANTITY
        return False, SKIP_REASON_MISSING_QUANTITY, record
    quantity = normalize_order_quantity(str(record.get("symbol", "")), raw_quantity)
    if quantity <= 0:
        record["validation_error"] = SKIP_REASON_MISSING_QUANTITY
        return False, SKIP_REASON_MISSING_QUANTITY, record
    record["quantity"] = quantity
    return True, "", record

def _make_result_row(candidate: dict[str, object], *, execution_type: str, processed_at_utc: str) -> dict[str, object]:
    row = _ensure_trade_identity(candidate)
    row.setdefault("execution_type", execution_type)
    row.setdefault("executed_at_utc", processed_at_utc)
    row.setdefault("position_status", "")
    row.setdefault("timeframe", str(row.get("timeframe", row.get("interval", "")) or ""))
    row.setdefault("duplicate_signal_key", str(row.get("duplicate_signal_key", "") or make_duplicate_signal_key(row)))
    row.setdefault("entry", row.get("entry_price", row.get("price", "")))
    row.setdefault("entry_price", row.get("price", row.get("entry", "")))
    row.setdefault("target_price", row.get("target", row.get("target_price", "")))
    row.setdefault("target", row.get("target_price", row.get("target", "")))
    row.setdefault("stop_loss", row.get("stop_loss", ""))
    row.setdefault("score", row.get("score", ""))
    return row
def _json_safe_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    return str(value)

def _append_structured_log(path: Path, event: str, **fields: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "timestamp_utc": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
        "event": event,
    }
    for key, value in fields.items():
        if value is None or value == "":
            continue
        payload[str(key)] = _json_safe_value(value)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")


def _is_live_enabled(explicit_flag: bool | None, broker_client: object | None, broker_name: str | None) -> bool:
    if explicit_flag is not None:
        return bool(explicit_flag)
    if broker_client is not None or str(broker_name or "").strip():
        return True
    raw = str(os.getenv("LIVE_TRADING_ENABLED", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on", "enabled"}


def _normalize_allowlist(symbol_allowlist: list[str] | set[str] | tuple[str, ...] | None) -> set[str]:
    if symbol_allowlist is None:
        raw = str(os.getenv("LIVE_SYMBOL_ALLOWLIST", "") or "").strip()
        if not raw:
            return set()
        symbol_allowlist = [part.strip() for part in raw.split(",") if part.strip()]
    return {str(item).strip().upper() for item in symbol_allowlist if str(item).strip()}


def _candidate_to_trade_candidate(record: dict[str, object], execution_type: str) -> TradeCandidate:
    return TradeCandidate(
        trade_id=str(record.get("trade_id", "") or ""),
        trade_key=str(record.get("trade_key", "") or ""),
        strategy=str(record.get("strategy", "TRADE_BOT") or "TRADE_BOT"),
        symbol=str(record.get("symbol", "UNKNOWN") or "UNKNOWN"),
        side=str(record.get("side", "") or ""),
        quantity=int(_safe_float(record.get("quantity", 0))),
        price=_price_value(record),
        signal_time=str(record.get("signal_time", record.get("entry_time", record.get("timestamp", ""))) or ""),
        reason=str(record.get("reason", record.get("strategy", "TRADE")) or "TRADE"),
        execution_type=execution_type,
        stop_loss=_safe_float(record.get("stop_loss")) or None,
        target=_safe_float(record.get("target_price", record.get("target"))) or None,
        order_type=str(record.get("order_type", "MARKET") or "MARKET"),
        product_type=str(record.get("product_type", "INTRADAY") or "INTRADAY"),
        validity=str(record.get("validity", "DAY") or "DAY"),
        trigger_price=_safe_float(record.get("trigger_price")) or None,
        metadata=dict(record),
    )


def _build_broker_order_request(candidate: TradeCandidate) -> BrokerOrderRequest:
    return BrokerOrderRequest(
        trade_id=candidate.trade_id,
        strategy=candidate.strategy,
        symbol=candidate.symbol,
        side=candidate.side,
        quantity=candidate.quantity,
        order_type=candidate.order_type,
        product_type=candidate.product_type,
        validity=candidate.validity,
        price=candidate.price if candidate.price > 0 else None,
        trigger_price=candidate.trigger_price,
        execution_type=candidate.execution_type,
        metadata=dict(candidate.metadata),
    )


def _coerce_live_broker(*, broker_client: object | None, broker_name: str | None, security_map: dict[str, Any] | None, live_enabled: bool) -> tuple[object | None, str]:
    resolved_name = str(broker_name or "DHAN").strip().upper() or "DHAN"
    if isinstance(broker_client, Broker):
        return broker_client, resolved_name
    if broker_client is not None and resolved_name == "DHAN":
        try:
            return DhanBroker(broker_client, security_map=security_map, config=DhanBrokerConfig(allow_live=live_enabled)), resolved_name
        except Exception:
            return broker_client, resolved_name
    if broker_client is not None:
        return broker_client, resolved_name
    if resolved_name == "DHAN":
        try:
            return DhanBroker.from_env(allow_live=live_enabled, security_map=security_map), resolved_name
        except Exception:
            return None, resolved_name
    return None, resolved_name


def _apply_broker_result(record: dict[str, object], broker_result: BrokerOrderResult, *, execution_type: str) -> None:
    record["broker_name"] = broker_result.broker_name
    record["broker_order_id"] = broker_result.order_id
    record["broker_status"] = broker_result.status
    record["broker_message"] = broker_result.message
    if broker_result.raw_response:
        try:
            record["broker_response_json"] = json.dumps(broker_result.raw_response, ensure_ascii=True)
        except Exception:
            record["broker_response_json"] = str(broker_result.raw_response)
    for key, value in broker_result.metadata.items():
        if value not in {None, ""}:
            record[key] = value
    if execution_type == "PAPER":
        record["trade_status"] = TRADE_STATUS_EXECUTED
        record["position_status"] = TRADE_STATUS_OPEN
        record["execution_status"] = "EXECUTED"
    elif broker_result.accepted:
        record["trade_status"] = TRADE_STATUS_PENDING_EXECUTION
        record["execution_status"] = "SENT"
    else:
        record["trade_status"] = TRADE_STATUS_ERROR
        record["execution_status"] = "ERROR"
        record["validation_error"] = SKIP_REASON_BROKER_ERROR


def _existing_csv_fieldnames(path: Path) -> list[str]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        return next(reader, [])


def _append_order_history(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_rows = _read_trade_rows(path)
    fieldnames = _stable_fieldnames([record], existing_rows)
    existing_fieldnames = _existing_csv_fieldnames(path)
    file_exists = path.exists() and path.stat().st_size > 0
    if file_exists and existing_fieldnames != fieldnames:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing_rows)
            writer.writerow(record)
    else:
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)
    try:
        persist_row(path, record)
    except Exception:
        pass
def _annotate_rejection_row(row: dict[str, object], *, reason: str, category: str, detail: str = '') -> dict[str, object]:
    annotated = dict(row)
    annotated['rejection_reason'] = reason
    annotated['rejection_category'] = category
    if detail:
        annotated['rejection_detail'] = detail
    return annotated


def _mark_skipped(result: ExecutionResult, row: dict[str, object], reason: str) -> None:
    category = 'deduplication' if reason.startswith("DUPLICATE_") else 'validation'
    skipped = _annotate_rejection_row(row, reason=reason, category=category)
    if reason.startswith("DUPLICATE_"):
        skipped["duplicate_reason"] = reason
        result.duplicate_count += 1
    else:
        skipped["validation_error"] = reason
    result.skipped_rows.append(skipped)
    result.skipped_count += 1


def _append_written_row(result: ExecutionResult, row: dict[str, object]) -> None:
    result.rows.append(row)
    execution_status = _normalize_text(row.get("execution_status"))
    if execution_status in {"EXECUTED", "SENT", "FILLED"}:
        result.executed_rows.append(row)
        result.executed_count += 1
    elif execution_status == "BLOCKED":
        result.blocked_rows.append(row)
        result.blocked_count += 1
    elif execution_status == "ERROR":
        result.error_rows.append(row)
        result.error_count += 1


def _stable_fieldnames(rows: list[dict[str, object]], existing_rows: list[dict[str, object]] | None = None) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for key in EXECUTION_SCHEMA:
        if key not in seen:
            ordered.append(key)
            seen.add(key)
    for source in (existing_rows or []) + rows:
        for key in source.keys():
            if key not in seen:
                ordered.append(key)
                seen.add(key)
    return ordered


def _write_execution_rows(path: Path, rows_to_write: list[dict[str, object]]) -> None:
    if not rows_to_write:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_rows = _read_trade_rows(path)
    fieldnames = _stable_fieldnames(rows_to_write, existing_rows)
    existing_fieldnames = _existing_csv_fieldnames(path)
    file_exists = path.exists() and path.stat().st_size > 0
    if file_exists and existing_fieldnames != fieldnames:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing_rows)
            writer.writerows(rows_to_write)
    else:
        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows_to_write)
    try:
        persist_rows(path, rows_to_write, write_mode='append')
    except Exception:
        pass
    try:
        execution_types = {
            _normalize_text(row.get("execution_type"))
            for row in rows_to_write
            if str(row.get("execution_type", "")).strip()
        }
        for row_execution_type in execution_types:
            refresh_execution_risk_state(path, row_execution_type)
    except Exception:
        pass

def execution_result_summary(result: ExecutionResult) -> list[tuple[str, str]]:
    messages: list[tuple[str, str]] = []
    if result.executed_count:
        label = "trade" if result.executed_count == 1 else "trades"
        messages.append(("success", f"{result.executed_count} {label} executed"))
    if result.blocked_count:
        reason_counts: dict[str, int] = {}
        for row in result.blocked_rows:
            reason = str(row.get("blocked_reason") or row.get("risk_limit_reason") or "BLOCKED").strip()
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        for reason, count in reason_counts.items():
            label = "trade" if count == 1 else "trades"
            messages.append(("warning", f"{count} {label} blocked by {reason.replace('_', ' ').lower()}"))
    if result.skipped_count:
        reason_counts: dict[str, int] = {}
        for row in result.skipped_rows:
            reason = str(row.get("duplicate_reason") or row.get("validation_error") or "SKIPPED").strip()
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        for reason, count in reason_counts.items():
            label = "trade" if count == 1 else "trades"
            messages.append(("info", f"{count} {label} skipped because {reason.replace('_', ' ').lower()}"))
    if result.error_count:
        label = "trade" if result.error_count == 1 else "trades"
        messages.append(("error", f"{result.error_count} {label} failed with broker or execution errors"))
    return messages



def _load_optimizer_gate_map(path: Path) -> dict[str, tuple[bool, str]]:
    rows = load_current_rows(path) or load_latest_batch_rows(path)
    if not rows:
        if not path.exists() or path.stat().st_size == 0:
            return {}
        rows = _read_trade_rows(path)
    gate_map: dict[str, tuple[bool, str]] = {}
    for row in rows:
        strategy_key = normalize_strategy_key(str(row.get("strategy", "") or ""))
        if not strategy_key:
            continue
        ready = str(row.get("deployment_ready", "NO") or "NO").strip().upper() == "YES"
        blockers = str(row.get("deployment_blockers", "") or "").strip()
        rank_value = _safe_float(row.get("optimizer_rank", row.get("rank_score", 999999)))
        existing = gate_map.get(strategy_key)
        if existing is None:
            gate_map[strategy_key] = (ready, blockers if not ready else "optimizer validated", rank_value)
            continue
        if rank_value < existing[2]:
            gate_map[strategy_key] = (ready, blockers if not ready else "optimizer validated", rank_value)
    return {key: (value[0], value[1]) for key, value in gate_map.items()}
def _optimizer_gate_for_strategy(strategy: object, gate_map: dict[str, tuple[bool, str]]) -> tuple[bool, str]:
    if not gate_map:
        return False, "optimizer report missing"
    strategy_key = normalize_strategy_key(str(strategy or ""))
    if strategy_key not in gate_map:
        return False, f"no optimizer row for {strategy}"
    return gate_map[strategy_key]

def _execute_candidates(candidates: list[dict[str, object]], output_path: Path, *, execution_type: str, deduplicate: bool, max_trades_per_day: int | None, max_daily_loss: float | None, max_open_trades: int | None = None, broker_client: object | None = None, broker_name: str | None = None, security_map: dict[str, dict[str, str]] | None = None, live_enabled: bool | None = None, symbol_allowlist: list[str] | set[str] | tuple[str, ...] | None = None, max_order_quantity: int | None = None, max_order_value: float | None = None, order_history_path: Path | None = None, optimizer_report_path: Path | None = None, enforce_optimizer_gate: bool | None = None) -> ExecutionResult:
    result = ExecutionResult()
    if not candidates:
        return result

    output_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    today_key = now[:10]
    historical_trade_ids = _load_historical_trade_ids(output_path, execution_type)
    active_trade_keys = load_active_trade_keys(output_path, execution_type)
    active_duplicate_signal_keys = load_active_duplicate_signal_keys(output_path, execution_type)
    recent_signal_times = _load_recent_signal_times(output_path, execution_type)
    recent_trade_times = _load_recent_trade_times(output_path, execution_type)
    open_trade_count = _count_open_trades(output_path, execution_type)
    daily_state = _load_daily_execution_state(output_path, execution_type)
    rows_to_write: list[dict[str, object]] = []
    resolved_allowlist = _normalize_allowlist(symbol_allowlist)
    resolved_order_history = order_history_path or (output_path.parent / "order_history.csv" if execution_type == "PAPER" else ORDER_HISTORY_OUTPUT)
    optimizer_gate_map = _load_optimizer_gate_map(optimizer_report_path or OPTIMIZER_REPORT_OUTPUT) if execution_type == "LIVE" and (enforce_optimizer_gate is not False) else {}

    if execution_type == "PAPER":
        broker: object = PaperBroker(PaperBrokerConfig(orders_path=resolved_order_history))
        resolved_broker_name = "PAPER"
        live_mode = False
    else:
        live_mode = _is_live_enabled(live_enabled, broker_client, broker_name)
        broker, resolved_broker_name = _coerce_live_broker(
            broker_client=broker_client,
            broker_name=broker_name,
            security_map=security_map,
            live_enabled=live_mode,
        )
        resolved_broker_name = resolved_broker_name or "LIVE"

    _append_structured_log(EXECUTION_LOG_PATH, 'execution_start', execution_type=execution_type, broker=resolved_broker_name, candidate_count=len(candidates), output_path=str(output_path))
    batch_seen_trade_ids: set[str] = set()
    batch_seen_trade_keys: set[str] = set()
    batch_seen_signal_keys: set[str] = set()
    batch_recent_signal_times: dict[str, datetime] = {}
    batch_recent_trade_times: dict[str, datetime] = {}

    for candidate in candidates:
        ok, validation_reason, validated = validate_candidate(candidate)
        base_row = _make_result_row(validated, execution_type=execution_type, processed_at_utc=now)
        trade_id = str(base_row.get("trade_id", ""))
        trade_key = str(base_row.get("trade_key", ""))
        duplicate_signal_key = str(base_row.get("duplicate_signal_key", "") or make_duplicate_signal_key(base_row))
        signal_time = _parse_dt(base_row.get("signal_time"))
        cooldown_group = _cooldown_group_key(base_row)
        if not ok:
            _append_structured_log(REJECTIONS_LOG_PATH, 'trade_rejected', execution_type=execution_type, trade_id=trade_id or 'UNKNOWN', strategy=base_row.get('strategy'), symbol=base_row.get('symbol'), reason=validation_reason, category='validation')
            append_log(f'execution_engine skipped invalid trade {trade_id} reason={validation_reason}')
            _mark_skipped(result, base_row, validation_reason)
            continue
        if deduplicate:
            duplicate_reason = _duplicate_reason_for_candidate(
                base_row,
                historical_trade_ids=historical_trade_ids,
                active_trade_keys=active_trade_keys,
                active_duplicate_signal_keys=active_duplicate_signal_keys,
                recent_signal_times=recent_signal_times,
                batch_seen_trade_ids=batch_seen_trade_ids,
                batch_seen_trade_keys=batch_seen_trade_keys,
                batch_seen_signal_keys=batch_seen_signal_keys,
                batch_recent_signal_times=batch_recent_signal_times,
            )
            if duplicate_reason:
                _append_structured_log(REJECTIONS_LOG_PATH, 'trade_skipped', execution_type=execution_type, trade_id=trade_id, strategy=base_row.get('strategy'), symbol=base_row.get('symbol'), reason=duplicate_reason, category='deduplication')
                _mark_skipped(result, base_row, duplicate_reason)
                continue
        batch_seen_trade_ids.add(trade_id)
        batch_seen_trade_keys.add(trade_key)
        batch_seen_signal_keys.add(duplicate_signal_key)
        trade_cooldown_group = _trade_cooldown_group_key(base_row)
        trade_cooldown_seconds = _cooldown_window_seconds(base_row)
        previous_trade_time = batch_recent_trade_times.get(trade_cooldown_group)
        if previous_trade_time is None:
            previous_trade_time = recent_trade_times.get(trade_cooldown_group)
        if trade_cooldown_seconds > 0 and signal_time is not None and previous_trade_time is not None:
            if (signal_time - previous_trade_time).total_seconds() < trade_cooldown_seconds:
                _append_structured_log(REJECTIONS_LOG_PATH, 'trade_skipped', execution_type=execution_type, trade_id=trade_id, strategy=base_row.get('strategy'), symbol=base_row.get('symbol'), reason=SKIP_REASON_DUPLICATE_SIGNAL_COOLDOWN, category='cooldown')
                _mark_skipped(result, base_row, SKIP_REASON_DUPLICATE_SIGNAL_COOLDOWN)
                continue
        if signal_time is not None:
            batch_recent_signal_times[cooldown_group] = signal_time
            batch_recent_trade_times[trade_cooldown_group] = signal_time
        day_key = _trade_day_key(base_row, today_key)
        state = daily_state.setdefault(day_key, {"count": 0.0, "realized_pnl": 0.0})
        effective_max_trades_per_day = _effective_max_trades_per_day(base_row, max_trades_per_day)
        trade_limit_hit = effective_max_trades_per_day is not None and int(state["count"]) >= int(effective_max_trades_per_day)
        loss_limit_hit = max_daily_loss is not None and float(max_daily_loss) > 0 and float(state["realized_pnl"]) <= -abs(float(max_daily_loss))
        open_trade_limit_hit = max_open_trades is not None and int(max_open_trades) > 0 and int(open_trade_count) >= int(max_open_trades)
        if trade_limit_hit or loss_limit_hit or open_trade_limit_hit:
            blocked = dict(base_row)
            blocked["trade_status"] = TRADE_STATUS_BLOCKED
            blocked["execution_status"] = "BLOCKED"
            blocked["blocked_reason"] = SKIP_REASON_MAX_OPEN_TRADES if open_trade_limit_hit else SKIP_REASON_MAX_TRADES_PER_DAY if trade_limit_hit else SKIP_REASON_MAX_DAILY_LOSS
            blocked = _annotate_rejection_row(blocked, reason=str(blocked["blocked_reason"]), category='risk')
            blocked["risk_limit_reason"] = f"max open trades={int(max_open_trades)}" if open_trade_limit_hit else _risk_limit_message(effective_max_trades_per_day, max_daily_loss)
            blocked["broker_name"] = resolved_broker_name
            blocked["broker_status"] = "RISK_LIMIT"
            blocked["broker_message"] = f"Execution blocked by {blocked['risk_limit_reason']}"
            rows_to_write.append(blocked)
            _append_written_row(result, blocked)
            _append_structured_log(REJECTIONS_LOG_PATH, 'trade_blocked', execution_type=execution_type, trade_id=trade_id, strategy=blocked.get('strategy'), symbol=blocked.get('symbol'), reason=blocked.get('blocked_reason'), broker_status=blocked.get('broker_status'), risk_limit_reason=blocked.get('risk_limit_reason'), category='risk')
            continue

        if execution_type == "LIVE" and (enforce_optimizer_gate is not False):
            optimizer_ready, optimizer_reason = _optimizer_gate_for_strategy(base_row.get("strategy", ""), optimizer_gate_map)
            if not optimizer_ready:
                blocked = dict(base_row)
                blocked["trade_status"] = TRADE_STATUS_BLOCKED
                blocked["execution_status"] = "BLOCKED"
                blocked["blocked_reason"] = SKIP_REASON_OPTIMIZER_GATE
                blocked = _annotate_rejection_row(blocked, reason=SKIP_REASON_OPTIMIZER_GATE, category='optimizer_gate', detail=str(optimizer_reason))
                blocked["broker_name"] = resolved_broker_name
                blocked["broker_status"] = "OPTIMIZER_GATE"
                blocked["broker_message"] = optimizer_reason
                blocked["risk_limit_reason"] = optimizer_reason
                rows_to_write.append(blocked)
                _append_written_row(result, blocked)
                _append_structured_log(REJECTIONS_LOG_PATH, 'trade_blocked', execution_type=execution_type, trade_id=trade_id, strategy=base_row.get('strategy'), symbol=base_row.get('symbol'), reason=SKIP_REASON_OPTIMIZER_GATE, broker_status=blocked.get('broker_status'), risk_limit_reason=optimizer_reason, category='optimizer_gate')
                continue
        if execution_type == "LIVE" and live_kill_switch_enabled():
            blocked = dict(base_row)
            blocked["trade_status"] = TRADE_STATUS_BLOCKED
            blocked["execution_status"] = "BLOCKED"
            blocked["blocked_reason"] = SKIP_REASON_KILL_SWITCH
            blocked = _annotate_rejection_row(blocked, reason=SKIP_REASON_KILL_SWITCH, category='kill_switch')
            blocked["broker_name"] = resolved_broker_name
            blocked["broker_status"] = "KILL_SWITCH"
            blocked["broker_message"] = "Live execution blocked by LIVE_TRADING_KILL_SWITCH."
            rows_to_write.append(blocked)
            _append_written_row(result, blocked)
            _append_structured_log(REJECTIONS_LOG_PATH, 'trade_blocked', execution_type=execution_type, trade_id=trade_id, strategy=blocked.get('strategy'), symbol=blocked.get('symbol'), reason=SKIP_REASON_KILL_SWITCH, broker_status=blocked.get('broker_status'), category='kill_switch')
            continue

        if execution_type == "LIVE" and not live_mode:
            blocked = dict(base_row)
            blocked["trade_status"] = TRADE_STATUS_BLOCKED
            blocked["execution_status"] = "BLOCKED"
            blocked["blocked_reason"] = "LIVE_DISABLED"
            blocked = _annotate_rejection_row(blocked, reason='LIVE_DISABLED', category='live_disabled')
            blocked["broker_name"] = resolved_broker_name
            blocked["broker_status"] = "LIVE_DISABLED"
            blocked["broker_message"] = "Live execution requires explicit enablement."
            rows_to_write.append(blocked)
            _append_written_row(result, blocked)
            _append_structured_log(REJECTIONS_LOG_PATH, 'trade_blocked', execution_type=execution_type, trade_id=trade_id, strategy=blocked.get('strategy'), symbol=blocked.get('symbol'), reason=blocked.get('blocked_reason'), broker_status=blocked.get('broker_status'), category='live_disabled')
            continue

        if execution_type == "LIVE" and resolved_allowlist and str(base_row.get("symbol", "")).strip().upper() not in resolved_allowlist:
            blocked = dict(base_row)
            blocked["trade_status"] = TRADE_STATUS_BLOCKED
            blocked["execution_status"] = "BLOCKED"
            blocked["blocked_reason"] = "SYMBOL_NOT_ALLOWED"
            blocked = _annotate_rejection_row(blocked, reason='SYMBOL_NOT_ALLOWED', category='allowlist')
            blocked["broker_name"] = resolved_broker_name
            blocked["broker_status"] = "SYMBOL_NOT_ALLOWED"
            blocked["broker_message"] = "Symbol is not part of the live allowlist."
            rows_to_write.append(blocked)
            _append_written_row(result, blocked)
            _append_structured_log(REJECTIONS_LOG_PATH, 'trade_blocked', execution_type=execution_type, trade_id=trade_id, strategy=blocked.get('strategy'), symbol=blocked.get('symbol'), reason=blocked.get('blocked_reason'), broker_status=blocked.get('broker_status'), category='allowlist')
            continue

        if max_order_quantity is not None and int(base_row.get("quantity", 0) or 0) > int(max_order_quantity):
            blocked = dict(base_row)
            blocked["trade_status"] = TRADE_STATUS_BLOCKED
            blocked["execution_status"] = "BLOCKED"
            blocked["blocked_reason"] = "MAX_ORDER_QUANTITY"
            blocked = _annotate_rejection_row(blocked, reason='MAX_ORDER_QUANTITY', category='order_limit')
            blocked["broker_name"] = resolved_broker_name
            blocked["broker_status"] = "MAX_ORDER_QUANTITY"
            blocked["broker_message"] = f"Order quantity exceeds configured limit {int(max_order_quantity)}."
            rows_to_write.append(blocked)
            _append_written_row(result, blocked)
            _append_structured_log(REJECTIONS_LOG_PATH, 'trade_blocked', execution_type=execution_type, trade_id=trade_id, strategy=blocked.get('strategy'), symbol=blocked.get('symbol'), reason=blocked.get('blocked_reason'), broker_status=blocked.get('broker_status'), category='order_limit')
            continue


        if max_order_value is not None and _price_value(base_row) * int(base_row.get("quantity", 0) or 0) > float(max_order_value):
            blocked = dict(base_row)
            blocked["trade_status"] = TRADE_STATUS_BLOCKED
            blocked["execution_status"] = "BLOCKED"
            blocked["blocked_reason"] = "MAX_ORDER_VALUE"
            blocked = _annotate_rejection_row(blocked, reason='MAX_ORDER_VALUE', category='order_limit')
            blocked["broker_name"] = resolved_broker_name
            blocked["broker_status"] = "MAX_ORDER_VALUE"
            blocked["broker_message"] = f"Order value exceeds configured limit {float(max_order_value):.2f}."
            rows_to_write.append(blocked)
            _append_written_row(result, blocked)
            _append_structured_log(REJECTIONS_LOG_PATH, 'trade_blocked', execution_type=execution_type, trade_id=trade_id, strategy=blocked.get('strategy'), symbol=blocked.get('symbol'), reason=blocked.get('blocked_reason'), broker_status=blocked.get('broker_status'), category='order_limit')
            continue

        broker_row = dict(base_row)
        trade_candidate = _candidate_to_trade_candidate(broker_row, execution_type)
        order_request = _build_broker_order_request(trade_candidate)
        _append_structured_log(BROKER_LOG_PATH, 'broker_order_routed', execution_type=execution_type, trade_id=trade_candidate.trade_id, broker=resolved_broker_name, strategy=trade_candidate.strategy, symbol=trade_candidate.symbol, side=trade_candidate.side, quantity=trade_candidate.quantity)

        try:
            if isinstance(broker, Broker):
                broker_result = broker.place_order(order_request)
            elif broker is not None and hasattr(broker, "place_order"):
                raw_result = broker.place_order(order_request)
                payload = raw_result if isinstance(raw_result, dict) else {"raw": str(raw_result)}
                status = str(payload.get("orderStatus", payload.get("status", "SENT")) or "SENT").upper()
                broker_result = BrokerOrderResult(
                    broker_name=resolved_broker_name,
                    order_id=str(payload.get("orderId", payload.get("order_id", "")) or ""),
                    status=status,
                    message=str(payload.get("message", payload.get("remarks", payload.get("omsErrorDescription", ""))) or ""),
                    accepted=status not in {"REJECTED", "FAILED", "ERROR", "CANCELLED", "CANCELED"},
                    raw_response=payload,
                    metadata={},
                )
            else:
                raise BrokerConfigurationError("Broker client not configured")
            _apply_broker_result(broker_row, broker_result, execution_type=execution_type)
        except (BrokerConfigurationError, BrokerExecutionError) as exc:
            broker_row["trade_status"] = TRADE_STATUS_ERROR
            broker_row["execution_status"] = "ERROR"
            broker_row["validation_error"] = SKIP_REASON_BROKER_ERROR
            broker_row["broker_name"] = resolved_broker_name
            broker_row["broker_status"] = "ERROR"
            broker_row["broker_message"] = str(exc)
            _append_structured_log(ERRORS_LOG_PATH, 'broker_execution_error', execution_type=execution_type, trade_id=trade_id, broker=resolved_broker_name, strategy=broker_row.get('strategy'), symbol=broker_row.get('symbol'), error_type=type(exc).__name__, error_message=str(exc))
        except Exception as exc:
            broker_row["trade_status"] = TRADE_STATUS_ERROR
            broker_row["execution_status"] = "ERROR"
            broker_row["validation_error"] = SKIP_REASON_BROKER_ERROR
            broker_row["broker_name"] = resolved_broker_name
            broker_row["broker_status"] = "ERROR"
            broker_row["broker_message"] = str(exc)
            _append_structured_log(ERRORS_LOG_PATH, 'broker_execution_error', execution_type=execution_type, trade_id=trade_id, broker=resolved_broker_name, strategy=broker_row.get('strategy'), symbol=broker_row.get('symbol'), error_type=type(exc).__name__, error_message=str(exc))


        rows_to_write.append(broker_row)
        _append_written_row(result, broker_row)
        _append_structured_log(BROKER_LOG_PATH, 'broker_order_result', execution_type=execution_type, trade_id=trade_id, broker=resolved_broker_name, strategy=broker_row.get('strategy'), symbol=broker_row.get('symbol'), status=broker_row.get('broker_status', broker_row.get('execution_status', 'NA')), execution_status=broker_row.get('execution_status'), broker_message=broker_row.get('broker_message'))
        if execution_type == "LIVE":
            _append_order_history(resolved_order_history, broker_row)
        if _normalize_text(broker_row.get("execution_status")) in {"EXECUTED", "SENT", "FILLED"}:
            state["count"] += 1.0
            state["realized_pnl"] += _safe_float(broker_row.get("pnl"))
            historical_trade_ids.add(trade_id)
            active_trade_keys.add(trade_key)
            active_duplicate_signal_keys.add(duplicate_signal_key)
            if signal_time is not None:
                recent_signal_times[cooldown_group] = signal_time
                recent_trade_times[trade_cooldown_group] = signal_time
            open_trade_count += 1

    _write_execution_rows(output_path, rows_to_write)
    _append_structured_log(EXECUTION_LOG_PATH, 'execution_complete', execution_type=execution_type, broker=resolved_broker_name, candidate_count=len(candidates), written_count=len(rows_to_write), executed_count=result.executed_count, blocked_count=result.blocked_count, skipped_count=result.skipped_count, error_count=result.error_count)
    return result


def execute_paper_trades(candidates: list[dict[str, object]], output_path: Path, deduplicate: bool = True, *, max_trades_per_day: int | None = None, max_daily_loss: float | None = None, max_open_trades: int | None = None, order_history_path: Path | None = None) -> ExecutionResult:
    return _execute_candidates(candidates, output_path, execution_type="PAPER", deduplicate=deduplicate, max_trades_per_day=max_trades_per_day, max_daily_loss=max_daily_loss, max_open_trades=max_open_trades, order_history_path=order_history_path)


def execute_live_trades(candidates: list[dict[str, object]], output_path: Path, deduplicate: bool = True, *, broker_client: object | None = None, broker_name: str | None = None, security_map: dict[str, dict[str, str]] | None = None, max_trades_per_day: int | None = None, max_daily_loss: float | None = None, max_open_trades: int | None = None, live_enabled: bool | None = None, symbol_allowlist: list[str] | set[str] | tuple[str, ...] | None = None, max_order_quantity: int | None = None, max_order_value: float | None = None, order_history_path: Path | None = None, optimizer_report_path: Path | None = None, enforce_optimizer_gate: bool | None = None) -> ExecutionResult:
    return _execute_candidates(candidates, output_path, execution_type="LIVE", deduplicate=deduplicate, max_trades_per_day=max_trades_per_day, max_daily_loss=max_daily_loss, max_open_trades=max_open_trades, broker_client=broker_client, broker_name=broker_name, security_map=security_map, live_enabled=live_enabled, symbol_allowlist=symbol_allowlist, max_order_quantity=max_order_quantity, max_order_value=max_order_value, order_history_path=order_history_path, optimizer_report_path=optimizer_report_path, enforce_optimizer_gate=enforce_optimizer_gate)



def summarize_execution_result(
    result: ExecutionResult,
    *,
    deduplicate_enabled: bool,
    execution_type: str,
) -> dict[str, object]:
    """Summarize execution outcomes for go-live validation."""
    malformed_reasons = {
        'INVALID_SIDE',
        'INVALID_TIMESTAMP',
        'INVALID_ENTRY',
        'INVALID_STOP_LOSS',
        'INVALID_TARGET',
        'BUY_LEVELS_NOT_ORDERED',
        'SELL_LEVELS_NOT_ORDERED',
        'MISSING_PRICE',
        'MISSING_QUANTITY',
        'MISSING_TIMESTAMP',
        'MISSING_STOP_LOSS',
        'MISSING_TARGET',
        'INVALID_TRADE_LEVELS',
    }
    invalid_trade_count = 0
    for row in list(result.skipped_rows) + list(result.error_rows):
        reason = _normalize_text(row.get("validation_error") or row.get("blocked_reason") or row.get("risk_limit_reason"))
        if reason in malformed_reasons:
            invalid_trade_count += 1
    return {
        'execution_type': str(execution_type or '').upper(),
        'execution_rows': len(result.rows),
        'executed_count': int(result.executed_count),
        'blocked_count': int(result.blocked_count),
        'skipped_count': int(result.skipped_count),
        'duplicate_trade_count': int(result.duplicate_count),
        'invalid_trade_count': int(invalid_trade_count),
        'execution_error_count': int(result.error_count),
        'execution_crash_count': int(result.error_count),
        'cooldown_controls_enforced': 'YES' if deduplicate_enabled else 'NO',
        'duplicate_controls_enforced': 'YES' if deduplicate_enabled else 'NO',
        'paper_execution_crashes': 'NO' if int(result.error_count) == 0 else 'YES',
    }

def _reconcile_execution_status(broker_status: str) -> str:
    normalized = str(broker_status or "").strip().upper()
    if normalized in {"TRADED", "FILLED", "COMPLETED", "EXECUTED", "SUCCESS"}:
        return "FILLED"
    if normalized in {"REJECTED", "FAILED", "ERROR", "CANCELLED", "CANCELED", "EXPIRED"}:
        return "ERROR"
    return "SENT"


def _symbol_key(record: dict[str, object]) -> str:
    for key in ("trading_symbol", "tradingSymbol", "contract_symbol", "contractSymbol", "option_strike", "optionStrike", "symbol"):
        raw = str(record.get(key, "") or "").strip().upper()
        if raw:
            return raw
    return "UNKNOWN"


def _signed_quantity(side: object, quantity: object) -> int:
    qty = int(_safe_float(quantity))
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "SELL":
        return -qty
    return qty


def _broker_position_map(positions: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    mapped: dict[str, dict[str, object]] = {}
    for raw in positions:
        record = dict(raw)
        symbol = _symbol_key(record)
        if symbol == "UNKNOWN":
            continue
        net_qty = int(_safe_float(record.get("netQty") or record.get("net_qty") or record.get("quantity") or record.get("qty") or 0))
        record["net_qty"] = net_qty
        mapped[symbol] = record
    return mapped


def reconcile_live_trades(
    live_log_path: Path,
    *,
    broker_client: object | None = None,
    broker_name: str | None = None,
) -> list[dict[str, object]]:
    if not live_log_path.exists():
        return []

    resolved_broker_client = broker_client
    resolved_broker_name = str(broker_name or "").strip().upper()
    if resolved_broker_client is None:
        resolved_broker_client, inferred_name = _default_live_broker_client()
        if not resolved_broker_name:
            resolved_broker_name = inferred_name
    if not resolved_broker_name:
        resolved_broker_name = "LIVE"

    existing_rows = read_csv_rows(live_log_path)
    if not existing_rows:
        return []

    reconciled_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    updated_rows: list[dict[str, object]] = []
    reconciled_rows: list[dict[str, object]] = []

    for row in existing_rows:
        record = dict(row)
        if str(record.get("execution_type", "")).upper() != "LIVE":
            updated_rows.append(record)
            continue

        current_status = str(record.get("execution_status", "")).upper()
        if current_status in {"BLOCKED", "ERROR", "FILLED"}:
            updated_rows.append(record)
            continue

        order_id = str(record.get("broker_order_id", "") or "").strip()
        if not order_id:
            record["reconciliation_status"] = "SKIPPED"
            record["reconciliation_note"] = "Missing broker_order_id"
            record["reconciled_at_utc"] = reconciled_at
            updated_rows.append(record)
            continue

        if resolved_broker_client is None or not hasattr(resolved_broker_client, "get_order_by_id"):
            record["reconciliation_status"] = "UNAVAILABLE"
            record["reconciliation_note"] = "Broker client does not support order reconciliation"
            record["reconciled_at_utc"] = reconciled_at
            updated_rows.append(record)
            continue

        try:
            result = resolved_broker_client.get_order_by_id(order_id)
            _apply_live_broker_result(record, resolved_broker_name, result)
            record["execution_status"] = _reconcile_execution_status(str(record.get("broker_status", "")))
            record["trade_status"] = TRADE_STATUS_OPEN if str(record.get("execution_status", "")).upper() == "FILLED" else TRADE_STATUS_ERROR
            record["position_status"] = TRADE_STATUS_OPEN if str(record.get("execution_status", "")).upper() == "FILLED" else record.get("position_status", "")
            record["reconciliation_status"] = "RECONCILED"
            record["reconciled_at_utc"] = reconciled_at
            reconciled_rows.append(record)
        except Exception as exc:
            record["reconciliation_status"] = "ERROR"
            record["reconciliation_note"] = str(exc)
            record["reconciled_at_utc"] = reconciled_at

        updated_rows.append(record)

    fieldnames: list[str] = []
    for r in updated_rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    with live_log_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    try:
        persist_rows(live_log_path, updated_rows, write_mode='replace')
        refresh_execution_risk_state(live_log_path, "LIVE")
    except Exception:
        pass

    return reconciled_rows


def reconcile_live_positions(
    live_log_path: Path,
    *,
    broker_client: object | None = None,
    broker_name: str | None = None,
) -> list[dict[str, object]]:
    if not live_log_path.exists():
        return []

    resolved_broker_client = broker_client
    resolved_broker_name = str(broker_name or "").strip().upper()
    if resolved_broker_client is None:
        resolved_broker_client, inferred_name = _default_live_broker_client()
        if not resolved_broker_name:
            resolved_broker_name = inferred_name
    if not resolved_broker_name:
        resolved_broker_name = "LIVE"

    if resolved_broker_client is None or not hasattr(resolved_broker_client, "get_positions"):
        return []

    existing_rows = read_csv_rows(live_log_path)
    if not existing_rows:
        return []

    expected: dict[str, int] = {}
    for row in existing_rows:
        if str(row.get("execution_type", "")).upper() != "LIVE":
            continue
        if str(row.get("execution_status", "")).upper() != "FILLED":
            continue
        symbol = _symbol_key(row)
        expected[symbol] = expected.get(symbol, 0) + _signed_quantity(row.get("side"), row.get("quantity"))

    positions_raw = resolved_broker_client.get_positions()
    actual_map = _broker_position_map(positions_raw if isinstance(positions_raw, list) else [])
    reconciled_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    position_rows: list[dict[str, object]] = []
    all_symbols = sorted(set(expected.keys()) | set(actual_map.keys()))
    for symbol in all_symbols:
        expected_qty = int(expected.get(symbol, 0))
        actual_qty = int(_safe_float(actual_map.get(symbol, {}).get("net_qty", 0)))
        delta = actual_qty - expected_qty
        position_rows.append(
            {
                "symbol": symbol,
                "broker_name": resolved_broker_name,
                "expected_net_qty": expected_qty,
                "broker_net_qty": actual_qty,
                "qty_delta": delta,
                "position_match": "YES" if delta == 0 else "NO",
                "reconciled_at_utc": reconciled_at,
            }
        )

    return position_rows

def close_paper_trades(
    paper_log_path: Path,
    candles: list[dict[str, object]],
    *,
    max_hold_minutes: int = 60,
) -> list[dict[str, object]]:
    """Close open PAPER trades using SL/TP, trailing SL, time-exit, then EOD."""

    if not paper_log_path.exists():
        return []

    candle_rows: list[dict[str, object]] = []
    for c in candles or []:
        ts = _parse_dt(c.get("timestamp"))
        if ts is None:
            continue
        try:
            candle_rows.append(
                {
                    "timestamp": ts,
                    "open": float(c.get("open", 0) or 0),
                    "high": float(c.get("high", 0) or 0),
                    "low": float(c.get("low", 0) or 0),
                    "close": float(c.get("close", 0) or 0),
                }
            )
        except (TypeError, ValueError):
            continue

    candle_rows.sort(key=lambda r: r["timestamp"])
    if not candle_rows:
        return []

    existing_rows = read_csv_rows(paper_log_path)

    def _to_float(val: object) -> Optional[float]:
        try:
            if val is None or str(val).strip() == "":
                return None
            return float(val)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    closed_now: list[dict[str, object]] = []
    updated_rows: list[dict[str, object]] = []

    for r in existing_rows:
        execution_type = str(r.get("execution_type", "")).upper()
        status = str(r.get("execution_status", "")).upper()
        if execution_type != "PAPER" or status in {"CLOSED", "EXITED", "BLOCKED", "ERROR"}:
            updated_rows.append(r)
            continue

        side = str(r.get("side", "")).upper()
        entry_time = _parse_dt(r.get("signal_time")) or _parse_dt(r.get("entry_time")) or _parse_dt(r.get("timestamp"))
        if entry_time is None or side not in {"BUY", "SELL"}:
            updated_rows.append(r)
            continue

        entry_price = _to_float(r.get("entry_price", r.get("price"))) or 0.0
        stop_loss = _to_float(r.get("stop_loss"))
        target_price = _to_float(r.get("target_price", r.get("target")))
        trail_stop = _to_float(r.get("trailing_stop_loss")) or stop_loss
        trailing_sl_pct = _to_float(r.get("trailing_sl_pct")) or 0.0

        exit_price: Optional[float] = None
        exit_time: Optional[datetime] = None
        exit_reason: str = ""

        time_exit_at = entry_time + timedelta(minutes=int(max_hold_minutes or 0)) if max_hold_minutes and max_hold_minutes > 0 else None

        for c in candle_rows:
            ts = c["timestamp"]
            if ts < entry_time:
                continue

            if trailing_sl_pct and trailing_sl_pct > 0 and trail_stop is not None:
                if side == "BUY":
                    trail_stop = max(float(trail_stop), float(c["high"]) * (1.0 - trailing_sl_pct))
                else:
                    trail_stop = min(float(trail_stop), float(c["low"]) * (1.0 + trailing_sl_pct))

            # Worst-case if both hit same candle -> stop first.
            if side == "BUY":
                if trail_stop is not None and float(c["low"]) <= float(trail_stop):
                    exit_price, exit_time, exit_reason = float(trail_stop), ts, "TRAILING_STOP" if stop_loss is not None and float(trail_stop) > float(stop_loss) else "STOP_LOSS"
                    break
                if target_price is not None and float(c["high"]) >= float(target_price):
                    exit_price, exit_time, exit_reason = float(target_price), ts, "TARGET"
                    break
            else:
                if trail_stop is not None and float(c["high"]) >= float(trail_stop):
                    exit_price, exit_time, exit_reason = float(trail_stop), ts, "TRAILING_STOP" if stop_loss is not None and float(trail_stop) < float(stop_loss) else "STOP_LOSS"
                    break
                if target_price is not None and float(c["low"]) <= float(target_price):
                    exit_price, exit_time, exit_reason = float(target_price), ts, "TARGET"
                    break

            if time_exit_at is not None and ts >= time_exit_at:
                exit_price, exit_time, exit_reason = float(c["close"]), ts, "TIME_EXIT"
                break

        if exit_price is None or exit_time is None:
            last = candle_rows[-1]
            exit_price, exit_time, exit_reason = float(last["close"]), last["timestamp"], "EOD"

        try:
            qty = int(float(r.get("quantity", 0) or 0))
        except (TypeError, ValueError):
            qty = 0

        gross_pnl = 0.0
        if qty > 0:
            gross_pnl = (float(exit_price) - float(entry_price)) * qty if side == "BUY" else (float(entry_price) - float(exit_price)) * qty
        trading_cost = _to_float(r.get("trading_cost")) or 0.0
        risk_per_unit = abs(float(entry_price) - float(stop_loss)) if stop_loss is not None else 0.0
        rr_achieved = (abs(float(exit_price) - float(entry_price)) / risk_per_unit) if risk_per_unit > 0 else 0.0
        pnl = gross_pnl - trading_cost

        r2 = dict(r)
        r2["execution_status"] = "CLOSED"
        r2["trade_status"] = TRADE_STATUS_CLOSED
        r2["position_status"] = TRADE_STATUS_CLOSED
        r2.setdefault("entry_time", entry_time.isoformat(sep=" "))
        r2.setdefault("entry", round(float(entry_price), 4))
        r2.setdefault("entry_price", round(float(entry_price), 4))
        r2.setdefault("target", r.get("target", r.get("target_price", "")))
        r2.setdefault("target_price", r.get("target_price", r.get("target", "")))
        r2["exit_time"] = exit_time.isoformat(sep=" ")
        r2["exit_price"] = round(float(exit_price), 4)
        r2["exit_reason"] = exit_reason
        r2["gross_pnl"] = round(float(gross_pnl), 2)
        r2["trading_cost"] = round(float(trading_cost), 2)
        r2["rr_achieved"] = round(float(rr_achieved), 4)
        r2["pnl"] = round(float(pnl), 2)
        if trail_stop is not None:
            r2["trailing_stop_loss"] = round(float(trail_stop), 4)

        updated_rows.append(r2)
        closed_now.append(r2)

    if not closed_now:
        return []

    fieldnames: list[str] = []
    for r in updated_rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    with paper_log_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    return closed_now











def _order_update_value(event: object, key: str, default: object = '') -> object:
    if isinstance(event, dict):
        return event.get(key, default)
    return getattr(event, key, default)


def _apply_live_order_update_row(record: dict[str, object], event: object) -> dict[str, object]:
    updated = dict(record)
    status = _normalize_text(_order_update_value(event, 'status'))
    filled_qty = int(_safe_float(_order_update_value(event, 'filled_qty', 0)))
    remaining_qty = int(_safe_float(_order_update_value(event, 'remaining_qty', 0)))
    average_price = _safe_float(_order_update_value(event, 'average_price', 0.0))
    traded_price = _safe_float(_order_update_value(event, 'traded_price', 0.0))
    update_time = str(_order_update_value(event, 'update_time', '') or '')
    message = str(_order_update_value(event, 'message', '') or '')
    order_id = str(_order_update_value(event, 'order_id', '') or '')
    correlation_id = str(_order_update_value(event, 'correlation_id', '') or '')

    if order_id:
        updated['broker_order_id'] = order_id
    if correlation_id:
        updated['correlation_id'] = correlation_id
    if status:
        updated['broker_status'] = status
    if message:
        updated['broker_message'] = message
    if update_time:
        updated['reconciled_at_utc'] = update_time
    if average_price > 0:
        updated['average_price'] = round(average_price, 4)
        updated['price'] = round(average_price, 4)
    if traded_price > 0:
        updated['traded_price'] = round(traded_price, 4)
    updated['filled_qty'] = filled_qty
    updated['remaining_qty'] = remaining_qty

    if status in {'TRADED', 'FILLED', 'COMPLETED', 'EXECUTED', 'SUCCESS'} or (filled_qty > 0 and remaining_qty == 0):
        updated['execution_status'] = 'FILLED'
        updated['trade_status'] = TRADE_STATUS_OPEN
        updated['position_status'] = TRADE_STATUS_OPEN
    elif status in {'PARTIAL', 'PARTIALLY_FILLED', 'PARTTRADED'} or (filled_qty > 0 and remaining_qty > 0):
        updated['execution_status'] = 'PARTIAL'
        updated['trade_status'] = TRADE_STATUS_PENDING_EXECUTION
        updated['position_status'] = updated.get('position_status', '') or ''
    elif status in {'REJECTED', 'FAILED', 'ERROR', 'CANCELLED', 'CANCELED', 'EXPIRED'}:
        updated['execution_status'] = 'ERROR'
        updated['trade_status'] = TRADE_STATUS_ERROR
    else:
        updated['execution_status'] = updated.get('execution_status', 'SENT')
        if not str(updated.get('trade_status', '')).strip():
            updated['trade_status'] = TRADE_STATUS_PENDING_EXECUTION

    return updated


def apply_live_order_updates_to_log(live_log_path: str | Path, order_updates: list[object]) -> list[dict[str, object]]:
    path = Path(live_log_path)
    if not path.exists() or not order_updates:
        return []

    existing_rows = _read_trade_rows(path)
    if not existing_rows:
        return []

    updated_rows: list[dict[str, object]] = []
    changed_rows: list[dict[str, object]] = []

    for raw in existing_rows:
        record = dict(raw)
        if _normalize_text(record.get('execution_type')) != 'LIVE':
            updated_rows.append(record)
            continue

        matched = record
        applied = False
        for event in order_updates:
            event_order_id = str(_order_update_value(event, 'order_id', '') or '').strip()
            event_trade_id = str(_order_update_value(event, 'trade_id', '') or '').strip()
            event_correlation_id = str(_order_update_value(event, 'correlation_id', '') or '').strip()
            if event_order_id and event_order_id == str(record.get('broker_order_id', '') or '').strip():
                matched = _apply_live_order_update_row(record, event)
                applied = True
                break
            if event_trade_id and event_trade_id == str(record.get('trade_id', '') or '').strip():
                matched = _apply_live_order_update_row(record, event)
                applied = True
                break
            if event_correlation_id and event_correlation_id == str(record.get('correlation_id', '') or '').strip():
                matched = _apply_live_order_update_row(record, event)
                applied = True
                break

        updated_rows.append(matched)
        if applied:
            changed_rows.append(matched)

    if not changed_rows:
        return []

    fieldnames = _stable_fieldnames(updated_rows)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    return changed_rows























