from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from src.execution.contracts import normalize_candidate_contract


@dataclass(slots=True)
class TradingState:
    executed_zone_ids: set[str] = field(default_factory=set)
    active_trade_ids: set[str] = field(default_factory=set)
    last_trade_time_by_group: dict[str, datetime] = field(default_factory=dict)
    daily_pnl_by_day: dict[str, float] = field(default_factory=dict)
    daily_trade_count_by_day: dict[str, int] = field(default_factory=dict)
    rejected_count: int = 0
    passed_count: int = 0
    executed_count: int = 0

    @classmethod
    def from_rows(cls, rows: list[dict[str, Any]]) -> "TradingState":
        state = cls()
        for row in rows:
            item = normalize_candidate_contract(dict(row))
            timestamp = pd.to_datetime(item.get("timestamp"), errors="coerce")
            day_key = timestamp.strftime("%Y-%m-%d") if not pd.isna(timestamp) else "UNKNOWN"
            zone_id = str(item.get("zone_id", "") or "").strip()
            trade_id = str(item.get("trade_id", "") or "").strip()
            execution_status = str(row.get("execution_status", row.get("trade_status", "")) or "").upper()
            if zone_id and execution_status in {"EXECUTED", "FILLED", "SENT", "OPEN", "CLOSED"}:
                state.executed_zone_ids.add(zone_id)
            if trade_id and execution_status in {"EXECUTED", "FILLED", "SENT", "OPEN"}:
                state.active_trade_ids.add(trade_id)
            state.daily_pnl_by_day[day_key] = float(state.daily_pnl_by_day.get(day_key, 0.0)) + float(row.get("pnl", 0.0) or 0.0)
            if execution_status in {"EXECUTED", "FILLED", "SENT", "OPEN", "CLOSED"}:
                state.daily_trade_count_by_day[day_key] = int(state.daily_trade_count_by_day.get(day_key, 0)) + 1
            if not pd.isna(timestamp):
                group = state._cooldown_group(item)
                previous = state.last_trade_time_by_group.get(group)
                parsed = pd.Timestamp(timestamp).to_pydatetime()
                if previous is None or parsed > previous:
                    state.last_trade_time_by_group[group] = parsed
        return state

    def _cooldown_group(self, candidate: dict[str, Any]) -> str:
        return "|".join(
            [
                str(candidate.get("symbol", "UNKNOWN")).upper(),
                str(candidate.get("strategy_name", candidate.get("strategy", "UNKNOWN"))).upper(),
                str(candidate.get("setup_type", "GENERIC")).upper(),
            ]
        )

    def mark_candidate_seen(self, candidate: dict[str, Any]) -> None:
        self.passed_count += 1

    def mark_executed(self, candidate: dict[str, Any], *, pnl: float = 0.0) -> None:
        item = normalize_candidate_contract(candidate)
        timestamp = pd.to_datetime(item.get("timestamp"), errors="coerce")
        day_key = timestamp.strftime("%Y-%m-%d") if not pd.isna(timestamp) else "UNKNOWN"
        zone_id = str(item.get("zone_id", "") or "").strip()
        trade_id = str(item.get("trade_id", "") or "").strip()
        if zone_id:
            self.executed_zone_ids.add(zone_id)
        if trade_id:
            self.active_trade_ids.add(trade_id)
        if not pd.isna(timestamp):
            self.last_trade_time_by_group[self._cooldown_group(item)] = pd.Timestamp(timestamp).to_pydatetime()
        self.daily_trade_count_by_day[day_key] = int(self.daily_trade_count_by_day.get(day_key, 0)) + 1
        self.daily_pnl_by_day[day_key] = float(self.daily_pnl_by_day.get(day_key, 0.0)) + float(pnl)
        self.executed_count += 1

    def mark_rejected(self, candidate: dict[str, Any]) -> None:
        self.rejected_count += 1

    def is_duplicate_zone(self, candidate: dict[str, Any]) -> bool:
        zone_id = str(candidate.get("zone_id", "") or "").strip()
        return bool(zone_id and zone_id in self.executed_zone_ids)

    def cooldown_ok(self, candidate: dict[str, Any], cooldown_minutes: int) -> bool:
        if cooldown_minutes <= 0:
            return True
        item = normalize_candidate_contract(candidate)
        timestamp = pd.to_datetime(item.get("timestamp"), errors="coerce")
        if pd.isna(timestamp):
            return False
        group = self._cooldown_group(item)
        last = self.last_trade_time_by_group.get(group)
        if last is None:
            return True
        return (pd.Timestamp(timestamp).to_pydatetime() - last).total_seconds() >= int(cooldown_minutes) * 60

    def daily_loss_ok(self, candidate: dict[str, Any], max_daily_loss: float) -> bool:
        if max_daily_loss <= 0:
            return True
        item = normalize_candidate_contract(candidate)
        timestamp = pd.to_datetime(item.get("timestamp"), errors="coerce")
        day_key = timestamp.strftime("%Y-%m-%d") if not pd.isna(timestamp) else "UNKNOWN"
        return float(self.daily_pnl_by_day.get(day_key, 0.0)) > -abs(float(max_daily_loss))

    def max_trades_ok(self, candidate: dict[str, Any], max_trades_per_day: int) -> bool:
        if max_trades_per_day <= 0:
            return True
        item = normalize_candidate_contract(candidate)
        timestamp = pd.to_datetime(item.get("timestamp"), errors="coerce")
        day_key = timestamp.strftime("%Y-%m-%d") if not pd.isna(timestamp) else "UNKNOWN"
        return int(self.daily_trade_count_by_day.get(day_key, 0)) < int(max_trades_per_day)

    def reset_for_new_session(self, day_key: str) -> None:
        self.daily_pnl_by_day[day_key] = 0.0
        self.daily_trade_count_by_day[day_key] = 0


__all__ = ["TradingState"]
