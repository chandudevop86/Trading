from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from src.execution.contracts import normalize_candidate_contract
from src.execution.guardrails import GuardConfig, check_all_guards
from src.execution.state import TradingState


@dataclass(slots=True)
class CanonicalExecutionConfig:
    output_path: Path
    order_history_path: Path | None = None
    deduplicate: bool = True
    max_trades_per_day: int = 3
    max_daily_loss: float = 0.0
    max_open_trades: int | None = None
    cooldown_minutes: int = 15
    allowed_start_time: str = "09:15"
    cutoff_time: str = "15:30"
    stale_after_minutes: int = 0


@dataclass(slots=True)
class CanonicalExecutionResult:
    allowed: bool
    status: str
    candidate: dict[str, Any]
    reasons: list[str]
    metrics: dict[str, Any]


class ExecutionAuditLogger:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.candidates_log = self.root / "candidates_log.csv"
        self.rejected_log = self.root / "rejected_candidates_log.csv"
        self.executed_log = self.root / "executed_trades_log.csv"
        self.readiness_log = self.root / "readiness_summary.csv"

    def _append(self, path: Path, row: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = []
        if path.exists():
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                existing = list(reader.fieldnames or [])
        fieldnames = list(dict.fromkeys(existing + list(row.keys())))
        rows: list[dict[str, Any]] = []
        if path.exists() and existing != fieldnames:
            with path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
        if rows:
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                for existing_row in rows:
                    writer.writerow({key: existing_row.get(key, "") for key in fieldnames})
                writer.writerow({key: row.get(key, "") for key in fieldnames})
            return
        write_header = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow({key: row.get(key, "") for key in fieldnames})

    def log_candidate(self, candidate: dict[str, Any], *, status: str, reasons: list[str]) -> None:
        row = {
            "timestamp": candidate.get("timestamp", ""),
            "symbol": candidate.get("symbol", ""),
            "strategy_name": candidate.get("strategy_name", candidate.get("strategy", "")),
            "zone_id": candidate.get("zone_id", ""),
            "status": status,
            "reasons": ",".join(reasons),
            "validation_score": candidate.get("validation_score", 0.0),
            "rr_ratio": candidate.get("rr_ratio", 0.0),
        }
        self._append(self.candidates_log, row)
        if status != "READY_FOR_EXECUTION":
            self._append(self.rejected_log, row)

    def log_execution(self, candidate: dict[str, Any], *, status: str, reasons: list[str]) -> None:
        self._append(
            self.executed_log,
            {
                "timestamp": candidate.get("timestamp", ""),
                "symbol": candidate.get("symbol", ""),
                "strategy_name": candidate.get("strategy_name", candidate.get("strategy", "")),
                "zone_id": candidate.get("zone_id", ""),
                "status": status,
                "reasons": ",".join(reasons),
                "validation_score": candidate.get("validation_score", 0.0),
                "rr_ratio": candidate.get("rr_ratio", 0.0),
            },
        )

    def log_readiness(self, summary: dict[str, Any]) -> None:
        self._append(self.readiness_log, summary)


def execute_candidate(
    candidate: dict[str, Any],
    state: TradingState,
    config: CanonicalExecutionConfig,
    logger: ExecutionAuditLogger,
) -> CanonicalExecutionResult:
    normalized = normalize_candidate_contract(candidate)
    logger.log_candidate(normalized, status="CANDIDATE_RECEIVED", reasons=[])
    guard_result = check_all_guards(
        normalized,
        state,
        GuardConfig(
            cooldown_minutes=config.cooldown_minutes,
            max_trades_per_day=config.max_trades_per_day,
            max_daily_loss=config.max_daily_loss,
            allowed_start_time=config.allowed_start_time,
            cutoff_time=config.cutoff_time,
            stale_after_minutes=config.stale_after_minutes,
        ),
    )
    if not guard_result.allowed:
        state.mark_rejected(normalized)
        logger.log_candidate(normalized, status="BLOCKED", reasons=guard_result.reasons)
        return CanonicalExecutionResult(
            allowed=False,
            status="BLOCKED",
            candidate=normalized,
            reasons=list(guard_result.reasons),
            metrics=dict(guard_result.metrics),
        )
    state.mark_candidate_seen(normalized)
    logger.log_candidate(normalized, status="READY_FOR_EXECUTION", reasons=[])
    return CanonicalExecutionResult(
        allowed=True,
        status="READY_FOR_EXECUTION",
        candidate=normalized,
        reasons=[],
        metrics=dict(guard_result.metrics),
    )


def run_canonical_paper_execution(
    candidates: list[dict[str, Any]],
    *,
    config: CanonicalExecutionConfig,
    adapter: Callable[..., Any],
    existing_rows: list[dict[str, Any]] | None = None,
) -> tuple[Any, list[dict[str, Any]], TradingState]:
    logger = ExecutionAuditLogger(config.output_path.parent)
    state = TradingState.from_rows(list(existing_rows or []))
    allowed: list[dict[str, Any]] = []
    blocked_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        decision = execute_candidate(candidate, state, config, logger)
        if decision.allowed:
            allowed.append(dict(decision.candidate))
        else:
            blocked = dict(decision.candidate)
            blocked["trade_status"] = "BLOCKED"
            blocked["execution_status"] = "BLOCKED"
            blocked["blocked_reason"] = decision.reasons[0] if decision.reasons else "BLOCKED"
            blocked["validation_error"] = blocked["blocked_reason"]
            blocked["reason_codes"] = list(decision.reasons)
            blocked_rows.append(blocked)
    adapter_result = adapter(
        allowed,
        config.output_path,
        deduplicate=config.deduplicate,
        max_trades_per_day=config.max_trades_per_day,
        max_daily_loss=config.max_daily_loss if config.max_daily_loss > 0 else None,
        max_open_trades=config.max_open_trades,
        order_history_path=config.order_history_path,
    )
    for row in getattr(adapter_result, "executed_rows", []):
        state.mark_executed(dict(row), pnl=float(row.get("pnl", 0.0) or 0.0))
        logger.log_execution(dict(row), status="EXECUTED", reasons=[])
    for row in blocked_rows:
        logger.log_execution(row, status="BLOCKED", reasons=list(row.get("reason_codes", [])))
    if blocked_rows:
        adapter_result.rows.extend(blocked_rows)
        adapter_result.blocked_rows.extend(blocked_rows)
        adapter_result.blocked_count += len(blocked_rows)
    return adapter_result, blocked_rows, state


__all__ = [
    "CanonicalExecutionConfig",
    "CanonicalExecutionResult",
    "ExecutionAuditLogger",
    "execute_candidate",
    "run_canonical_paper_execution",
]


