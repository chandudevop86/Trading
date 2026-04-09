from __future__ import annotations

import csv
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from vinayak.validation.trade_evaluation import build_trade_evaluation_summary
from vinayak.analytics.readiness import evaluate_readiness
from vinayak.db.repositories.execution_audit_log_repository import ExecutionAuditLogRepository
from vinayak.db.repositories.execution_repository import ExecutionRepository
from vinayak.db.repositories.reviewed_trade_repository import ReviewedTradeRepository
from vinayak.execution.broker.dhan_client import DhanClient


DEFAULT_PAPER_LOG_PATH = Path('app/vinayak/data/paper_trading_logs_all.csv')
_VALIDATION_SNAPSHOT_TTL_SECONDS = 2.0
_VALIDATION_SNAPSHOT_CACHE: dict[str, dict[str, Any]] = {}


def _load_csv_rows(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    try:
        with path.open('r', encoding='utf-8', newline='') as handle:
            return [dict(row) for row in csv.DictReader(handle) if row]
    except Exception:
        return []


def _build_validation_snapshot(rows: list[dict[str, object]]) -> dict[str, object]:
    if not rows:
        return {}
    summary = build_trade_evaluation_summary(rows, strategy_name='VINAYAK_PAPER')
    readiness = evaluate_readiness(rows, rows, trade_summary=summary)
    return {
        'clean_trades': summary.get('clean_trades', summary.get('closed_trades', 0)),
        'expectancy_per_trade': summary.get('expectancy_per_trade', 0.0),
        'expectancy_stability_score': summary.get('expectancy_stability_score', 0.0),
        'profit_factor': summary.get('profit_factor', 0.0),
        'profit_factor_stability_score': summary.get('profit_factor_stability_score', 0.0),
        'max_drawdown_pct': summary.get('max_drawdown_pct', 0.0),
        'recovery_factor': summary.get('recovery_factor', 0.0),
        'pass_fail_status': summary.get('pass_fail_status', 'NEED_MORE_DATA'),
        'confidence_label': summary.get('confidence_label', 'NEED_MORE_DATA'),
        'go_live_status': summary.get('go_live_status', 'PAPER_ONLY'),
        'promotion_status': summary.get('promotion_status', 'RESEARCH_ONLY'),
        'warnings': summary.get('warnings', []),
        'pass_fail_reasons': summary.get('pass_fail_reasons', []),
        'system_status': readiness.get('verdict', 'NOT_READY'),
        'readiness_reasons': readiness.get('reasons', []),
        'validation_pass_rate': readiness.get('validation_pass_rate', 0.0),
        'top_rejection_reasons': readiness.get('top_rejection_reasons', {}),
        'clean_trade_metrics_only': readiness.get('clean_trade_metrics_only', False),
        'clean_trade_count': readiness.get('clean_trade_count', 0),
        'edge_proof_status': readiness.get('edge_proof_status', 'PAPER_ONLY'),
        'readiness_summary': readiness.get('readiness_summary', ''),
        'edge_report': readiness.get('edge_report', {}),
        'regime_report': readiness.get('regime_report', {}),
        'walkforward_report': readiness.get('walkforward_report', {}),
        'regime_consistency_score': readiness.get('regime_consistency_score', 0.0),
        'regime_consistency_label': readiness.get('regime_consistency_label', 'DEPENDENT'),
        'dominant_regime': readiness.get('dominant_regime', 'none'),
        'weakest_regime': readiness.get('weakest_regime', 'none'),
        'walkforward_windows': readiness.get('walkforward_windows', 0),
        'oos_status': readiness.get('oos_status', 'OOS_NEED_MORE_DATA'),
        'oos_pass_rate': readiness.get('oos_pass_rate', 0.0),
        'overfit_risk_score': readiness.get('overfit_risk_score', 10.0),
        'overfit_risk_label': readiness.get('overfit_risk_label', 'HIGH'),
    }


def _validation_snapshot(path: Path) -> dict[str, object]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    stat = path.stat()
    cache_key = str(path.resolve())
    signature = (stat.st_mtime_ns, stat.st_size)
    now = time.monotonic()
    cached = _VALIDATION_SNAPSHOT_CACHE.get(cache_key)
    if cached and cached.get('signature') == signature and (now - float(cached.get('loaded_at', 0.0))) <= _VALIDATION_SNAPSHOT_TTL_SECONDS:
        return dict(cached.get('value', {}))
    value = _build_validation_snapshot(_load_csv_rows(path))
    _VALIDATION_SNAPSHOT_CACHE[cache_key] = {
        'signature': signature,
        'loaded_at': now,
        'value': value,
    }
    return dict(value)


class DashboardSummaryService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.reviewed_trade_repository = ReviewedTradeRepository(session)
        self.execution_repository = ExecutionRepository(session)
        self.execution_audit_log_repository = ExecutionAuditLogRepository(session)
        self.dhan_client = DhanClient(
            client_id=os.getenv('DHAN_CLIENT_ID'),
            access_token=os.getenv('DHAN_ACCESS_TOKEN'),
        )

    def build_summary(self) -> dict[str, object]:
        reviewed_trades = self.reviewed_trade_repository.list_reviewed_trades()
        executions = self.execution_repository.list_executions()
        audit_logs = self.execution_audit_log_repository.list_audit_logs()

        reviewed_trade_counts = Counter(record.status.upper() for record in reviewed_trades)
        execution_mode_counts = Counter(record.mode.upper() for record in executions)
        execution_status_counts = Counter(record.status.upper() for record in executions)
        audit_status_counts = Counter(record.status.upper() for record in audit_logs)
        recent_audit_failures = sum(1 for record in audit_logs if record.status.upper() in {'BLOCKED', 'REJECTED', 'ERROR'})

        return {
            'broker_ready': self.dhan_client.is_ready(),
            'broker_name': 'DHAN',
            'reviewed_trade_counts': dict(reviewed_trade_counts),
            'execution_mode_counts': dict(execution_mode_counts),
            'execution_status_counts': dict(execution_status_counts),
            'audit_status_counts': dict(audit_status_counts),
            'recent_audit_failures': recent_audit_failures,
            'validation_summary': _validation_snapshot(DEFAULT_PAPER_LOG_PATH),
        }
