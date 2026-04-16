from __future__ import annotations

from vinayak.core.statuses import ExecutionStatus, HealthStatus, ReadinessStatus, ValidationStatus


def test_status_constants_expose_canonical_values() -> None:
    assert ValidationStatus.PASS == 'PASS'
    assert ValidationStatus.FAIL == 'FAIL'
    assert ExecutionStatus.BLOCKED == 'BLOCKED'
    assert ExecutionStatus.FILLED == 'FILLED'
    assert ReadinessStatus.PAPER_ONLY == 'PAPER_ONLY'
    assert HealthStatus.DEGRADED == 'DEGRADED'
