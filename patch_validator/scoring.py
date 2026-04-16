from __future__ import annotations

from patch_validator.models import Finding, OverallStatus, Severity


SEVERITY_POINTS = {
    Severity.CRITICAL: 35,
    Severity.HIGH: 20,
    Severity.MEDIUM: 10,
    Severity.LOW: 5,
}


def compute_risk_score(findings: list[Finding]) -> int:
    total = sum(SEVERITY_POINTS[item.severity] for item in findings)
    return max(0, min(total, 100))


def decide_status(findings: list[Finding], risk_score: int) -> tuple[OverallStatus, bool]:
    severities = {item.severity for item in findings}
    manual_review = bool(severities & {Severity.CRITICAL, Severity.HIGH}) or risk_score >= 46
    if Severity.CRITICAL in severities or risk_score >= 71:
        return OverallStatus.REJECT, True
    if Severity.HIGH in severities or Severity.MEDIUM in severities or risk_score >= 21:
        return OverallStatus.NEEDS_FIX, manual_review
    return OverallStatus.APPROVE, manual_review
