from __future__ import annotations

import json
from collections import defaultdict

from patch_validator.models import Finding, Severity, ValidationReport


def build_json(report: ValidationReport) -> str:
    return json.dumps(report.to_dict(), indent=2)


def build_markdown(report: ValidationReport) -> str:
    lines = [
        "# Patch Validation Report",
        "",
        f"- Overall status: `{report.overall_status.value}`",
        f"- Risk score: `{report.risk_score}`",
        f"- Patch types: `{', '.join(report.patch_intent.patch_types)}`",
        f"- Manual review required: `{report.manual_review_required}`",
        "",
        "## Findings",
    ]
    if not report.findings:
        lines.extend(["", "No findings detected."])
        return "\n".join(lines)
    grouped: dict[Severity, list[Finding]] = defaultdict(list)
    for finding in report.findings:
        grouped[finding.severity].append(finding)
    for severity in (Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW):
        items = grouped.get(severity, [])
        if not items:
            continue
        lines.extend(["", f"### {severity.value.title()}"])
        for item in items:
            location = f"{item.file}:{item.line}" if item.line else item.file
            lines.extend(
                [
                    f"- `{item.rule_id}` {item.title} in `{location}`",
                    f"  - Explanation: {item.explanation}",
                    f"  - Evidence: `{item.evidence}`",
                    f"  - Suggested fix: {item.suggested_fix}",
                ]
            )
    if report.required_follow_ups:
        lines.extend(["", "## Required Follow-ups"])
        for item in report.required_follow_ups:
            lines.append(f"- {item}")
    return "\n".join(lines)
