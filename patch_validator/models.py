from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class OverallStatus(str, Enum):
    APPROVE = "APPROVE"
    NEEDS_FIX = "NEEDS_FIX"
    REJECT = "REJECT"


@dataclass(slots=True)
class PatchLine:
    line_no_old: int | None
    line_no_new: int | None
    content: str
    kind: str


@dataclass(slots=True)
class PatchFile:
    path: str
    old_path: str
    is_new: bool = False
    is_deleted: bool = False
    added_lines: list[PatchLine] = field(default_factory=list)
    removed_lines: list[PatchLine] = field(default_factory=list)

    @property
    def extension(self) -> str:
        return Path(self.path).suffix.lower()

    @property
    def added_line_count(self) -> int:
        return len(self.added_lines)

    @property
    def removed_line_count(self) -> int:
        return len(self.removed_lines)


@dataclass(slots=True)
class PatchIntent:
    patch_types: list[str]
    mixed_concerns: bool
    oversized: bool
    changed_files: list[str]
    total_added_lines: int
    total_removed_lines: int


@dataclass(slots=True)
class Finding:
    rule_id: str
    severity: Severity
    title: str
    file: str
    explanation: str
    evidence: str
    suggested_fix: str
    line: int | None = None
    follow_up: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["severity"] = self.severity.value
        return payload


@dataclass(slots=True)
class RuleContext:
    repo_root: Path
    changed_files: list[PatchFile]
    all_changed_paths: list[str]
    patch_text: str
    config: "ValidatorConfig"
    detected_test_files: set[str] = field(default_factory=set)
    intent: PatchIntent | None = None


@dataclass(slots=True)
class ValidationReport:
    overall_status: OverallStatus
    risk_score: int
    patch_intent: PatchIntent
    findings: list[Finding]
    findings_by_severity: dict[str, list[dict[str, Any]]]
    per_file_findings: dict[str, list[dict[str, Any]]]
    suggested_fixes: list[str]
    required_follow_ups: list[str]
    manual_review_required: bool
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status.value,
            "risk_score": self.risk_score,
            "patch_intent": {
                "patch_types": self.patch_intent.patch_types,
                "mixed_concerns": self.patch_intent.mixed_concerns,
                "oversized": self.patch_intent.oversized,
                "changed_files": self.patch_intent.changed_files,
                "total_added_lines": self.patch_intent.total_added_lines,
                "total_removed_lines": self.patch_intent.total_removed_lines,
            },
            "findings": [finding.to_dict() for finding in self.findings],
            "findings_by_severity": self.findings_by_severity,
            "per_file_findings": self.per_file_findings,
            "suggested_fixes": self.suggested_fixes,
            "required_follow_ups": self.required_follow_ups,
            "manual_review_required": self.manual_review_required,
            "metadata": self.metadata,
        }


from patch_validator.config import ValidatorConfig  # noqa: E402
