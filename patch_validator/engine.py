from __future__ import annotations

from pathlib import Path

from patch_validator.config import ValidatorConfig
from patch_validator.diff_parser import load_patch_text, parse_patch
from patch_validator.intent import detect_patch_intent
from patch_validator.models import Finding, RuleContext, ValidationReport
from patch_validator.rules import ArchitectureRules, ReliabilityRules, ScopeRules, SecurityRules, TestingRules
from patch_validator.scoring import compute_risk_score, decide_status


class PatchValidatorEngine:
    def __init__(self, repo_root: Path, *, config_path: Path | None = None) -> None:
        self.repo_root = repo_root
        self.config = ValidatorConfig.from_file(repo_root, config_path)
        self.rules = [
            ScopeRules(),
            SecurityRules(),
            ArchitectureRules(),
            ReliabilityRules(),
            TestingRules(),
        ]

    def validate(self, *, diff_path: Path | None = None, base_ref: str | None = None) -> ValidationReport:
        patch_text = load_patch_text(self.repo_root, diff_path=diff_path, base_ref=base_ref)
        changed_files = parse_patch(patch_text)
        intent = detect_patch_intent(changed_files, self.config)
        context = RuleContext(
            repo_root=self.repo_root,
            changed_files=changed_files,
            all_changed_paths=[item.path for item in changed_files],
            patch_text=patch_text,
            config=self.config,
            intent=intent,
        )
        findings: list[Finding] = []
        for rule in self.rules:
            findings.extend(rule.evaluate(context))
        risk_score = compute_risk_score(findings)
        overall_status, manual_review = decide_status(findings, risk_score)
        suggested_fixes = _unique([item.suggested_fix for item in findings])
        required_follow_ups = _unique([item.follow_up for item in findings if item.follow_up])
        findings_by_severity: dict[str, list[dict[str, object]]] = {}
        per_file_findings: dict[str, list[dict[str, object]]] = {}
        for severity in ("critical", "high", "medium", "low"):
            findings_by_severity[severity] = [item.to_dict() for item in findings if item.severity.value == severity]
        for item in findings:
            per_file_findings.setdefault(item.file, []).append(item.to_dict())
        return ValidationReport(
            overall_status=overall_status,
            risk_score=risk_score,
            patch_intent=intent,
            findings=findings,
            findings_by_severity=findings_by_severity,
            per_file_findings=per_file_findings,
            suggested_fixes=suggested_fixes,
            required_follow_ups=required_follow_ups,
            manual_review_required=manual_review,
            metadata={"changed_file_count": len(changed_files)},
        )


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            output.append(item)
    return output
