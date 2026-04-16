from __future__ import annotations

from pathlib import Path

from patch_validator.models import Finding, RuleContext, Severity
from patch_validator.rules.base import Rule


class TestingRules(Rule):
    def evaluate(self, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        changed_paths = context.all_changed_paths
        detected_tests = {path for path in changed_paths if _is_test_path(path)}
        context.detected_test_files = detected_tests
        if not context.config.require_tests_for_python_changes:
            return findings
        if detected_tests:
            return findings
        for patch_file in context.changed_files:
            if not patch_file.path.endswith(".py") or _is_test_path(patch_file.path):
                continue
            mapped_tests = _guess_test_candidates(patch_file.path)
            findings.append(
                Finding(
                    rule_id="TEST-001",
                    severity=Severity.HIGH,
                    title="Code change lacks corresponding test update",
                    file=patch_file.path,
                    explanation="Python source changed without a matching test file in the patch.",
                    evidence=", ".join(mapped_tests) or "No candidate tests inferred",
                    suggested_fix="Add or update focused unit/integration tests covering the changed behavior and edge cases.",
                    follow_up="Add regression tests before merge.",
                )
            )
        return findings


def _is_test_path(path: str) -> bool:
    normalized = path.replace("\\", "/")
    return normalized.startswith("tests/") or Path(normalized).name.startswith("test_")


def _guess_test_candidates(path: str) -> list[str]:
    file_name = Path(path).stem
    return [
        f"tests/unit/test_{file_name}.py",
        f"tests/unit/vinayak/test_{file_name}.py",
        f"tests/integration/test_{file_name}.py",
        f"tests/integration/vinayak/test_{file_name}.py",
    ]
