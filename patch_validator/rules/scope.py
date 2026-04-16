from __future__ import annotations

from patch_validator.models import Finding, RuleContext, Severity
from patch_validator.rules.base import Rule


class ScopeRules(Rule):
    def evaluate(self, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        intent = context.intent
        if intent is None:
            return findings
        if intent.oversized:
            findings.append(
                Finding(
                    rule_id="SCOPE-001",
                    severity=Severity.HIGH,
                    title="Oversized patch requires staged review",
                    file="*",
                    explanation="Patch size exceeds configured thresholds and raises regression risk.",
                    evidence=f"{len(intent.changed_files)} files, {intent.total_added_lines} added lines",
                    suggested_fix="Split the patch into smaller focused changes or add a manual rollout and validation plan.",
                    follow_up="Require manual review.",
                )
            )
        if intent.mixed_concerns:
            findings.append(
                Finding(
                    rule_id="SCOPE-002",
                    severity=Severity.MEDIUM,
                    title="Mixed-concern patch detected",
                    file="*",
                    explanation="Patch combines multiple change intents, which makes review and rollback harder.",
                    evidence=", ".join(intent.patch_types),
                    suggested_fix="Separate unrelated concerns into distinct pull requests or document rollout ordering.",
                )
            )
        return findings
