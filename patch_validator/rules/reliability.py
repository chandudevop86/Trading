from __future__ import annotations

import re

from patch_validator.models import Finding, RuleContext, Severity
from patch_validator.rules.base import Rule


REQUEST_CALL_RE = re.compile(r"\b(?P<module>requests|httpx)\.(get|post|put|delete|patch|request)\(")
EMPTY_EXCEPT_RE = re.compile(r"^\s*except\b.*:\s*$")
PASS_RE = re.compile(r"^\s*(pass|return\s+None)\s*$")


class ReliabilityRules(Rule):
    def evaluate(self, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        for patch_file in context.changed_files:
            findings.extend(self._check_timeouts(patch_file, context))
            findings.extend(self._check_silent_exceptions(patch_file))
        return findings

    def _check_timeouts(self, patch_file, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        for line in patch_file.added_lines:
            content = line.content.strip()
            match = REQUEST_CALL_RE.search(content)
            if not match:
                continue
            module = match.group("module")
            if module not in context.config.timeout_required_modules:
                continue
            if "timeout=" in content:
                continue
            findings.append(
                Finding(
                    rule_id="REL-001",
                    severity=Severity.HIGH,
                    title="External call missing timeout",
                    file=patch_file.path,
                    line=line.line_no_new,
                    explanation="Network calls in production paths should declare an explicit timeout.",
                    evidence=content,
                    suggested_fix="Add a timeout=... argument or route the call through a shared client with default timeouts and retries.",
                )
            )
        return findings

    def _check_silent_exceptions(self, patch_file) -> list[Finding]:
        findings: list[Finding] = []
        added = patch_file.added_lines
        for index, line in enumerate(added):
            if not EMPTY_EXCEPT_RE.match(line.content):
                continue
            next_line = added[index + 1].content if index + 1 < len(added) else ""
            if PASS_RE.match(next_line.strip()):
                findings.append(
                    Finding(
                        rule_id="REL-002",
                        severity=Severity.MEDIUM,
                        title="Silent exception swallowing detected",
                        file=patch_file.path,
                        line=line.line_no_new,
                        explanation="Exceptions should be logged, re-raised, or translated into explicit failure states.",
                        evidence=f"{line.content} {next_line.strip()}".strip(),
                        suggested_fix="Log the exception with context and fail safely instead of swallowing it.",
                    )
                )
        return findings
