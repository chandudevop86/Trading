from __future__ import annotations

import re

from patch_validator.models import Finding, RuleContext, Severity
from patch_validator.rules.base import Rule


SECRET_RE = re.compile(r"(?i)\b(password|secret|token|api[_-]?key|access[_-]?key)\b\s*[:=]\s*['\"][^'\"]{6,}['\"]")
AUTH_BYPASS_RE = re.compile(r"(?i)\b(skip_auth|disable_auth|auth_disabled|bypass_auth|allow_all|verify=False)\b")
GUARD_BYPASS_RE = re.compile(r"(?i)\b(execution_allowed\s*=\s*True|kill_switch_enabled\s*=\s*False|return\s+True\s*#?\s*(auth|guard|validation)?)")
SHELL_RE = re.compile(r"\b(os\.system|subprocess\.(run|Popen|call)|shell=True)\b")
SQLI_RE = re.compile(r"(?i)(SELECT|UPDATE|DELETE|INSERT).*(\%s|f['\"]|format\()")
DESERIALIZE_RE = re.compile(r"\b(pickle\.loads|yaml\.load\(|eval\(|exec\()")


class SecurityRules(Rule):
    def evaluate(self, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        for patch_file in context.changed_files:
            for line in patch_file.added_lines:
                content = line.content.strip()
                if not content or content.startswith("#"):
                    continue
                findings.extend(self._check_line(patch_file.path, line.line_no_new, content))
        return findings

    def _check_line(self, path: str, line_no: int | None, content: str) -> list[Finding]:
        findings: list[Finding] = []
        if SECRET_RE.search(content):
            findings.append(
                Finding(
                    rule_id="SEC-001",
                    severity=Severity.CRITICAL,
                    title="Hardcoded credential detected",
                    file=path,
                    line=line_no,
                    explanation="A literal credential-like value is introduced in code or config.",
                    evidence=content,
                    suggested_fix="Move secrets to environment variables or a secret manager and reference them securely.",
                )
            )
        if AUTH_BYPASS_RE.search(content) or GUARD_BYPASS_RE.search(content):
            findings.append(
                Finding(
                    rule_id="SEC-002",
                    severity=Severity.CRITICAL,
                    title="Authentication or validation bypass pattern detected",
                    file=path,
                    line=line_no,
                    explanation="The patch introduces a pattern that may disable authentication, guards, or validation logic.",
                    evidence=content,
                    suggested_fix="Restore the guard path and gate any debug-only behavior behind an explicit secure test seam.",
                )
            )
        if SHELL_RE.search(content):
            findings.append(
                Finding(
                    rule_id="SEC-003",
                    severity=Severity.HIGH,
                    title="Unsafe shell execution introduced",
                    file=path,
                    line=line_no,
                    explanation="Shell execution is risky and should be tightly controlled or avoided.",
                    evidence=content,
                    suggested_fix="Use structured subprocess argument lists, disable shell expansion, and wrap commands in a reviewed execution helper.",
                )
            )
        if SQLI_RE.search(content):
            findings.append(
                Finding(
                    rule_id="SEC-004",
                    severity=Severity.HIGH,
                    title="Potential SQL injection pattern",
                    file=path,
                    line=line_no,
                    explanation="Dynamic SQL construction appears in the patch.",
                    evidence=content,
                    suggested_fix="Use parameterized ORM or database APIs instead of interpolated SQL strings.",
                )
            )
        if DESERIALIZE_RE.search(content):
            findings.append(
                Finding(
                    rule_id="SEC-005",
                    severity=Severity.HIGH,
                    title="Dangerous evaluation or deserialization pattern",
                    file=path,
                    line=line_no,
                    explanation="Unsafe deserialization or evaluation can execute attacker-controlled code.",
                    evidence=content,
                    suggested_fix="Replace with safe parsing APIs such as yaml.safe_load or typed JSON decoding.",
                )
            )
        return findings
