from __future__ import annotations

from pathlib import Path

from patch_validator.models import Finding, RuleContext, Severity
from patch_validator.rules.base import Rule


class ArchitectureRules(Rule):
    def evaluate(self, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        findings.extend(self._check_forbidden_imports(context))
        findings.extend(self._check_controller_boundary_tokens(context))
        findings.extend(self._check_duplicate_logic(context))
        findings.extend(self._check_protected_files(context))
        return findings

    def _check_forbidden_imports(self, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        for patch_file in context.changed_files:
            for path_prefix, banned_imports in context.config.rules.forbidden_imports.items():
                if not patch_file.path.startswith(path_prefix):
                    continue
                for line in patch_file.added_lines:
                    content = line.content.strip()
                    for banned_import in banned_imports:
                        if f"import {banned_import}" in content or f"from {banned_import} import" in content:
                            findings.append(
                                Finding(
                                    rule_id="ARCH-001",
                                    severity=Severity.HIGH,
                                    title="Forbidden import violates layer boundary",
                                    file=patch_file.path,
                                    line=line.line_no_new,
                                    explanation="The patch imports a module that is forbidden for this layer.",
                                    evidence=content,
                                    suggested_fix="Move the logic into an approved service layer or use the existing abstraction.",
                                )
                            )
        return findings

    def _check_controller_boundary_tokens(self, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        for patch_file in context.changed_files:
            if not any(patch_file.path.startswith(prefix) for prefix in context.config.rules.controller_layer_paths):
                continue
            for line in patch_file.added_lines:
                content = line.content.strip()
                for token in context.config.rules.forbidden_controller_tokens:
                    if token in content:
                        findings.append(
                            Finding(
                                rule_id="ARCH-002",
                                severity=Severity.HIGH,
                                title="Controller layer bypasses approved service boundary",
                                file=patch_file.path,
                                line=line.line_no_new,
                                explanation="Controller or UI code should delegate IO and business logic to service layers.",
                                evidence=content,
                                suggested_fix="Move IO or business logic into a service, repository, or domain module and call it from the controller.",
                            )
                        )
        return findings

    def _check_duplicate_logic(self, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        min_chars = context.config.rules.duplicate_logic_min_chars
        for patch_file in context.changed_files:
            for line in patch_file.added_lines:
                content = line.content.strip()
                if len(content) < min_chars or content.startswith(("def ", "class ", "#", "return ", "if ")):
                    continue
                if _line_exists_elsewhere(context.repo_root, patch_file.path, content):
                    findings.append(
                        Finding(
                            rule_id="ARCH-003",
                            severity=Severity.MEDIUM,
                            title="Possible duplicate logic introduced",
                            file=patch_file.path,
                            line=line.line_no_new,
                            explanation="A substantially similar line already exists elsewhere in the repository.",
                            evidence=content,
                            suggested_fix="Reuse the existing helper or extract the shared logic into a common module.",
                        )
                    )
                    break
        return findings

    def _check_protected_files(self, context: RuleContext) -> list[Finding]:
        findings: list[Finding] = []
        protected = context.config.rules.protected_files
        for patch_file in context.changed_files:
            if patch_file.path in protected:
                findings.append(
                    Finding(
                        rule_id="ARCH-004",
                        severity=Severity.HIGH,
                        title="Protected file modified",
                        file=patch_file.path,
                        explanation="This file is marked as protected and requires stricter review.",
                        evidence=patch_file.path,
                        suggested_fix="Ensure required reviewers are included and justify the change in the patch summary.",
                        follow_up="Require manual review.",
                    )
                )
        return findings


def _line_exists_elsewhere(repo_root: Path, changed_path: str, candidate: str) -> bool:
    for path in repo_root.rglob("*.py"):
        relative = path.relative_to(repo_root).as_posix()
        if relative == changed_path or "snapshots/" in relative or "__pycache__" in relative:
            continue
        try:
            if candidate in path.read_text(encoding="utf-8", errors="ignore"):
                return True
        except OSError:
            continue
    return False
