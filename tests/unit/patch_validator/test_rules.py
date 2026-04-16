from __future__ import annotations

from pathlib import Path

from patch_validator.config import ValidatorConfig
from patch_validator.diff_parser import parse_patch
from patch_validator.intent import detect_patch_intent
from patch_validator.models import RuleContext
from patch_validator.rules.architecture import ArchitectureRules
from patch_validator.rules.reliability import ReliabilityRules
from patch_validator.rules.security import SecurityRules
from patch_validator.rules.testing import TestingRules


def _context(tmp_path: Path, patch_text: str, *, config_text: str = "{}") -> RuleContext:
    (tmp_path / ".git").mkdir(exist_ok=True)
    (tmp_path / ".patch-validator.yaml").write_text(config_text, encoding="utf-8")
    config = ValidatorConfig.from_file(tmp_path, tmp_path / ".patch-validator.yaml")
    changed_files = parse_patch(patch_text)
    return RuleContext(
        repo_root=tmp_path,
        changed_files=changed_files,
        all_changed_paths=[item.path for item in changed_files],
        patch_text=patch_text,
        config=config,
        intent=detect_patch_intent(changed_files, config),
    )


def test_security_rule_detects_auth_bypass(tmp_path: Path) -> None:
    context = _context(
        tmp_path,
        "\n".join(
            [
                "diff --git a/app/vinayak/auth/service.py b/app/vinayak/auth/service.py",
                "--- a/app/vinayak/auth/service.py",
                "+++ b/app/vinayak/auth/service.py",
                "@@ -1,0 +1,1 @@",
                "+skip_auth = True",
            ]
        ),
    )

    findings = SecurityRules().evaluate(context)

    assert any(item.rule_id == "SEC-002" for item in findings)


def test_architecture_rule_detects_forbidden_route_import(tmp_path: Path) -> None:
    context = _context(
        tmp_path,
        "\n".join(
            [
                "diff --git a/app/vinayak/api/routes/sample.py b/app/vinayak/api/routes/sample.py",
                "--- a/app/vinayak/api/routes/sample.py",
                "+++ b/app/vinayak/api/routes/sample.py",
                "@@ -1,0 +1,1 @@",
                "+import requests",
            ]
        ),
        config_text=(
            "rules:\n"
            "  forbidden_imports:\n"
            "    \"app/vinayak/api/routes/\":\n"
            "      - requests\n"
        ),
    )

    findings = ArchitectureRules().evaluate(context)

    assert any(item.rule_id == "ARCH-001" for item in findings)


def test_reliability_rule_detects_silent_except(tmp_path: Path) -> None:
    context = _context(
        tmp_path,
        "\n".join(
            [
                "diff --git a/app/vinayak/service.py b/app/vinayak/service.py",
                "--- a/app/vinayak/service.py",
                "+++ b/app/vinayak/service.py",
                "@@ -1,0 +1,2 @@",
                "+except Exception:",
                "+    pass",
            ]
        ),
    )

    findings = ReliabilityRules().evaluate(context)

    assert any(item.rule_id == "REL-002" for item in findings)


def test_testing_rule_flags_python_change_without_tests(tmp_path: Path) -> None:
    context = _context(
        tmp_path,
        "\n".join(
            [
                "diff --git a/app/vinayak/service.py b/app/vinayak/service.py",
                "--- a/app/vinayak/service.py",
                "+++ b/app/vinayak/service.py",
                "@@ -1,0 +1,1 @@",
                "+print('hello')",
            ]
        ),
    )

    findings = TestingRules().evaluate(context)

    assert any(item.rule_id == "TEST-001" for item in findings)
