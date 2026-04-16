from __future__ import annotations

from pathlib import Path

from patch_validator.engine import PatchValidatorEngine


def test_engine_detects_hardcoded_secret_and_missing_tests(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / ".git").mkdir()
    (repo_root / "app" / "vinayak" / "auth").mkdir(parents=True)
    (repo_root / ".patch-validator.yaml").write_text("{}", encoding="utf-8")
    diff_path = repo_root / "patch.diff"
    diff_path.write_text(
        "\n".join(
            [
                "diff --git a/app/vinayak/auth/service.py b/app/vinayak/auth/service.py",
                "--- a/app/vinayak/auth/service.py",
                "+++ b/app/vinayak/auth/service.py",
                "@@ -1,1 +1,3 @@",
                "+password = 'admin123456'",
                "+requests.get('https://example.com/api')",
            ]
        ),
        encoding="utf-8",
    )
    engine = PatchValidatorEngine(repo_root)
    report = engine.validate(diff_path=diff_path)

    assert report.overall_status.value in {"NEEDS_FIX", "REJECT"}
    assert any(item.rule_id == "SEC-001" for item in report.findings)
    assert any(item.rule_id == "REL-001" for item in report.findings)
    assert any(item.rule_id == "TEST-001" for item in report.findings)


def test_engine_marks_oversized_patch(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / ".git").mkdir()
    (repo_root / ".patch-validator.yaml").write_text(
        "oversized_patch_added_lines: 1\noversized_patch_file_count: 1\n",
        encoding="utf-8",
    )
    diff_path = repo_root / "patch.diff"
    diff_path.write_text(
        "\n".join(
            [
                "diff --git a/infra/deploy.yml b/infra/deploy.yml",
                "--- a/infra/deploy.yml",
                "+++ b/infra/deploy.yml",
                "@@ -1,0 +1,2 @@",
                "+feature_flag: true",
                "+kind: deploy",
            ]
        ),
        encoding="utf-8",
    )

    report = PatchValidatorEngine(repo_root).validate(diff_path=diff_path)

    assert report.patch_intent.oversized is True
    assert any(item.rule_id == "SCOPE-001" for item in report.findings)
