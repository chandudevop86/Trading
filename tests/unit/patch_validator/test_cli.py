from __future__ import annotations

from pathlib import Path

from patch_validator.cli import main


def test_cli_writes_json_report(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / ".git").mkdir()
    (repo_root / ".patch-validator.yaml").write_text("{}", encoding="utf-8")
    diff_path = repo_root / "patch.diff"
    diff_path.write_text(
        "\n".join(
            [
                "diff --git a/tests/unit/test_ok.py b/tests/unit/test_ok.py",
                "--- a/tests/unit/test_ok.py",
                "+++ b/tests/unit/test_ok.py",
                "@@ -0,0 +1,1 @@",
                "+assert True",
            ]
        ),
        encoding="utf-8",
    )
    output_path = repo_root / "report.json"

    exit_code = main(
        [
            "--repo-root",
            str(repo_root),
            "--diff-path",
            str(diff_path),
            "--format",
            "json",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert output_path.exists()
    assert '"overall_status": "APPROVE"' in output_path.read_text(encoding="utf-8")
