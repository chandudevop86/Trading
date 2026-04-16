from __future__ import annotations

import re
import subprocess
from pathlib import Path

from patch_validator.models import PatchFile, PatchLine


HUNK_RE = re.compile(r"^@@ -(?P<old>\d+)(?:,\d+)? \+(?P<new>\d+)(?:,\d+)? @@")


class DiffParserError(RuntimeError):
    """Raised when the validator cannot obtain or parse a patch."""


def load_patch_text(repo_root: Path, *, diff_path: Path | None = None, base_ref: str | None = None) -> str:
    if diff_path is not None:
        return diff_path.read_text(encoding="utf-8")
    git_dir = repo_root / ".git"
    if not git_dir.exists():
        raise DiffParserError("No diff_path provided and repository does not contain a .git directory.")
    command = ["git", "-C", str(repo_root), "diff", "--no-ext-diff"]
    if base_ref:
        command.append(base_ref)
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=True)
    except FileNotFoundError as exc:
        raise DiffParserError("Git is not installed or not available on PATH. Provide --diff-path instead.") from exc
    except subprocess.CalledProcessError as exc:
        raise DiffParserError(exc.stderr.strip() or "Unable to load git diff.") from exc
    return completed.stdout


def parse_patch(patch_text: str) -> list[PatchFile]:
    files: list[PatchFile] = []
    current: PatchFile | None = None
    old_line_no: int | None = None
    new_line_no: int | None = None

    for raw_line in patch_text.splitlines():
        if raw_line.startswith("diff --git "):
            if current is not None:
                files.append(current)
            parts = raw_line.split()
            old_path = parts[2][2:]
            new_path = parts[3][2:]
            current = PatchFile(path=new_path, old_path=old_path)
            old_line_no = None
            new_line_no = None
            continue
        if current is None:
            continue
        if raw_line.startswith("--- "):
            current.old_path = _normalize_diff_path(raw_line[4:])
            current.is_new = current.old_path == "/dev/null"
            continue
        if raw_line.startswith("+++ "):
            current.path = _normalize_diff_path(raw_line[4:])
            current.is_deleted = current.path == "/dev/null"
            continue
        if raw_line.startswith("@@"):
            match = HUNK_RE.match(raw_line)
            if not match:
                continue
            old_line_no = int(match.group("old"))
            new_line_no = int(match.group("new"))
            continue
        if raw_line.startswith("+") and not raw_line.startswith("+++"):
            current.added_lines.append(PatchLine(old_line_no, new_line_no, raw_line[1:], "add"))
            if new_line_no is not None:
                new_line_no += 1
            continue
        if raw_line.startswith("-") and not raw_line.startswith("---"):
            current.removed_lines.append(PatchLine(old_line_no, new_line_no, raw_line[1:], "remove"))
            if old_line_no is not None:
                old_line_no += 1
            continue
        if raw_line.startswith("\\ No newline at end of file"):
            continue
        if old_line_no is not None:
            old_line_no += 1
        if new_line_no is not None:
            new_line_no += 1

    if current is not None:
        files.append(current)
    return [item for item in files if item.path != "/dev/null"]


def _normalize_diff_path(raw_path: str) -> str:
    value = raw_path.strip()
    if value.startswith("a/") or value.startswith("b/"):
        return value[2:]
    return value
