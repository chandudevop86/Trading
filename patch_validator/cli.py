from __future__ import annotations

import argparse
from pathlib import Path

from patch_validator.engine import PatchValidatorEngine
from patch_validator.reporting import build_json, build_markdown


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a git patch or unified diff before merge.")
    parser.add_argument("--repo-root", default=".", help="Repository root to inspect.")
    parser.add_argument("--diff-path", help="Path to a unified diff file. Uses git diff when omitted.")
    parser.add_argument("--base-ref", help="Optional git base ref for diff generation.")
    parser.add_argument("--config", help="Optional path to .patch-validator YAML or JSON config.")
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown", help="Output format.")
    parser.add_argument("--output", help="Optional file path for report output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    engine = PatchValidatorEngine(Path(args.repo_root).resolve(), config_path=Path(args.config).resolve() if args.config else None)
    report = engine.validate(diff_path=Path(args.diff_path).resolve() if args.diff_path else None, base_ref=args.base_ref)
    rendered = build_json(report) if args.format == "json" else build_markdown(report)
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    return 0 if report.overall_status.value == "APPROVE" else 1
