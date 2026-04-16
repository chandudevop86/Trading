# Patch Validator

`patch_validator/` adds a production-oriented patch audit engine for this repository. It inspects a unified diff or live git diff and emits:

- overall status: `APPROVE`, `NEEDS_FIX`, or `REJECT`
- risk score from `0-100`
- grouped findings by severity
- per-file findings
- suggested fixes and required follow-ups
- machine-readable JSON or human-readable Markdown

## Repo understanding

This repository is a Python codebase centered on `app/vinayak/` with layered modules for API routes and services, auth, execution, strategies, observability, repositories, and tests under `tests/unit` and `tests/integration`. The validator is implemented in Python and tuned for layered boundaries common in this repo, especially auth, API route, and execution paths.

## Architecture

The validator is split into small modules:

- `patch_validator/diff_parser.py`: loads a diff from git or a file and parses changed files and added lines
- `patch_validator/config.py`: loads `.patch-validator.yaml` or JSON config
- `patch_validator/intent.py`: classifies patch type and scope risk
- `patch_validator/rules/`: modular rule evaluators
- `patch_validator/scoring.py`: converts findings into risk score and status
- `patch_validator/reporting.py`: JSON and Markdown report rendering
- `patch_validator/engine.py`: orchestration layer
- `patch_validator/cli.py`: CLI entrypoint

## MVP checks

The current MVP implements the highest-priority checks:

1. hardcoded secrets
2. auth or validation bypass patterns
3. forbidden imports by layer
4. missing tests for changed Python modules
5. missing timeout on `requests` and `httpx` calls
6. duplicate logic heuristics
7. structured JSON and Markdown output

Additional checks in the MVP:

- oversized patches
- mixed-concern patches
- protected file modifications
- controller/service boundary bypass heuristics
- silent exception swallowing

## Usage

Run against the current git diff:

```bash
python -m patch_validator --repo-root . --format markdown
```

Run against a base branch:

```bash
python -m patch_validator --repo-root . --base-ref origin/main --format json
```

Run against a saved diff:

```bash
python -m patch_validator --repo-root . --diff-path artifacts/patch.diff --output artifacts/patch-report.json --format json
```

Use a custom config:

```bash
python -m patch_validator --repo-root . --config .patch-validator.yaml --format markdown
```

## Final report format

Example JSON finding:

```json
{
  "rule_id": "SEC-001",
  "severity": "critical",
  "title": "Hardcoded credential detected",
  "file": "app/config.py",
  "explanation": "A literal credential-like value is introduced in code or config.",
  "evidence": "password = 'admin123'",
  "suggested_fix": "Move secrets to environment variables or a secret manager."
}
```

Top-level JSON output:

```json
{
  "overall_status": "NEEDS_FIX",
  "risk_score": 55,
  "patch_intent": {
    "patch_types": ["feature", "security"],
    "mixed_concerns": true,
    "oversized": false,
    "changed_files": ["app/vinayak/auth/service.py"],
    "total_added_lines": 42,
    "total_removed_lines": 10
  },
  "findings": [],
  "findings_by_severity": {
    "critical": [],
    "high": [],
    "medium": [],
    "low": []
  },
  "per_file_findings": {},
  "suggested_fixes": [],
  "required_follow_ups": [],
  "manual_review_required": true
}
```

## Local and CI execution

Local:

```bash
python -m patch_validator --repo-root . --diff-path patch.diff --format markdown
```

CI:

Use the sample workflow at `.github/workflows/patch-validator.yml`.

## Next improvements

- add AST-aware analysis for imports, auth flows, and timeout detection
- add migration and rollback-plan checks
- add observability checks for structured logs and metrics on critical paths
- add required-reviewer enforcement from repo policy files
- add feature-flag and backward-compatibility checks for API and schema changes
