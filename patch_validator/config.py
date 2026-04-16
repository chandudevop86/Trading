from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:  # pragma: no cover - optional dependency fallback
    yaml = None


@dataclass(slots=True)
class RiskThresholds:
    low_max: int = 20
    moderate_max: int = 45
    high_max: int = 70
    critical_max: int = 100


@dataclass(slots=True)
class RuleConfig:
    forbidden_imports: dict[str, list[str]] = field(default_factory=dict)
    protected_files: list[str] = field(default_factory=list)
    required_tests: dict[str, list[str]] = field(default_factory=dict)
    required_reviewers: dict[str, list[str]] = field(default_factory=dict)
    critical_modules: list[str] = field(default_factory=list)
    controller_layer_paths: list[str] = field(default_factory=lambda: ["app/vinayak/api/routes/", "app/vinayak/web/app/", "app/vinayak/ui/"])
    forbidden_controller_tokens: list[str] = field(default_factory=lambda: ["session.query(", "requests.", "httpx.", "open(", "Path(", "subprocess.", "os.system("])
    duplicate_logic_min_chars: int = 80


@dataclass(slots=True)
class ValidatorConfig:
    repo_root: Path
    oversized_patch_added_lines: int = 500
    oversized_patch_file_count: int = 20
    require_tests_for_python_changes: bool = True
    timeout_required_modules: list[str] = field(default_factory=lambda: ["requests", "httpx"])
    max_timeoutless_calls_per_file: int = 0
    risk_thresholds: RiskThresholds = field(default_factory=RiskThresholds)
    rules: RuleConfig = field(default_factory=RuleConfig)

    @classmethod
    def from_file(cls, repo_root: Path, config_path: Path | None = None) -> "ValidatorConfig":
        resolved = config_path or repo_root / ".patch-validator.yaml"
        if not resolved.exists():
            return cls(repo_root=repo_root)
        payload = _load_config_payload(resolved)
        return cls(
            repo_root=repo_root,
            oversized_patch_added_lines=int(payload.get("oversized_patch_added_lines", 500)),
            oversized_patch_file_count=int(payload.get("oversized_patch_file_count", 20)),
            require_tests_for_python_changes=bool(payload.get("require_tests_for_python_changes", True)),
            timeout_required_modules=list(payload.get("timeout_required_modules", ["requests", "httpx"])),
            max_timeoutless_calls_per_file=int(payload.get("max_timeoutless_calls_per_file", 0)),
            risk_thresholds=RiskThresholds(**payload.get("risk_thresholds", {})),
            rules=RuleConfig(**payload.get("rules", {})),
        )


def _load_config_payload(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(raw)
    if yaml is not None:
        payload = yaml.safe_load(raw) or {}
    else:
        payload = _simple_yaml_load(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"Config file {path} must deserialize to an object.")
    return payload


def _simple_yaml_load(raw: str) -> dict[str, Any]:
    items = []
    for raw_line in raw.splitlines():
        if not raw_line.strip() or raw_line.strip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        items.append((indent, raw_line.strip()))
    parsed, _ = _parse_yaml_block(items, 0, 0)
    if not isinstance(parsed, dict):
        raise ValueError("Fallback YAML parser expected a mapping at the document root.")
    return parsed


def _parse_yaml_block(items: list[tuple[int, str]], start: int, indent: int) -> tuple[Any, int]:
    if start >= len(items):
        return {}, start
    if items[start][1].startswith("- "):
        output: list[Any] = []
        index = start
        while index < len(items):
            current_indent, line = items[index]
            if current_indent < indent:
                break
            if current_indent != indent or not line.startswith("- "):
                break
            output.append(_coerce_scalar(line[2:].strip()))
            index += 1
        return output, index

    output_dict: dict[str, Any] = {}
    index = start
    while index < len(items):
        current_indent, line = items[index]
        if current_indent < indent:
            break
        if current_indent != indent:
            raise ValueError("Unsupported YAML indentation structure in fallback parser.")
        key, _, value_part = line.partition(":")
        key = key.strip().strip("\"'")
        value_part = value_part.strip()
        if value_part:
            output_dict[key] = _coerce_scalar(value_part)
            index += 1
            continue
        next_index = index + 1
        if next_index >= len(items) or items[next_index][0] <= current_indent:
            output_dict[key] = {}
            index = next_index
            continue
        nested_value, index = _parse_yaml_block(items, next_index, items[next_index][0])
        output_dict[key] = nested_value
    return output_dict, index


def _coerce_scalar(value: str) -> Any:
    text = value.strip().strip("\"'")
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        return int(text)
    except ValueError:
        return text
