from __future__ import annotations

from typing import Any


def _to_number_if_possible(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            return int(text)
        try:
            return float(text)
        except ValueError:
            return value
    return value


def evaluate_rule(value: Any, op: str, target: Any) -> bool:
    value = _to_number_if_possible(value)
    target = _to_number_if_possible(target)

    if op == ">":
        return value > target
    if op == "<":
        return value < target
    if op == ">=":
        return value >= target
    if op == "<=":
        return value <= target
    if op == "==":
        return value == target
    if op == "!=":
        return value != target
    if op == "in":
        return value in target
    if op == "not in":
        return value not in target
    raise ValueError(f"Unsupported operator: {op}")
