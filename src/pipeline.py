from __future__ import annotations

from pathlib import Path

import yaml

from src.csv_io import read_csv_rows, write_csv_rows
from src.rule_engine import evaluate_rule


def apply_rules(rows: list[dict[str, str]], rules: list[dict]) -> list[dict[str, str]]:
    for row in rows:
        for rule in rules:
            name = rule["name"]
            field = rule["field"]
            op = rule["op"]
            target = rule["value"]
            row[name] = str(evaluate_rule(row[field], op, target))
    return rows


def load_rules(rules_path: Path) -> list[dict]:
    with rules_path.open("r", encoding="utf-8") as f:
        rules = yaml.safe_load(f)
    if not isinstance(rules, list):
        raise ValueError("Rules YAML must contain a list of rule objects")
    return rules


def run(input_path: Path, rules_path: Path, output_path: Path) -> None:
    rows = read_csv_rows(input_path)
    rules = load_rules(rules_path)
    processed = apply_rules(rows, rules)
    write_csv_rows(output_path, processed)
