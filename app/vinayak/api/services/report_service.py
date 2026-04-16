from __future__ import annotations

"""Report persistence helpers for live analysis workflows."""

from typing import Any, Callable


def build_report_artifacts(
    result: dict[str, Any],
    *,
    summary_text: str | None,
    build_trade_summary_fn: Callable[[list[dict[str, Any]]], str],
    store_json_report_fn: Callable[[str, dict[str, Any]], dict[str, str]],
    store_text_report_fn: Callable[..., dict[str, str]],
    cache_json_artifact_fn: Callable[[str, dict[str, Any]], Any],
) -> dict[str, dict[str, str]]:
    trace_rows = result.get("execution_rows") or result.get("signals") or []
    resolved_summary_text = summary_text if summary_text is not None else (build_trade_summary_fn(trace_rows) if trace_rows else "No signals generated for this run.")
    json_artifact = store_json_report_fn("live_analysis_result", result)
    summary_artifact = store_text_report_fn("live_analysis_summary", resolved_summary_text, extension="txt", content_type="text/plain")
    cache_json_artifact_fn("latest_live_analysis", result)
    return {
        "json_report": json_artifact,
        "summary_report": summary_artifact,
    }


def empty_report_artifacts() -> dict[str, dict[str, str]]:
    return {
        "json_report": {"local_path": ""},
        "summary_report": {"local_path": ""},
    }
