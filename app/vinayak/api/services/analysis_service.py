from __future__ import annotations

"""Analysis helpers for the live trading workspace."""

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from vinayak.analytics.readiness import evaluate_readiness
from vinayak.validation.trade_evaluation import build_trade_evaluation_summary


def resolve_workspace_auto_execution_mode(requested_execution_type: str, auto_execute: bool) -> tuple[str, str]:
    requested = str(requested_execution_type or "NONE").strip().upper()
    if not auto_execute:
        return requested, ""
    if requested == "LIVE":
        return "PAPER", "Auto-execute forced to PAPER mode. Live entry and exit require an explicit manual route."
    if requested == "PAPER":
        return "PAPER", "Auto-execute is operating in PAPER mode."
    return requested, ""


def format_expiry(expiry: object) -> str:
    if expiry is None:
        return ""
    text = str(expiry).strip()
    if not text or text in {"-", "N/A"}:
        return ""
    for fmt in ("%Y-%m-%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except Exception:
            pass
    return text


def estimate_weekly_expiry(symbol: str, now: datetime | None = None) -> str:
    normalized_symbol = (symbol or "").strip().upper()
    if normalized_symbol in {"^NSEI", "NIFTY", "NIFTY 50", "NIFTY50"}:
        dt = now or datetime.now(UTC) + timedelta(hours=5, minutes=30)
        days_ahead = (3 - dt.weekday()) % 7
        expiry = dt.date() + timedelta(days=days_ahead)
        return expiry.isoformat()
    return ""


def attach_indicator_trade_levels(rows: list[dict[str, object]], rr_ratio: float, trailing_sl_pct: float) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    sl_frac = max(0.0, float(trailing_sl_pct) / 100.0)
    if sl_frac <= 0:
        sl_frac = 0.002
    for item in rows:
        row = dict(item)
        side = str(row.get("side", "") or "").upper()
        try:
            entry = float(row.get("close", row.get("price", row.get("entry_price", 0.0))) or 0.0)
        except Exception:
            entry = 0.0
        if entry > 0 and side in {"BUY", "SELL"}:
            row["entry_price"] = round(entry, 2)
            if side == "BUY":
                sl = entry * (1.0 - sl_frac)
                tp = entry + (entry - sl) * float(rr_ratio)
            else:
                sl = entry * (1.0 + sl_frac)
                tp = entry - (sl - entry) * float(rr_ratio)
            row["stop_loss"] = round(sl, 2)
            row["target_price"] = round(tp, 2)
        if "timestamp" in row:
            row["timestamp"] = str(row["timestamp"])
        out.append(row)
    return out


def attach_option_metrics(
    rows: list[dict[str, object]],
    *,
    symbol: str,
    fetch_option_metrics: bool,
    fetch_legacy_option_chain_fn: Callable[..., Any] | None,
    extract_legacy_option_records_fn: Callable[[Any], list[dict[str, object]]] | None,
    build_legacy_option_metrics_map_fn: Callable[[list[dict[str, object]]], dict[tuple[int, str], dict[str, object]]] | None,
    normalize_legacy_index_symbol_fn: Callable[[str], str] | None,
) -> list[dict[str, object]]:
    if not rows:
        return rows

    metrics_map: dict[tuple[int, str], dict[str, object]] = {}
    status = "DISABLED"
    if (
        fetch_option_metrics
        and fetch_legacy_option_chain_fn is not None
        and extract_legacy_option_records_fn is not None
        and build_legacy_option_metrics_map_fn is not None
        and normalize_legacy_index_symbol_fn is not None
    ):
        try:
            payload = fetch_legacy_option_chain_fn(normalize_legacy_index_symbol_fn(symbol), timeout=10.0)
            metrics_map = build_legacy_option_metrics_map_fn(extract_legacy_option_records_fn(payload))
            status = "FETCH_OK"
        except Exception:
            metrics_map = {}
            status = "FETCH_FAILED"

    enriched: list[dict[str, object]] = []
    any_nse_match = False
    any_estimated_expiry = False
    for item in rows:
        row = dict(item)
        strike_raw = row.get("strike_price", row.get("option_strike", ""))
        option_type = str(row.get("option_type", "") or "").upper()
        try:
            strike = int(float(strike_raw))
        except Exception:
            strike = 0

        metrics = metrics_map.get((strike, option_type), {}) if strike and option_type else {}
        if isinstance(metrics, dict) and metrics:
            row.update(metrics)
            any_nse_match = True
            if metrics.get("option_expiry"):
                row["option_expiry_source"] = "NSE"

        if not row.get("option_expiry") and row.get("option_strike"):
            estimated_expiry = estimate_weekly_expiry(symbol)
            if estimated_expiry:
                row["option_expiry"] = estimated_expiry
                row["option_expiry_source"] = "ESTIMATED"
                any_estimated_expiry = True

        if row.get("option_expiry"):
            row["option_expiry"] = format_expiry(row.get("option_expiry"))
        enriched.append(row)

    final_status = status
    if any_estimated_expiry and not any_nse_match:
        final_status = "ESTIMATED_EXPIRY_ONLY"
    elif status == "FETCH_OK" and not any_nse_match:
        final_status = "NO_MATCH"
    elif status == "FETCH_OK" and any_nse_match:
        final_status = "NSE_OK"

    for row in enriched:
        row["_option_metrics_status"] = final_status
    return enriched


def attach_lots(rows: list[dict[str, object]], *, lot_size: int, lots: int) -> list[dict[str, object]]:
    lot_size = int(lot_size) if lot_size and int(lot_size) > 0 else 0
    lots = int(lots) if lots and int(lots) > 0 else 0
    if lot_size <= 0 or lots <= 0:
        return rows

    quantity = lot_size * lots
    out: list[dict[str, object]] = []
    for item in rows:
        row = dict(item)
        row["lots"] = lots
        row["quantity"] = quantity
        try:
            ltp = float(row.get("option_ltp", 0) or 0)
        except Exception:
            ltp = 0.0
        if ltp > 0:
            row["order_value"] = round(ltp * quantity, 2)
        out.append(row)
    return out


def normalize_rows(rows: list[dict[str, object]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in [
            "entry_price",
            "stop_loss",
            "target_price",
            "spot_price",
            "option_ltp",
            "option_oi",
            "option_vol",
            "option_iv",
            "order_value",
            "price",
            "share_price",
        ]:
            try:
                if key in item and item[key] is not None and str(item[key]).strip() != "":
                    item[key] = float(item[key])
            except Exception:
                pass
        for key in ["trade_no", "strike_price", "quantity", "lots"]:
            try:
                if key in item and item[key] is not None and str(item[key]).strip() != "":
                    item[key] = int(float(item[key]))
            except Exception:
                pass
        for key in ["timestamp", "entry_time", "signal_time", "executed_at_utc", "option_expiry"]:
            if key in item and item[key] is not None:
                item[key] = str(item[key])
        normalized.append(item)
    return normalized


def validation_summary_from_rows(rows: list[dict[str, Any]], *, strategy: str) -> dict[str, Any]:
    if not rows:
        return {}
    summary = build_trade_evaluation_summary(rows, strategy_name=str(strategy or "VINAYAK"))
    readiness = evaluate_readiness(rows, rows, trade_summary=summary)
    return {
        "clean_trades": summary.get("clean_trades", summary.get("closed_trades", 0)),
        "expectancy_per_trade": summary.get("expectancy_per_trade", 0.0),
        "expectancy_stability_score": summary.get("expectancy_stability_score", 0.0),
        "profit_factor": summary.get("profit_factor", 0.0),
        "profit_factor_stability_score": summary.get("profit_factor_stability_score", 0.0),
        "max_drawdown_pct": summary.get("max_drawdown_pct", 0.0),
        "recovery_factor": summary.get("recovery_factor", 0.0),
        "pass_fail_status": summary.get("pass_fail_status", "NEED_MORE_DATA"),
        "confidence_label": summary.get("confidence_label", "NEED_MORE_DATA"),
        "paper_readiness_summary": summary.get("paper_readiness_summary", ""),
        "go_live_status": summary.get("go_live_status", "PAPER_ONLY"),
        "promotion_status": summary.get("promotion_status", "RESEARCH_ONLY"),
        "warnings": summary.get("warnings", []),
        "pass_fail_reasons": summary.get("pass_fail_reasons", []),
        "system_status": readiness.get("verdict", "NOT_READY"),
        "readiness_reasons": readiness.get("reasons", []),
        "validation_pass_rate": readiness.get("validation_pass_rate", 0.0),
        "top_rejection_reasons": readiness.get("top_rejection_reasons", {}),
        "clean_trade_metrics_only": readiness.get("clean_trade_metrics_only", False),
        "clean_trade_count": readiness.get("clean_trade_count", 0),
        "edge_proof_status": readiness.get("edge_proof_status", "PAPER_ONLY"),
        "readiness_summary": readiness.get("readiness_summary", ""),
        "edge_report": readiness.get("edge_report", {}),
        "regime_report": readiness.get("regime_report", {}),
        "walkforward_report": readiness.get("walkforward_report", {}),
        "regime_consistency_score": readiness.get("regime_consistency_score", 0.0),
        "regime_consistency_label": readiness.get("regime_consistency_label", "DEPENDENT"),
        "dominant_regime": readiness.get("dominant_regime", "none"),
        "weakest_regime": readiness.get("weakest_regime", "none"),
        "walkforward_windows": readiness.get("walkforward_windows", 0),
        "oos_status": readiness.get("oos_status", "OOS_NEED_MORE_DATA"),
        "oos_pass_rate": readiness.get("oos_pass_rate", 0.0),
        "overfit_risk_score": readiness.get("overfit_risk_score", 10.0),
        "overfit_risk_label": readiness.get("overfit_risk_label", "HIGH"),
    }


def build_analysis(
    *,
    context: Any,
    run_strategy_workflow_fn: Callable[..., list[dict[str, object]]],
    attach_levels_fn: Callable[[list[dict[str, object]], float, float], list[dict[str, object]]],
    attach_option_strikes_fn: Callable[..., list[dict[str, object]]],
    attach_option_metrics_fn: Callable[..., list[dict[str, object]]],
    attach_lots_fn: Callable[..., list[dict[str, object]]],
    lot_size: int,
    lots: int,
) -> dict[str, Any]:
    signal_rows = run_strategy_workflow_fn(
        context,
        attach_levels_fn=attach_levels_fn,
        attach_option_strikes_fn=attach_option_strikes_fn,
        attach_option_metrics_fn=lambda rows, **kwargs: rows,
    )
    signal_rows = attach_option_metrics_fn(signal_rows, symbol=context.symbol, fetch_option_metrics=context.fetch_option_metrics)
    signal_rows = attach_lots_fn(signal_rows, lot_size=lot_size, lots=lots)
    signal_rows = normalize_rows(signal_rows)
    side_counts = Counter(str(row.get("side", "") or "").upper() for row in signal_rows if row.get("side"))
    return {
        "signals": signal_rows,
        "signal_count": len(signal_rows),
        "side_counts": dict(side_counts),
    }

