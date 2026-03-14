from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.breakout_bot import generate_trades as generate_breakout_trades
from src.breakout_bot import load_candles
from src.btst_bot import generate_trades as generate_btst_trades
from src.supply_demand import generate_trades as generate_demand_supply_trades
from src.execution_engine import build_execution_candidates, execute_paper_trades
from src.indicator_bot import IndicatorConfig, generate_indicator_rows
from src.live_ohlcv import fetch_live_ohlcv, write_csv
from src.one_trade_day import generate_trades as generate_one_trade_day_trades


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _pnl_summary(strategy: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnl_values = [_safe_float(r.get("pnl")) for r in rows]
    wins = sum(1 for p in pnl_values if p > 0)
    losses = sum(1 for p in pnl_values if p < 0)
    total = len(rows)
    return {
        "strategy": strategy,
        "trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round((wins / total) * 100.0, 2) if total else 0.0,
        "total_pnl": round(sum(pnl_values), 2),
    }


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _append_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto backtest all bots and write paper logs with timeframe metadata")
    parser.add_argument("--symbol", default="^NSEI")
    parser.add_argument("--interval", default="5m", choices=["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"])
    parser.add_argument("--period", default="5d")
    parser.add_argument("--capital", type=float, default=100000.0)
    parser.add_argument("--risk-pct", type=float, default=0.01)
    parser.add_argument("--rr-ratio", type=float, default=2.0)
    parser.add_argument("--trailing-sl-pct", type=float, default=0.0, help="Trailing stop percent, e.g. 0.005 = 0.5%%")
    parser.add_argument("--pivot-window", type=int, default=2)
    parser.add_argument("--entry-cutoff", default="11:30")
    parser.add_argument("--execution-symbol", default="NIFTY")
    parser.add_argument("--data-output", type=Path, default=Path("data/live_ohlcv.csv"))
    parser.add_argument("--summary-output", type=Path, default=Path("data/backtest_results_all.csv"))
    parser.add_argument("--summary-history-output", type=Path, default=Path("data/backtest_results_history.csv"))
    parser.add_argument("--paper-log-output", type=Path, default=Path("data/paper_trading_logs_all.csv"))
    return parser.parse_args()


def run(args: argparse.Namespace) -> dict[str, Any]:
    run_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    rows = fetch_live_ohlcv(args.symbol, args.interval, args.period)
    if not rows:
        raise ValueError("No OHLCV rows fetched.")
    write_csv(args.data_output, rows)

    candles = load_candles(rows)
    if not candles:
        raise ValueError("No candles generated from OHLCV rows.")

    breakout_rows = generate_breakout_trades(candles, capital=args.capital, risk_pct=args.risk_pct, rr_ratio=args.rr_ratio, trailing_sl_pct=args.trailing_sl_pct)
    for r in breakout_rows:
        r.setdefault("strategy", "BREAKOUT")

    ds_rows = generate_demand_supply_trades(
        candles,
        capital=args.capital,
        risk_pct=args.risk_pct,
        rr_ratio=args.rr_ratio,
        pivot_window=args.pivot_window,
        entry_cutoff_hhmm=args.entry_cutoff,
    )

    indicator_cfg = IndicatorConfig()
    indicator_rows = generate_indicator_rows(candles, config=indicator_cfg)

    one_trade_rows = generate_one_trade_day_trades(
        candles,
        capital=args.capital,
        risk_pct=args.risk_pct,
        rr_ratio=args.rr_ratio,
        config=indicator_cfg,
        entry_cutoff_hhmm=args.entry_cutoff,
    )

    btst_rows = generate_btst_trades(candles, capital=args.capital, risk_pct=args.risk_pct, allow_stbt=True)

    summary_rows = [
        _pnl_summary("BREAKOUT", breakout_rows),
        _pnl_summary("DEMAND_SUPPLY", ds_rows),
        _pnl_summary("ONE_TRADE_DAY", one_trade_rows),
        _pnl_summary("BTST", btst_rows),
    ]

    timeframe = f"{args.symbol}|{args.interval}|{args.period}"
    data_start = rows[0]["timestamp"]
    data_end = rows[-1]["timestamp"]
    for s in summary_rows:
        s["timeframe"] = timeframe
        s["data_start"] = data_start
        s["data_end"] = data_end
        s["run_at_utc"] = run_at

    _write_rows(args.summary_output, summary_rows)
    _append_rows(args.summary_history_output, summary_rows)

    cand_breakout = build_execution_candidates("Breakout (15m)", breakout_rows, args.execution_symbol)
    cand_ds = build_execution_candidates("Demand/Supply", ds_rows, args.execution_symbol)
    cand_ind = build_execution_candidates("Indicator (RSI/ADX/MACD+VWAP)", indicator_rows, args.execution_symbol)
    cand_one = build_execution_candidates("One Trade/Day (All Indicators)", one_trade_rows, args.execution_symbol)
    cand_btst = build_execution_candidates("BTST", btst_rows, args.execution_symbol)
    candidates = cand_breakout + cand_ds + cand_ind + cand_one + cand_btst

    for c in candidates:
        c["timeframe"] = timeframe
        c["data_start"] = data_start
        c["data_end"] = data_end
        c["backtest_run_at_utc"] = run_at

    paper_rows = execute_paper_trades(candidates, args.paper_log_output, deduplicate=False)

    return {
        "summary_rows": summary_rows,
        "paper_rows_count": len(paper_rows),
        "timeframe": timeframe,
        "data_points": len(rows),
        "data_start": data_start,
        "data_end": data_end,
    }


def main() -> None:
    args = parse_args()
    out = run(args)
    print(f"Backtest timeframe: {out['timeframe']}")
    print(f"Data points: {out['data_points']} | Start: {out['data_start']} | End: {out['data_end']}")
    print(f"Paper rows written: {out['paper_rows_count']}")
    for row in out["summary_rows"]:
        print(
            f"{row['strategy']}: trades={row['trades']} wins={row['wins']} "
            f"losses={row['losses']} pnl={row['total_pnl']} win_rate={row['win_rate_pct']}%"
        )


if __name__ == "__main__":
    main()