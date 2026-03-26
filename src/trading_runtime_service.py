from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.amd_fvg_sd_bot import generate_trades as generate_amd_fvg_sd_trades
from src.backtest_engine import run_backtest, summarize_trade_log
from src.breakout_bot import Candle, generate_trades as generate_breakout_trades
from src.demand_supply_bot import generate_trades as generate_demand_supply_trades
from src.execution_engine import build_execution_candidates, close_paper_trades, execute_live_trades, execute_paper_trades, execution_result_summary
from src.indicator_bot import generate_indicator_rows
from src.live_ohlcv import fetch_live_ohlcv
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.strike_selector import attach_option_strikes
from src.strategy_service import StrategyContext, run_strategy_workflow
from src.strategy_tuning import normalize_strategy_key, strategy_backtest_config
from src.trading_core import prepare_trading_data, write_rows

DATA_DIR = Path("data")
OHLCV_OUTPUT = DATA_DIR / "ohlcv.csv"
LIVE_OHLCV_OUTPUT = DATA_DIR / "live_ohlcv.csv"
TRADES_OUTPUT = DATA_DIR / "trades.csv"
SIGNAL_OUTPUT = DATA_DIR / "output.csv"
EXECUTED_TRADES_OUTPUT = DATA_DIR / "executed_trades.csv"
PAPER_LOG_OUTPUT = DATA_DIR / "paper_trading_logs_all.csv"
LIVE_LOG_OUTPUT = DATA_DIR / "live_trading_logs_all.csv"
PAPER_SUMMARY_OUTPUT = DATA_DIR / "paper_trade_summary.csv"
BACKTEST_TRADES_OUTPUT = DATA_DIR / "backtest_trades.csv"
BACKTEST_SUMMARY_OUTPUT = DATA_DIR / "backtest_summary.csv"
BACKTEST_RESULTS_OUTPUT = DATA_DIR / "backtest_results_all.csv"
BACKTEST_VALIDATION_UI_OUTPUT = DATA_DIR / "backtest_validation.csv"
OPTIMIZER_OUTPUT = DATA_DIR / "strategy_optimizer_report.csv"
ORDER_HISTORY_OUTPUT = DATA_DIR / "order_history.csv"
PAPER_ORDER_HISTORY_OUTPUT = DATA_DIR / "paper_order_history.csv"

DEFAULT_INTERVAL = os.getenv("TRADING_INTERVAL", "5m").strip() or "5m"
DEFAULT_PERIOD = os.getenv("TRADING_PERIOD", "5d").strip() or "5d"


@dataclass(slots=True)
class TradingActionRequest:
    strategy: str
    symbol: str
    timeframe: str
    capital: float
    risk_pct: float
    rr_ratio: float
    mode: str
    broker_choice: str
    run_requested: bool = False
    backtest_requested: bool = False


@dataclass(slots=True)
class TradingActionResult:
    candles: pd.DataFrame
    trades: list[dict[str, object]]
    period: str
    status: str
    broker_status: str
    active_summary: dict[str, object]
    backtest_summary: dict[str, object]
    paper_summary: dict[str, object]
    todays_trades: int
    execution_messages: list[tuple[str, str]]


def period_for_interval(interval: str, *, default_period: str = DEFAULT_PERIOD) -> str:
    mapping = {
        "1m": "7d",
        "5m": "60d",
        "15m": "60d",
        "30m": "60d",
        "1h": "730d",
        "1d": "1y",
    }
    return mapping.get(interval, default_period)


def _df_to_candles(df: pd.DataFrame) -> list[Candle]:
    prepared = prepare_trading_data(df)
    candles: list[Candle] = []
    for row in prepared.itertuples(index=False):
        candles.append(
            Candle(
                timestamp=pd.Timestamp(row.timestamp).to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
            )
        )
    return candles


def fetch_ohlcv_data(symbol: str, interval: str = DEFAULT_INTERVAL, period: str = DEFAULT_PERIOD) -> pd.DataFrame:
    rows = fetch_live_ohlcv(symbol, interval, period)
    return prepare_trading_data(pd.DataFrame(rows or []))


def _attach_option_metrics(rows: list[dict[str, object]], symbol: str, fetch_option_metrics: bool) -> list[dict[str, object]]:
    del symbol, fetch_option_metrics
    return [dict(row) for row in rows]


def _attach_indicator_trade_levels(rows: list[dict[str, object]], *, rr_ratio: float, trailing_sl_pct: float) -> list[dict[str, object]]:
    enriched: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        side = str(item.get("side", "")).upper()
        if side not in {"BUY", "SELL"}:
            enriched.append(item)
            continue
        entry = float(item.get("entry_price", item.get("close", 0.0)) or 0.0)
        if entry <= 0:
            enriched.append(item)
            continue
        stop_loss = entry * (0.995 if side == "BUY" else 1.005)
        target_price = entry + (entry - stop_loss) * float(rr_ratio) if side == "BUY" else entry - (stop_loss - entry) * float(rr_ratio)
        item.setdefault("entry", entry)
        item.setdefault("entry_price", round(entry, 4))
        item.setdefault("stop_loss", round(stop_loss, 4))
        item.setdefault("trailing_stop_loss", round(stop_loss, 4))
        item.setdefault("trailing_sl_pct", round(float(trailing_sl_pct), 4))
        item.setdefault("target", round(target_price, 4))
        item.setdefault("target_price", round(target_price, 4))
        item.setdefault("score", float(item.get("score", 0.0) or 0.0))
        item.setdefault("reason", str(item.get("market_signal", "INDICATOR_SIGNAL")))
        enriched.append(item)
    return enriched


def run_strategy(
    *,
    strategy: str,
    candles: pd.DataFrame,
    capital: float,
    risk_pct: float,
    rr_ratio: float,
    trailing_sl_pct: float,
    symbol: str,
    strike_step: int,
    moneyness: str,
    strike_steps: int,
    fetch_option_metrics: bool,
    mtf_ema_period: int,
    mtf_setup_mode: str,
    mtf_retest_strength: bool,
    mtf_max_trades_per_day: int,
    amd_mode: str = "Balanced",
    amd_swing_window: int = 3,
    amd_min_fvg_size: float = 0.35,
    amd_min_bvg_size: float = 0.25,
    amd_zone_fresh_bars: int = 24,
    amd_retest_tolerance_pct: float = 0.0015,
    amd_max_retest_bars: int = 6,
    amd_min_score_conservative: float = 7.0,
    amd_min_score_balanced: float = 5.0,
    amd_min_score_aggressive: float = 3.0,
) -> list[dict[str, object]]:
    context = StrategyContext(
        strategy=strategy,
        candles=candles,
        candle_rows=_df_to_candles(candles),
        capital=float(capital),
        risk_pct=float(risk_pct),
        rr_ratio=float(rr_ratio),
        trailing_sl_pct=float(trailing_sl_pct),
        symbol=str(symbol),
        strike_step=int(strike_step),
        moneyness=str(moneyness),
        strike_steps=int(strike_steps),
        fetch_option_metrics=bool(fetch_option_metrics),
        mtf_ema_period=int(mtf_ema_period),
        mtf_setup_mode=str(mtf_setup_mode),
        mtf_retest_strength=bool(mtf_retest_strength),
        mtf_max_trades_per_day=int(mtf_max_trades_per_day),
        amd_mode=str(amd_mode),
        amd_swing_window=int(amd_swing_window),
        amd_min_fvg_size=float(amd_min_fvg_size),
        amd_min_bvg_size=float(amd_min_bvg_size),
        amd_zone_fresh_bars=int(amd_zone_fresh_bars),
        amd_retest_tolerance_pct=float(amd_retest_tolerance_pct),
        amd_max_retest_bars=int(amd_max_retest_bars),
        amd_min_score_conservative=float(amd_min_score_conservative),
        amd_min_score_balanced=float(amd_min_score_balanced),
        amd_min_score_aggressive=float(amd_min_score_aggressive),
    )
    return run_strategy_workflow(
        context,
        breakout_generator=generate_breakout_trades,
        demand_supply_generator=generate_demand_supply_trades,
        indicator_generator=generate_indicator_rows,
        one_trade_generator=generate_one_trade_day_trades,
        mtf_generator=generate_mtf_trade_trades,
        amd_generator=generate_amd_fvg_sd_trades,
        attach_levels_fn=_attach_indicator_trade_levels,
        attach_option_strikes_fn=attach_option_strikes,
        attach_option_metrics_fn=_attach_option_metrics,
    )


def _strategy_callable(strategy: str, symbol: str) -> Callable[[pd.DataFrame, float, float, float, Any], list[dict[str, object]]]:
    mapping: dict[str, Callable[[pd.DataFrame, float, float, float, Any], list[dict[str, object]]]] = {
        "Breakout": generate_breakout_trades,
        "Demand Supply": generate_demand_supply_trades,
        "AMD + FVG + Supply/Demand": lambda df, capital, risk_pct, rr_ratio, config=None: run_strategy(
            strategy="AMD + FVG + Supply/Demand",
            candles=df,
            capital=capital,
            risk_pct=risk_pct * 100 if risk_pct <= 1 else risk_pct,
            rr_ratio=rr_ratio,
            trailing_sl_pct=0.0,
            symbol=symbol,
            strike_step=50,
            moneyness="ATM",
            strike_steps=0,
            fetch_option_metrics=False,
            mtf_ema_period=3,
            mtf_setup_mode="either",
            mtf_retest_strength=True,
            mtf_max_trades_per_day=3,
        ),
        "Indicator": lambda df, capital, risk_pct, rr_ratio, config=None: run_strategy(
            strategy="Indicator",
            candles=df,
            capital=capital,
            risk_pct=risk_pct * 100 if risk_pct <= 1 else risk_pct,
            rr_ratio=rr_ratio,
            trailing_sl_pct=0.0,
            symbol=symbol,
            strike_step=50,
            moneyness="ATM",
            strike_steps=0,
            fetch_option_metrics=False,
            mtf_ema_period=3,
            mtf_setup_mode="either",
            mtf_retest_strength=True,
            mtf_max_trades_per_day=3,
        ),
    }
    return mapping.get(strategy, generate_breakout_trades)


def _save_runtime_outputs(candles: pd.DataFrame, trades: list[dict[str, object]]) -> None:
    candle_rows = candles.to_dict(orient="records")
    write_rows(OHLCV_OUTPUT, candle_rows)
    write_rows(LIVE_OHLCV_OUTPUT, candle_rows)
    write_rows(TRADES_OUTPUT, trades)
    write_rows(SIGNAL_OUTPUT, trades)
    for path in (EXECUTED_TRADES_OUTPUT, PAPER_LOG_OUTPUT, LIVE_LOG_OUTPUT):
        if not path.exists() or path.stat().st_size == 0:
            pd.DataFrame().to_csv(path, index=False)


def _mirror_output_file(source: Path, *destinations: Path) -> None:
    if not source.exists():
        return
    for destination in destinations:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)


def _paper_candle_rows(candles: pd.DataFrame) -> list[dict[str, object]]:
    return prepare_trading_data(candles).to_dict(orient="records")


def _refresh_paper_trade_summary(candles: pd.DataFrame, capital: float) -> dict[str, object]:
    close_paper_trades(EXECUTED_TRADES_OUTPUT, _paper_candle_rows(candles), max_hold_minutes=60)
    return summarize_trade_log(
        EXECUTED_TRADES_OUTPUT,
        capital=float(capital),
        strategy_name="PAPER_EXECUTION",
        summary_output=PAPER_SUMMARY_OUTPUT,
        validation_output=BACKTEST_VALIDATION_UI_OUTPUT,
    )


def _load_csv_rows(path: Path) -> list[dict[str, object]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        frame = pd.read_csv(path)
    except Exception:
        return []
    if frame.empty:
        return []
    return frame.to_dict(orient="records")


def _current_execution_rows(path: Path, strategy: str, symbol: str, execution_type: str = "PAPER") -> list[dict[str, object]]:
    normalized_strategy = normalize_strategy_key(strategy)
    normalized_symbol = (symbol or "").strip().upper()
    rows = _load_csv_rows(path)
    filtered: list[dict[str, object]] = []
    for row in rows:
        if str(row.get("execution_type", "") or "").strip().upper() != execution_type:
            continue
        row_strategy = normalize_strategy_key(str(row.get("strategy", "") or ""))
        row_symbol = str(row.get("symbol", "") or "").strip().upper()
        if normalized_strategy and row_strategy and row_strategy != normalized_strategy:
            continue
        if normalized_symbol and row_symbol and row_symbol != normalized_symbol:
            continue
        filtered.append(dict(row))
    if filtered or not normalized_symbol:
        return filtered
    return [
        dict(row)
        for row in rows
        if str(row.get("execution_type", "") or "").strip().upper() == execution_type
        and normalize_strategy_key(str(row.get("strategy", "") or "")) == normalized_strategy
    ]


def paper_execution_summary(path: Path, strategy: str, symbol: str, capital: float) -> dict[str, object]:
    rows = _current_execution_rows(path, strategy, symbol, execution_type="PAPER")
    if not rows:
        return {}
    return summarize_trade_log(rows, capital=float(capital), strategy_name=normalize_strategy_key(strategy) or "PAPER_EXECUTION")


def latest_actionable_trades(trades: list[dict[str, object]]) -> list[dict[str, object]]:
    if not trades:
        return []
    stamped: list[tuple[pd.Timestamp, dict[str, object]]] = []
    for trade in trades:
        raw = trade.get("signal_time") or trade.get("entry_time") or trade.get("timestamp") or ""
        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.isna(parsed):
            continue
        stamped.append((pd.Timestamp(parsed), dict(trade)))
    if not stamped:
        return [dict(trades[-1])]
    latest_stamp = max(stamp for stamp, _ in stamped)
    latest_rows = [row for stamp, row in stamped if stamp == latest_stamp]
    return latest_rows or [dict(trades[-1])]


def todays_trade_count(path: Path, strategy: str, symbol: str, execution_type: str = "PAPER") -> int:
    rows = _current_execution_rows(path, strategy, symbol, execution_type=execution_type)
    if not rows:
        return 0
    today_key = pd.Timestamp.now().strftime("%Y-%m-%d")
    count = 0
    for row in rows:
        status = str(row.get("execution_status", "") or "").strip().upper()
        if status not in {"EXECUTED", "FILLED", "CLOSED", "EXITED"}:
            continue
        for column in ("executed_at_utc", "exit_time", "entry_time", "signal_time", "timestamp"):
            value = str(row.get(column, "") or "").strip()
            if value[:10] == today_key:
                count += 1
                break
    return count


def _latest_optimizer_gate(strategy: str) -> tuple[bool, str]:
    if not OPTIMIZER_OUTPUT.exists() or OPTIMIZER_OUTPUT.stat().st_size == 0:
        return False, "optimizer report missing"
    try:
        frame = pd.read_csv(OPTIMIZER_OUTPUT)
    except Exception:
        return False, "optimizer report unreadable"
    if frame.empty:
        return False, "optimizer report empty"
    strategy_key = normalize_strategy_key(strategy)
    frame["normalized_strategy"] = frame["strategy"].map(lambda value: normalize_strategy_key(str(value)))
    matched = frame[frame["normalized_strategy"] == strategy_key].copy()
    if matched.empty:
        return False, f"no optimizer row for {strategy}"
    if "optimizer_rank" in matched.columns:
        matched = matched.sort_values("optimizer_rank")
    elif "rank_score" in matched.columns:
        matched = matched.sort_values("rank_score", ascending=False)
    row = matched.iloc[0].to_dict()
    deployment_ready = str(row.get("deployment_ready", "NO")).strip().upper() == "YES"
    blockers = str(row.get("deployment_blockers", "") or "").strip()
    if deployment_ready:
        return True, "optimizer validated"
    return False, blockers or "optimizer gate failed"


def _status_message(run_clicked: bool, backtest_clicked: bool) -> str:
    if backtest_clicked:
        return "Backtest completed"
    if run_clicked:
        return "Run completed"
    return "Ready"


def _run_live_strategy(request: TradingActionRequest) -> tuple[pd.DataFrame, list[dict[str, object]], str]:
    period = period_for_interval(request.timeframe)
    candles = fetch_ohlcv_data(request.symbol, interval=request.timeframe, period=period)
    trades = run_strategy(
        strategy=request.strategy,
        candles=candles,
        capital=float(request.capital),
        risk_pct=float(request.risk_pct),
        rr_ratio=float(request.rr_ratio),
        trailing_sl_pct=0.0,
        symbol=request.symbol,
        strike_step=50,
        moneyness="ATM",
        strike_steps=0,
        fetch_option_metrics=False,
        mtf_ema_period=3,
        mtf_setup_mode="either",
        mtf_retest_strength=True,
        mtf_max_trades_per_day=3,
        amd_mode=request.mode,
        amd_swing_window=3,
        amd_min_fvg_size=0.35,
        amd_min_bvg_size=0.25,
        amd_zone_fresh_bars=24,
        amd_retest_tolerance_pct=0.0015,
        amd_max_retest_bars=6,
        amd_min_score_conservative=7.0,
        amd_min_score_balanced=5.0,
        amd_min_score_aggressive=3.0,
    )
    normalized_trades: list[dict[str, object]] = []
    for trade in trades:
        item = dict(trade)
        item.setdefault("symbol", request.symbol)
        item.setdefault("timeframe", request.timeframe)
        item.setdefault("duplicate_signal_cooldown_bars", item.get("duplicate_signal_cooldown_bars", 0))
        normalized_trades.append(item)
    trades = normalized_trades
    _save_runtime_outputs(candles, trades)
    return candles, trades, period


def _run_strategy_backtest(candles: pd.DataFrame, request: TradingActionRequest) -> dict[str, object]:
    config = strategy_backtest_config(
        request.strategy,
        capital=float(request.capital),
        risk_pct=float(request.risk_pct) / 100.0,
        rr_ratio=float(request.rr_ratio),
        trades_output=BACKTEST_TRADES_OUTPUT,
        summary_output=BACKTEST_SUMMARY_OUTPUT,
        validation_output=BACKTEST_VALIDATION_UI_OUTPUT,
    )
    summary = run_backtest(candles, _strategy_callable(request.strategy, request.symbol), config)
    _mirror_output_file(BACKTEST_SUMMARY_OUTPUT, BACKTEST_RESULTS_OUTPUT)
    return summary


def _run_execution(request: TradingActionRequest, trades: list[dict[str, object]], candles: pd.DataFrame) -> tuple[object | None, list[tuple[str, str]], str]:
    if not trades:
        return None, [("info", "No actionable trade candidates")], "Paper standby"
    actionable_trades = latest_actionable_trades(trades)
    if not actionable_trades:
        return None, [("info", "No actionable trade candidates")], "Paper standby"
    candidates = build_execution_candidates(request.strategy, actionable_trades, request.symbol)
    if request.broker_choice == "Dhan Live":
        optimizer_ready, optimizer_reason = _latest_optimizer_gate(request.strategy)
        if not optimizer_ready:
            return None, [("warning", f"Live blocked: {optimizer_reason}")], "Live broker blocked by optimizer gate"
        live_enabled = str(os.getenv("LIVE_TRADING_ENABLED", "") or "").strip().lower() in {"1", "true", "yes", "on"}
        result = execute_live_trades(
            candidates,
            EXECUTED_TRADES_OUTPUT,
            broker_name="DHAN",
            live_enabled=live_enabled,
            max_trades_per_day=1,
            max_open_trades=1,
            order_history_path=ORDER_HISTORY_OUTPUT,
        )
        _mirror_output_file(EXECUTED_TRADES_OUTPUT, LIVE_LOG_OUTPUT)
        status = "Live broker armed" if live_enabled else "Live broker blocked by config"
    else:
        result = execute_paper_trades(
            candidates,
            EXECUTED_TRADES_OUTPUT,
            max_trades_per_day=1,
            max_open_trades=1,
            order_history_path=PAPER_ORDER_HISTORY_OUTPUT,
        )
        _mirror_output_file(EXECUTED_TRADES_OUTPUT, PAPER_LOG_OUTPUT)
        _refresh_paper_trade_summary(candles, request.capital)
        status = "Paper broker active"
    return result, execution_result_summary(result), status


def run_operator_action(request: TradingActionRequest) -> TradingActionResult:
    candles, trades, period = _run_live_strategy(request)
    broker_status = "Paper broker active" if request.broker_choice == "Paper" else "Idle"
    backtest_summary: dict[str, object] = {}
    paper_summary: dict[str, object] = {}
    execution_messages: list[tuple[str, str]] = []

    if request.run_requested:
        _, execution_messages, broker_status = _run_execution(request, trades, candles)
        paper_summary = paper_execution_summary(EXECUTED_TRADES_OUTPUT, request.strategy, request.symbol, float(request.capital))
        active_summary = dict(paper_summary)
    elif request.backtest_requested:
        backtest_summary = _run_strategy_backtest(candles, request)
        active_summary = dict(backtest_summary)
    else:
        active_summary = {}

    todays_trades = todays_trade_count(EXECUTED_TRADES_OUTPUT, request.strategy, request.symbol)
    return TradingActionResult(
        candles=candles,
        trades=trades,
        period=period,
        status=_status_message(request.run_requested, request.backtest_requested),
        broker_status=broker_status,
        active_summary=active_summary,
        backtest_summary=backtest_summary,
        paper_summary=paper_summary,
        todays_trades=todays_trades,
        execution_messages=execution_messages,
    )