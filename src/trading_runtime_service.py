from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable

import pandas as pd

from src.amd_fvg_sd_bot import generate_trades as generate_amd_fvg_sd_trades
from src.breakout_bot import Candle, generate_trades as generate_breakout_trades
from src.strategy_demand_supply import generate_trades as generate_demand_supply_trades
from src.indicator_bot import generate_indicator_rows
from src.nifty_data_integration import NiftyDataBundle, fetch_nifty_data_bundle, fetch_nifty_ohlcv_frame
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.nse_option_chain import build_metrics_map, extract_option_records, fetch_option_chain
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
<<<<<<< HEAD
from src.runtime_defaults import DEFAULT_INTERVAL, DEFAULT_PERIOD, EXECUTED_TRADES_OUTPUT
from src.runtime_models import TradingActionRequest, TradingActionResult, period_for_interval
from src.runtime_reporting_service import paper_execution_summary, todays_trade_count
from src.runtime_workflow_service import (
    latest_actionable_trades as latest_actionable_trades_workflow,
    run_execution as run_execution_workflow,
    run_live_strategy as run_live_strategy_workflow,
    run_strategy_backtest as run_strategy_backtest_workflow,
    status_message,
)
from src.strike_selector import attach_option_strikes
from src.strategy_service import StrategyContext, run_strategy_workflow
from src.trading_core import prepare_trading_data
=======
from src.runtime_config import RuntimeConfig
from src.strike_selector import attach_option_strikes
from src.strategy_service import StrategyContext, run_strategy_workflow
from src.strategy_tuning import normalize_strategy_key, strategy_backtest_config
from src.trading_core import prepare_trading_data, write_rows
from src.runtime_persistence import load_current_rows, load_latest_batch_rows

RUNTIME_CONFIG = RuntimeConfig.load()
DATA_DIR = RUNTIME_CONFIG.paths.data_dir
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
>>>>>>> fed8576 ( modifyed with ltp verson2)


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
    return fetch_nifty_ohlcv_frame(symbol, interval=interval, period=period, require_freshness=True)


def _safe_option_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _days_to_expiry(value: object) -> int | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    parsed: datetime | None = None
    for fmt in ('%Y-%m-%d', '%d-%b-%Y', '%Y/%m/%d'):
        try:
            parsed = datetime.strptime(raw, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
    today = datetime.now(UTC).date()
    return max((parsed.date() - today).days, 0)


def _option_risk_flags(row: dict[str, object]) -> dict[str, object]:
    expiry_days = _days_to_expiry(row.get('option_expiry'))
    strike = _safe_option_float(row.get('strike_price'))
    spot = _safe_option_float(row.get('spot_price', row.get('entry_price', row.get('entry'))))
    iv = _safe_option_float(row.get('option_iv'))
    near_atm = bool(strike > 0 and spot > 0 and abs(spot - strike) <= max(50.0, strike * 0.0025))

    theta_risk = 'UNKNOWN'
    gamma_risk = 'UNKNOWN'
    if expiry_days is not None:
        if expiry_days <= 1:
            theta_risk = 'HIGH'
        elif expiry_days <= 3:
            theta_risk = 'MEDIUM'
        else:
            theta_risk = 'LOW'

        if near_atm and expiry_days <= 2:
            gamma_risk = 'HIGH'
        elif near_atm or expiry_days <= 5:
            gamma_risk = 'MEDIUM'
        else:
            gamma_risk = 'LOW'

    risk_summary = (
        f'Theta decay {theta_risk.lower()}'
        if theta_risk != 'UNKNOWN'
        else 'Theta decay unavailable'
    )
    if gamma_risk != 'UNKNOWN':
        risk_summary += f' | Gamma risk {gamma_risk.lower()}'

    payload: dict[str, object] = {
        'days_to_expiry': expiry_days if expiry_days is not None else '',
        'theta_decay_risk': theta_risk,
        'gamma_risk': gamma_risk,
        'option_decay_summary': risk_summary,
    }
    if iv > 0:
        payload['option_iv'] = round(iv, 4)
    return payload


def _attach_option_metrics(rows: list[dict[str, object]], symbol: str, fetch_option_metrics: bool) -> list[dict[str, object]]:
    base_rows = [dict(row) for row in rows]
    if not fetch_option_metrics or not base_rows:
        return base_rows
    try:
        payload = fetch_option_chain(symbol, timeout=10.0)
        metrics_map = build_metrics_map(extract_option_records(payload))
    except Exception:
        return [
            dict(row, option_metrics_status='UNAVAILABLE', **_option_risk_flags(row))
            for row in base_rows
        ]

    enriched: list[dict[str, object]] = []
    for row in base_rows:
        item = dict(row)
        strike_value = item.get('strike_price', item.get('strike'))
        option_type = str(item.get('option_type', '') or '').upper()
        try:
            strike = int(float(strike_value))
        except (TypeError, ValueError):
            strike = 0
        metrics = metrics_map.get((strike, option_type), {}) if strike > 0 and option_type in {'CE', 'PE'} else {}
        if metrics:
            item.update(metrics)
            item['option_metrics_status'] = 'ATTACHED'
        else:
            item['option_metrics_status'] = 'MISSING'
        item.update(_option_risk_flags(item))
        enriched.append(item)
    return enriched


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
        "Demand Supply (Retest)": generate_demand_supply_trades,
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


<<<<<<< HEAD
=======
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


def _log_runtime_error(context: str, exc: Exception) -> None:
    from src.trading_core import append_log

    append_log(f"runtime_error[{context}] {type(exc).__name__}: {exc}")

def _load_csv_rows(path: Path) -> list[dict[str, object]]:
    db_rows = load_current_rows(path)
    if db_rows:
        return [dict(row) for row in db_rows]
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        _log_runtime_error(f"csv-read:{path}", exc)
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


>>>>>>> fed8576 ( modifyed with ltp verson2)
def latest_actionable_trades(trades: list[dict[str, object]]) -> list[dict[str, object]]:
    return latest_actionable_trades_workflow(trades)


def _log_runtime_error(context: str, exc: Exception) -> None:
    from src.trading_core import append_log

    append_log(f"runtime_error[{context}] {type(exc).__name__}: {exc}")


def _status_message(run_clicked: bool, backtest_clicked: bool) -> str:
    return status_message(run_clicked, backtest_clicked)


def _use_dhan_market_data(broker_choice: str) -> bool:
    return str(broker_choice or '').strip().upper() == 'DHAN LIVE'



def _load_runtime_security_map() -> dict[str, object] | None:
    try:
        from src.dhan_api import load_security_map

        return load_security_map()
    except Exception:
        return None



def _market_data_summary(bundle: NiftyDataBundle, request: TradingActionRequest) -> dict[str, object]:
    summary = dict(bundle.validation_report or {})
    summary['provider'] = bundle.provider
    summary['provider_attempts'] = [attempt.to_dict() for attempt in bundle.provider_attempts]
    summary['provider_attempt_count'] = len(bundle.provider_attempts)
    summary['symbol'] = bundle.symbol
    summary['interval'] = bundle.interval
    summary['period'] = bundle.period
    summary['broker_market_data_enabled'] = _use_dhan_market_data(request.broker_choice)
    return summary



def _run_live_strategy(request: TradingActionRequest) -> tuple[pd.DataFrame, list[dict[str, object]], str, dict[str, object]]:
    period = period_for_interval(request.timeframe)
    bundle = fetch_nifty_data_bundle(
        request.symbol,
        interval=request.timeframe,
        period=period,
        provider='DHAN' if _use_dhan_market_data(request.broker_choice) else 'AUTO',
        security_map=_load_runtime_security_map() if _use_dhan_market_data(request.broker_choice) else None,
        require_freshness=True,
    )
    candles, trades, resolved_period = run_live_strategy_workflow(
        request,
        fetch_ohlcv_data_fn=lambda symbol, interval, requested_period: bundle.frame.copy(),
        run_strategy_fn=run_strategy,
    )
    return candles, trades, resolved_period, _market_data_summary(bundle, request)


def _run_strategy_backtest(candles: pd.DataFrame, request: TradingActionRequest) -> dict[str, object]:
    return run_strategy_backtest_workflow(
        candles,
        request,
        strategy_callable_fn=_strategy_callable,
    )


def _run_execution(request: TradingActionRequest, trades: list[dict[str, object]], candles: pd.DataFrame) -> tuple[object | None, list[tuple[str, str]], str]:
    return run_execution_workflow(request, trades, candles)


def run_operator_action(request: TradingActionRequest) -> TradingActionResult:
    try:
<<<<<<< HEAD
        live_strategy_result = _run_live_strategy(request)
        market_data_summary: dict[str, object] = {}
        if len(live_strategy_result) == 3:
            candles, trades, period = live_strategy_result
        else:
            candles, trades, period, market_data_summary = live_strategy_result
        backtest_summary: dict[str, object] = {}
        paper_summary: dict[str, object] = {}
        execution_messages: list[tuple[str, str]] = []

        if request.backtest_requested and not request.run_requested:
            backtest_summary = _run_strategy_backtest(candles, request)
            return TradingActionResult(
                candles=candles,
                trades=[],
                period=period,
                status='Backtest completed',
                broker_status='Backtest mode',
                active_summary=backtest_summary,
                backtest_summary=backtest_summary,
                paper_summary={},
                market_data_summary=market_data_summary,
                execution_messages=[],
                todays_trades=0,
            )

        broker_status = str(request.broker_choice or 'Paper')
        if request.run_requested:
            _execution_result, execution_messages, broker_status = _run_execution(request, trades, candles)
            paper_summary = paper_execution_summary(EXECUTED_TRADES_OUTPUT, request.strategy, request.symbol, request.capital)
        active_summary = dict(backtest_summary or paper_summary or {})
        today_count = todays_trade_count(EXECUTED_TRADES_OUTPUT, request.strategy, request.symbol)
        return TradingActionResult(
            candles=candles,
            trades=trades,
            period=period,
            status=_status_message(request.run_requested, request.backtest_requested),
            broker_status=broker_status,
            active_summary=active_summary,
            backtest_summary=backtest_summary,
            paper_summary=paper_summary,
            market_data_summary=market_data_summary,
            execution_messages=execution_messages,
            todays_trades=today_count,
        )
    except Exception as exc:
        _log_runtime_error('run_operator_action', exc)
        failure_prefix = 'Backtest failed' if request.backtest_requested and not request.run_requested else 'Run failed'
        return TradingActionResult(
            candles=pd.DataFrame(),
            trades=[],
            period=period_for_interval(request.timeframe),
            status=f'{failure_prefix}: {exc}',
            broker_status='Runtime error',
            active_summary={},
            backtest_summary={},
            paper_summary={},
            market_data_summary={},
            execution_messages=[('error', str(exc))],
            todays_trades=0,
        )


=======
        candles, trades, period = _run_live_strategy(request)
        backtest_summary: dict[str, object] = {}
        paper_summary: dict[str, object] = {}
        execution_messages: list[tuple[str, str]] = []

        if request.backtest_requested and not request.run_requested:
            backtest_summary = _run_strategy_backtest(candles, request)
            return TradingActionResult(
                candles=candles,
                trades=[],
                period=period,
                status=_status_message(False, True),
                broker_status="Backtest mode",
                active_summary=dict(backtest_summary),
                backtest_summary=backtest_summary,
                paper_summary={},
                todays_trades=0,
                execution_messages=[],
            )

        broker_status = "Paper broker active" if request.broker_choice == "Paper" else "Idle"
        if request.run_requested:
            _, execution_messages, broker_status = _run_execution(request, trades, candles)
            paper_summary = paper_execution_summary(EXECUTED_TRADES_OUTPUT, request.strategy, request.symbol, float(request.capital))
            active_summary = dict(paper_summary)
        else:
            active_summary = {}

        todays_trades = todays_trade_count(EXECUTED_TRADES_OUTPUT, request.strategy, request.symbol)
        return TradingActionResult(
            candles=candles,
            trades=trades,
            period=period,
            status=_status_message(request.run_requested, False),
            broker_status=broker_status,
            active_summary=active_summary,
            backtest_summary=backtest_summary,
            paper_summary=paper_summary,
            todays_trades=todays_trades,
            execution_messages=execution_messages,
        )
    except Exception as exc:
        _log_runtime_error("run_operator_action", exc)
        return TradingActionResult(
            candles=pd.DataFrame(),
            trades=[],
            period=period_for_interval(request.timeframe),
            status=f"Run failed: {exc}",
            broker_status="Runtime error",
            active_summary={},
            backtest_summary={},
            paper_summary={},
            todays_trades=0,
            execution_messages=[("error", str(exc))],
        )
>>>>>>> fed8576 ( modifyed with ltp verson2)


