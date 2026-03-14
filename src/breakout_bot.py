from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from math import floor
from pathlib import Path

from src.csv_io import read_csv_rows, write_csv_rows
from src.telegram_notifier import build_trade_summary, send_telegram_message
from dateutil import parser


# ==============================
# DATA STRUCTURES
# ==============================

@dataclass
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float = 0.0


# ==============================
# TIMESTAMP PARSER
# ==============================


def parse_timestamp_robust(text: str) -> datetime:
    if not text:
        raise ValueError("Empty timestamp")
    try:
        # Try built-in ISO format first (fastest and standard)
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    
    # Use dateutil.parser as a flexible fallback
    try:
        return parser.parse(text)
    except (ValueError, parser.ParserError):
        # Fallback to specific formats if dateutil struggles with very custom ones
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%d-%m-%Y %H:%M:%S',
            '%d-%m-%Y %H:%M',
        ]
        for fmt in formats:
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
                
        raise ValueError(f"Unsupported timestamp format: {text}")

# ==============================
# CSV → CANDLE LOADER
# ==============================

def load_candles(rows: list[dict[str, str]]) -> list[Candle]:
    candles: list[Candle] = []

    for row in rows:
        try:
            # normalize keys
            row = {k.strip().lower(): v.strip() for k, v in row.items()}

            ts = (
                row.get("datetime")
                or row.get("timestamp")
                or row.get("date")
                or row.get("time")
            )

            if not ts:
                continue

            candle = Candle(
                timestamp=parse_timestamp(ts),
                open=float(row.get("open", 0)),
                high=float(row.get("high", 0)),
                low=float(row.get("low", 0)),
                close=float(row.get("close", 0)),
                volume=float(row.get("volume", 0)),
            )

            candles.append(candle)

        except Exception as e:
            print(f"Skipping bad row: {row} | Error: {e}")

    candles.sort(key=lambda c: c.timestamp)

    return candles


# ==============================
# VWAP CALCULATION
# ==============================

def add_intraday_vwap(candles: list[Candle]) -> None:

    current_day = None
    cumulative_pv = 0.0
    cumulative_volume = 0.0

    for candle in candles:

        day = candle.timestamp.date()

        if day != current_day:
            current_day = day
            cumulative_pv = 0.0
            cumulative_volume = 0.0

        cumulative_pv += candle.close * candle.volume
        cumulative_volume += candle.volume

        if cumulative_volume == 0:
            candle.vwap = candle.close
        else:
            candle.vwap = cumulative_pv / cumulative_volume


# ==============================
# HELPERS
# ==============================

def _group_by_day(candles: list[Candle]) -> dict:

    by_day: dict = {}

    for candle in candles:
        day = candle.timestamp.date()
        by_day.setdefault(day, []).append(candle)

    return by_day


def _calculate_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:

    risk_per_unit = abs(entry - stop)

    if risk_per_unit <= 0:
        return 0

    risk_amount = capital * risk_pct

    return floor(risk_amount / risk_per_unit)


# ==============================
# STRATEGY ENGINE
# ==============================

def generate_trades(
    candles: list[Candle],
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    trailing_sl_pct: float = 0.0,
) -> list[dict[str, object]]:

    add_intraday_vwap(candles)

    by_day = _group_by_day(candles)

    trades: list[dict[str, object]] = []

    for day in sorted(by_day.keys()):

        day_candles = by_day[day]

        if len(day_candles) < 5:
            continue

        first_15m = day_candles[0]

        first_hour = day_candles[:4]

        hour_open = first_hour[0].open
        hour_close = first_hour[-1].close

        if hour_close > hour_open:
            bias = "BUY"
        elif hour_close < hour_open:
            bias = "SELL"
        else:
            continue

        trade_open = None
        entry_idx = -1

        for idx in range(1, len(day_candles)):

            candle = day_candles[idx]

            if bias == "BUY":

                if candle.high <= first_15m.high:
                    continue

                if candle.close <= candle.vwap:
                    continue

                entry = first_15m.high
                stop = candle.low
                target = entry + (entry - stop) * rr_ratio
                side = "BUY"

            else:

                if candle.low >= first_15m.low:
                    continue

                if candle.close >= candle.vwap:
                    continue

                entry = first_15m.low
                stop = candle.high
                target = entry - (stop - entry) * rr_ratio
                side = "SELL"

            qty = _calculate_qty(capital, risk_pct, entry, stop)

            if qty <= 0:
                break

            trade_open = {
                    "day": day.isoformat(),
                    "entry_time": candle.timestamp,
                    "side": side,
                    "entry_price": round(entry, 4),
                    "stop_loss": round(stop, 4),
                    "trailing_stop_loss": round(stop, 4),
                    "target_price": round(target, 4),
                    "quantity": qty,
            }

            entry_idx = idx

            break

        if not trade_open:
            continue

        exit_price = day_candles[-1].close
        exit_time = day_candles[-1].timestamp
        exit_reason = "EOD"

        side = str(trade_open["side"])
        stop = float(trade_open["stop_loss"])
        trail_stop = float(trade_open.get("trailing_stop_loss", stop))
        target = float(trade_open["target_price"])
        entry = float(trade_open["entry_price"])

        for idx in range(entry_idx + 1, len(day_candles)):

            candle = day_candles[idx]

            if trailing_sl_pct > 0:

                if side == "BUY":
                    trail_stop = max(trail_stop, candle.high * (1 - trailing_sl_pct))
                else:
                    trail_stop = min(trail_stop, candle.low * (1 + trailing_sl_pct))

                trade_open["trailing_stop_loss"] = round(trail_stop, 4)

            if side == "BUY":

                if candle.low <= trail_stop:
                    exit_price = trail_stop
                    exit_time = candle.timestamp
                    exit_reason = "STOP_LOSS"
                    break

                if candle.high >= target:
                    exit_price = target
                    exit_time = candle.timestamp
                    exit_reason = "TARGET"
                    break

            else:

                if candle.high >= trail_stop:
                    exit_price = trail_stop
                    exit_time = candle.timestamp
                    exit_reason = "STOP_LOSS"
                    break

                if candle.low <= target:
                    exit_price = target
                    exit_time = candle.timestamp
                    exit_reason = "TARGET"
                    break

        qty = int(trade_open["quantity"])

        pnl = (exit_price - entry) * qty if side == "BUY" else (entry - exit_price) * qty

        risk_per_unit = abs(entry - stop)

        rr_achieved = 0.0 if risk_per_unit == 0 else abs(exit_price - entry) / risk_per_unit

        trade_open.update(
            {
                "exit_time": exit_time,
                "exit_price": round(exit_price, 4),
                "exit_reason": exit_reason,
                "pnl": round(pnl, 2),
                "rr_achieved": round(rr_achieved, 2),
            }
        )

        trades.append(trade_open)

    return trades

def build_trade_summary(trades: list[dict[str, object]]) -> str:
    if not trades:
        return "Intratrade: no trades generated for this run."

    closed_trades = [
        t for t in trades
        if "pnl" in t and "exit_time" in t and "exit_reason" in t
    ]

    if not closed_trades:
        return (
            "Intratrade alert\n"
            f"Trades opened: {len(trades)}\n"
            "Trades closed: 0\n"
            "Win rate: N/A\n"
            "Total PnL: 0.00\n"
            "Last exit: N/A\n"
            "Last reason: N/A"
        )

    total_pnl = sum(float(t.get("pnl", 0)) for t in closed_trades)
    wins = sum(1 for t in closed_trades if float(t.get("pnl", 0)) > 0)
    win_rate = (wins / len(closed_trades)) * 100.0
    last_trade = closed_trades[-1]

    return (
        "Intratrade alert\n"
        f"Trades opened: {len(trades)}\n"
        f"Trades closed: {len(closed_trades)}\n"
        f"Win rate: {win_rate:.2f}%\n"
        f"Total PnL: {total_pnl:.2f}\n"
        f"Last exit: {last_trade.get('exit_time', 'N/A')}\n"
        f"Last reason: {last_trade.get('exit_reason', 'N/A')}"
    )

# ==============================
# BOT RUNNER
# ==============================

def run(
    input_path: Path,
    output_path: Path,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    trailing_sl_pct: float = 0.0,
    telegram_token: str = "",
    telegram_chat_id: str = "",
):

    rows = read_csv_rows(input_path)

    candles = load_candles(rows)

    trades = generate_trades(
        candles,
        capital=capital,
        risk_pct=risk_pct,
        rr_ratio=rr_ratio,
        trailing_sl_pct=trailing_sl_pct,
    )

    write_csv_rows(output_path, trades)

    if telegram_token and telegram_chat_id:

        message = build_trade_summary(trades)

        send_telegram_message(
            telegram_token,
            telegram_chat_id,
            message,
        )

    return trades


# ==============================
# CLI ENTRY
# ==============================

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--capital", type=float, default=100000)
    parser.add_argument("--risk-pct", type=float, default=0.01)
    parser.add_argument("--rr-ratio", type=float, default=2.0)
    parser.add_argument("--trailing-sl-pct", type=float, default=0.0)
    parser.add_argument("--telegram-token", default="")
    parser.add_argument("--telegram-chat-id", default="")

    args = parser.parse_args()

    run(
        args.input,
        args.output,
        args.capital,
        args.risk_pct,
        args.rr_ratio,
        args.trailing_sl_pct,
        args.telegram_token,
        args.telegram_chat_id,
    )