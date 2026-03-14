from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from src.breakout_bot import Candle, add_intraday_vwap, load_candles
from src.csv_io import read_csv_rows, write_csv_rows
from datetime import datetime

dt = datetime.now()
print(dt.timestamp())

@dataclass
class IndicatorConfig:
    rsi_period: int = 14
    adx_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0
    adx_trend_min: float = 20.0


def _ema(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out

    multiplier = 2.0 / (period + 1)
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    ema_prev = seed

    for idx in range(period, len(values)):
        ema_prev = (values[idx] - ema_prev) * multiplier + ema_prev
        out[idx] = ema_prev
    return out


def _rsi(closes: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= period:
        return out

    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, len(closes)):
        delta = closes[idx] - closes[idx - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    def to_rsi(g: float, l: float) -> float:
        if l == 0:
            return 100.0
        rs = g / l
        return 100.0 - (100.0 / (1.0 + rs))

    out[period] = to_rsi(avg_gain, avg_loss)

    for idx in range(period + 1, len(closes)):
        gain = gains[idx - 1]
        loss = losses[idx - 1]
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        out[idx] = to_rsi(avg_gain, avg_loss)

    return out


def _adx(candles: list[Candle], period: int) -> tuple[list[float | None], list[float | None], list[float | None]]:
    n = len(candles)
    adx: list[float | None] = [None] * n
    plus_di: list[float | None] = [None] * n
    minus_di: list[float | None] = [None] * n
    if n <= period + 1:
        return adx, plus_di, minus_di

    tr: list[float] = [0.0] * n
    pdm: list[float] = [0.0] * n
    mdm: list[float] = [0.0] * n

    for i in range(1, n):
        high_diff = candles[i].high - candles[i - 1].high
        low_diff = candles[i - 1].low - candles[i].low
        pdm[i] = high_diff if (high_diff > low_diff and high_diff > 0) else 0.0
        mdm[i] = low_diff if (low_diff > high_diff and low_diff > 0) else 0.0
        tr[i] = max(
            candles[i].high - candles[i].low,
            abs(candles[i].high - candles[i - 1].close),
            abs(candles[i].low - candles[i - 1].close),
        )

    atr = sum(tr[1 : period + 1])
    sm_pdm = sum(pdm[1 : period + 1])
    sm_mdm = sum(mdm[1 : period + 1])

    if atr == 0:
        return adx, plus_di, minus_di

    plus_di[period] = 100.0 * (sm_pdm / atr)
    minus_di[period] = 100.0 * (sm_mdm / atr)
    dx_values: list[float] = [0.0] * n

    denom = (plus_di[period] or 0.0) + (minus_di[period] or 0.0)
    dx_values[period] = 0.0 if denom == 0 else 100.0 * abs((plus_di[period] or 0.0) - (minus_di[period] or 0.0)) / denom

    for i in range(period + 1, n):
        atr = atr - (atr / period) + tr[i]
        sm_pdm = sm_pdm - (sm_pdm / period) + pdm[i]
        sm_mdm = sm_mdm - (sm_mdm / period) + mdm[i]

        if atr == 0:
            continue

        pdi = 100.0 * (sm_pdm / atr)
        mdi = 100.0 * (sm_mdm / atr)
        plus_di[i] = pdi
        minus_di[i] = mdi

        denom = pdi + mdi
        dx_values[i] = 0.0 if denom == 0 else 100.0 * abs(pdi - mdi) / denom

    start = period * 2
    if start < n:
        seed_values = [dx_values[i] for i in range(period, start)]
        if seed_values:
            adx_val = sum(seed_values) / len(seed_values)
            adx[start - 1] = adx_val
            for i in range(start, n):
                adx_val = ((adx_val * (period - 1)) + dx_values[i]) / period
                adx[i] = adx_val

    return adx, plus_di, minus_di


def _macd(closes: list[float], fast: int, slow: int, signal_period: int) -> tuple[list[float | None], list[float | None], list[float | None]]:
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)

    macd_line: list[float | None] = [None] * len(closes)
    for i in range(len(closes)):
        if ema_fast[i] is not None and ema_slow[i] is not None:
            macd_line[i] = (ema_fast[i] or 0.0) - (ema_slow[i] or 0.0)

    compact = [v for v in macd_line if v is not None]
    signal_compact = _ema(compact, signal_period)

    signal_line: list[float | None] = [None] * len(closes)
    hist: list[float | None] = [None] * len(closes)

    compact_idx = 0
    for i in range(len(closes)):
        if macd_line[i] is None:
            continue
        sig = signal_compact[compact_idx]
        signal_line[i] = sig
        if sig is not None:
            hist[i] = (macd_line[i] or 0.0) - sig
        compact_idx += 1

    return macd_line, signal_line, hist


def _trend_strength(adx_val: float | None, adx_trend_min: float) -> str:
    if adx_val is None:
        return "NA"
    if adx_val < adx_trend_min:
        return "WEAK"
    if adx_val < adx_trend_min + 10:
        return "MODERATE"
    if adx_val < adx_trend_min + 25:
        return "STRONG"
    return "VERY_STRONG"


def _market_signal(
    close: float,
    vwap: float,
    rsi_val: float | None,
    adx_val: float | None,
    macd_val: float | None,
    macd_signal: float | None,
    config: IndicatorConfig,
) -> str:
    if rsi_val is None or adx_val is None or macd_val is None or macd_signal is None:
        return "INSUFFICIENT_DATA"

    if rsi_val > config.rsi_overbought:
        return "OVERBOUGHT"
    if rsi_val < config.rsi_oversold:
        return "OVERSOLD"

    bullish = close > vwap and macd_val > macd_signal and rsi_val >= 50
    bearish = close < vwap and macd_val < macd_signal and rsi_val <= 50

    if adx_val < config.adx_trend_min and 40 <= rsi_val <= 60:
        return "RANGE"
    if bullish and adx_val >= config.adx_trend_min:
        return "BULLISH_TREND"
    if bearish and adx_val >= config.adx_trend_min:
        return "BEARISH_TREND"
    return "NEUTRAL"


def generate_indicator_rows(candles: list[Candle], config: IndicatorConfig | None = None) -> list[dict[str, object]]:
    cfg = config or IndicatorConfig()
    add_intraday_vwap(candles)
    closes = [c.close for c in candles]

    rsi_vals = _rsi(closes, cfg.rsi_period)
    adx_vals, plus_di, minus_di = _adx(candles, cfg.adx_period)
    macd_line, macd_signal, macd_hist = _macd(closes, cfg.macd_fast, cfg.macd_slow, cfg.macd_signal)

    rows: list[dict[str, object]] = []
    for i, candle in enumerate(candles):
        signal = _market_signal(
            close=candle.close,
            vwap=candle.vwap,
            rsi_val=rsi_vals[i],
            adx_val=adx_vals[i],
            macd_val=macd_line[i],
            macd_signal=macd_signal[i],
            config=cfg,
        )
        rows.append(
            {
                "timestamp": candle.timestamp.isoformat(sep=" "),
                "open": round(candle.open, 4),
                "high": round(candle.high, 4),
                "low": round(candle.low, 4),
                "close": round(candle.close, 4),
                "volume": round(candle.volume, 4),
                "vwap": round(candle.vwap, 4),
                "rsi": "" if rsi_vals[i] is None else round(rsi_vals[i] or 0.0, 2),
                "adx": "" if adx_vals[i] is None else round(adx_vals[i] or 0.0, 2),
                "+di": "" if plus_di[i] is None else round(plus_di[i] or 0.0, 2),
                "-di": "" if minus_di[i] is None else round(minus_di[i] or 0.0, 2),
                "macd": "" if macd_line[i] is None else round(macd_line[i] or 0.0, 4),
                "macd_signal": "" if macd_signal[i] is None else round(macd_signal[i] or 0.0, 4),
                "macd_hist": "" if macd_hist[i] is None else round(macd_hist[i] or 0.0, 4),
                "trend_strength": _trend_strength(adx_vals[i], cfg.adx_trend_min),
                "market_signal": signal,
            }
        )

    return rows


def build_indicator_summary(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "Indicator bot: no data available."

    last = rows[-1]
    rsi_val = last.get("rsi", last.get("rsi_14", ""))
    adx_val = last.get("adx", last.get("adx_14", ""))

    return (
        "Indicator Bot Alert\n"
        f"Time: {last['timestamp']}\n"
        f"Signal: {last['market_signal']}\n"
        f"Trend: {last.get('trend_strength', 'NA')}\n"
        f"RSI: {rsi_val}\n"
        f"ADX: {adx_val}\n"
        f"MACD: {last.get('macd', '')} | Signal: {last.get('macd_signal', '')}\n"
        f"Close: {last.get('close', '')} | VWAP: {last.get('vwap', '')}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RSI + ADX + MACD + VWAP indicator bot")
    parser.add_argument("--input", required=True, type=Path, help="Input intraday OHLCV CSV")
    parser.add_argument("--output", required=True, type=Path, help="Output indicator CSV")
    parser.add_argument("--rsi-overbought", type=float, default=70.0)
    parser.add_argument("--rsi-oversold", type=float, default=30.0)
    parser.add_argument("--adx-trend-min", type=float, default=20.0)
    return parser.parse_args()


def run(input_path: Path, output_path: Path, config: IndicatorConfig | None = None) -> list[dict[str, object]]:
    rows = read_csv_rows(input_path)
    candles = load_candles(rows)
    out = generate_indicator_rows(candles, config=config)
    write_csv_rows(output_path, out)
    return out


if __name__ == "__main__":
    args = parse_args()
    cfg = IndicatorConfig(
        rsi_overbought=args.rsi_overbought,
        rsi_oversold=args.rsi_oversold,
        adx_trend_min=args.adx_trend_min,
    )
    run(args.input, args.output, config=cfg)
