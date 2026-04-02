from __future__ import annotations

from dataclasses import dataclass, replace
from math import floor
from typing import Any

import pandas as pd

from vinayak.strategies.common.base import StrategySignal


@dataclass(slots=True)
class ConfluenceConfig:
    mode: str = 'Balanced'
    swing_window: int = 3
    accumulation_lookback: int = 10
    manipulation_lookback: int = 6
    distribution_lookback: int = 8
    min_fvg_size: float = 0.35
    min_bvg_size: float = 0.25
    zone_fresh_bars: int = 24
    retest_tolerance_pct: float = 0.0015
    max_retest_bars: int = 6
    rr_ratio: float = 2.0
    trailing_sl_pct: float = 0.0
    duplicate_signal_cooldown_bars: int = 12
    min_score_conservative: float = 7.4
    min_score_balanced: float = 6.2
    min_score_aggressive: float = 4.8
    require_vwap_alignment: bool = True
    require_trend_alignment: bool = True
    require_retest_confirmation: bool = True
    require_liquidity_sweep: bool = True
    require_fvg_confirmation: bool = True
    allow_bvg_entries: bool = False
    require_distribution_phase: bool = True
    minimum_amd_confidence: float = 1.2
    allow_secondary_entries: bool = False
    max_trades_per_day: int = 1

    @classmethod
    def for_mode(cls, mode: str) -> 'ConfluenceConfig':
        normalized = _normalize_mode(mode)
        base = cls(mode=normalized)
        if normalized == 'Conservative':
            return replace(
                base,
                swing_window=4,
                accumulation_lookback=12,
                manipulation_lookback=7,
                distribution_lookback=10,
                min_fvg_size=0.45,
                min_bvg_size=0.35,
                zone_fresh_bars=18,
                retest_tolerance_pct=0.0012,
                max_retest_bars=4,
                minimum_amd_confidence=1.35,
                duplicate_signal_cooldown_bars=14,
            )
        if normalized == 'Aggressive':
            return replace(
                base,
                swing_window=2,
                accumulation_lookback=8,
                manipulation_lookback=4,
                distribution_lookback=6,
                min_fvg_size=0.25,
                min_bvg_size=0.2,
                zone_fresh_bars=30,
                retest_tolerance_pct=0.002,
                max_retest_bars=8,
                require_distribution_phase=False,
                minimum_amd_confidence=0.9,
                duplicate_signal_cooldown_bars=8,
            )
        return base


def _normalize_mode(mode: str) -> str:
    raw = str(mode or '').strip().lower()
    if raw == 'conservative':
        return 'Conservative'
    if raw == 'aggressive':
        return 'Aggressive'
    return 'Balanced'


def _score_threshold(config: ConfluenceConfig, mode: str) -> float:
    normalized = _normalize_mode(mode)
    if normalized == 'Conservative':
        return float(config.min_score_conservative)
    if normalized == 'Aggressive':
        return float(config.min_score_aggressive)
    return float(config.min_score_balanced)


def _risk_fraction(risk_pct: float) -> float:
    value = float(risk_pct or 0.0)
    return value / 100.0 if value > 1 else value


def _calculate_quantity(capital: float, risk_pct: float, entry: float, stop_loss: float) -> int:
    risk_per_unit = abs(float(entry) - float(stop_loss))
    if risk_per_unit <= 0:
        return 0
    risk_amount = max(0.0, float(capital)) * _risk_fraction(risk_pct)
    if risk_amount <= 0:
        return 1
    return max(1, floor(risk_amount / risk_per_unit))


def _prepare_df(data: Any) -> pd.DataFrame:
    df = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    df.columns = [str(column).strip().lower() for column in df.columns]
    rename_map = {'datetime': 'timestamp', 'date': 'timestamp', 'time': 'timestamp', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'vol': 'volume'}
    for source, target in rename_map.items():
        if source in df.columns and target not in df.columns:
            df = df.rename(columns={source: target})

    required = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f'Missing required AMD columns: {missing}')

    df = df.loc[:, required].copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    for column in ['open', 'high', 'low', 'close', 'volume']:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    df = df.dropna(subset=['timestamp', 'open', 'high', 'low', 'close']).drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
    if df.empty:
        return df

    df['bar_range'] = (df['high'] - df['low']).clip(lower=0.0)
    df['body_size'] = (df['close'] - df['open']).abs()
    df['ema_fast'] = df['close'].ewm(span=8, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=21, adjust=False).mean()
    df['session_day'] = df['timestamp'].dt.strftime('%Y-%m-%d')
    typical_price = (df['high'] + df['low'] + df['close']) / 3.0
    session_value = (typical_price * df['volume'].fillna(0.0)).groupby(df['session_day']).cumsum()
    session_volume = df['volume'].fillna(0.0).groupby(df['session_day']).cumsum().replace(0.0, pd.NA)
    df['vwap'] = session_value.div(session_volume).fillna(df['close']).astype(float)
    df['avg_range_5'] = df['bar_range'].rolling(5, min_periods=1).mean()
    df['recent_high'] = df['high'].shift(1).rolling(6, min_periods=2).max()
    df['recent_low'] = df['low'].shift(1).rolling(6, min_periods=2).min()
    df['bullish_fvg_gap'] = (df['low'] - df['high'].shift(2)).clip(lower=0.0)
    df['bearish_fvg_gap'] = (df['low'].shift(2) - df['high']).clip(lower=0.0)
    return df


def _recent_true(series: pd.Series, index: int, lookback: int) -> int | None:
    left = max(0, int(index) - int(lookback))
    window = series.iloc[left:index + 1]
    matches = window[window.fillna(False)]
    if matches.empty:
        return None
    return int(matches.index[-1])


def _build_signal(row: pd.Series, *, symbol: str, side: str, stop_loss: float, target_price: float, quantity: int, total_score: float, imbalance_type: str, amd_phase: str) -> StrategySignal:
    return StrategySignal(
        strategy_name='AMD + FVG + Supply/Demand',
        symbol=symbol,
        side=side,
        entry_price=round(float(row['close']), 4),
        stop_loss=round(float(stop_loss), 4),
        target_price=round(float(target_price), 4),
        signal_time=pd.Timestamp(row['timestamp']).to_pydatetime(),
        metadata={
            'quantity': int(quantity),
            'total_score': round(float(total_score), 2),
            'score': round(float(total_score), 2),
            'zone_type': 'demand' if side == 'BUY' else 'supply',
            'imbalance_type': imbalance_type,
            'amd_phase': amd_phase,
            'market_signal': f'{side} + {imbalance_type} + {amd_phase}',
        },
    )


def run_amd_strategy(
    data: Any,
    symbol: str,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    config: ConfluenceConfig | None = None,
) -> list[StrategySignal]:
    cfg = config or ConfluenceConfig()
    candles = _prepare_df(data)
    if candles.empty:
        return []

    mode = _normalize_mode(cfg.mode)
    threshold = _score_threshold(cfg, mode)
    trades: list[StrategySignal] = []
    trade_counts: dict[str, int] = {}
    last_signal_index: dict[str, int] = {'BUY': -10_000, 'SELL': -10_000}

    candles['bullish_manipulation'] = (candles['low'] < candles['recent_low']) & (candles['close'] > candles['recent_low'])
    candles['bearish_manipulation'] = (candles['high'] > candles['recent_high']) & (candles['close'] < candles['recent_high'])
    candles['bullish_distribution'] = (candles['close'] > candles['recent_high']) & (candles['close'] > candles['ema_fast'])
    candles['bearish_distribution'] = (candles['close'] < candles['recent_low']) & (candles['close'] < candles['ema_fast'])
    candles['bullish_fvg'] = candles['bullish_fvg_gap'] >= float(cfg.min_fvg_size)
    candles['bearish_fvg'] = candles['bearish_fvg_gap'] >= float(cfg.min_fvg_size)

    start_index = max(5, int(cfg.accumulation_lookback), int(cfg.manipulation_lookback))
    for index in range(start_index, len(candles)):
        row = candles.iloc[index]
        day_key = str(row['session_day'])
        if trade_counts.get(day_key, 0) >= max(1, int(cfg.max_trades_per_day)):
            continue

        for side in ('BUY', 'SELL'):
            if index - last_signal_index[side] < int(cfg.duplicate_signal_cooldown_bars):
                continue

            is_buy = side == 'BUY'
            manip_col = 'bullish_manipulation' if is_buy else 'bearish_manipulation'
            dist_col = 'bullish_distribution' if is_buy else 'bearish_distribution'
            fvg_col = 'bullish_fvg' if is_buy else 'bearish_fvg'
            recent_manip = _recent_true(candles[manip_col], index, int(cfg.max_retest_bars))
            recent_fvg = _recent_true(candles[fvg_col], index, int(cfg.max_retest_bars))
            if bool(cfg.require_liquidity_sweep) and recent_manip is None:
                continue
            if bool(cfg.require_fvg_confirmation) and recent_fvg is None:
                continue
            if bool(cfg.require_distribution_phase) and not bool(row[dist_col]):
                continue

            trend_ok = float(row['close']) >= float(row['ema_fast']) >= float(row['ema_slow']) if is_buy else float(row['close']) <= float(row['ema_fast']) <= float(row['ema_slow'])
            vwap_ok = float(row['close']) >= float(row['vwap']) if is_buy else float(row['close']) <= float(row['vwap'])
            if bool(cfg.require_trend_alignment) and not trend_ok:
                continue
            if bool(cfg.require_vwap_alignment) and not vwap_ok:
                continue

            amd_score = 4.0 if recent_manip is not None else 0.0
            amd_score += 3.0 if bool(row[dist_col]) else 0.0
            fvg_score = 3.0 if recent_fvg is not None else 0.0
            fvg_score += 1.0 if float(row['bullish_fvg_gap'] if is_buy else row['bearish_fvg_gap']) >= float(cfg.min_fvg_size) * 1.25 else 0.0
            sd_score = 2.0 if trend_ok else 0.0
            sd_score += 1.0 if vwap_ok else 0.0
            total_score = amd_score + fvg_score + sd_score
            if total_score < threshold:
                continue

            buffer = max(float(row['avg_range_5']) * 0.2, float(row['close']) * float(cfg.retest_tolerance_pct), 0.05)
            if is_buy:
                stop_anchor = min(float(row['low']), float(candles.iloc[recent_fvg]['high']) if recent_fvg is not None else float(row['low']))
                stop_loss = stop_anchor - buffer
                if stop_loss >= float(row['close']):
                    stop_loss = float(row['close']) - max(buffer, 0.1)
                target_price = float(row['close']) + ((float(row['close']) - stop_loss) * float(rr_ratio))
                imbalance_type = 'FVG'
            else:
                stop_anchor = max(float(row['high']), float(candles.iloc[recent_fvg]['low']) if recent_fvg is not None else float(row['high']))
                stop_loss = stop_anchor + buffer
                if stop_loss <= float(row['close']):
                    stop_loss = float(row['close']) + max(buffer, 0.1)
                target_price = float(row['close']) - ((stop_loss - float(row['close'])) * float(rr_ratio))
                imbalance_type = 'FVG'

            quantity = _calculate_quantity(capital, risk_pct, float(row['close']), stop_loss)
            if quantity <= 0:
                continue

            trades.append(
                _build_signal(
                    row,
                    symbol=symbol,
                    side=side,
                    stop_loss=stop_loss,
                    target_price=target_price,
                    quantity=quantity,
                    total_score=total_score,
                    imbalance_type=imbalance_type,
                    amd_phase='distribution',
                )
            )
            trade_counts[day_key] = trade_counts.get(day_key, 0) + 1
            last_signal_index[side] = index
            break

    return trades
