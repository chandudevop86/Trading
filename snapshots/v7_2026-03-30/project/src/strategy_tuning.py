from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.backtest_engine import BacktestConfig, BacktestValidationConfig


@dataclass(frozen=True, slots=True)
class StrategyTuningPreset:
    strategy_key: str
    conservative_threshold: float
    balanced_threshold: float
    aggressive_threshold: float
    duplicate_signal_cooldown_bars: int
    max_trades_per_day: int
    min_trades: int
    target_trades_low: int
    target_trades_high: int
    min_win_rate: float
    min_profit_factor: float
    min_expectancy_per_trade: float
    min_avg_rr: float
    max_drawdown_pct: float
    risk_pct: float = 0.005
    max_daily_loss_pct: float = 0.02
    duplicate_cooldown_minutes: int = 15
    commission_per_trade: float = 20.0
    slippage_bps: float = 3.0
    allow_secondary_entries: bool = False
    require_positive_expectancy: bool = True


STRATEGY_TUNING_PRESETS: dict[str, StrategyTuningPreset] = {
    'BREAKOUT': StrategyTuningPreset(
        strategy_key='BREAKOUT',
        conservative_threshold=7.0,
        balanced_threshold=5.5,
        aggressive_threshold=4.5,
        duplicate_signal_cooldown_bars=8,
        max_trades_per_day=1,
        min_trades=20,
        target_trades_low=20,
        target_trades_high=30,
        min_win_rate=40.0,
        min_profit_factor=1.35,
        min_expectancy_per_trade=0.0,
        min_avg_rr=1.30,
        max_drawdown_pct=10.0,
    ),
    'DEMAND_SUPPLY': StrategyTuningPreset(
        strategy_key='DEMAND_SUPPLY',
        conservative_threshold=8.2,
        balanced_threshold=6.8,
        aggressive_threshold=5.8,
        duplicate_signal_cooldown_bars=18,
        max_trades_per_day=1,
        min_trades=100,
        target_trades_low=120,
        target_trades_high=180,
        min_win_rate=40.0,
        min_profit_factor=1.30,
        min_expectancy_per_trade=0.0,
        min_avg_rr=1.30,
        max_drawdown_pct=10.0,
    ),
    'INDICATOR': StrategyTuningPreset(
        strategy_key='INDICATOR',
        conservative_threshold=6.5,
        balanced_threshold=5.0,
        aggressive_threshold=4.0,
        duplicate_signal_cooldown_bars=6,
        max_trades_per_day=3,
        min_trades=100,
        target_trades_low=140,
        target_trades_high=200,
        min_win_rate=45.0,
        min_profit_factor=1.25,
        min_expectancy_per_trade=0.0,
        min_avg_rr=1.05,
        max_drawdown_pct=12.0,
    ),
    'AMD_FVG_SD': StrategyTuningPreset(
        strategy_key='AMD_FVG_SD',
        conservative_threshold=7.4,
        balanced_threshold=6.2,
        aggressive_threshold=4.8,
        duplicate_signal_cooldown_bars=12,
        max_trades_per_day=1,
        min_trades=20,
        target_trades_low=20,
        target_trades_high=30,
        min_win_rate=42.0,
        min_profit_factor=1.40,
        min_expectancy_per_trade=0.0,
        min_avg_rr=1.35,
        max_drawdown_pct=10.0,
        allow_secondary_entries=False,
    ),
}


def normalize_strategy_key(strategy_name: str) -> str:
    raw = str(strategy_name or '').strip().upper().replace('+', ' ').replace('/', ' ')
    collapsed = '_'.join(part for part in raw.replace('-', ' ').split() if part)
    aliases = {
        'BREAKOUT': 'BREAKOUT',
        'DEMAND_SUPPLY': 'DEMAND_SUPPLY',
        'DEMANDSUPPLY': 'DEMAND_SUPPLY',
        'INDICATOR': 'INDICATOR',
        'AMD_FVG_SUPPLY_DEMAND': 'AMD_FVG_SD',
        'AMD_FVG_SD': 'AMD_FVG_SD',
        'AMD_FVG_SUPPLY_DEMAND_STRATEGY': 'AMD_FVG_SD',
    }
    return aliases.get(collapsed, collapsed)


def strategy_tuning_preset(strategy_name: str) -> StrategyTuningPreset:
    normalized = normalize_strategy_key(strategy_name)
    return STRATEGY_TUNING_PRESETS.get(normalized, STRATEGY_TUNING_PRESETS['BREAKOUT'])


def _rr_adjusted_min_win_rate(rr_ratio: float, preset: StrategyTuningPreset) -> float:
    reward_multiple = max(float(rr_ratio or 0.0), 0.5)
    breakeven_win_rate = 100.0 / (reward_multiple + 1.0)
    return round(max(float(preset.min_win_rate), breakeven_win_rate + 6.0), 2)


def strategy_validation_config(strategy_name: str, rr_ratio: float = 2.0) -> BacktestValidationConfig:
    preset = strategy_tuning_preset(strategy_name)
    return BacktestValidationConfig(
        min_trades=preset.min_trades,
        target_trades=int((preset.target_trades_low + preset.target_trades_high) / 2),
        max_trades=preset.target_trades_high,
        min_profit_factor=preset.min_profit_factor,
        min_expectancy_per_trade=preset.min_expectancy_per_trade,
        min_win_rate=_rr_adjusted_min_win_rate(rr_ratio, preset),
        min_avg_rr=preset.min_avg_rr,
        max_drawdown_pct=preset.max_drawdown_pct,
        max_duplicate_rejections=0,
        require_positive_expectancy=preset.require_positive_expectancy,
    )


def strategy_backtest_config(
    strategy_name: str,
    *,
    capital: float = 100000.0,
    risk_pct: float | None = None,
    rr_ratio: float = 2.0,
    trades_output: Path,
    summary_output: Path,
    validation_output: Path,
) -> BacktestConfig:
    preset = strategy_tuning_preset(strategy_name)
    return BacktestConfig(
        capital=float(capital),
        risk_pct=float(risk_pct if risk_pct is not None else preset.risk_pct),
        rr_ratio=float(rr_ratio),
        trades_output=trades_output,
        summary_output=summary_output,
        validation_output=validation_output,
        strategy_name=strategy_name,
        max_trades_per_day=preset.max_trades_per_day,
        max_daily_loss=max(float(capital) * float(preset.max_daily_loss_pct), 0.0),
        duplicate_cooldown_minutes=preset.duplicate_cooldown_minutes,
        commission_per_trade=preset.commission_per_trade,
        slippage_bps=preset.slippage_bps,
        validation=strategy_validation_config(strategy_name, rr_ratio=float(rr_ratio)),
    )


def apply_strategy_benchmark(summary_row: dict[str, Any]) -> dict[str, Any]:
    item = dict(summary_row)
    preset = strategy_tuning_preset(str(item.get('strategy', '')))
    total_trades = int(float(item.get('total_trades', item.get('trades', 0)) or 0))
    profit_factor_raw = item.get('profit_factor', 0.0)
    profit_factor = float('inf') if str(profit_factor_raw).strip().lower() == 'inf' else float(profit_factor_raw or 0.0)
    expectancy = float(item.get('expectancy_per_trade', 0.0) or 0.0)
    avg_rr = float(item.get('avg_rr', 0.0) or 0.0)
    win_rate = float(item.get('win_rate', item.get('win_rate_pct', 0.0)) or 0.0)
    max_drawdown_pct = float(item.get('max_drawdown_pct', 0.0) or 0.0)
    duplicate_rejections = int(float(item.get('duplicate_rejections', 0) or 0))
    risk_rule_rejections = int(float(item.get('risk_rule_rejections', 0) or 0))
    positive_expectancy = 'YES' if expectancy > 0 else 'NO'
    blockers: list[str] = []
    if total_trades < preset.min_trades:
        blockers.append(f'MIN_TRADES<{preset.min_trades}')
    if total_trades > preset.target_trades_high:
        blockers.append(f'MAX_TRADES>{preset.target_trades_high}')
    if profit_factor != float('inf') and profit_factor < preset.min_profit_factor:
        blockers.append(f'PROFIT_FACTOR<{preset.min_profit_factor:.2f}')
    if expectancy <= preset.min_expectancy_per_trade:
        blockers.append('NEGATIVE_EXPECTANCY')
    if avg_rr < preset.min_avg_rr:
        blockers.append(f'AVG_RR<{preset.min_avg_rr:.2f}')
    if win_rate < preset.min_win_rate:
        blockers.append(f'WIN_RATE<{preset.min_win_rate:.2f}')
    if max_drawdown_pct > preset.max_drawdown_pct:
        blockers.append(f'MAX_DD_PCT>{preset.max_drawdown_pct:.2f}')
    if duplicate_rejections > 0:
        blockers.append('DUPLICATES>0')
    if total_trades > 0 and (risk_rule_rejections / total_trades) > 0.15:
        blockers.append('RISK_RULE_REJECTIONS_ELEVATED')
    item['positive_expectancy'] = positive_expectancy
    item['sample_window_passed'] = 'YES' if preset.min_trades <= total_trades <= preset.target_trades_high else 'NO'
    item['deployment_ready'] = 'YES' if not blockers else 'NO'
    item['deployment_blockers'] = '; '.join(blockers)
    item['mode'] = item.get('mode', 'Balanced')
    return item


def optimizer_report_rows(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in summary_rows:
        item = dict(row)
        preset = strategy_tuning_preset(str(item.get('strategy', '')))
        deployment_ready = str(item.get('deployment_ready', 'NO')).upper()
        positive_expectancy = str(item.get('positive_expectancy', 'NO')).upper()
        expectancy = float(item.get('expectancy_per_trade', 0.0) or 0.0)
        profit_factor_raw = item.get('profit_factor', 0.0)
        profit_factor = 3.0 if str(profit_factor_raw).strip().lower() == 'inf' else float(profit_factor_raw or 0.0)
        avg_rr = float(item.get('avg_rr', 0.0) or 0.0)
        win_rate = float(item.get('win_rate', item.get('win_rate_pct', 0.0)) or 0.0)
        max_drawdown_pct = float(item.get('max_drawdown_pct', 0.0) or 0.0)
        second_half_expectancy = float(item.get('second_half_expectancy_per_trade', 0.0) or 0.0)
        expectancy_stability_gap_ratio = float(item.get('expectancy_stability_gap_ratio', 0.0) or 0.0)
        drawdown_proven = str(item.get('drawdown_proven', 'NO') or 'NO').upper()
        validation_status = str(item.get('validation_status', 'FAIL') or 'FAIL').upper()
        retest_only_trade_pct = float(item.get('retest_only_trade_pct', 0.0) or 0.0)
        vwap_pass_pct = float(item.get('vwap_pass_pct', 0.0) or 0.0)
        session_pass_pct = float(item.get('session_pass_pct', 0.0) or 0.0)
        duplicate_rejections = int(float(item.get('duplicate_rejections', 0) or 0))
        risk_rule_rejections = int(float(item.get('risk_rule_rejections', 0) or 0))
        rank_score = (
            (1600 if deployment_ready == 'YES' else 0)
            + (500 if validation_status == 'PASS' else 0)
            + (250 if drawdown_proven == 'YES' else 0)
            + (300 if positive_expectancy == 'YES' else 0)
            + (expectancy * 25.0)
            + (second_half_expectancy * 20.0)
            + (min(profit_factor, 3.0) * 40.0)
            + (avg_rr * 20.0)
            + (win_rate * 1.5)
            + (retest_only_trade_pct * 0.8)
            + (vwap_pass_pct * 0.6)
            + (session_pass_pct * 0.6)
            - (max_drawdown_pct * 10.0)
            - (expectancy_stability_gap_ratio * 120.0)
            - (duplicate_rejections * 6.0)
            - (risk_rule_rejections * 2.0)
        )
        rows.append(
            {
                'strategy': item.get('strategy', ''),
                'mode': item.get('mode', 'Balanced'),
                'timeframe': item.get('timeframe', ''),
                'sample_start': item.get('data_start', ''),
                'sample_end': item.get('data_end', ''),
                'total_trades': item.get('total_trades', item.get('trades', 0)),
                'wins': item.get('wins', 0),
                'losses': item.get('losses', 0),
                'win_rate': round(win_rate, 2),
                'total_pnl': item.get('total_pnl', 0.0),
                'avg_pnl': item.get('avg_pnl', 0.0),
                'avg_win': item.get('avg_win', 0.0),
                'avg_loss': item.get('avg_loss', 0.0),
                'profit_factor': item.get('profit_factor', 0.0),
                'expectancy_per_trade': round(expectancy, 2),
                'second_half_expectancy_per_trade': round(second_half_expectancy, 2),
                'expectancy_stability_gap_ratio': round(expectancy_stability_gap_ratio, 4),
                'expectancy_r': item.get('expectancy_r', 0.0),
                'avg_rr': round(avg_rr, 2),
                'max_drawdown': item.get('max_drawdown', 0.0),
                'max_drawdown_pct': round(max_drawdown_pct, 2),
                'drawdown_proven': drawdown_proven,
                'positive_expectancy': positive_expectancy,
                'duplicate_rejections': duplicate_rejections,
                'risk_rule_rejections': risk_rule_rejections,
                'commission_per_trade': preset.commission_per_trade,
                'slippage_bps': preset.slippage_bps,
                'score_threshold': preset.balanced_threshold,
                'cooldown_bars': preset.duplicate_signal_cooldown_bars,
                'max_trades_per_day': preset.max_trades_per_day,
                'allow_secondary_entries': 'YES' if preset.allow_secondary_entries else 'NO',
                'validation_status': validation_status,
                'retest_only_trade_pct': round(retest_only_trade_pct, 2),
                'vwap_pass_pct': round(vwap_pass_pct, 2),
                'session_pass_pct': round(session_pass_pct, 2),
                'deployment_ready': deployment_ready,
                'deployment_blockers': item.get('deployment_blockers', ''),
                'rank_score': round(rank_score, 2),
            }
        )
    rows.sort(key=lambda item: float(item.get('rank_score', 0.0)), reverse=True)
    for idx, row in enumerate(rows, start=1):
        row['optimizer_rank'] = idx
    return rows

