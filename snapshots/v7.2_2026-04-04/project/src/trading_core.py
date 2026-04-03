from __future__ import annotations

import logging
import os
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from src.runtime_config import RuntimeConfig
from src.runtime_persistence import persist_rows

LOGGER = logging.getLogger('trading_system')
RUNTIME_CONFIG = RuntimeConfig.load()


def round_half_up(value: float, places: int) -> float:
    quantum = Decimal('1').scaleb(-places)
    normalized = Decimal(f'{float(value):.{places + 8}f}')
    return float(normalized.quantize(quantum, rounding=ROUND_HALF_UP))


@dataclass(slots=True)
class ScoreWeights:
    trend: float = 2.0
    vwap: float = 1.0
    rsi: float = 1.0
    adx: float = 1.0
    macd: float = 1.0
    zone: float = 2.0
    fvg: float = 2.0
    sweep: float = 1.0
    retest: float = 1.0
    reaction: float = 1.0
    breakout_quality: float = 1.5


@dataclass(slots=True)
class ScoreThresholds:
    conservative: float = 7.0
    balanced: float = 5.0
    aggressive: float = 3.0


@dataclass(slots=True)
class ScoringConfig:
    mode: str = 'Balanced'
    weights: ScoreWeights = field(default_factory=ScoreWeights)
    thresholds: ScoreThresholds = field(default_factory=ScoreThresholds)

    def normalized_mode(self) -> str:
        raw = str(self.mode or '').strip().lower()
        if raw == 'conservative':
            return 'Conservative'
        if raw == 'aggressive':
            return 'Aggressive'
        return 'Balanced'

    def threshold(self) -> float:
        mode = self.normalized_mode()
        if mode == 'Conservative':
            return float(self.thresholds.conservative)
        if mode == 'Aggressive':
            return float(self.thresholds.aggressive)
        return float(self.thresholds.balanced)


@dataclass(slots=True)
class WeightedScore:
    total: float
    threshold: float
    accepted: bool
    components: dict[str, float]
    reasons: list[str]


@dataclass(slots=True)
class StandardTrade:
    timestamp: str
    side: str
    entry: float
    stop_loss: float
    target: float
    strategy: str
    reason: str
    score: float
    entry_price: float
    target_price: float
    risk_per_unit: float
    quantity: int = 0
    pnl: float | None = None
    amd_phase: str = ''
    zone_type: str = ''
    imbalance_type: str = ''
    extra: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        base = asdict(self)
        extra = dict(base.pop('extra', {}) or {})
        base['timestamp'] = str(base['timestamp'])
        base['entry_time'] = str(base['timestamp'])
        rounded_entry = round_half_up(float(base['entry']), 4)
        rounded_entry_price = round_half_up(float(base['entry_price']), 4)
        rounded_stop_loss = round_half_up(float(base['stop_loss']), 4)
        rounded_target = round_half_up(float(base['target']), 4)
        rounded_target_price = round_half_up(float(base['target_price']), 4)
        base['entry'] = rounded_entry
        base['entry_price'] = rounded_entry_price
        base['stop_loss'] = rounded_stop_loss
        base['target'] = rounded_target
        base['target_price'] = rounded_target_price
        base['score'] = round_half_up(float(base['score']), 2)
        # Keep the displayed risk distance aligned with the displayed entry and
        # stop prices so exported rows stay internally consistent.
        base['risk_per_unit'] = round_half_up(abs(rounded_entry_price - rounded_stop_loss), 4)
        base.update(extra)
        return base


def configure_file_logging(log_path: Path | str = RUNTIME_CONFIG.paths.errors_log) -> logging.Logger:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not any(isinstance(handler, logging.FileHandler) and Path(getattr(handler, 'baseFilename', '')) == path.resolve() for handler in LOGGER.handlers):
        handler = logging.FileHandler(path, encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    return LOGGER


def prepare_trading_data(df: Any, *, include_derived: bool = True) -> pd.DataFrame:
    from src.preprocessing import prepare_trading_data as _prepare_trading_data

    return _prepare_trading_data(df, include_derived=include_derived)

def weighted_score(signals: dict[str, bool], config: ScoringConfig | None = None) -> WeightedScore:
    scoring = config or ScoringConfig()
    components: dict[str, float] = {}
    for name, weight in asdict(scoring.weights).items():
        components[name] = float(weight if bool(signals.get(name)) else 0.0)
    total = round(sum(components.values()), 2)
    threshold = round(float(scoring.threshold()), 2)
    reasons = [name for name, value in components.items() if value <= 0 and name in signals]
    return WeightedScore(total=total, threshold=threshold, accepted=total >= threshold, components=components, reasons=reasons)


def normalize_risk_pct(risk_pct: float) -> float:
    value = float(risk_pct or 0.0)
    return value / 100.0 if value > 1 else value


def safe_quantity(capital: float, risk_pct: float, entry: float, stop_loss: float) -> int:
    risk_per_unit = abs(float(entry) - float(stop_loss))
    if risk_per_unit <= 0:
        return 0
    risk_amount = max(0.0, float(capital)) * normalize_risk_pct(risk_pct)
    if risk_amount <= 0:
        return 0
    return max(int(risk_amount / risk_per_unit), 1)


def write_rows(path: Path | str, rows: list[dict[str, object]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    temp_target = target.with_name(f'{target.stem}.tmp{target.suffix}')
    persisted_target = target
    try:
        frame.to_csv(temp_target, index=False)
        os.replace(temp_target, target)
    except PermissionError as exc:
        fallback = target.with_name(f'{target.stem}_latest{target.suffix}')
        frame.to_csv(fallback, index=False)
        persisted_target = fallback
        append_log(f'write_rows fallback path used for {target}: {type(exc).__name__}: {exc}', level=logging.WARNING)
        if temp_target.exists():
            try:
                temp_target.unlink()
            except Exception:
                pass
    except Exception:
        if temp_target.exists():
            try:
                temp_target.unlink()
            except Exception:
                pass
        raise
    try:
        persist_rows(persisted_target, frame.to_dict(orient='records'), write_mode='replace')
    except Exception as exc:
        append_log(f'write_rows persistence failed path={persisted_target}: {type(exc).__name__}: {exc}', level=logging.WARNING)
    try:
        from src.aws_storage import sync_path_to_s3_if_enabled

        key_prefix = persisted_target.parent.name if persisted_target.parent.name else 'data'
        sync_path_to_s3_if_enabled(persisted_target, key_prefix=key_prefix)
    except Exception as exc:
        append_log(f'write_rows s3 sync failed path={persisted_target}: {type(exc).__name__}: {exc}', level=logging.WARNING)

def append_log(message: str, *, level: int = logging.INFO) -> None:
    configure_file_logging()
    LOGGER.log(level, message)


