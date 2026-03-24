from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger('trading_system')

_REQUIRED_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']


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
        base['entry'] = round(float(base['entry']), 4)
        base['entry_price'] = round(float(base['entry_price']), 4)
        base['stop_loss'] = round(float(base['stop_loss']), 4)
        base['target'] = round(float(base['target']), 4)
        base['target_price'] = round(float(base['target_price']), 4)
        base['score'] = round(float(base['score']), 2)
        base['risk_per_unit'] = round(float(base['risk_per_unit']), 4)
        base.update(extra)
        return base


def configure_file_logging(log_path: Path | str = Path('logs/errors.log')) -> logging.Logger:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not any(isinstance(handler, logging.FileHandler) and Path(getattr(handler, 'baseFilename', '')) == path.resolve() for handler in LOGGER.handlers):
        handler = logging.FileHandler(path, encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    return LOGGER


def prepare_trading_data(df: Any) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame):
        prepared = df.copy()
    else:
        prepared = pd.DataFrame(df or [])

    if prepared.empty:
        return pd.DataFrame(columns=_REQUIRED_COLUMNS)

    if isinstance(prepared.columns, pd.MultiIndex):
        prepared.columns = [str(col[0]).strip().lower() for col in prepared.columns]
    else:
        prepared.columns = [str(col).strip().lower() for col in prepared.columns]

    rename_map: dict[str, str] = {}
    if 'datetime' in prepared.columns:
        rename_map['datetime'] = 'timestamp'
    if 'date' in prepared.columns and 'timestamp' not in prepared.columns:
        rename_map['date'] = 'timestamp'
    if 'time' in prepared.columns and 'timestamp' not in prepared.columns:
        rename_map['time'] = 'timestamp'
    if rename_map:
        prepared = prepared.rename(columns=rename_map)

    missing_columns = [column for column in _REQUIRED_COLUMNS if column not in prepared.columns]
    if missing_columns:
        raise ValueError(f'Missing required columns: {missing_columns}')

    prepared = prepared.loc[:, _REQUIRED_COLUMNS].copy()
    prepared['timestamp'] = pd.to_datetime(prepared['timestamp'], errors='coerce')
    for column in ['open', 'high', 'low', 'close', 'volume']:
        prepared[column] = pd.to_numeric(prepared[column], errors='coerce')

    prepared = prepared.dropna(subset=['timestamp', 'open', 'high', 'low', 'close'])
    prepared = prepared[(prepared['high'] >= prepared['low']) & (prepared['open'] >= 0) & (prepared['close'] >= 0)]
    prepared = prepared.drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
    return prepared


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
    frame.to_csv(target, index=False)
    try:
        from src.aws_storage import sync_path_to_s3_if_enabled

        key_prefix = target.parent.name if target.parent.name else 'data'
        sync_path_to_s3_if_enabled(target, key_prefix=key_prefix)
    except Exception:
        pass


def append_log(message: str, *, level: int = logging.INFO) -> None:
    configure_file_logging()
    LOGGER.log(level, message)


