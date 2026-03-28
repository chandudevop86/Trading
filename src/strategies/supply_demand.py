from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from src import demand_supply_bot as ds


@dataclass(slots=True)
class ZoneRecord:
    symbol: str
    side: str
    pattern: str
    zone_start_time: str
    zone_end_time: str
    zone_low: float
    zone_high: float
    zone_score: float
    score_bucket: str
    score_interpretation: str
    retest_status: str
    fresh_status: str
    touch_count: int
    base_candle_count: int
    zone_kind: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _score_bucket(score: float) -> str:
    if score >= 13:
        return '13+'
    if score >= 10:
        return '10-12'
    if score >= 7:
        return '7-9'
    return '0-6'


def detect_scored_zones(
    rows: list[dict[str, Any]] | list[ds.Candle],
    *,
    symbol: str = '^NSEI',
    config: ds.DemandSupplyConfig | None = None,
) -> list[ZoneRecord]:
    cfg = config or ds.DemandSupplyConfig()
    candles = ds._coerce_candles(rows)
    if not candles:
        return []
    ds.calculate_vwap(candles)
    by_day = ds._group_by_day(candles)
    zone_records: list[ZoneRecord] = []

    for _, day_candles in by_day.items():
        zones = ds._find_zones(day_candles, cfg.pivot_window)
        for zone in zones:
            side = 'BUY' if zone.kind == 'demand' else 'SELL'
            expected_pattern = 'DBR' if side == 'BUY' else 'RBD'
            prequal_idx = min(len(day_candles) - 1, zone.idx + 2)
            zone_score = ds._zone_selection_score(day_candles, prequal_idx, zone, side, cfg)
            touch_count = ds._touch_count(day_candles, zone, side, prequal_idx, cfg)
            fresh_status = 'FRESH' if touch_count == 0 else 'TOUCHED'
            zone_end_time = day_candles[-1].timestamp.strftime('%Y-%m-%d %H:%M:%S')
            retest_status = 'NOT_RETESTED'
            score_interpretation = 'SKIP'
            base_candle_count = 0

            retest = ds.detect_retest(day_candles, zone, side, zone.idx + 1, cfg)
            if retest is not None:
                touch_idx, confirmation_idx, _ = retest
                score_result = ds._quality_score(
                    day_candles,
                    confirmation_idx,
                    zone,
                    side,
                    cfg,
                    touch=True,
                    retest_confirmed=True,
                    touch_idx=touch_idx,
                )
                if score_result is not None:
                    score_value, _, _, _, diagnostics = score_result
                    zone_score = float(diagnostics.get('raw_total_score', score_value) or score_value)
                    score_interpretation = str(diagnostics.get('score_interpretation', 'SKIP') or 'SKIP')
                    base_candle_count = int(diagnostics.get('base_candle_count', 0) or 0)
                    zone_end_time = day_candles[confirmation_idx].timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    retest_status = 'CONFIRMED'
                else:
                    retest_status = 'RETESTED_REJECTED'
            elif touch_count > 0:
                retest_status = 'REVISITED'

            zone_records.append(
                ZoneRecord(
                    symbol=str(symbol),
                    side=side,
                    pattern=expected_pattern if str(zone.pattern or 'UNKNOWN').upper() == expected_pattern else str(zone.pattern or 'UNKNOWN'),
                    zone_start_time=day_candles[zone.idx].timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    zone_end_time=zone_end_time,
                    zone_low=round(float(zone.low), 4),
                    zone_high=round(float(zone.high), 4),
                    zone_score=round(float(zone_score), 2),
                    score_bucket=_score_bucket(float(zone_score)),
                    score_interpretation=score_interpretation,
                    retest_status=retest_status,
                    fresh_status=fresh_status,
                    touch_count=int(touch_count),
                    base_candle_count=int(base_candle_count),
                    zone_kind=zone.kind,
                )
            )
    return zone_records


def zone_records_to_rows(records: list[ZoneRecord]) -> list[dict[str, object]]:
    return [record.to_dict() for record in records]
