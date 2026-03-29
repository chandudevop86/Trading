from __future__ import annotations

from typing import Any

import altair as alt
import pandas as pd


_BUCKET_COLORS = {
    '0-6': '#cfd8dc',
    '7-9': '#90caf9',
    '10-12': '#42a5f5',
    '13+': '#1565c0',
}


def _score_bucket(score: float) -> str:
    if score >= 13:
        return '13+'
    if score >= 10:
        return '10-12'
    if score >= 7:
        return '7-9'
    return '0-6'


def _zone_opacity(score: float) -> float:
    bucket = _score_bucket(score)
    return {
        '0-6': 0.18,
        '7-9': 0.30,
        '10-12': 0.45,
        '13+': 0.60,
    }[bucket]


def _prepare_candles(rows: list[dict[str, Any]] | pd.DataFrame) -> pd.DataFrame:
    frame = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close'])
    frame.columns = [str(column).strip().lower() for column in frame.columns]
    if 'datetime' in frame.columns and 'timestamp' not in frame.columns:
        frame = frame.rename(columns={'datetime': 'timestamp'})
    frame['timestamp'] = pd.to_datetime(frame.get('timestamp'), errors='coerce')
    for column in ['open', 'high', 'low', 'close']:
        frame[column] = pd.to_numeric(frame.get(column), errors='coerce')
    frame = frame.dropna(subset=['timestamp', 'open', 'high', 'low', 'close']).sort_values('timestamp').reset_index(drop=True)
    return frame


def _prepare_zones(zone_rows: list[dict[str, Any]] | pd.DataFrame) -> pd.DataFrame:
    frame = zone_rows.copy() if isinstance(zone_rows, pd.DataFrame) else pd.DataFrame(zone_rows)
    if frame.empty:
        return pd.DataFrame(columns=['zone_start_time', 'zone_end_time', 'zone_low', 'zone_high', 'zone_score'])
    frame['zone_start_time'] = pd.to_datetime(frame.get('zone_start_time'), errors='coerce')
    frame['zone_end_time'] = pd.to_datetime(frame.get('zone_end_time'), errors='coerce')
    frame['zone_low'] = pd.to_numeric(frame.get('zone_low'), errors='coerce')
    frame['zone_high'] = pd.to_numeric(frame.get('zone_high'), errors='coerce')
    frame['zone_score'] = pd.to_numeric(frame.get('zone_score', frame.get('score')), errors='coerce').fillna(0.0)
    frame['score_bucket'] = frame['zone_score'].apply(_score_bucket)
    frame['opacity'] = frame['zone_score'].apply(_zone_opacity)
    frame['zone_label'] = frame.apply(lambda row: f"{row.get('pattern', row.get('zone_pattern', 'ZONE'))} | Score {int(round(float(row.get('zone_score', 0.0))))}", axis=1)
    frame = frame.dropna(subset=['zone_start_time', 'zone_end_time', 'zone_low', 'zone_high'])
    return frame


def build_zone_heatmap(
    candles: list[dict[str, Any]] | pd.DataFrame,
    zones: list[dict[str, Any]] | pd.DataFrame,
    *,
    show_labels: bool = True,
) -> alt.Chart:
    candle_frame = _prepare_candles(candles)
    zone_frame = _prepare_zones(zones)

    base = alt.Chart(candle_frame)
    wick = base.mark_rule(color='#455a64').encode(
        x='timestamp:T',
        y='low:Q',
        y2='high:Q',
    )
    body = base.mark_bar(size=6).encode(
        x='timestamp:T',
        y='open:Q',
        y2='close:Q',
        color=alt.condition('datum.close >= datum.open', alt.value('#2e7d32'), alt.value('#c62828')),
    )

    heatmap = alt.Chart(zone_frame).mark_rect(stroke='#546e7a', strokeOpacity=0.35).encode(
        x='zone_start_time:T',
        x2='zone_end_time:T',
        y='zone_low:Q',
        y2='zone_high:Q',
        color=alt.Color('score_bucket:N', scale=alt.Scale(domain=list(_BUCKET_COLORS.keys()), range=list(_BUCKET_COLORS.values())), legend=alt.Legend(title='Zone strength')),
        opacity=alt.Opacity('opacity:Q', scale=None),
        tooltip=[
            alt.Tooltip('pattern:N', title='Pattern'),
            alt.Tooltip('zone_score:Q', title='Score'),
            alt.Tooltip('score_bucket:N', title='Bucket'),
            alt.Tooltip('retest_status:N', title='Retest'),
            alt.Tooltip('fresh_status:N', title='Freshness'),
            alt.Tooltip('touch_count:Q', title='Touches'),
            alt.Tooltip('zone_low:Q', title='Zone low'),
            alt.Tooltip('zone_high:Q', title='Zone high'),
        ],
    )

    chart = alt.layer(heatmap, wick, body).resolve_scale(color='independent')

    if show_labels and not zone_frame.empty:
        labels = alt.Chart(zone_frame).mark_text(align='left', baseline='top', dx=4, dy=4, color='#0d1b2a').encode(
            x='zone_start_time:T',
            y='zone_high:Q',
            text='zone_label:N',
        )
        chart = alt.layer(heatmap, wick, body, labels).resolve_scale(color='independent')

    return chart.properties(height=420)
