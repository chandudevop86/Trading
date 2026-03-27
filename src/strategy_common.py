from __future__ import annotations

from datetime import datetime, time


def parse_hhmm(value: str, fallback: str) -> time:
    raw = str(value or fallback).strip() or fallback
    try:
        hh, mm = raw.split(":", 1)
        return time(hour=max(0, min(23, int(hh))), minute=max(0, min(59, int(mm))))
    except Exception:
        fh, fm = fallback.split(":", 1)
        return time(hour=int(fh), minute=int(fm))


def session_window(
    current: datetime | time,
    *,
    morning_start: str = "09:20",
    morning_end: str = "11:30",
    midday_start: str = "12:00",
    midday_end: str = "13:30",
    allow_afternoon_session: bool = False,
    afternoon_start: str = "13:45",
    afternoon_end: str = "15:00",
) -> str:
    current_time = current if isinstance(current, time) else current.time()
    normalized = current_time.replace(second=0, microsecond=0)
    if parse_hhmm(morning_start, "09:20") <= normalized <= parse_hhmm(morning_end, "11:30"):
        return "MORNING"
    if parse_hhmm(midday_start, "12:00") <= normalized <= parse_hhmm(midday_end, "13:30"):
        return "MIDDAY_BLOCKED"
    if allow_afternoon_session and parse_hhmm(afternoon_start, "13:45") <= normalized <= parse_hhmm(afternoon_end, "15:00"):
        return "AFTERNOON"
    return ""


def session_allowed(
    current: datetime | time,
    *,
    morning_start: str = "09:20",
    morning_end: str = "11:30",
    midday_start: str = "12:00",
    midday_end: str = "13:30",
    allow_afternoon_session: bool = False,
    afternoon_start: str = "13:45",
    afternoon_end: str = "15:00",
) -> bool:
    return session_window(
        current,
        morning_start=morning_start,
        morning_end=morning_end,
        midday_start=midday_start,
        midday_end=midday_end,
        allow_afternoon_session=allow_afternoon_session,
        afternoon_start=afternoon_start,
        afternoon_end=afternoon_end,
    ) in {"MORNING", "AFTERNOON"}
