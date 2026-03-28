from __future__ import annotations

NSE_MARKET_OPEN = '09:15'
NSE_MARKET_CLOSE = '15:30'

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
    morning_start: str = "09:25",
    morning_end: str = "11:15",
    midday_start: str = "11:16",
    midday_end: str = "13:45",
    allow_afternoon_session: bool = False,
    afternoon_start: str = "13:46",
    afternoon_end: str = "14:45",
) -> str:
    current_time = current if isinstance(current, time) else current.time()
    normalized = current_time.replace(second=0, microsecond=0)
    market_open = parse_hhmm(NSE_MARKET_OPEN, "09:15")
    market_close = parse_hhmm(NSE_MARKET_CLOSE, "15:30")
    morning_start_time = parse_hhmm(morning_start, "09:25")
    morning_end_time = parse_hhmm(morning_end, "11:15")
    midday_start_time = parse_hhmm(midday_start, "11:16")
    midday_end_time = parse_hhmm(midday_end, "13:45")
    afternoon_start_time = parse_hhmm(afternoon_start, "13:46")
    afternoon_end_time = parse_hhmm(afternoon_end, "14:45")

    if normalized < market_open or normalized > market_close:
        return "OUTSIDE_MARKET"
    if market_open <= normalized < morning_start_time:
        return "OPENING_BUFFER"
    if morning_start_time <= normalized <= morning_end_time:
        return "MORNING"
    if morning_end_time < normalized < midday_start_time:
        return "POST_MORNING_BLOCKED"
    if midday_start_time <= normalized <= midday_end_time:
        return "MIDDAY_BLOCKED"
    if allow_afternoon_session and afternoon_start_time <= normalized <= afternoon_end_time:
        return "AFTERNOON"
    if normalized <= market_close:
        return "CLOSING_BLOCKED"
    return "OUTSIDE_MARKET"


def session_allowed(
    current: datetime | time,
    *,
    morning_start: str = "09:25",
    morning_end: str = "11:15",
    midday_start: str = "11:16",
    midday_end: str = "13:45",
    allow_afternoon_session: bool = False,
    afternoon_start: str = "13:46",
    afternoon_end: str = "14:45",
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

