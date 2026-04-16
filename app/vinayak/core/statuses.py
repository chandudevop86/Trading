from __future__ import annotations

"""Centralized status constants for Vinayak workflow modules."""


class ValidationStatus:
    PASS = "PASS"
    FAIL = "FAIL"
    PENDING = "PENDING"


class ExecutionStatus:
    FILLED = "FILLED"
    EXECUTED = "EXECUTED"
    BLOCKED = "BLOCKED"
    ERROR = "ERROR"
    ACCEPTED = "ACCEPTED"
    SENT = "SENT"


class ReadinessStatus:
    READY = "READY"
    PAPER_ONLY = "PAPER_ONLY"
    NOT_READY = "NOT_READY"


class HealthStatus:
    OK = "OK"
    WARN = "WARN"
    FAIL = "FAIL"
    STALE = "STALE"
    FRESH = "FRESH"
    DEGRADED = "DEGRADED"
    UP = "UP"
    DOWN = "DOWN"


__all__ = [
    "ExecutionStatus",
    "HealthStatus",
    "ReadinessStatus",
    "ValidationStatus",
]
