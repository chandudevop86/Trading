from __future__ import annotations

from collections import defaultdict


_COUNTERS: dict[tuple[str, str], float] = defaultdict(float)
_LATENCIES: dict[tuple[str, str], list[float]] = defaultdict(list)


def record_provider_latency(provider: str, seconds: float) -> None:
    _LATENCIES[('provider_latency_seconds', provider)].append(float(seconds))


def record_provider_failure(provider: str) -> None:
    _COUNTERS[('provider_failure_total', provider)] += 1.0


def record_cache_hit(provider: str) -> None:
    _COUNTERS[('cache_hit_total', provider)] += 1.0


def record_cache_miss(provider: str) -> None:
    _COUNTERS[('cache_miss_total', provider)] += 1.0


def record_signal_generated(strategy_name: str) -> None:
    _COUNTERS[('signal_generated_total', strategy_name)] += 1.0


def record_validation_failure(reason: str) -> None:
    _COUNTERS[('validation_failure_total', reason)] += 1.0


def record_execution_latency(mode: str, seconds: float) -> None:
    _LATENCIES[('execution_latency_seconds', mode)].append(float(seconds))


def record_order_outcome(status: str) -> None:
    _COUNTERS[('order_outcome_total', status)] += 1.0


def snapshot_metrics() -> dict[str, object]:
    return {
        'counters': {f'{metric}:{label}': value for (metric, label), value in _COUNTERS.items()},
        'latencies': {f'{metric}:{label}': values[:] for (metric, label), values in _LATENCIES.items()},
    }
