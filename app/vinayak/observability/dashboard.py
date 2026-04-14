from __future__ import annotations


GRAFANA_METRIC_SUGGESTIONS = {
    'signal_generation': ['signal_generated_total:*', 'validation_failure_total:*'],
    'market_data': ['provider_latency_seconds:*', 'provider_failure_total:*', 'cache_hit_total:*', 'cache_miss_total:*'],
    'execution': ['execution_latency_seconds:*', 'order_outcome_total:*'],
}
