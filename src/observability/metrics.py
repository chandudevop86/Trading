"""Legacy-safe observability metrics facade for the ``src`` package."""

from vinayak.observability.observability_metrics import increment_metric, record_stage, set_metric

__all__ = ["increment_metric", "record_stage", "set_metric"]
