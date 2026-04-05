"""Legacy-safe observability logging facade for the ``src`` package."""

from vinayak.observability.observability_logger import log_event, log_exception

__all__ = ["log_event", "log_exception"]
