from __future__ import annotations


class DomainValidationError(ValueError):
    """Base class for canonical vinayak domain validation failures."""


class DataNormalizationError(DomainValidationError):
    """Raised when OHLCV rows cannot be normalized safely."""


class MissingRequiredColumnError(DataNormalizationError):
    """Raised when canonical OHLCV columns are missing after normalization."""


class TimestampParseError(DataNormalizationError):
    """Raised when timestamps cannot be parsed into UTC-safe values."""


class WorkflowError(DomainValidationError):
    """Base class for workflow/lifecycle validation failures."""


class InvalidStatusTransitionError(WorkflowError):
    """Raised when a lifecycle transition violates the allowed state machine."""


class DuplicateExecutionRequestError(WorkflowError):
    """Raised when a duplicate execution request is detected before persistence."""
