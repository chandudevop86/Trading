from __future__ import annotations


class DomainValidationError(ValueError):
    """Base class for canonical vinayak domain validation failures."""


class DataNormalizationError(DomainValidationError):
    """Raised when OHLCV rows cannot be normalized safely."""


class MissingRequiredColumnError(DataNormalizationError):
    """Raised when canonical OHLCV columns are missing after normalization."""


class TimestampParseError(DataNormalizationError):
    """Raised when timestamps cannot be parsed into UTC-safe values."""
