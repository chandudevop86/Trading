from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Mapping

DEFAULT_DHAN_API_URL = "https://api-hq.dhan.co"
DEFAULT_DHAN_TIMEOUT = 30
DEFAULT_MIN_TOKEN_VALIDITY_SECONDS = 300


@dataclass(slots=True)
class DhanAuthConfig:
    client_id: str
    access_token: str
    base_url: str
    timeout: int
    access_token_expires_at: datetime | None = None
    token_source: str = "environment"
    expiry_source: str = ""


@dataclass(slots=True)
class DhanStartupValidation:
    ok: bool
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    access_token_expires_at: datetime | None = None
    expires_in_seconds: int | None = None
    is_expired: bool = False


class DhanAuthManager:
    @classmethod
    def load_from_env(cls, env: Mapping[str, str] | None = None) -> DhanAuthConfig:
        source = env if env is not None else os.environ
        client_id = str(source.get("DHAN_CLIENT_ID", "") or "").strip()
        access_token = str(source.get("DHAN_ACCESS_TOKEN", "") or "").strip()
        base_url = str(source.get("DHAN_BASE_URL", DEFAULT_DHAN_API_URL) or DEFAULT_DHAN_API_URL).strip().rstrip("/")
        timeout = cls._safe_int(source.get("DHAN_TIMEOUT", str(DEFAULT_DHAN_TIMEOUT)), default=DEFAULT_DHAN_TIMEOUT)
        access_token_expires_at, expiry_source = cls._resolve_token_expiry(access_token, source)
        return DhanAuthConfig(
            client_id=client_id,
            access_token=access_token,
            base_url=base_url,
            timeout=timeout,
            access_token_expires_at=access_token_expires_at,
            token_source="environment",
            expiry_source=expiry_source,
        )

    @classmethod
    def validate_startup(
        cls,
        config: DhanAuthConfig,
        *,
        now: datetime | None = None,
        min_validity_seconds: int = DEFAULT_MIN_TOKEN_VALIDITY_SECONDS,
    ) -> DhanStartupValidation:
        issues: list[str] = []
        warnings: list[str] = []
        current_time = now.astimezone(UTC) if now is not None else datetime.now(UTC)

        if not config.client_id:
            issues.append("Missing DHAN_CLIENT_ID")
        if not config.access_token:
            issues.append("Missing DHAN_ACCESS_TOKEN")

        expires_in_seconds: int | None = None
        is_expired = False
        if config.access_token and config.access_token_expires_at is None:
            warnings.append("Access token expiry could not be determined")
        elif config.access_token_expires_at is not None:
            expires_in_seconds = int((config.access_token_expires_at - current_time).total_seconds())
            if expires_in_seconds <= 0:
                is_expired = True
                issues.append("Dhan access token is expired")
            elif expires_in_seconds < max(0, int(min_validity_seconds)):
                warnings.append(
                    f"Dhan access token expires soon ({expires_in_seconds} seconds remaining)"
                )

        return DhanStartupValidation(
            ok=not issues,
            issues=issues,
            warnings=warnings,
            access_token_expires_at=config.access_token_expires_at,
            expires_in_seconds=expires_in_seconds,
            is_expired=is_expired,
        )

    @staticmethod
    def _safe_int(value: object, *, default: int) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @classmethod
    def _resolve_token_expiry(
        cls,
        access_token: str,
        env: Mapping[str, str],
    ) -> tuple[datetime | None, str]:
        explicit_keys = (
            "DHAN_ACCESS_TOKEN_EXPIRES_AT",
            "DHAN_ACCESS_TOKEN_EXPIRY",
            "DHAN_TOKEN_EXPIRES_AT",
        )
        for key in explicit_keys:
            raw_value = str(env.get(key, "") or "").strip()
            if not raw_value:
                continue
            parsed = cls._parse_datetime(raw_value)
            if parsed is not None:
                return parsed, key
        decoded = cls._decode_jwt_expiry(access_token)
        if decoded is not None:
            return decoded, "JWT_EXP"
        return None, ""

    @staticmethod
    def _parse_datetime(value: str) -> datetime | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.isdigit():
            try:
                return datetime.fromtimestamp(int(raw), tz=UTC)
            except (OverflowError, OSError, ValueError):
                return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _decode_jwt_expiry(access_token: str) -> datetime | None:
        token = str(access_token or "").strip()
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload = parts[1]
        padding = "=" * (-len(payload) % 4)
        try:
            decoded = base64.urlsafe_b64decode(payload + padding)
            data = json.loads(decoded.decode("utf-8"))
        except Exception:
            return None
        exp_value = data.get("exp")
        try:
            return datetime.fromtimestamp(int(exp_value), tz=UTC)
        except (TypeError, ValueError, OverflowError, OSError):
            return None
