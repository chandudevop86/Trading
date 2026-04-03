import unittest
from datetime import UTC, datetime, timedelta

from src.dhan_auth import DhanAuthConfig, DhanAuthManager


def _encode_jwt(expiry: datetime) -> str:
    import base64
    import json

    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode("utf-8")).decode("utf-8").rstrip("=")
    payload = base64.urlsafe_b64encode(json.dumps({"exp": int(expiry.timestamp())}).encode("utf-8")).decode("utf-8").rstrip("=")
    return f"{header}.{payload}.signature"


class TestDhanAuth(unittest.TestCase):
    def test_load_from_env_decodes_jwt_expiry(self):
        expiry = datetime(2026, 3, 24, 15, 0, tzinfo=UTC)
        config = DhanAuthManager.load_from_env(
            {
                "DHAN_CLIENT_ID": "1100",
                "DHAN_ACCESS_TOKEN": _encode_jwt(expiry),
                "DHAN_BASE_URL": "https://api-hq.dhan.co",
                "DHAN_TIMEOUT": "45",
            }
        )
        self.assertEqual(config.client_id, "1100")
        self.assertEqual(config.timeout, 45)
        self.assertEqual(config.access_token_expires_at, expiry)
        self.assertEqual(config.expiry_source, "JWT_EXP")

    def test_load_from_env_prefers_explicit_expiry_metadata(self):
        config = DhanAuthManager.load_from_env(
            {
                "DHAN_CLIENT_ID": "1100",
                "DHAN_ACCESS_TOKEN": "opaque-token",
                "DHAN_ACCESS_TOKEN_EXPIRES_AT": "2026-03-24T15:00:00+00:00",
            }
        )
        self.assertEqual(config.expiry_source, "DHAN_ACCESS_TOKEN_EXPIRES_AT")
        self.assertEqual(config.access_token_expires_at, datetime(2026, 3, 24, 15, 0, tzinfo=UTC))

    def test_validate_startup_fails_for_missing_credentials(self):
        status = DhanAuthManager.validate_startup(
            DhanAuthConfig(client_id="", access_token="", base_url="https://api-hq.dhan.co", timeout=30)
        )
        self.assertFalse(status.ok)
        self.assertIn("Missing DHAN_CLIENT_ID", status.issues)
        self.assertIn("Missing DHAN_ACCESS_TOKEN", status.issues)

    def test_validate_startup_flags_expired_token(self):
        config = DhanAuthConfig(
            client_id="1100",
            access_token="token",
            base_url="https://api-hq.dhan.co",
            timeout=30,
            access_token_expires_at=datetime(2026, 3, 24, 9, 0, tzinfo=UTC),
        )
        status = DhanAuthManager.validate_startup(
            config,
            now=datetime(2026, 3, 24, 9, 5, tzinfo=UTC),
        )
        self.assertFalse(status.ok)
        self.assertTrue(status.is_expired)
        self.assertIn("Dhan access token is expired", status.issues)

    def test_validate_startup_warns_when_token_expires_soon(self):
        config = DhanAuthConfig(
            client_id="1100",
            access_token="token",
            base_url="https://api-hq.dhan.co",
            timeout=30,
            access_token_expires_at=datetime(2026, 3, 24, 10, 3, tzinfo=UTC),
        )
        status = DhanAuthManager.validate_startup(
            config,
            now=datetime(2026, 3, 24, 10, 0, tzinfo=UTC),
            min_validity_seconds=300,
        )
        self.assertTrue(status.ok)
        self.assertFalse(status.is_expired)
        self.assertTrue(any("expires soon" in warning for warning in status.warnings))


if __name__ == "__main__":
    unittest.main()
