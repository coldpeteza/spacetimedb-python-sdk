"""
Unit tests for Phase 4: OIDC / Bearer token authentication support.

Covers:
  - _is_oidc_token() detection in spacetime_websocket_client
  - _build_auth_headers() selects correct header format
      - No token  → None
      - Legacy    → Basic base64("token:<value>")
      - OIDC/JWT  → Bearer <value>
  - local_config.is_oidc_token() mirrors websocket detection logic
  - local_config.get/set_token() and get/set_refresh_token() round-trips
"""

import base64
import sys
import os
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from spacetimedb_sdk.spacetime_websocket_client import _is_oidc_token, _build_auth_headers
import spacetimedb_sdk.local_config as local_config


# ── sample tokens ─────────────────────────────────────────────────────────────

# Minimal valid-looking JWT: three base64url segments, header starts with eyJ
_JWT = (
    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJzdWIiOiJ1c2VyMTIzIiwiaWF0IjoxNzAwMDAwMDAwfQ"
    ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

# Opaque legacy token (no dots, no eyJ prefix)
_LEGACY_TOKEN = "abc123xyz_opaque_token"


# ── _is_oidc_token ─────────────────────────────────────────────────────────────

class TestIsOidcToken:

    def test_jwt_recognised(self):
        assert _is_oidc_token(_JWT) is True

    def test_legacy_token_not_oidc(self):
        assert _is_oidc_token(_LEGACY_TOKEN) is False

    def test_empty_string_not_oidc(self):
        assert _is_oidc_token("") is False

    def test_none_not_oidc(self):
        assert _is_oidc_token(None) is False

    def test_two_dots_no_eyj_prefix_not_oidc(self):
        assert _is_oidc_token("abc.def.ghi") is False

    def test_eyj_prefix_but_wrong_dot_count_not_oidc(self):
        assert _is_oidc_token("eyJhbGci.only_one_dot") is False

    def test_eyj_prefix_four_dots_not_oidc(self):
        assert _is_oidc_token("eyJa.b.c.d") is False


# ── _build_auth_headers ────────────────────────────────────────────────────────

class TestBuildAuthHeaders:

    def test_no_token_returns_none(self):
        assert _build_auth_headers(None) is None
        assert _build_auth_headers("") is None

    def test_legacy_token_uses_basic_auth(self):
        headers = _build_auth_headers(_LEGACY_TOKEN)
        assert headers is not None
        auth_value = headers["Authorization"]
        assert auth_value.startswith("Basic ")
        # Decode and verify content
        encoded = auth_value[len("Basic "):]
        decoded = base64.b64decode(encoded).decode("utf-8")
        assert decoded == f"token:{_LEGACY_TOKEN}"

    def test_oidc_token_uses_bearer_auth(self):
        headers = _build_auth_headers(_JWT)
        assert headers is not None
        auth_value = headers["Authorization"]
        assert auth_value == f"Bearer {_JWT}"

    def test_basic_header_is_only_key(self):
        headers = _build_auth_headers(_LEGACY_TOKEN)
        assert list(headers.keys()) == ["Authorization"]

    def test_bearer_header_is_only_key(self):
        headers = _build_auth_headers(_JWT)
        assert list(headers.keys()) == ["Authorization"]

    def test_legacy_token_with_special_chars(self):
        token = "tok/en+with=special_chars"
        headers = _build_auth_headers(token)
        encoded = headers["Authorization"][len("Basic "):]
        decoded = base64.b64decode(encoded).decode("utf-8")
        assert decoded == f"token:{token}"


# ── local_config.is_oidc_token ─────────────────────────────────────────────────

class TestLocalConfigIsOidcToken:

    def test_jwt_recognised(self):
        assert local_config.is_oidc_token(_JWT) is True

    def test_legacy_not_oidc(self):
        assert local_config.is_oidc_token(_LEGACY_TOKEN) is False

    def test_none_not_oidc(self):
        assert local_config.is_oidc_token(None) is False

    def test_matches_websocket_client_detection(self):
        """Both modules must agree on token classification."""
        for tok in [_JWT, _LEGACY_TOKEN, "", None, "eyJa.b.c.d", "eyJa.b"]:
            assert local_config.is_oidc_token(tok) == _is_oidc_token(tok), (
                f"Mismatch for token: {tok!r}"
            )


# ── local_config token storage ─────────────────────────────────────────────────

@pytest.fixture()
def tmp_config(tmp_path):
    """Initialise local_config pointing at a temp directory."""
    local_config.init(
        config_root=str(tmp_path),
        config_folder=".test_sdk",
        config_file="settings.ini",
    )
    yield
    # Reset module-level state so tests don't bleed into each other
    local_config.config = None
    local_config.settings_path = None


class TestLocalConfigTokenStorage:

    def test_get_token_returns_none_when_unset(self, tmp_config):
        assert local_config.get_token() is None

    def test_set_and_get_token_roundtrip(self, tmp_config):
        local_config.set_token(_LEGACY_TOKEN)
        assert local_config.get_token() == _LEGACY_TOKEN

    def test_set_and_get_oidc_token_roundtrip(self, tmp_config):
        local_config.set_token(_JWT)
        assert local_config.get_token() == _JWT

    def test_get_refresh_token_returns_none_when_unset(self, tmp_config):
        assert local_config.get_refresh_token() is None

    def test_set_and_get_refresh_token_roundtrip(self, tmp_config):
        refresh = "refresh_abc123"
        local_config.set_refresh_token(refresh)
        assert local_config.get_refresh_token() == refresh

    def test_token_and_refresh_token_stored_independently(self, tmp_config):
        local_config.set_token(_LEGACY_TOKEN)
        local_config.set_refresh_token("my_refresh")
        assert local_config.get_token() == _LEGACY_TOKEN
        assert local_config.get_refresh_token() == "my_refresh"

    def test_overwrite_token(self, tmp_config):
        local_config.set_token("first_token")
        local_config.set_token("second_token")
        assert local_config.get_token() == "second_token"

    def test_token_persisted_to_disk(self, tmp_config):
        """Token written by set_token() must survive a re-read of the file."""
        local_config.set_token(_LEGACY_TOKEN)
        path = local_config.settings_path

        # Reload from disk
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(path)
        assert cfg["main"]["auth_token"] == _LEGACY_TOKEN
