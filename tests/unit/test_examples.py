"""
Unit tests for Phase 5: updated example module bindings.

Covers:
  - Reducer bindings expose then= parameter and no longer have register_on_*
  - send_message / set_name forward then= to _reducer_call
  - SystemNotification is marked as an event table
  - SystemNotification __init__ populates level and message fields
  - main.py uses local_config.get_token / set_token (verified via inspection)
"""

import sys
import os
import inspect
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Add the examples directory so module_bindings is importable
EXAMPLES_DIR = os.path.join(
    os.path.dirname(__file__), '..', '..',
    'examples', 'quickstart', 'client',
)
sys.path.insert(0, EXAMPLES_DIR)


# ── reducer binding: send_message ──────────────────────────────────────────────

class TestSendMessageReducer:

    def setup_method(self):
        import importlib, module_bindings.send_message_reducer as m
        importlib.reload(m)
        self.mod = m

    def test_no_register_on_send_message(self):
        assert not hasattr(self.mod, "register_on_send_message"), (
            "register_on_send_message must be removed in 2.0 bindings"
        )

    def test_send_message_accepts_then_kwarg(self):
        sig = inspect.signature(self.mod.send_message)
        assert "then" in sig.parameters

    def test_send_message_then_defaults_to_none(self):
        sig = inspect.signature(self.mod.send_message)
        assert sig.parameters["then"].default is None

    def test_send_message_forwards_then_to_reducer_call(self):
        mock_instance = MagicMock()
        with patch(
            'spacetimedb_sdk.spacetimedb_client.SpacetimeDBClient.instance',
            new=mock_instance,
        ):
            cb = MagicMock()
            self.mod.send_message("hello", then=cb)
            mock_instance._reducer_call.assert_called_once_with(
                "send_message", "hello", then=cb
            )

    def test_send_message_without_then_passes_none(self):
        mock_instance = MagicMock()
        with patch(
            'spacetimedb_sdk.spacetimedb_client.SpacetimeDBClient.instance',
            new=mock_instance,
        ):
            self.mod.send_message("hello")
            mock_instance._reducer_call.assert_called_once_with(
                "send_message", "hello", then=None
            )

    def test_reducer_name_constant(self):
        assert self.mod.reducer_name == "send_message"

    def test_decode_args(self):
        assert self.mod._decode_args(["world"]) == ["world"]


# ── reducer binding: set_name ──────────────────────────────────────────────────

class TestSetNameReducer:

    def setup_method(self):
        import importlib, module_bindings.set_name_reducer as m
        importlib.reload(m)
        self.mod = m

    def test_no_register_on_set_name(self):
        assert not hasattr(self.mod, "register_on_set_name"), (
            "register_on_set_name must be removed in 2.0 bindings"
        )

    def test_set_name_accepts_then_kwarg(self):
        sig = inspect.signature(self.mod.set_name)
        assert "then" in sig.parameters

    def test_set_name_then_defaults_to_none(self):
        sig = inspect.signature(self.mod.set_name)
        assert sig.parameters["then"].default is None

    def test_set_name_forwards_then_to_reducer_call(self):
        mock_instance = MagicMock()
        with patch(
            'spacetimedb_sdk.spacetimedb_client.SpacetimeDBClient.instance',
            new=mock_instance,
        ):
            cb = MagicMock()
            self.mod.set_name("Alice", then=cb)
            mock_instance._reducer_call.assert_called_once_with(
                "set_name", "Alice", then=cb
            )

    def test_reducer_name_constant(self):
        assert self.mod.reducer_name == "set_name"

    def test_decode_args(self):
        assert self.mod._decode_args(["Bob"]) == ["Bob"]


# ── event table: SystemNotification ───────────────────────────────────────────

class TestSystemNotification:

    def setup_method(self):
        import importlib, module_bindings.system_notification as m
        importlib.reload(m)
        self.mod = m
        self.cls = m.SystemNotification

    def test_is_table_class(self):
        assert self.cls.is_table_class is True

    def test_is_event_table(self):
        assert self.cls.is_event_table is True, (
            "SystemNotification must be marked as an event table"
        )

    def test_init_populates_level_and_message(self):
        obj = self.cls(["warning", "Server restarting in 5 minutes"])
        assert obj.level == "warning"
        assert obj.message == "Server restarting in 5 minutes"

    def test_getattr_for_unknown_key_returns_none(self):
        obj = self.cls(["info", "Hello"])
        assert obj.nonexistent is None

    def test_register_row_update_calls_sdk(self):
        mock_instance = MagicMock()
        cb = MagicMock()
        with patch(
            'spacetimedb_sdk.spacetimedb_client.SpacetimeDBClient.instance',
            new=mock_instance,
        ):
            self.cls.register_row_update(cb)
            mock_instance._register_row_update.assert_called_once_with(
                "SystemNotification", cb
            )


# ── main.py uses updated local_config API ─────────────────────────────────────

class TestMainUsesNewLocalConfigAPI:
    """Inspect main.py source to confirm it uses the new token helpers."""

    def _main_source(self):
        main_path = os.path.join(EXAMPLES_DIR, "main.py")
        with open(main_path) as f:
            return f.read()

    def test_uses_get_token_not_get_string(self):
        src = self._main_source()
        assert "local_config.get_token()" in src
        assert 'get_string("auth_token")' not in src

    def test_uses_set_token_not_set_string(self):
        src = self._main_source()
        assert "local_config.set_token(" in src
        assert 'set_string("auth_token"' not in src

    def test_no_register_on_reducer_calls(self):
        src = self._main_source()
        assert "register_on_send_message" not in src
        assert "register_on_set_name" not in src

    def test_imports_system_notification(self):
        src = self._main_source()
        assert "SystemNotification" in src
