"""
Unit tests for Phase 3: Reducer API overhaul and Event Tables support.

Covers:
  - _reducer_call() stores _then() callback in _pending_then queue
  - _then() callback is invoked (FIFO) when TransactionUpdate arrives
  - No callback stored when then=None
  - Multiple in-flight calls to the same reducer use FIFO ordering
  - Event table rows fire row_update callbacks but are NOT persisted in cache
  - Normal table rows are persisted as before
"""

import json
import sys
import os
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from spacetimedb_sdk.spacetimedb_client import (
    SpacetimeDBClient,
    TransactionUpdateMessage,
    TransactionUpdateLightMessage,
    _SubscriptionUpdateMessage,
    Identity,
    Address,
    ReducerEvent,
)
from spacetimedb_sdk.client_cache import ClientCache, TableCache


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_client():
    """Return a SpacetimeDBClient with a mocked WebSocket and empty autogen."""
    autogen = MagicMock()
    autogen.__path__ = []
    with patch('spacetimedb_sdk.spacetimedb_client.WebSocketClient'):
        client = SpacetimeDBClient(autogen)
    # Attach a mock wsc so _reducer_call() can check is_connected and send()
    client.wsc = MagicMock()
    client.wsc.is_connected = True
    client.client_cache.decode = MagicMock(side_effect=lambda _table, row: row)
    return client


def _make_transaction_update_msg(reducer_name="my_reducer", status="committed"):
    identity_hex = "aa" * 32
    address_hex = "bb" * 16
    return TransactionUpdateMessage(
        Identity.from_string(identity_hex),
        Address.from_string(address_hex),
        status,
        "",
        reducer_name,
        [],
    )


def _v2_transaction_update_json(reducer_name="my_reducer", tables=None):
    """Build a raw v2 TransactionUpdate JSON dict."""
    tables = tables or []
    return json.dumps({
        "TransactionUpdate": {
            "status": {"Committed": {"tables": tables}},
            "caller_identity": "aa" * 32,
            "caller_connection_id": "bb" * 16,
            "reducer_call": {"reducer_name": reducer_name, "args": []},
        }
    })


# ── _reducer_call / _then() tests ─────────────────────────────────────────────

class TestReducerCallThen:

    def test_no_callback_when_then_is_none(self):
        client = _make_client()
        client._reducer_call("my_reducer", "arg1", then=None)
        assert len(client._pending_then["my_reducer"]) == 0

    def test_callback_stored_when_then_provided(self):
        client = _make_client()
        cb = MagicMock()
        client._reducer_call("my_reducer", "arg1", then=cb)
        assert len(client._pending_then["my_reducer"]) == 1
        assert client._pending_then["my_reducer"][0] is cb

    def test_multiple_callbacks_stored_in_fifo_order(self):
        client = _make_client()
        cb1, cb2, cb3 = MagicMock(), MagicMock(), MagicMock()
        client._reducer_call("my_reducer", then=cb1)
        client._reducer_call("my_reducer", then=cb2)
        client._reducer_call("my_reducer", then=cb3)
        q = client._pending_then["my_reducer"]
        assert list(q) == [cb1, cb2, cb3]

    def test_callbacks_for_different_reducers_are_independent(self):
        client = _make_client()
        cb_a = MagicMock()
        cb_b = MagicMock()
        client._reducer_call("reducer_a", then=cb_a)
        client._reducer_call("reducer_b", then=cb_b)
        assert len(client._pending_then["reducer_a"]) == 1
        assert len(client._pending_then["reducer_b"]) == 1


# ── _then() dispatch via _do_update() ─────────────────────────────────────────

class TestThenDispatch:

    def test_then_called_on_transaction_update(self):
        client = _make_client()
        cb = MagicMock()
        client._reducer_call("my_reducer", then=cb)

        # Simulate the server response arriving
        client._on_message(_v2_transaction_update_json("my_reducer"))
        client._do_update()

        cb.assert_called_once()
        reducer_event = cb.call_args[0][0]
        assert isinstance(reducer_event, ReducerEvent)
        assert reducer_event.reducer_name == "my_reducer"

    def test_then_not_called_when_no_callback_registered(self):
        client = _make_client()
        # No _reducer_call() made, so no pending callback
        on_event = MagicMock()
        client.register_on_event(on_event)

        client._on_message(_v2_transaction_update_json("my_reducer"))
        client._do_update()

        # _on_event fires, but no _then() was registered
        on_event.assert_called_once()
        assert len(client._pending_then["my_reducer"]) == 0

    def test_then_callbacks_consumed_in_fifo_order(self):
        client = _make_client()
        results = []
        cb1 = lambda evt: results.append(("cb1", evt.status))
        cb2 = lambda evt: results.append(("cb2", evt.status))

        client._reducer_call("my_reducer", then=cb1)
        client._reducer_call("my_reducer", then=cb2)

        client._on_message(_v2_transaction_update_json("my_reducer"))
        client._do_update()
        client._on_message(_v2_transaction_update_json("my_reducer"))
        client._do_update()

        assert results == [("cb1", "committed"), ("cb2", "committed")]

    def test_then_not_called_for_different_reducer(self):
        client = _make_client()
        cb = MagicMock()
        client._reducer_call("reducer_a", then=cb)

        # TransactionUpdate for a different reducer arrives
        client._on_message(_v2_transaction_update_json("reducer_b"))
        client._do_update()

        cb.assert_not_called()
        assert len(client._pending_then["reducer_a"]) == 1  # still pending

    def test_then_queue_empty_after_consumption(self):
        client = _make_client()
        cb = MagicMock()
        client._reducer_call("my_reducer", then=cb)
        client._on_message(_v2_transaction_update_json("my_reducer"))
        client._do_update()
        assert len(client._pending_then["my_reducer"]) == 0


# ── Event Table support ────────────────────────────────────────────────────────

def _make_event_table_class(name="EventAlert"):
    """Return a minimal table class marked as an event table."""
    cls = type(name, (), {
        "is_table_class": True,
        "is_event_table": True,
    })
    cls.__init__ = lambda self, data: setattr(self, "data", data)
    return cls


def _make_normal_table_class(name="NormalRow"):
    cls = type(name, (), {
        "is_table_class": True,
        "is_event_table": False,
        "primary_key": None,
    })
    cls.__init__ = lambda self, data: setattr(self, "data", data)
    return cls


class TestEventTables:

    def test_table_cache_marks_event_table(self):
        EventCls = _make_event_table_class()
        tc = TableCache(EventCls, is_event_table=True)
        assert tc.is_event_table is True

    def test_table_cache_normal_table_not_event(self):
        NormalCls = _make_normal_table_class()
        tc = TableCache(NormalCls)
        assert tc.is_event_table is False

    def test_client_cache_detects_event_table_flag(self):
        """ClientCache should pick up is_event_table from the class attribute."""
        EventCls = _make_event_table_class("Alert")
        autogen = MagicMock()
        autogen.__path__ = ["/fake"]
        autogen.__name__ = "fake_autogen"

        import pkgutil
        fake_module = MagicMock()
        fake_module.Alert = EventCls

        with patch('pkgutil.iter_modules', return_value=[
            (None, "alert", False)
        ]):
            with patch('importlib.import_module', return_value=fake_module):
                cache = ClientCache(autogen)

        assert cache.is_event_table("Alert") is True

    def test_event_table_rows_not_persisted(self):
        """Rows from event tables must not be stored in the client cache."""
        client = _make_client()

        # Inject an event table cache entry directly
        EventCls = _make_event_table_class("Alert")
        from spacetimedb_sdk.client_cache import TableCache
        event_tc = TableCache(EventCls, is_event_table=True)
        client.client_cache.tables["Alert"] = event_tc

        # Override decode to return a recognisable object
        sentinel = object()
        client.client_cache.decode = MagicMock(return_value=sentinel)

        # Simulate a TransactionUpdate with an insert into the event table
        msg = {
            "TransactionUpdate": {
                "status": {"Committed": {"tables": [{
                    "table_name": "Alert",
                    "updates": [{"inserts": [{"id": 1}], "deletes": []}],
                }]}},
                "caller_identity": "aa" * 32,
                "caller_connection_id": "bb" * 16,
                "reducer_call": {"reducer_name": "fire_alert", "args": []},
            }
        }
        client._on_message(json.dumps(msg))
        client._do_update()

        # The event table cache must remain empty
        assert len(event_tc.entries) == 0

    def test_event_table_row_update_callbacks_still_fired(self):
        """Row-update callbacks for event tables must still be called."""
        client = _make_client()

        EventCls = _make_event_table_class("Alert")
        from spacetimedb_sdk.client_cache import TableCache
        event_tc = TableCache(EventCls, is_event_table=True)
        client.client_cache.tables["Alert"] = event_tc

        row_data = {"id": 42}
        client.client_cache.decode = MagicMock(return_value=row_data)

        row_cb = MagicMock()
        client._register_row_update("Alert", row_cb)

        msg = {
            "TransactionUpdate": {
                "status": {"Committed": {"tables": [{
                    "table_name": "Alert",
                    "updates": [{"inserts": [row_data], "deletes": []}],
                }]}},
                "caller_identity": "aa" * 32,
                "caller_connection_id": "bb" * 16,
                "reducer_call": {"reducer_name": "fire_alert", "args": []},
            }
        }
        client._on_message(json.dumps(msg))
        client._do_update()

        row_cb.assert_called_once()
        call_args = row_cb.call_args[0]
        assert call_args[0] == "insert"

    def test_normal_table_rows_still_persisted(self):
        """Sanity check: normal table rows must still be written to the cache."""
        client = _make_client()

        NormalCls = _make_normal_table_class("Widget")
        from spacetimedb_sdk.client_cache import TableCache
        normal_tc = TableCache(NormalCls, is_event_table=False)
        client.client_cache.tables["Widget"] = normal_tc

        row_data = {"id": 7}
        decoded = MagicMock()
        decoded.data = {"id": 7}
        client.client_cache.decode = MagicMock(return_value=decoded)

        msg = {
            "TransactionUpdate": {
                "status": {"Committed": {"tables": [{
                    "table_name": "Widget",
                    "updates": [{"inserts": [row_data], "deletes": []}],
                }]}},
                "caller_identity": "aa" * 32,
                "caller_connection_id": "bb" * 16,
                "reducer_call": {"reducer_name": "create_widget", "args": []},
            }
        }
        client._on_message(json.dumps(msg))
        client._do_update()

        assert len(normal_tc.entries) == 1
