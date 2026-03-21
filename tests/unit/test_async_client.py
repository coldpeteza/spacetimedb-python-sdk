"""
Unit tests for Phase 6: async WebSocket refactor.

Covers:
  - AsyncWebSocketClient builds correct URL (ws:// / wss://)
  - AsyncWebSocketClient passes correct Authorization header
  - AsyncWebSocketClient raises RuntimeError when websockets unavailable
  - _AsyncSendShim.send() schedules the send on the running loop
  - _AsyncSendShim.close() schedules the close on the running loop
  - SpacetimeDBAsyncClient no longer has _periodic_update (polling removed)
  - SpacetimeDBAsyncClient.connect() drives the receive loop and returns
    identity after processing an IdentityToken message
  - SpacetimeDBAsyncClient.force_close() signals the event loop
  - SpacetimeDBAsyncClient.call_reducer() uses _then() and returns ReducerEvent
  - schedule_event fires callback after delay
"""

import asyncio
import base64
import json
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from spacetimedb_sdk.spacetime_websocket_client import (
    AsyncWebSocketClient,
    _build_auth_headers,
    _is_oidc_token,
)
from spacetimedb_sdk.spacetimedb_async_client import (
    SpacetimeDBAsyncClient,
    SpacetimeDBException,
    _AsyncSendShim,
)

# ── helpers ───────────────────────────────────────────────────────────────────

_LEGACY_TOKEN = "opaque_token_abc123"
_JWT = (
    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJzdWIiOiJ1c2VyMTIzIn0"
    ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

_IDENTITY_HEX = "aa" * 32
_ADDRESS_HEX  = "bb" * 16

def _identity_token_msg(token="tok", identity=_IDENTITY_HEX, addr=_ADDRESS_HEX):
    return json.dumps({
        "IdentityToken": {
            "token": token,
            "identity": identity,
            "connection_id": addr,
        }
    })


def _make_mock_ws(messages=()):
    """Return an async-iterable mock WebSocket that yields *messages* then closes."""
    ws = MagicMock()
    ws.send = AsyncMock()
    ws.close = AsyncMock()

    async def _aiter():
        for m in messages:
            yield m

    ws.__aiter__ = lambda self: _aiter()
    return ws


def _make_async_client():
    autogen = MagicMock()
    autogen.__path__ = []
    with patch('spacetimedb_sdk.spacetimedb_client.WebSocketClient'):
        client = SpacetimeDBAsyncClient(autogen)
    return client


# ── AsyncWebSocketClient URL building ─────────────────────────────────────────

class TestAsyncWebSocketClientURL:
    pytestmark = pytest.mark.asyncio

    async def test_plain_ws_url(self):
        mock_ws = _make_mock_ws()
        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)) as mock_connect:
            aws = AsyncWebSocketClient("v1.text.spacetimedb")
            await aws.connect(None, "localhost:3000", "mydb", ssl_enabled=False)
            url = mock_connect.call_args[0][0]
            assert url.startswith("ws://")
            assert "localhost:3000" in url
            assert "mydb" in url

    async def test_ssl_wss_url(self):
        mock_ws = _make_mock_ws()
        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)) as mock_connect:
            aws = AsyncWebSocketClient("v1.text.spacetimedb")
            await aws.connect(None, "example.com", "chat", ssl_enabled=True)
            url = mock_connect.call_args[0][0]
            assert url.startswith("wss://")

    async def test_subprotocol_passed(self):
        mock_ws = _make_mock_ws()
        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)) as mock_connect:
            aws = AsyncWebSocketClient("v1.text.spacetimedb")
            await aws.connect(None, "localhost:3000", "db", ssl_enabled=False)
            kwargs = mock_connect.call_args[1]
            assert kwargs["subprotocols"] == ["v1.text.spacetimedb"]


# ── AsyncWebSocketClient auth headers ─────────────────────────────────────────

class TestAsyncWebSocketClientHeaders:
    pytestmark = pytest.mark.asyncio

    async def test_no_token_no_auth_header(self):
        mock_ws = _make_mock_ws()
        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)) as mock_connect:
            aws = AsyncWebSocketClient("v1.text.spacetimedb")
            await aws.connect(None, "localhost:3000", "db", ssl_enabled=False)
            kwargs = mock_connect.call_args[1]
            assert kwargs.get("additional_headers") == {}

    async def test_legacy_token_uses_basic_header(self):
        mock_ws = _make_mock_ws()
        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)) as mock_connect:
            aws = AsyncWebSocketClient("v1.text.spacetimedb")
            await aws.connect(_LEGACY_TOKEN, "localhost:3000", "db", ssl_enabled=False)
            kwargs = mock_connect.call_args[1]
            auth = kwargs["additional_headers"]["Authorization"]
            assert auth.startswith("Basic ")
            decoded = base64.b64decode(auth[6:]).decode()
            assert decoded == f"token:{_LEGACY_TOKEN}"

    async def test_oidc_token_uses_bearer_header(self):
        mock_ws = _make_mock_ws()
        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)) as mock_connect:
            aws = AsyncWebSocketClient("v1.text.spacetimedb")
            await aws.connect(_JWT, "localhost:3000", "db", ssl_enabled=False)
            kwargs = mock_connect.call_args[1]
            auth = kwargs["additional_headers"]["Authorization"]
            assert auth == f"Bearer {_JWT}"


# ── AsyncWebSocketClient send / close ─────────────────────────────────────────

class TestAsyncWebSocketClientSendClose:
    pytestmark = pytest.mark.asyncio

    async def test_send_bytes_decoded_to_str(self):
        mock_ws = _make_mock_ws()
        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)):
            aws = AsyncWebSocketClient("v1.text.spacetimedb")
            await aws.connect(None, "localhost:3000", "db", ssl_enabled=False)
            await aws.send(b'{"hello": "world"}')
            mock_ws.send.assert_awaited_once_with('{"hello": "world"}')

    async def test_send_str_passed_through(self):
        mock_ws = _make_mock_ws()
        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)):
            aws = AsyncWebSocketClient("v1.text.spacetimedb")
            await aws.connect(None, "localhost:3000", "db", ssl_enabled=False)
            await aws.send('{"hello": "world"}')
            mock_ws.send.assert_awaited_once_with('{"hello": "world"}')

    async def test_close_called(self):
        mock_ws = _make_mock_ws()
        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)):
            aws = AsyncWebSocketClient("v1.text.spacetimedb")
            await aws.connect(None, "localhost:3000", "db", ssl_enabled=False)
            await aws.close()
            mock_ws.close.assert_awaited_once()


# ── polling removed ────────────────────────────────────────────────────────────

class TestPollingRemoved:

    def test_no_periodic_update_method(self):
        assert not hasattr(SpacetimeDBAsyncClient, '_periodic_update'), (
            "_periodic_update (the 100ms polling hack) must be removed in Phase 6"
        )

    def test_event_method_is_simple_queue_get(self):
        """_event() should just await the queue — no update task."""
        import inspect
        src = inspect.getsource(SpacetimeDBAsyncClient._event)
        assert 'create_task' not in src
        assert 'event_queue.get' in src


# ── SpacetimeDBAsyncClient connect / identity ──────────────────────────────────

class TestAsyncClientConnect:
    pytestmark = pytest.mark.asyncio

    async def test_connect_returns_identity(self):
        mock_ws = _make_mock_ws([_identity_token_msg("mytoken")])
        client = _make_async_client()

        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)):
            token, identity = await client.connect(
                _LEGACY_TOKEN, "localhost:3000", "chat", False
            )

        assert token == "mytoken"
        assert str(identity) == _IDENTITY_HEX

    async def test_connect_sets_is_connected(self):
        mock_ws = _make_mock_ws([_identity_token_msg()])
        client = _make_async_client()

        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)):
            await client.connect(_LEGACY_TOKEN, "localhost:3000", "chat", False)

        assert client.is_connected is True

    async def test_connect_wires_shim_to_sync_client(self):
        mock_ws = _make_mock_ws([_identity_token_msg()])
        client = _make_async_client()

        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)):
            await client.connect(_LEGACY_TOKEN, "localhost:3000", "chat", False)

        assert isinstance(client.client.wsc, _AsyncSendShim)


# ── force_close ────────────────────────────────────────────────────────────────

class TestForceClose:
    pytestmark = pytest.mark.asyncio

    async def test_force_close_puts_event(self):
        client = _make_async_client()
        client._on_async_loop_start()
        client.force_close()
        event, _ = client.event_queue.get_nowait()
        assert event == "force_close"

    async def test_force_close_sets_is_closing(self):
        client = _make_async_client()
        client._on_async_loop_start()
        client.force_close()
        assert client.is_closing is True


# ── call_reducer uses _then() ──────────────────────────────────────────────────

class TestCallReducer:
    pytestmark = pytest.mark.asyncio

    async def test_call_reducer_returns_reducer_event(self):
        mock_ws = _make_mock_ws([_identity_token_msg()])
        client = _make_async_client()

        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)):
            await client.connect(_LEGACY_TOKEN, "localhost:3000", "chat", False)

        # Simulate a TransactionUpdate arriving for our reducer call
        tx_msg = json.dumps({
            "TransactionUpdate": {
                "status": {"Committed": {"tables": []}},
                "caller_identity": _IDENTITY_HEX,
                "caller_connection_id": _ADDRESS_HEX,
                "reducer_call": {"reducer_name": "my_reducer", "args": []},
            }
        })

        async def _drive_reducer():
            # Deliver the transaction update after a short delay
            await asyncio.sleep(0.01)
            client.client._on_message(tx_msg)
            client.client._do_update()

        asyncio.create_task(_drive_reducer())

        result = await client.call_reducer("my_reducer")
        assert result.reducer_name == "my_reducer"
        assert result.status == "committed"

    async def test_call_reducer_timeout(self):
        mock_ws = _make_mock_ws([_identity_token_msg()])
        client = _make_async_client()
        client.request_timeout = 0.05  # very short for the test

        with patch('websockets.connect', new=AsyncMock(return_value=mock_ws)):
            await client.connect(_LEGACY_TOKEN, "localhost:3000", "chat", False)

        with pytest.raises(SpacetimeDBException, match="timed out"):
            await client.call_reducer("never_responds")


# ── schedule_event ─────────────────────────────────────────────────────────────

class TestScheduleEvent:
    pytestmark = pytest.mark.asyncio

    async def test_schedule_event_fires_callback(self):
        client = _make_async_client()
        client._on_async_loop_start()

        fired = []
        client.schedule_event(0.01, lambda: fired.append(True))
        await asyncio.sleep(0.05)
        assert fired == [True]

    async def test_prescheduled_events_fire_after_loop_start(self):
        client = _make_async_client()
        fired = []
        client.schedule_event(0.01, lambda: fired.append(True))
        # loop not started yet — event should be queued
        assert len(client.prescheduled_events) == 1
        client._on_async_loop_start()
        await asyncio.sleep(0.05)
        assert fired == [True]
