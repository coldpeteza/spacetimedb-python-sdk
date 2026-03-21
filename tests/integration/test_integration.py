"""
Integration tests for the SpacetimeDB Python SDK.

These tests require a live SpacetimeDB server with the chat module published.
They are NOT run as part of the normal unit test suite — see the dedicated
.github/workflows/integration.yml workflow.

Environment variables:
  SPACETIMEDB_HOST    host:port of the server  (default: localhost:3000)
  SPACETIMEDB_MODULE  module name              (default: chat)

Run locally (requires a running server):
  spacetime start &
  spacetime publish --server http://localhost:3000 \
      --project-path tests/integration/module --yes chat
  PYTHONPATH=src pytest tests/integration/ -v
"""

import asyncio
import os
import sys

import pytest

# Make the SDK importable when running from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

# Make the integration module_bindings importable
sys.path.insert(0, os.path.dirname(__file__))

import module_bindings  # noqa: E402
from module_bindings.message import Message  # noqa: E402
from module_bindings.user import User  # noqa: E402
import module_bindings.send_message_reducer as send_message_reducer  # noqa: E402
import module_bindings.set_name_reducer as set_name_reducer  # noqa: E402
from spacetimedb_sdk.spacetimedb_async_client import (  # noqa: E402
    SpacetimeDBAsyncClient,
    SpacetimeDBException,
)

HOST = os.environ.get("SPACETIMEDB_HOST", "localhost:3000")
MODULE = os.environ.get("SPACETIMEDB_MODULE", "chat")
TIMEOUT = float(os.environ.get("SPACETIMEDB_TIMEOUT", "15"))

pytestmark = pytest.mark.asyncio


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_client() -> SpacetimeDBAsyncClient:
    return SpacetimeDBAsyncClient(module_bindings)


async def _connect(client: SpacetimeDBAsyncClient, queries=None):
    """Connect to the server, optionally subscribing to *queries*."""
    return await asyncio.wait_for(
        client.connect(
            None,  # auth_token — let the server assign a new identity
            HOST,
            MODULE,
            ssl_enabled=False,
            subscription_queries=queries or [],
        ),
        timeout=TIMEOUT,
    )


# ── Test 1: connection ─────────────────────────────────────────────────────────


async def test_connect_receives_identity():
    """SDK can open a connection and receive a valid identity from the server."""
    client = _make_client()
    try:
        token, identity = await _connect(client)

        assert token is not None, "Expected an auth token from the server"
        assert identity is not None, "Expected an identity from the server"
        # Identity is a 32-byte value → 64-character hex string
        assert len(str(identity)) == 64, f"Unexpected identity length: {str(identity)!r}"
    finally:
        await client.close()


# ── Test 2: subscription applied ──────────────────────────────────────────────


async def test_subscription_applied_fires():
    """on_subscription_applied callback is invoked after the initial snapshot."""
    client = _make_client()
    applied = asyncio.Event()

    client.register_on_subscription_applied(applied.set)

    try:
        await _connect(client, queries=["SELECT * FROM User", "SELECT * FROM Message"])

        await asyncio.wait_for(applied.wait(), timeout=TIMEOUT)
        assert applied.is_set()
    finally:
        await client.close()


# ── Test 3: send_message reducer ──────────────────────────────────────────────


async def test_send_message_reducer_committed():
    """send_message reducer call returns a committed ReducerEvent."""
    client = _make_client()
    received_texts = []
    applied = asyncio.Event()

    def on_message_row(row_op, old, new, reducer_event):
        if row_op == "insert" and new is not None:
            received_texts.append(new.text)

    Message.register_row_update(on_message_row)
    client.register_on_subscription_applied(applied.set)

    try:
        await _connect(client, queries=["SELECT * FROM Message"])
        await asyncio.wait_for(applied.wait(), timeout=TIMEOUT)

        # Call the reducer and wait for the server's response
        event = await asyncio.wait_for(
            client.call_reducer("send_message", "hello from integration test"),
            timeout=TIMEOUT,
        )

        assert event.status == "committed", (
            f"Expected status 'committed', got {event.status!r}: {event.message}"
        )

        # Give the subscription update a moment to arrive
        await asyncio.sleep(0.5)
        assert "hello from integration test" in received_texts, (
            f"Message not received in subscription update; got: {received_texts}"
        )
    finally:
        await client.close()


# ── Test 4: set_name reducer ──────────────────────────────────────────────────


async def test_set_name_reducer_committed():
    """set_name reducer updates the user row visible via the cache."""
    client = _make_client()
    applied = asyncio.Event()
    client.register_on_subscription_applied(applied.set)

    try:
        _, identity = await _connect(
            client, queries=["SELECT * FROM User"]
        )
        await asyncio.wait_for(applied.wait(), timeout=TIMEOUT)

        event = await asyncio.wait_for(
            client.call_reducer("set_name", "Alice"),
            timeout=TIMEOUT,
        )
        assert event.status == "committed", (
            f"Expected status 'committed', got {event.status!r}: {event.message}"
        )

        # Allow the subscription update to propagate
        await asyncio.sleep(0.5)
        user = User.filter_by_identity(identity)
        assert user is not None, "User row not found in local cache after set_name"
        assert user.name == "Alice", f"Expected name 'Alice', got {user.name!r}"
    finally:
        await client.close()


# ── Test 5: invalid reducer call returns failed status ────────────────────────


async def test_send_empty_message_fails():
    """send_message with an empty string is rejected by the module with 'failed'."""
    client = _make_client()
    try:
        await _connect(client)

        event = await asyncio.wait_for(
            client.call_reducer("send_message", ""),
            timeout=TIMEOUT,
        )
        assert event.status == "failed", (
            f"Expected status 'failed' for empty message, got {event.status!r}"
        )
    finally:
        await client.close()
