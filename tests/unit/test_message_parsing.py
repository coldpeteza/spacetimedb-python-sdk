"""
Unit tests for SpaceTimeDB 2.0 message parsing in SpacetimeDBClient._on_message().

Covers:
  - Legacy v1 IdentityToken / SubscriptionUpdate / TransactionUpdate formats
  - New v2 IdentityToken (connection_id), InitialSubscription,
    TransactionUpdate (Committed / Failed / OutOfEnergy), TransactionUpdateLight
  - _parse_v1_table_updates and _parse_v2_table_updates helpers
"""

import json
import sys
import os
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from spacetimedb_sdk.spacetimedb_client import (
    SpacetimeDBClient,
    TransactionUpdateMessage,
    TransactionUpdateLightMessage,
    _SubscriptionUpdateMessage,
    _IdentityReceivedMessage,
    Identity,
    Address,
    DbEvent,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_client():
    """Return a SpacetimeDBClient with a mocked WebSocket and empty autogen."""
    autogen = MagicMock()
    autogen.__path__ = []
    with patch('spacetimedb_sdk.spacetimedb_client.WebSocketClient'):
        client = SpacetimeDBClient(autogen)
    # Give decode() a simple echo so table-update tests can run end-to-end.
    client.client_cache.decode = MagicMock(side_effect=lambda _table, row: row)
    return client


def _drain(client):
    """Drain and return all messages currently in the client's message queue."""
    messages = []
    while not client.message_queue.empty():
        messages.append(client.message_queue.get())
    return messages


IDENTITY_HEX = "a" * 64   # 32-byte identity as 64 hex chars
ADDRESS_HEX  = "b" * 32   # 16-byte address as 32 hex chars


# ── IdentityToken ─────────────────────────────────────────────────────────────

def test_identity_token_v1():
    client = _make_client()
    payload = json.dumps({
        "IdentityToken": {
            "token": "tok_abc",
            "identity": IDENTITY_HEX,
            "address": ADDRESS_HEX,
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    assert len(msgs) == 1
    msg = msgs[0]
    assert isinstance(msg, _IdentityReceivedMessage)
    assert msg.auth_token == "tok_abc"
    assert str(msg.identity) == IDENTITY_HEX
    assert str(msg.address) == ADDRESS_HEX


def test_identity_token_v2_connection_id():
    """SpaceTimeDB 2.0 uses 'connection_id' instead of 'address'."""
    client = _make_client()
    payload = json.dumps({
        "IdentityToken": {
            "token": "tok_xyz",
            "identity": IDENTITY_HEX,
            "connection_id": ADDRESS_HEX,
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    assert len(msgs) == 1
    msg = msgs[0]
    assert msg.auth_token == "tok_xyz"
    assert str(msg.address) == ADDRESS_HEX


# ── SubscriptionUpdate (v1 legacy) ────────────────────────────────────────────

def test_subscription_update_v1_insert():
    client = _make_client()
    payload = json.dumps({
        "SubscriptionUpdate": {
            "table_updates": [
                {
                    "table_name": "Player",
                    "table_row_operations": [
                        {"op": "insert", "row_pk": "pk1", "row": [1, "Alice"]},
                    ],
                }
            ]
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    assert len(msgs) == 1
    msg = msgs[0]
    assert isinstance(msg, _SubscriptionUpdateMessage)
    assert "Player" in msg.events
    events = msg.events["Player"]
    assert len(events) == 1
    assert events[0].row_op == "insert"
    assert events[0].row_pk == "pk1"


def test_subscription_update_v1_delete():
    client = _make_client()
    payload = json.dumps({
        "SubscriptionUpdate": {
            "table_updates": [
                {
                    "table_name": "Enemy",
                    "table_row_operations": [
                        {"op": "delete", "row_pk": "pk_del", "row": [42]},
                    ],
                }
            ]
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert msg.events["Enemy"][0].row_op == "delete"
    assert msg.events["Enemy"][0].row_pk == "pk_del"


def test_subscription_update_v1_multiple_tables():
    client = _make_client()
    payload = json.dumps({
        "SubscriptionUpdate": {
            "table_updates": [
                {
                    "table_name": "TableA",
                    "table_row_operations": [
                        {"op": "insert", "row_pk": "a1", "row": [1]},
                    ],
                },
                {
                    "table_name": "TableB",
                    "table_row_operations": [
                        {"op": "insert", "row_pk": "b1", "row": [2]},
                    ],
                },
            ]
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert "TableA" in msg.events
    assert "TableB" in msg.events


# ── InitialSubscription (v2) ──────────────────────────────────────────────────

def test_initial_subscription_v2_insert():
    client = _make_client()
    payload = json.dumps({
        "InitialSubscription": {
            "database_update": {
                "tables": [
                    {
                        "table_id": 1,
                        "table_name": "Player",
                        "num_rows": 1,
                        "updates": [
                            {
                                "inserts": [[10, "Bob"]],
                                "deletes": [],
                            }
                        ],
                    }
                ]
            },
            "request_id": 1,
            "total_host_execution_duration": {"microseconds": 100},
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    assert len(msgs) == 1
    msg = msgs[0]
    assert isinstance(msg, _SubscriptionUpdateMessage)
    assert "Player" in msg.events
    events = msg.events["Player"]
    assert len(events) == 1
    assert events[0].row_op == "insert"
    assert events[0].row_pk == json.dumps([10, "Bob"], separators=(",", ":"))


def test_initial_subscription_v2_empty_tables():
    client = _make_client()
    payload = json.dumps({
        "InitialSubscription": {
            "database_update": {"tables": []},
            "request_id": 2,
            "total_host_execution_duration": {"microseconds": 0},
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert isinstance(msg, _SubscriptionUpdateMessage)
    assert msg.events == {}


# ── TransactionUpdate v1 (legacy) ─────────────────────────────────────────────

def test_transaction_update_v1_committed():
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdate": {
            "event": {
                "caller_identity": IDENTITY_HEX,
                "caller_address": ADDRESS_HEX,
                "status": "committed",
                "message": "",
                "function_call": {
                    "reducer": "create_player",
                    "args": json.dumps([1, "Alice"]),
                },
            },
            "subscription_update": {
                "table_updates": [
                    {
                        "table_name": "Player",
                        "table_row_operations": [
                            {"op": "insert", "row_pk": "pk1", "row": [1, "Alice"]},
                        ],
                    }
                ]
            },
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    assert len(msgs) == 1
    msg = msgs[0]
    assert isinstance(msg, TransactionUpdateMessage)
    re = msg.reducer_event
    assert re.reducer_name == "create_player"
    assert re.status == "committed"
    assert re.args == [1, "Alice"]
    assert str(re.caller_identity) == IDENTITY_HEX
    assert "Player" in msg.events
    assert msg.events["Player"][0].row_op == "insert"


def test_transaction_update_v1_delete():
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdate": {
            "event": {
                "caller_identity": IDENTITY_HEX,
                "caller_address": ADDRESS_HEX,
                "status": "committed",
                "message": "",
                "function_call": {
                    "reducer": "remove_player",
                    "args": "[]",
                },
            },
            "subscription_update": {
                "table_updates": [
                    {
                        "table_name": "Player",
                        "table_row_operations": [
                            {"op": "delete", "row_pk": "pk_del", "row": [1]},
                        ],
                    }
                ]
            },
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert msg.events["Player"][0].row_op == "delete"


# ── TransactionUpdate v2 (SpaceTimeDB 2.0) ────────────────────────────────────

def test_transaction_update_v2_committed():
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdate": {
            "status": {
                "Committed": {
                    "tables": [
                        {
                            "table_id": 1,
                            "table_name": "Player",
                            "num_rows": 1,
                            "updates": [
                                {
                                    "inserts": [[5, "Carol"]],
                                    "deletes": [],
                                }
                            ],
                        }
                    ]
                }
            },
            "timestamp": {"microseconds": 9999},
            "caller_identity": IDENTITY_HEX,
            "caller_connection_id": ADDRESS_HEX,
            "reducer_call": {
                "reducer_name": "create_player",
                "reducer_id": 1,
                "args": [],
                "request_id": 7,
            },
            "energy_quanta_used": {"quanta": 100},
            "total_host_execution_duration": {"microseconds": 50},
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    assert len(msgs) == 1
    msg = msgs[0]
    assert isinstance(msg, TransactionUpdateMessage)
    re = msg.reducer_event
    assert re.reducer_name == "create_player"
    assert re.status == "committed"
    assert re.message == ""
    assert str(re.caller_identity) == IDENTITY_HEX
    assert "Player" in msg.events
    assert msg.events["Player"][0].row_op == "insert"


def test_transaction_update_v2_failed():
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdate": {
            "status": {"Failed": "reducer panicked"},
            "timestamp": {"microseconds": 1},
            "caller_identity": IDENTITY_HEX,
            "caller_connection_id": ADDRESS_HEX,
            "reducer_call": {
                "reducer_name": "bad_reducer",
                "reducer_id": 2,
                "args": [],
                "request_id": 8,
            },
            "energy_quanta_used": {"quanta": 10},
            "total_host_execution_duration": {"microseconds": 5},
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert isinstance(msg, TransactionUpdateMessage)
    re = msg.reducer_event
    assert re.status == "failed"
    assert re.message == "reducer panicked"
    assert msg.events == {}


def test_transaction_update_v2_out_of_energy():
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdate": {
            "status": {"OutOfEnergy": []},
            "timestamp": {"microseconds": 2},
            "caller_identity": IDENTITY_HEX,
            "caller_connection_id": ADDRESS_HEX,
            "reducer_call": {
                "reducer_name": "expensive_reducer",
                "reducer_id": 3,
                "args": [],
                "request_id": 9,
            },
            "energy_quanta_used": {"quanta": 0},
            "total_host_execution_duration": {"microseconds": 1},
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert isinstance(msg, TransactionUpdateMessage)
    assert msg.reducer_event.status == "outofenergy"


def test_transaction_update_v2_no_reducer_call():
    """A v2 TransactionUpdate without reducer_call (non-caller path)."""
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdate": {
            "status": {
                "Committed": {
                    "tables": [
                        {
                            "table_id": 2,
                            "table_name": "Score",
                            "num_rows": 1,
                            "updates": [
                                {"inserts": [[42, 100]], "deletes": []}
                            ],
                        }
                    ]
                }
            },
            "timestamp": {"microseconds": 3},
            "caller_identity": IDENTITY_HEX,
            "caller_connection_id": ADDRESS_HEX,
            "energy_quanta_used": {"quanta": 0},
            "total_host_execution_duration": {"microseconds": 1},
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert isinstance(msg, TransactionUpdateMessage)
    assert msg.reducer_event.reducer_name == ""
    assert msg.reducer_event.args == []
    assert "Score" in msg.events


def test_transaction_update_v2_caller_address_fallback():
    """v2 messages that still use caller_address instead of caller_connection_id."""
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdate": {
            "status": {"Committed": {"tables": []}},
            "timestamp": {"microseconds": 0},
            "caller_identity": IDENTITY_HEX,
            "caller_address": ADDRESS_HEX,
            "reducer_call": {
                "reducer_name": "noop",
                "reducer_id": 0,
                "args": [],
                "request_id": 0,
            },
            "energy_quanta_used": {"quanta": 0},
            "total_host_execution_duration": {"microseconds": 0},
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert str(msg.reducer_event.caller_address) == ADDRESS_HEX


# ── TransactionUpdateLight (v2, non-caller) ───────────────────────────────────

def test_transaction_update_light_with_rows():
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdateLight": {
            "request_id": 99,
            "update": {
                "tables": [
                    {
                        "table_id": 3,
                        "table_name": "Chat",
                        "num_rows": 2,
                        "updates": [
                            {
                                "inserts": [[1, "hello"], [2, "world"]],
                                "deletes": [],
                            }
                        ],
                    }
                ]
            },
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    assert len(msgs) == 1
    msg = msgs[0]
    assert isinstance(msg, TransactionUpdateLightMessage)
    assert "Chat" in msg.events
    assert len(msg.events["Chat"]) == 2
    assert msg.events["Chat"][0].row_op == "insert"
    assert msg.events["Chat"][1].row_op == "insert"


def test_transaction_update_light_with_deletes():
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdateLight": {
            "request_id": 5,
            "update": {
                "tables": [
                    {
                        "table_id": 4,
                        "table_name": "Item",
                        "num_rows": 1,
                        "updates": [
                            {
                                "inserts": [],
                                "deletes": [[7, "sword"]],
                            }
                        ],
                    }
                ]
            },
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert msg.events["Item"][0].row_op == "delete"


def test_transaction_update_light_empty():
    client = _make_client()
    payload = json.dumps({
        "TransactionUpdateLight": {
            "request_id": 0,
            "update": {"tables": []},
        }
    })
    client._on_message(payload)
    msgs = _drain(client)
    msg = msgs[0]
    assert isinstance(msg, TransactionUpdateLightMessage)
    assert msg.events == {}


# ── v2 row_pk determinism ─────────────────────────────────────────────────────

def test_v2_row_pk_is_deterministic_json():
    """The synthetic row_pk must be stable across insert and delete of same row."""
    row = [42, "test", True]
    expected_pk = json.dumps(row, separators=(",", ":"))

    client = _make_client()

    insert_payload = json.dumps({
        "InitialSubscription": {
            "database_update": {
                "tables": [
                    {
                        "table_id": 1,
                        "table_name": "Obj",
                        "num_rows": 1,
                        "updates": [{"inserts": [row], "deletes": []}],
                    }
                ]
            },
            "request_id": 1,
            "total_host_execution_duration": {"microseconds": 0},
        }
    })
    client._on_message(insert_payload)
    msgs = _drain(client)
    insert_event = msgs[0].events["Obj"][0]
    assert insert_event.row_pk == expected_pk

    delete_payload = json.dumps({
        "InitialSubscription": {
            "database_update": {
                "tables": [
                    {
                        "table_id": 1,
                        "table_name": "Obj",
                        "num_rows": 1,
                        "updates": [{"inserts": [], "deletes": [row]}],
                    }
                ]
            },
            "request_id": 2,
            "total_host_execution_duration": {"microseconds": 0},
        }
    })
    client._on_message(delete_payload)
    msgs = _drain(client)
    delete_event = msgs[0].events["Obj"][0]
    assert delete_event.row_pk == expected_pk


# ── transaction_type values ───────────────────────────────────────────────────

def test_transaction_types():
    client = _make_client()

    # SubscriptionUpdate
    client._on_message(json.dumps({
        "SubscriptionUpdate": {"table_updates": []}
    }))
    msgs = _drain(client)
    assert msgs[0].transaction_type == "SubscriptionUpdate"

    # InitialSubscription → also SubscriptionUpdate internally
    client._on_message(json.dumps({
        "InitialSubscription": {
            "database_update": {"tables": []},
            "request_id": 1,
            "total_host_execution_duration": {"microseconds": 0},
        }
    }))
    msgs = _drain(client)
    assert msgs[0].transaction_type == "SubscriptionUpdate"

    # TransactionUpdateLight
    client._on_message(json.dumps({
        "TransactionUpdateLight": {"request_id": 0, "update": {"tables": []}}
    }))
    msgs = _drain(client)
    assert msgs[0].transaction_type == "TransactionUpdateLight"
