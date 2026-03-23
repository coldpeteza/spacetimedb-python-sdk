from typing import List, Dict, Callable
from types import ModuleType
from collections import defaultdict, deque

import json
import queue
import random

from spacetimedb_sdk.spacetime_websocket_client import WebSocketClient
from spacetimedb_sdk.client_cache import ClientCache


class Identity:
    """
    Represents a user identity. This is a wrapper around the Uint8Array that is recieved from SpacetimeDB.

    Attributes:
        data (bytes): The identity data.
    """

    def __init__(self, data):
        self.data = bytes(data)  # Ensure data is always bytes

    @staticmethod
    def from_string(string):
        """
        Returns an Identity object with the data attribute set to the byte representation of the input string.

        Args:
            string (str): The input string.

        Returns:
            Identity: The Identity object.
        """
        if isinstance(string, str):
            return Identity(bytes.fromhex(string.removeprefix("0x")))
        raise TypeError(f"Expected str, got {type(string).__name__}")

    @staticmethod
    def from_json(value):
        """Parse an Identity from a v2 JSON value (dict with __identity__ key) or a plain hex string."""
        if isinstance(value, dict):
            hex_str = value.get("__identity__", "")
            return Identity.from_string(hex_str)
        return Identity.from_string(value)

    @staticmethod
    def from_bytes(data):
        """
        Returns an Identity object with the data attribute set to the input bytes.

        Args:
            data (bytes): The input bytes.

        Returns:
            Identity: The Identity object.
        """
        return Identity(data)

    # override to_string
    def __str__(self):
        return self.data.hex()

    # override = operator
    def __eq__(self, other):
        return isinstance(other, Identity) and self.data == other.data

    def __hash__(self):
        return hash(self.data)

class Address:
    """
    Represents a user address. This is a wrapper around the Uint8Array that is recieved from SpacetimeDB.

    Attributes:
        data (bytes): The address data.
    """

    def __init__(self, data):
        self.data = bytes(data)  # Ensure data is always bytes

    @staticmethod
    def from_string(string):
        """
        Returns an Address object with the data attribute set to the byte representation of the input string.
        Returns None if the string is all zeros.

        Args:
            string (str): The input string.

        Returns:
            Address: The Address object.
        """
        if isinstance(string, str):
            address_bytes = bytes.fromhex(string.removeprefix("0x"))
        else:
            raise TypeError(f"Expected str, got {type(string).__name__}")
        if all(byte == 0 for byte in address_bytes):
            return None
        else:
            return Address(address_bytes)

    @staticmethod
    def from_json(value):
        """Parse an Address from a v2 JSON value (dict with __connection_id__) or a plain hex string."""
        if isinstance(value, dict):
            # v2 format: {"__connection_id__": <integer>}
            conn_id = value.get("__connection_id__", 0)
            if isinstance(conn_id, int):
                hex_str = format(conn_id, "032x")
                return Address.from_string(hex_str)
            return Address.from_string(str(conn_id).removeprefix("0x"))
        return Address.from_string(value)

    @staticmethod
    def from_bytes(data):
        """
        Returns an Address object with the data attribute set to the input bytes.

        Args:
            data (bytes): The input bytes.

        Returns:
            Address: The Address object.
        """
        if all(byte == 0 for byte in address_bytes):
            return None
        else:
            return Address(data)

    @staticmethod
    def random():
        """
        Returns a random Address.
        """
        return Address(bytes(random.getrandbits(8) for _ in range(16)))

    # override to_string
    def __str__(self):
        return self.data.hex()

    # override = operator
    def __eq__(self, other):
        return isinstance(other, Address) and self.data == other.data

    def __hash__(self):
        return hash(self.data)


class DbEvent:
    """
    Represents a database event.

    Attributes:
        table_name (str): The name of the table associated with the event.
        row_pk (str): The primary key of the affected row.
        row_op (str): The operation performed on the row (e.g., "insert", "update", "delete").
        decoded_value (Any, optional): The decoded value of the affected row. Defaults to None.
    """

    def __init__(self, table_name, row_pk, row_op, decoded_value=None):
        self.table_name = table_name
        self.row_pk = row_pk
        self.row_op = row_op
        self.decoded_value = decoded_value


class _ClientApiMessage:
    """
    This class is intended for internal use only and should not be used externally.
    """

    def __init__(self, transaction_type):
        self.transaction_type = transaction_type
        self.events = {}

    def append_event(self, table_name, event):
        self.events.setdefault(table_name, []).append(event)


class _IdentityReceivedMessage(_ClientApiMessage):
    """
    This class is intended for internal use only and should not be used externally.
    """

    def __init__(self, auth_token, identity, address):
        super().__init__("IdentityReceived")

        self.auth_token = auth_token
        self.identity = identity
        self.address = address


class _SubscriptionUpdateMessage(_ClientApiMessage):
    """
    This class is intended for internal use only and should not be used externally.
    """

    def __init__(self):
        super().__init__("SubscriptionUpdate")


class ReducerEvent:
    """
    This class contains the information about a reducer event to be passed to row update callbacks.
    """

    def __init__(self, caller_identity, caller_address, reducer_name, status, message, args):
        self.caller_identity = caller_identity
        self.caller_address = caller_address
        self.reducer_name = reducer_name
        self.status = status
        self.message = message
        self.args = args


class TransactionUpdateMessage(_ClientApiMessage):
    """
    Represents a transaction update message. Used in on_event callbacks.

    For more details, see `spacetimedb_client.SpacetimeDBClient.register_on_event`

    Attributes:
        reducer_event (ReducerEvent): The reducer event that triggered this update.
        events (List[DbEvent]): List of DBEvents that were committed.
    """

    def __init__(
        self,
        caller_identity: Identity,
        caller_address: Address,
        status: str,
        message: str,
        reducer_name: str,
        args: Dict,
    ):
        super().__init__("TransactionUpdate")
        self.reducer_event = ReducerEvent(
            caller_identity, caller_address, reducer_name, status, message, args
        )


class TransactionUpdateLightMessage(_ClientApiMessage):
    """
    Represents a lightweight transaction update for non-calling clients.

    Introduced in SpaceTimeDB 2.0.  Non-callers receive this message instead
    of a full TransactionUpdateMessage; it carries table row changes without
    reducer metadata.
    """

    def __init__(self):
        super().__init__("TransactionUpdateLight")


class SpacetimeDBClient:
    """
    The SpacetimeDBClient class is the primary interface for communication with the SpacetimeDB Module in the SDK, facilitating interaction with the database.
    """

    instance = None
    client_cache = None

    @classmethod
    def init(
        cls,
        auth_token: str,
        host: str,
        address_or_name: str,
        ssl_enabled: bool,
        autogen_package: ModuleType,
        on_connect: Callable[[], None] = None,
        on_disconnect: Callable[[str], None] = None,
        on_identity: Callable[[str, Identity, Address], None] = None,
        on_error: Callable[[str], None] = None,
    ):
        """
        Create a network manager instance.

        Args:
            auth_token (str): This is the token generated by SpacetimeDB that matches the user's identity. If None, token will be generated
            host (str): Hostname:port for SpacetimeDB connection
            address_or_name (str): The name or address of the database to connect to
            autogen_package (ModuleType): Python package where SpacetimeDB module generated files are located.
            on_connect (Callable[[], None], optional): Optional callback called when a connection is made to the SpacetimeDB module.
            on_disconnect (Callable[[str], None], optional): Optional callback called when the Python client is disconnected from the SpacetimeDB module. The argument is the close message.
            on_identity (Callable[[str, Identity, Address], None], optional): Called when the user identity is recieved from SpacetimeDB. First argument is the auth token used to login in future sessions.
            on_error (Callable[[str], None], optional): Optional callback called when the Python client connection encounters an error. The argument is the error message.

        Example:
            SpacetimeDBClient.init(autogen, on_connect=self.on_connect)
        """
        client = SpacetimeDBClient(autogen_package)
        client.connect(
            auth_token,
            host,
            address_or_name,
            ssl_enabled,
            on_connect,
            on_disconnect,
            on_identity,
            on_error,
        )

    # Do not call this directly. Use init to instantiate the instance.
    def __init__(self, autogen_package):
        SpacetimeDBClient.instance = self

        self._row_update_callbacks = {}
        self._pending_then = defaultdict(deque)
        self._on_subscription_applied = []
        self._on_event = []

        self.identity = None
        self.address = Address.random()

        self.client_cache = ClientCache(autogen_package)
        self.message_queue = queue.Queue()

        self.processed_message_queue = queue.Queue()

    def connect(
        self,
        auth_token,
        host,
        address_or_name,
        ssl_enabled,
        on_connect,
        on_disconnect,
        on_identity,
        on_error,
    ):
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        self._on_identity = on_identity
        self._on_error = on_error

        self.wsc = WebSocketClient(
            "v1.json.spacetimedb",
            on_connect=on_connect,
            on_error=on_error,
            on_close=on_disconnect,
            on_message=self._on_message,
            client_address=self.address,
        )
        # print("CONNECTING " + host + " " + address_or_name)
        self.wsc.connect(
            auth_token,
            host,
            address_or_name,
            ssl_enabled,
        )

    def update(self):
        """
        Process all pending incoming messages from the SpacetimeDB module.

        NOTE: This function must be called on a regular interval to process incoming messages.

        Example:
            SpacetimeDBClient.init(autogen, on_connect=self.on_connect)
            while True:
                SpacetimeDBClient.instance.update()  # Call the update function in a loop to process incoming messages
                # Additional logic or code can be added here
        """
        self._do_update()

    def close(self):
        """
        Close the WebSocket connection.

        This function closes the WebSocket connection to the SpacetimeDB module.

        Notes:
            - This needs to be called when exiting the application to terminate the websocket threads.

        Example:
            SpacetimeDBClient.instance.close()
        """

        self.wsc.close()

    def subscribe(self, queries: List[str]):
        """
        Subscribe to receive data and transaction updates for the provided queries.

        This function sends a subscription request to the SpacetimeDB module, indicating that the client
        wants to receive data and transaction updates related to the specified queries.

        Args:
            queries (List[str]): A list of queries to subscribe to. Each query is a string representing
                an sql formatted query statement.

        Example:
            queries = ["SELECT * FROM table1", "SELECT * FROM table2 WHERE col2 = 0"]
            SpacetimeDBClient.instance.subscribe(queries)
        """
        message = {
            "Subscribe": {
                "query_strings": queries,
                "request_id": 0,
            }
        }
        self.wsc.send(json.dumps(message))

    def register_on_subscription_applied(self, callback: Callable[[], None]):
        """
        Register a callback function to be executed when the local cache is updated as a result of a change to the subscription queries.

        Args:
            callback (Callable[[], None]): A callback function that will be invoked on each subscription update.
                The callback function should not accept any arguments and should not return any value.

        Example:
            def subscription_callback():
                # Code to be executed on each subscription update

            SpacetimeDBClient.instance.register_on_subscription_applied(subscription_callback)
        """
        if self._on_subscription_applied is None:
            self._on_subscription_applied = []

        self._on_subscription_applied.append(callback)

    def unregister_on_subscription_applied(self, callback: Callable[[], None]):
        """
        Unregister a callback function from the subscription update event.

        Args:
            callback (Callable[[], None]): A callback function that was previously registered with the `register_on_subscription_applied` function.

        Example:
            def subscription_callback():
                # Code to be executed on each subscription update

            SpacetimeDBClient.instance.register_on_subscription_applied(subscription_callback)
            SpacetimeDBClient.instance.unregister_on_subscription_applied(subscription_callback)
        """
        if self._on_subscription_applied is not None:
            self._on_subscription_applied.remove(callback)

    def register_on_event(self, callback: Callable[[TransactionUpdateMessage], None]):
        """
        Register a callback function to handle transaction update events.

        This function registers a callback function that will be called when a reducer modifies a table
        matching any of the subscribed queries or if a reducer called by this Python client encounters a failure.

        Args:
            callback (Callable[[TransactionUpdateMessage], None]):
                A callback function that takes a single argument of type `TransactionUpdateMessage`.
                This function will be invoked with a `TransactionUpdateMessage` instance containing information
                about the transaction update event.

        Example:
            def handle_event(transaction_update):
                # Code to handle the transaction update event

            SpacetimeDBClient.instance.register_on_event(handle_event)
        """
        if self._on_event is None:
            self._on_event = []

        self._on_event.append(callback)

    def unregister_on_event(self, callback: Callable[[TransactionUpdateMessage], None]):
        """
        Unregister a callback function that was previously registered using `register_on_event`.

        Args:
            callback (Callable[[TransactionUpdateMessage], None]): The callback function to unregister.

        Example:
            SpacetimeDBClient.instance.unregister_on_event(handle_event)
        """
        if self._on_event is not None:
            self._on_event.remove(callback)

    def _get_table_cache(self, table_name: str):
        return self.client_cache.get_table_cache(table_name)

    def _register_row_update(
        self,
        table_name: str,
        callback: Callable[[str, object, object, ReducerEvent], None],
    ):
        if table_name not in self._row_update_callbacks:
            self._row_update_callbacks[table_name] = []

        self._row_update_callbacks[table_name].append(callback)

    def _unregister_row_update(
        self,
        table_name: str,
        callback: Callable[[str, object, object, ReducerEvent], None],
    ):
        if table_name in self._row_update_callbacks:
            self._row_update_callbacks[table_name].remove(callback)

    def _reducer_call(self, reducer, *args, then=None):
        if not self.wsc.is_connected:
            print("[reducer_call] Not connected")

        if then is not None:
            self._pending_then[reducer].append(then)

        message = {
            "CallReducer": {
                "reducer": reducer,
                "args": json.dumps(list(args)),
                "request_id": 0,
                "flags": 0,
            }
        }
        self.wsc.send(json.dumps(message))

    def _on_message(self, data):
        message = json.loads(data)
        if "IdentityToken" in message:
            token = message["IdentityToken"]["token"]
            identity = Identity.from_json(message["IdentityToken"]["identity"])
            # SpaceTimeDB 2.0 renamed "address" to "connection_id"; support both.
            addr_value = message["IdentityToken"].get(
                "connection_id", message["IdentityToken"].get("address", "0" * 32)
            )
            address = Address.from_json(addr_value)
            self.message_queue.put(_IdentityReceivedMessage(token, identity, address))

        elif "SubscriptionUpdate" in message:
            # v1 (legacy) initial subscription
            clientapi_message = _SubscriptionUpdateMessage()
            self._parse_v1_table_updates(
                clientapi_message, message["SubscriptionUpdate"]["table_updates"]
            )
            self.message_queue.put(clientapi_message)

        elif "InitialSubscription" in message:
            # v2 (SpaceTimeDB 2.0) initial subscription
            clientapi_message = _SubscriptionUpdateMessage()
            tables = message["InitialSubscription"]["database_update"]["tables"]
            self._parse_v2_table_updates(clientapi_message, tables)
            self.message_queue.put(clientapi_message)

        elif "TransactionUpdate" in message:
            msg = message["TransactionUpdate"]
            if "event" in msg:
                # v1 (legacy) TransactionUpdate
                clientapi_message = TransactionUpdateMessage(
                    Identity.from_json(msg["event"]["caller_identity"]),
                    Address.from_json(msg["event"]["caller_address"]),
                    msg["event"]["status"],
                    msg["event"]["message"],
                    msg["event"]["function_call"]["reducer"],
                    json.loads(msg["event"]["function_call"]["args"]),
                )
                self._parse_v1_table_updates(
                    clientapi_message,
                    msg["subscription_update"]["table_updates"],
                )
            else:
                # v2 (SpaceTimeDB 2.0) TransactionUpdate
                status_dict = msg["status"]
                if "Committed" in status_dict:
                    status_str = "committed"
                    err_message = ""
                    tables = status_dict["Committed"].get("tables", [])
                elif "Failed" in status_dict:
                    status_str = "failed"
                    err_message = status_dict["Failed"]
                    tables = []
                else:
                    # OutOfEnergy or unknown
                    status_str = list(status_dict.keys())[0].lower()
                    err_message = ""
                    tables = []

                caller_identity = Identity.from_json(msg["caller_identity"])
                # 2.0 uses caller_connection_id; fall back to caller_address
                conn_id_value = msg.get(
                    "caller_connection_id", msg.get("caller_address", "0" * 32)
                )
                caller_address = Address.from_json(conn_id_value)

                reducer_call = msg.get("reducer_call")
                if reducer_call is not None:
                    reducer_name = reducer_call["reducer_name"]
                    args = reducer_call.get("args", [])
                    if isinstance(args, str):
                        args = json.loads(args)
                else:
                    reducer_name = ""
                    args = []

                clientapi_message = TransactionUpdateMessage(
                    caller_identity,
                    caller_address,
                    status_str,
                    err_message,
                    reducer_name,
                    args,
                )
                self._parse_v2_table_updates(clientapi_message, tables)

            self.message_queue.put(clientapi_message)

        elif "TransactionUpdateLight" in message:
            # v2 non-caller update — table changes only, no reducer info
            clientapi_message = TransactionUpdateLightMessage()
            tables = message["TransactionUpdateLight"].get("update", {}).get(
                "tables", []
            )
            self._parse_v2_table_updates(clientapi_message, tables)
            self.message_queue.put(clientapi_message)

    # ── table-update parsing helpers ──────────────────────────────────────────

    def _parse_v1_table_updates(self, clientapi_message, table_updates):
        """Parse legacy v1 table_updates (table_row_operations with row_pk)."""
        for table_update in table_updates:
            table_name = table_update["table_name"]
            for table_row_op in table_update["table_row_operations"]:
                row_op = table_row_op["op"]
                if row_op == "insert":
                    decoded_value = self.client_cache.decode(
                        table_name, table_row_op["row"]
                    )
                    clientapi_message.append_event(
                        table_name,
                        DbEvent(
                            table_name,
                            table_row_op["row_pk"],
                            row_op,
                            decoded_value,
                        ),
                    )
                elif row_op == "delete":
                    clientapi_message.append_event(
                        table_name,
                        DbEvent(table_name, table_row_op["row_pk"], row_op),
                    )

    def _parse_v2_table_updates(self, clientapi_message, tables):
        """Parse v2 table updates (separate inserts/deletes arrays, no row_pk)."""
        for table_update in tables:
            table_name = table_update["table_name"]
            for query_update in table_update.get("updates", []):
                for row in query_update.get("inserts", []):
                    row_pk = json.dumps(row, separators=(",", ":"))
                    decoded_value = self.client_cache.decode(table_name, row)
                    clientapi_message.append_event(
                        table_name,
                        DbEvent(table_name, row_pk, "insert", decoded_value),
                    )
                for row in query_update.get("deletes", []):
                    row_pk = json.dumps(row, separators=(",", ":"))
                    clientapi_message.append_event(
                        table_name,
                        DbEvent(table_name, row_pk, "delete"),
                    )

    def _do_update(self):
        while not self.message_queue.empty():
            next_message = self.message_queue.get()

            if next_message.transaction_type == "IdentityReceived":
                self.identity = next_message.identity
                self.address = next_message.address
                if self._on_identity:
                    self._on_identity(next_message.auth_token, self.identity, self.address)
            else:
                # print(f"next_message: {next_message.transaction_type}")
                # apply all the event state before calling callbacks
                for table_name, table_events in next_message.events.items():
                    # first retrieve the old values for all events
                    for db_event in table_events:
                        # get the old value for sending callbacks
                        db_event.old_value = self.client_cache.get_entry(
                            db_event.table_name, db_event.row_pk
                        )

                    # if this table has a primary key, find table updates by looking for matching insert/delete events
                    primary_key = getattr(
                        self.client_cache.get_table_cache(table_name).table_class,
                        "primary_key",
                        None,
                    )
                    # print(f"Primary key: {primary_key}")
                    if primary_key is not None:
                        primary_key_row_ops = {}

                        for db_event in table_events:
                            if db_event.row_op == "insert":
                                # NOTE: we have to do look up in actual data dict because primary_key is a property of the table class
                                primary_key_value = db_event.decoded_value.data[
                                    primary_key
                                ]
                            else:
                                primary_key_value = db_event.old_value.data[primary_key]

                            if primary_key_value in primary_key_row_ops:
                                other_db_event = primary_key_row_ops[primary_key_value]
                                if (
                                    db_event.row_op == "insert"
                                    and other_db_event.row_op == "delete"
                                ):
                                    # this is a row update so we need to replace the insert
                                    db_event.row_op = "update"
                                    db_event.old_pk = other_db_event.row_pk
                                    db_event.old_value = other_db_event.old_value
                                    primary_key_row_ops[primary_key_value] = db_event
                                elif (
                                    db_event.row_op == "delete"
                                    and other_db_event.row_op == "insert"
                                ):
                                    # the insert was the row update so just upgrade it to update
                                    primary_key_row_ops[
                                        primary_key_value
                                    ].row_op = "update"
                                    primary_key_row_ops[
                                        primary_key_value
                                    ].old_pk = db_event.row_pk
                                    primary_key_row_ops[
                                        primary_key_value
                                    ].old_value = db_event.old_value
                                else:
                                    print(
                                        f"Error: duplicate primary key {table_name}:{primary_key_value}"
                                    )
                            else:
                                primary_key_row_ops[primary_key_value] = db_event

                        table_events = primary_key_row_ops.values()
                        next_message.events[table_name] = table_events

                    # now we can apply the events to the cache
                    # event tables are ephemeral: fire callbacks but don't persist
                    is_event = self.client_cache.is_event_table(table_name)
                    for db_event in table_events:
                        # print(f"db_event: {db_event.row_op} {table_name}")
                        if not is_event:
                            if db_event.row_op == "insert" or db_event.row_op == "update":
                                # in the case of updates we need to delete the old entry
                                if db_event.row_op == "update":
                                    self.client_cache.delete_entry(
                                        db_event.table_name, db_event.old_pk
                                    )
                                self.client_cache.set_entry_decoded(
                                    db_event.table_name,
                                    db_event.row_pk,
                                    db_event.decoded_value,
                                )
                            elif db_event.row_op == "delete":
                                self.client_cache.delete_entry(
                                    db_event.table_name, db_event.row_pk
                                )

                # now that we have applied the state we can call the callbacks
                for table_events in next_message.events.values():
                    for db_event in table_events:
                        # call row update callback
                        if db_event.table_name in self._row_update_callbacks:
                            reducer_event = (
                                next_message.reducer_event
                                if next_message.transaction_type == "TransactionUpdate"
                                else None
                            )
                            for row_update_callback in self._row_update_callbacks[
                                db_event.table_name
                            ]:
                                row_update_callback(
                                    db_event.row_op,
                                    db_event.old_value,
                                    db_event.decoded_value,
                                    reducer_event,
                                )

                if next_message.transaction_type == "SubscriptionUpdate":
                    # call ontransaction callback
                    for on_subscription_applied in self._on_subscription_applied:
                        on_subscription_applied()

                if next_message.transaction_type == "TransactionUpdate":
                    # call on event callback
                    for event_callback in self._on_event:
                        event_callback(next_message)

                    # invoke per-call _then() callback if one is pending
                    reducer_event = next_message.reducer_event
                    reducer_name = reducer_event.reducer_name
                    if reducer_name and self._pending_then[reducer_name]:
                        then_callback = self._pending_then[reducer_name].popleft()
                        then_callback(reducer_event)
