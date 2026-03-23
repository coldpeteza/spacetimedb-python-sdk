"""SpacetimeDB Python SDK AsyncIO Client

This module provides a client interface to your SpacetimeDB module using the
asyncio library.  Essentially, you create your client object, register
callbacks, and then start the client using asyncio.run().

Phase 6 refactor: replaces the threaded ``websocket-client`` + 100 ms polling
hack with the async-native ``websockets`` library via ``AsyncWebSocketClient``.
Messages are delivered immediately as they arrive; no background thread or
periodic poll is required.
"""

from typing import List
import asyncio
from datetime import timedelta, datetime

from spacetimedb_sdk.spacetimedb_client import SpacetimeDBClient
from spacetimedb_sdk.spacetime_websocket_client import AsyncWebSocketClient


class SpacetimeDBException(Exception):
    pass


class SpacetimeDBScheduledEvent:
    def __init__(self, fire_time, callback, args):
        self.fire_time = fire_time
        self.callback = callback
        self.args = args


class _AsyncSendShim:
    """Sync-compatible send interface that routes calls through the async loop.

    ``SpacetimeDBClient._reducer_call()`` and ``subscribe()`` call
    ``self.wsc.send(data)`` synchronously.  This shim schedules the
    corresponding ``await ws.send(data)`` as an asyncio Task so the calls
    work transparently from within the running event loop.
    """

    def __init__(self, aws: AsyncWebSocketClient):
        self._aws = aws
        self.is_connected = True

    def send(self, data):
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8")
        asyncio.get_running_loop().create_task(self._aws.send(data))

    def close(self):
        asyncio.get_running_loop().create_task(self._aws.close())


class SpacetimeDBAsyncClient:
    request_timeout = 5

    is_connected = False
    is_closing = False
    identity = None
    address = None

    def __init__(self, autogen_package):
        """
        Create a SpacetimeDBAsyncClient object.

        Args:
            autogen_package: package folder created by running the generate
                command from the CLI.
        """
        self.client = SpacetimeDBClient(autogen_package)
        self.prescheduled_events = []
        self.event_queue = None
        self._aws = None
        self._receive_task = None

    def schedule_event(self, delay_secs, callback, *args):
        """
        Schedule an event to be fired after a delay.

        To create a repeating event, call schedule_event() again from within
        the callback function.

        Args:
            delay_secs: number of seconds to wait before firing the event.
            callback: function to call when the event fires.
            args: arguments to pass to the callback function.
        """
        if self.event_queue is None:
            self.prescheduled_events.append((delay_secs, callback, args))
        else:
            fire_time = datetime.now() + timedelta(seconds=delay_secs)
            scheduled_event = SpacetimeDBScheduledEvent(fire_time, callback, args)

            async def wait_for_delay():
                await asyncio.sleep(
                    (scheduled_event.fire_time - datetime.now()).total_seconds()
                )
                self.event_queue.put_nowait(("scheduled_event", scheduled_event))
                scheduled_event.callback(*scheduled_event.args)

            asyncio.create_task(wait_for_delay())

    def register_on_subscription_applied(self, callback):
        """
        Register a callback to be executed when the local cache is updated
        as a result of a change to the subscription queries.
        """
        self.client.register_on_subscription_applied(callback)

    def subscribe(self, queries: List[str]):
        """
        Subscribe to receive data and transaction updates for *queries*.

        Args:
            queries: list of SQL query strings.
        """
        self.client.subscribe(queries)

    def force_close(self):
        """
        Signal the client to stop processing events and close the connection.
        """
        self.is_closing = True
        self.event_queue.put_nowait(("force_close", None))

    async def run(
        self,
        auth_token,
        host,
        address_or_name,
        ssl_enabled,
        on_connect,
        subscription_queries=[],
    ):
        """
        Run the client.  This coroutine does not return until the client is
        closed.

        Args:
            auth_token: authentication token for the server.
            host: hostname:port of the SpacetimeDB server.
            address_or_name: module name or address to connect to.
            ssl_enabled: True to use wss://, False for ws://.
            on_connect: called with (auth_token, identity) once connected.
            subscription_queries: SQL queries to subscribe to on connect.
        """
        if not self.event_queue:
            self._on_async_loop_start()

        identity_result = await self.connect(
            auth_token, host, address_or_name, ssl_enabled, subscription_queries
        )

        if on_connect is not None:
            on_connect(identity_result[0], identity_result[1])

        def on_subscription_applied():
            self.event_queue.put_nowait(("subscription_applied", None))

        def on_event(event):
            self.event_queue.put_nowait(("reducer_transaction", event))

        self.client.register_on_event(on_event)
        self.client.register_on_subscription_applied(on_subscription_applied)

        while not self.is_closing:
            event, payload = await self._event()
            if event == "disconnected":
                if self.is_closing:
                    return payload
                else:
                    raise SpacetimeDBException(payload)
            elif event == "error":
                raise payload
            elif event == "force_close":
                break

        await self.close()

    async def connect(
        self,
        auth_token,
        host,
        address_or_name,
        ssl_enabled,
        subscription_queries=[],
    ):
        """
        Connect to the server and wait until the identity is received.

        Returns:
            (auth_token, identity) tuple.
        """
        if not self.event_queue:
            self._on_async_loop_start()

        self._aws = AsyncWebSocketClient("v1.json.spacetimedb")
        await self._aws.connect(auth_token, host, address_or_name, ssl_enabled)

        # Attach a sync-compatible shim so _reducer_call() / subscribe() work.
        shim = _AsyncSendShim(self._aws)
        self.client.wsc = shim

        def on_identity_received(token, identity, address):
            self.identity = identity
            self.address = address
            self.client.subscribe(subscription_queries)
            self.event_queue.put_nowait(("connected", (token, identity)))

        self.client._on_identity = on_identity_received
        self.client._on_error = None
        self.client._on_disconnect = None

        # Start the async receive loop — no polling thread required.
        self._receive_task = asyncio.create_task(self._receive_loop())

        while True:
            event, payload = await self._event()
            if event == "error":
                raise payload
            elif event == "connected":
                self.is_connected = True
                return payload

    async def _receive_loop(self):
        """Drain incoming WebSocket messages and dispatch them immediately."""
        import websockets.exceptions
        try:
            async for message in self._aws:
                self.client._on_message(message)
                self.client._do_update()
        except websockets.exceptions.ConnectionClosed as exc:
            self.is_connected = False
            if self.is_closing:
                self.event_queue.put_nowait(("disconnected", str(exc)))
            else:
                self.event_queue.put_nowait((
                    "error",
                    SpacetimeDBException(f"Connection closed unexpectedly: {exc}"),
                ))
        except Exception as exc:
            self.event_queue.put_nowait(("error", SpacetimeDBException(str(exc))))

    async def call_reducer(self, reducer_name, *reducer_args):
        """
        Call a reducer and await its result.

        Uses the Phase 3 ``_then()`` callback pattern rather than the old
        broadcast-listener approach.

        Args:
            reducer_name: name of the reducer to call.
            reducer_args: positional arguments for the reducer.

        Returns:
            The ``ReducerEvent`` for this call.

        Raises:
            SpacetimeDBException: if the call times out.
        """
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def on_result(reducer_event):
            if not future.done():
                loop.call_soon_threadsafe(future.set_result, reducer_event)

        self.client._reducer_call(reducer_name, *reducer_args, then=on_result)

        try:
            return await asyncio.wait_for(
                asyncio.shield(future), timeout=self.request_timeout
            )
        except asyncio.TimeoutError:
            raise SpacetimeDBException("Reducer call timed out.")

    async def close(self):
        """Close the WebSocket connection and wait for the receive loop to finish."""
        self.is_closing = True
        if self._aws is not None:
            await self._aws.close()
        if self._receive_task is not None:
            try:
                await self._receive_task
            except Exception:
                pass

    def _on_async_loop_start(self):
        self.event_queue = asyncio.Queue()
        for delay, callback, args in self.prescheduled_events:
            self.schedule_event(delay, callback, *args)
        self.prescheduled_events.clear()

    async def _event(self):
        """Wait for and return the next event from the queue."""
        return await self.event_queue.get()
