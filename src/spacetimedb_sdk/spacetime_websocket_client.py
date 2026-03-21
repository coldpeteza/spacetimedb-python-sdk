import websocket
import threading
import base64
import binascii

try:
    import websockets
    import websockets.exceptions
    _WEBSOCKETS_AVAILABLE = True
except ImportError:
    _WEBSOCKETS_AVAILABLE = False


def _is_oidc_token(token: str) -> bool:
    """Return True if *token* looks like a JWT/OIDC Bearer token.

    JWTs are three base64url segments separated by dots; the header segment
    always decodes to a JSON object, so its base64url encoding starts with
    'eyJ' (the encoding of '{"').
    """
    return isinstance(token, str) and token.startswith("eyJ") and token.count(".") == 2


def _build_auth_headers(auth_token: str) -> dict:
    """Return the appropriate ``Authorization`` header dict for *auth_token*.

    - No token  → ``None`` (no header added)
    - OIDC/JWT  → ``Bearer <token>``
    - Legacy    → ``Basic base64("token:<token>")``
    """
    if not auth_token:
        return None
    if _is_oidc_token(auth_token):
        return {"Authorization": f"Bearer {auth_token}"}
    token_bytes = bytes(f"token:{auth_token}", "utf-8")
    base64_str = base64.b64encode(token_bytes).decode("utf-8")
    return {"Authorization": f"Basic {base64_str}"}


class WebSocketClient:
    def __init__(self, protocol, on_connect=None, on_close=None, on_error=None, on_message=None, client_address=None):
        self._on_connect = on_connect
        self._on_close = on_close
        self._on_error = on_error
        self._on_message = on_message

        self.protocol = protocol
        self.ws = None
        self.message_thread = None
        self.host = None
        self.name_or_address = None
        self.is_connected = False
        self.client_address = client_address

    def connect(self, auth, host, name_or_address, ssl_enabled):
        protocol = "wss" if ssl_enabled else "ws"
        url = f"{protocol}://{host}/database/subscribe/{name_or_address}"

        if self.client_address is not None:
            url += f"?client_address={self.client_address}"

        self.host = host
        self.name_or_address = name_or_address

        headers = _build_auth_headers(auth)

        self.ws = websocket.WebSocketApp(url,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close,
                                         header=headers,
                                         subprotocols=[self.protocol])

        self.message_thread = threading.Thread(target=self.ws.run_forever)
        self.message_thread.start()

    def decode_hex_string(hex_string):
        try:
            return binascii.unhexlify(hex_string)
        except binascii.Error:
            return None

    def send(self, data):
        if not self.is_connected:
            print("[send] Not connected")

        self.ws.send(data)

    def close(self):
        self.ws.close()

    def on_open(self, ws):
        self.is_connected = True
        if self._on_connect:
            self._on_connect()

    def on_message(self, ws, message):
        # Process incoming message on a separate thread here
        t = threading.Thread(target=self.process_message, args=(message,))
        t.start()

    def process_message(self, message):
        if self._on_message:
            self._on_message(message)
        pass

    def on_error(self, ws, error):
        if self._on_error:
            self._on_error(error)

    def on_close(self, ws, status_code, close_msg):
        if self._on_close:
            self._on_close(close_msg)


class AsyncWebSocketClient:
    """Async WebSocket client using the ``websockets`` library.

    Replaces the threaded ``WebSocketClient`` for use with
    ``SpacetimeDBAsyncClient``, eliminating the background thread and the
    100 ms polling hack.  Requires the ``websockets`` package.
    """

    def __init__(self, protocol: str):
        if not _WEBSOCKETS_AVAILABLE:
            raise RuntimeError(
                "The 'websockets' package is required for AsyncWebSocketClient. "
                "Install it with: pip install websockets"
            )
        self.protocol = protocol
        self._ws = None

    async def connect(self, auth_token: str, host: str, name_or_address: str, ssl_enabled: bool):
        """Open the WebSocket connection to the SpacetimeDB server."""
        proto = "wss" if ssl_enabled else "ws"
        url = f"{proto}://{host}/database/subscribe/{name_or_address}"

        headers = _build_auth_headers(auth_token)
        additional_headers = dict(headers) if headers else {}

        self._ws = await websockets.connect(
            url,
            subprotocols=[self.protocol],
            additional_headers=additional_headers,
        )

    def __aiter__(self):
        return self._ws.__aiter__()

    async def send(self, data):
        """Send *data* over the WebSocket (str or bytes)."""
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8")
        await self._ws.send(data)

    async def close(self):
        """Close the WebSocket connection gracefully."""
        if self._ws is not None:
            await self._ws.close()
