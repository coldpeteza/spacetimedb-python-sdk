from typing import Callable, Optional

from spacetimedb_sdk.spacetimedb_client import SpacetimeDBClient, ReducerEvent

reducer_name = "send_message"


def send_message(text: str, then: Optional[Callable[["ReducerEvent"], None]] = None):
    SpacetimeDBClient.instance._reducer_call("send_message", text, then=then)


def _decode_args(data):
    return [str(data[0])]
