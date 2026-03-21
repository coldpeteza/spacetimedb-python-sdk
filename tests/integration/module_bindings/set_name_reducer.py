from typing import Callable, Optional

from spacetimedb_sdk.spacetimedb_client import SpacetimeDBClient, ReducerEvent

reducer_name = "set_name"


def set_name(name: str, then: Optional[Callable[["ReducerEvent"], None]] = None):
    SpacetimeDBClient.instance._reducer_call("set_name", name, then=then)


def _decode_args(data):
    return [str(data[0])]
