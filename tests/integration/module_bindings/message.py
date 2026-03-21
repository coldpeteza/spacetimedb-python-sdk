from __future__ import annotations
from typing import List, Iterator, Callable

from spacetimedb_sdk.spacetimedb_client import SpacetimeDBClient, Identity
from spacetimedb_sdk.spacetimedb_client import ReducerEvent


class Message:
    """
    Reflects the ``Message`` table defined in tests/integration/module/src/lib.rs.

    Row encoding (SpacetimeDB 2.0 JSON protocol):
      data[0]  sender  Identity    →  ["<64-char hex>"]
      data[1]  sent    Timestamp   →  <integer microseconds>
      data[2]  text    String      →  "<text>"
    """

    is_table_class = True

    TABLE_NAME = "Message"

    @classmethod
    def register_row_update(
        cls, callback: Callable[[str, "Message", "Message", ReducerEvent], None]
    ):
        SpacetimeDBClient.instance._register_row_update(cls.TABLE_NAME, callback)

    @classmethod
    def iter(cls) -> Iterator["Message"]:
        return SpacetimeDBClient.instance._get_table_cache(cls.TABLE_NAME).values()

    @classmethod
    def filter_by_sender(cls, sender: Identity) -> List["Message"]:
        return [
            m
            for m in SpacetimeDBClient.instance._get_table_cache(
                cls.TABLE_NAME
            ).values()
            if m.sender == sender
        ]

    @classmethod
    def filter_by_text(cls, text: str) -> List["Message"]:
        return [
            m
            for m in SpacetimeDBClient.instance._get_table_cache(
                cls.TABLE_NAME
            ).values()
            if m.text == text
        ]

    def __init__(self, data: List[object]):
        self.data = {}
        self.data["sender"] = Identity.from_string(data[0][0])
        self.data["sent"] = int(data[1])
        self.data["text"] = str(data[2])

    def encode(self) -> List[object]:
        return [[str(self.sender)], self.sent, self.text]

    def __getattr__(self, name: str):
        return self.data.get(name)
