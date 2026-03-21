from __future__ import annotations
from typing import List, Iterator, Callable, Optional

from spacetimedb_sdk.spacetimedb_client import SpacetimeDBClient, Identity, Address
from spacetimedb_sdk.spacetimedb_client import ReducerEvent


class User:
    """
    Reflects the ``User`` table defined in tests/integration/module/src/lib.rs.

    Row encoding (SpacetimeDB 2.0 JSON protocol):
      data[0]  identity  Identity        →  ["<64-char hex>"]
      data[1]  name      Option<String>  →  {"0": "value"} | {}
      data[2]  online    bool            →  true | false
    """

    is_table_class = True
    primary_key = "identity"

    # Table name as sent by the SpacetimeDB server.  With the 1.0 macro
    # ``#[spacetimedb::table(public)]`` on a struct named ``User`` the server
    # uses the struct name as-is.
    TABLE_NAME = "User"

    @classmethod
    def register_row_update(
        cls, callback: Callable[[str, "User", "User", ReducerEvent], None]
    ):
        SpacetimeDBClient.instance._register_row_update(cls.TABLE_NAME, callback)

    @classmethod
    def iter(cls) -> Iterator["User"]:
        return SpacetimeDBClient.instance._get_table_cache(cls.TABLE_NAME).values()

    @classmethod
    def filter_by_identity(cls, identity: Identity) -> Optional["User"]:
        return next(
            (
                u
                for u in SpacetimeDBClient.instance._get_table_cache(
                    cls.TABLE_NAME
                ).values()
                if u.identity == identity
            ),
            None,
        )

    @classmethod
    def filter_by_online(cls, online: bool) -> List["User"]:
        return [
            u
            for u in SpacetimeDBClient.instance._get_table_cache(
                cls.TABLE_NAME
            ).values()
            if u.online == online
        ]

    def __init__(self, data: List[object]):
        self.data = {}
        self.data["identity"] = Identity.from_string(data[0][0])
        self.data["name"] = str(data[1]["0"]) if "0" in data[1] else None
        self.data["online"] = bool(data[2])

    def encode(self) -> List[object]:
        return [
            [str(self.identity)],
            {"0": self.name} if self.name is not None else {},
            self.online,
        ]

    def __getattr__(self, name: str):
        return self.data.get(name)
