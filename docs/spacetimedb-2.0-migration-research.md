# Research: Updating spacetimedb-python-sdk to SpaceTimeDB 2.0

## Context

The current SDK targets SpaceTimeDB ~0.7.0 and uses the `v1.text.spacetimedb` JSON WebSocket protocol. SpaceTimeDB 2.0 introduces significant breaking changes to the wire protocol, authentication model, reducer event system, and SDK API surface. This is a research document outlining what would be involved in a full update.

---

## Scope of Changes Required

### 1. WebSocket Protocol Layer (`spacetime_websocket_client.py`)

**Current:** Negotiates only `v1.text.spacetimedb` (JSON text frames).

**Required changes:**
- Add support for `v2.bin.spacetimedb` (binary BSATN frames) as the preferred subprotocol
- Update subprotocol negotiation to offer all three: `v2.bin.spacetimedb`, `v1.bin.spacetimedb`, `v1.text.spacetimedb`
- Add binary frame handling (websocket-client supports binary frames; need to route them differently from text frames)
- Implement BSATN deserializer (or use a library if one exists for Python)

**File:** `src/spacetimedb_sdk/spacetime_websocket_client.py` (89 lines — small but needs significant logic added)

---

### 2. BSATN Serialization (New Module Required)

**Current:** No BSATN support — SDK uses JSON exclusively.

**Required changes:**
- Implement a BSATN encoder/decoder in Python:
  - `Bool`: `[0]` or `[1]`
  - Integers: little-endian fixed-width
  - Floats: raw IEEE 754 bit representation, little-endian
  - Strings: 4-byte little-endian length prefix + UTF-8 bytes
  - ProductValues (structs): concatenation of field BSATN encodings (no field names)
  - SumValues (enums/variants): tag byte + payload
  - Arrays/lists: 4-byte length prefix + repeated elements
- This likely warrants a new file: `src/spacetimedb_sdk/bsatn.py`
- This is the most significant piece of new work

---

### 3. Message Format (`spacetimedb_client.py`)

**Current:** Parses JSON `IdentityToken`, `SubscriptionUpdate`, `TransactionUpdate` messages.

**Required changes:**

**`TransactionUpdate` restructured:**
- Callers now see: `{ tag: 'Reducer', value: ReducerEvent }` with full reducer info
- Non-callers see: `{ tag: 'Transaction' }` with no reducer details
- Multiple subscription sets can be bundled into a single `TransactionUpdate`
- Update `_handle_transaction_update()` and `TransactionUpdateMessage` class

**`IdentityToken` → new auth model:**
- Server may now issue OIDC tokens rather than opaque tokens
- Token-Identity pairing is still maintained; handling logic is similar but token format may differ

**`SubscriptionUpdate` changes:**
- Should remain largely compatible at the structure level
- Subscription sets may be referenced differently in bundled `TransactionUpdate`

**File:** `src/spacetimedb_sdk/spacetimedb_client.py` (684 lines — extensive changes needed)

---

### 4. Reducer API Overhaul (`spacetimedb_client.py` + codegen)

**Current:** Reducers broadcast events to all subscribers via `TransactionUpdate`; SDK registers callbacks via `register_on_REDUCER_NAME()`.

**Required changes (breaking):**
- **Remove** `CallReducerFlags` and `light_mode` parameters
- **Remove** broadcast-style reducer callbacks (other clients no longer receive reducer details)
- **Add** per-call `_then()` callback pattern for the calling client's own reducer results
- **Add** Event Tables support — server publishes transient events via a new table type; clients subscribe to these instead of reducer broadcasts
- Update `_reducer_call()` to support `_then()` callback attachment
- Update `ClientCache` to handle event table rows (ephemeral, not persisted in cache)

**Files:**
- `src/spacetimedb_sdk/spacetimedb_client.py`
- `src/spacetimedb_sdk/client_cache.py`
- Example auto-generated bindings: `examples/quickstart/client/module_bindings/`

---

### 5. Authentication (`spacetime_websocket_client.py` + `local_config.py`)

**Current:** HTTP Basic Auth with `token:{auth_token}` base64-encoded; token stored in INI file.

**Required changes:**
- Support OIDC tokens (Bearer token format) in addition to legacy tokens
- Update `Authorization` header to use `Bearer {token}` if OIDC token detected
- SpacetimeAuth integration (optional managed OIDC provider) — may require an OAuth flow
- `local_config.py` may need to store OIDC refresh tokens alongside access tokens
- Consider token refresh logic (OIDC tokens expire; refresh tokens needed)

**Files:**
- `src/spacetimedb_sdk/spacetime_websocket_client.py`
- `src/spacetimedb_sdk/local_config.py`

---

### 6. Subscription API (`spacetimedb_client.py` + `spacetimedb_async_client.py`)

**Current:** `subscribe(queries: List[str])` accepts raw SQL strings only.

**Required changes:**
- Keep SQL string support for backwards compatibility
- Add typed query builder API (low priority / nice-to-have for Python; other SDKs have this)
- Handle subscription set bundling in `TransactionUpdate` responses
- Note: The async polling hack (100ms `asyncio.sleep` loop) could be replaced with proper async WebSocket library (`websockets` or `aiohttp`) — this would be a quality improvement alongside the 2.0 update

**Files:**
- `src/spacetimedb_sdk/spacetimedb_client.py`
- `src/spacetimedb_sdk/spacetimedb_async_client.py`

---

### 7. Code Generation / Module Bindings

**Current:** Auto-generated table and reducer classes use `primary_key` and JSON decode patterns.

**Required changes:**
- Update codegen (server-side `spacetime generate`) to produce 2.0-compatible bindings
- Table `name =` vs `accessor =` distinction affects generated Python class names/accessors
- Reducer classes need to support `_then()` callbacks instead of broadcast listeners
- Event table classes needed (new generated class type)
- Example bindings in `examples/quickstart/` will need regeneration

**Files:** `examples/quickstart/client/module_bindings/` (and any future `spacetime generate` output)

---

### 8. Dependency Updates (`pyproject.toml`)

- Consider replacing `websocket-client` (sync only) with `websockets` (async-native) to eliminate the Queue polling hack
- Add BSATN library if a suitable Python one exists, otherwise implement from scratch
- Bump version to `2.0.0` (or `0.8.0` following current versioning scheme)

---

## Effort Estimate (Relative Complexity)

| Area | Complexity | Notes |
|------|-----------|-------|
| BSATN encoder/decoder | High | New module, ~300-500 lines, needs thorough testing |
| Message format updates | High | Core protocol changes, affects all message handling |
| Reducer API overhaul | High | Breaking API change, affects user-facing SDK |
| WebSocket protocol negotiation | Medium | Small file but requires binary frame routing |
| Authentication (OIDC) | Medium | New auth flow, token refresh logic |
| Subscription API | Low-Medium | Mostly additive; SQL strings still work |
| Code generation updates | Medium | Requires running updated `spacetime generate` |
| Async refactor (optional) | Medium | Quality improvement, not strictly required |

**Overall:** This is a substantial rewrite of the core protocol and API layers. The SDK is relatively small (~1,300 lines) which helps, but the breaking changes touch nearly every file.

---

## Recommended Approach

Given the SDK is currently unmaintained and seeking community contributors:

1. **Phase 1**: Implement BSATN module + v2 binary protocol support (keeping v1 text as fallback)
2. **Phase 2**: Update message parsing for new TransactionUpdate format
3. **Phase 3**: Update reducer API (remove callbacks, add `_then()`, add event table support)
4. **Phase 4**: Update authentication for OIDC
5. **Phase 5**: Update examples and codegen output
6. **Phase 6** (optional): Async library refactor

---

## Key Files to Modify

- `src/spacetimedb_sdk/spacetime_websocket_client.py` — Protocol negotiation + binary frames
- `src/spacetimedb_sdk/spacetimedb_client.py` — Message parsing, reducer API, subscriptions
- `src/spacetimedb_sdk/spacetimedb_async_client.py` — Async wrapper updates
- `src/spacetimedb_sdk/client_cache.py` — Event table support
- `src/spacetimedb_sdk/local_config.py` — OIDC token storage
- `src/spacetimedb_sdk/bsatn.py` — **New file** for BSATN encoding/decoding
- `pyproject.toml` — Dependency and version updates
- `examples/quickstart/client/module_bindings/` — Regenerated bindings

---

## Testing Strategy

The repository currently has **zero test infrastructure** (no tests, no pytest config, no test dependencies). A test suite must be built from scratch alongside the 2.0 changes.

### Test Infrastructure to Add

- **Framework:** `pytest` + `pytest-asyncio` (for async client tests)
- **Mocking:** `unittest.mock` (stdlib) for WebSocket and network calls
- **Add to `pyproject.toml`:** `pytest`, `pytest-asyncio` as optional dev dependencies

### Unit Tests to Write

| Module | What to Test |
|--------|-------------|
| `bsatn.py` (new) | Encode/decode round-trips for all primitive types, strings, structs, enums, arrays |
| `spacetime_websocket_client.py` | Protocol negotiation (v1 text, v2 binary), auth header format, binary vs text frame routing |
| `spacetimedb_client.py` | `IdentityToken` parsing, `SubscriptionUpdate` parsing, `TransactionUpdate` parsing (Reducer vs Transaction tags), row insert/update/delete detection, `_then()` callback on reducer call |
| `client_cache.py` | Table cache insert/update/delete, event table ephemeral handling |
| `spacetimedb_async_client.py` | Async connect/close flow, event queue processing |
| `local_config.py` | Token read/write (legacy opaque token and OIDC Bearer token) |

### Integration / End-to-End Tests (Optional)

- Mock a full WebSocket server using `websockets` library to simulate SpaceTimeDB 2.0 message flows
- Test full connect → subscribe → receive update → call reducer → receive `_then()` callback cycle

### New Directory Structure

```
tests/
├── unit/
│   ├── test_bsatn.py
│   ├── test_websocket_client.py
│   ├── test_spacetimedb_client.py
│   ├── test_client_cache.py
│   ├── test_async_client.py
│   └── test_local_config.py
└── integration/
    └── test_mock_server.py  (optional)
```

---

## References

- [SpaceTimeDB 2.0 Migration Guide](https://spacetimedb.com/docs/2.0.0-rc1/upgrade/)
- [BSATN Format Docs](https://spacetimedb.com/docs/bsatn/)
- [Event Tables](https://spacetimedb.com/docs/tables/event-tables/)
- [Authentication](https://spacetimedb.com/docs/core-concepts/authentication/)
