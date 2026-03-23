# SpacetimeDB Python SDK — Quickstart Chat Demo

A real-time multi-user chat app running against your local SpacetimeDB instance.

## What it does

- Connects to a local SpacetimeDB server at `localhost:3000`
- Deploys a Rust module with two tables: **User** and **Message**
- Runs a Python async client that subscribes to live updates
- Multiple terminal windows = multiple chat participants in real-time

## Prerequisites

| Tool | Install |
|------|---------|
| **SpacetimeDB CLI** | https://spacetimedb.com/install |
| **Rust toolchain** | https://rustup.rs — then run `rustup target add wasm32-unknown-unknown` |
| **Python 3.8+** | https://python.org |
| **pip** | Usually bundled with Python |

## Quick Start

### 1. Start SpacetimeDB locally

```bash
spacetime start
```

Leave this running in its own terminal.

### 2. Run setup (once)

From the root of this repo:

```bash
./demo/setup.sh
```

This will:
- Verify your SpacetimeDB instance is running
- Install the Python SDK
- Compile and publish the Rust chat module to your local SpacetimeDB

> **First run:** Compiling the Rust module to WebAssembly takes ~1 minute. Subsequent runs are instant.

### 3. Run the chat client

```bash
./demo/run.sh
```

Open multiple terminal windows and run `./demo/run.sh` in each one to chat between clients!

## Chat Commands

| Input | Action |
|-------|--------|
| `/name <username>` | Set your display name |
| Any text | Send a message |
| Empty line (Enter) | Exit |

## How it works

```
┌─────────────────────┐         WebSocket (ws://)        ┌────────────────────────────────┐
│   Python Client     │ ◄──────────────────────────────► │   SpacetimeDB (localhost:3000) │
│  (main.py)          │                                   │                                │
│                     │   Reducer calls (set_name,        │   Module: "chat"               │
│  SpacetimeDBAsync   │   send_message)                   │   ┌─────────┐  ┌──────────┐   │
│  Client             │ ──────────────────────────────►   │   │  User   │  │ Message  │   │
│                     │                                   │   └─────────┘  └──────────┘   │
│  Subscribes to:     │   Table updates (INSERT/UPDATE)   │                                │
│  SELECT * FROM User │ ◄──────────────────────────────   │   Reducers:                    │
│  SELECT * FROM Msg  │                                   │   • identity_connected         │
└─────────────────────┘                                   │   • identity_disconnected      │
                                                          │   • set_name                   │
                                                          │   • send_message               │
                                                          └────────────────────────────────┘
```

### Server module (`examples/quickstart/server/src/lib.rs`)

A Rust SpacetimeDB module with:
- **User table** — tracks connected clients (identity, display name, online status)
- **Message table** — stores all chat messages (sender, timestamp, text)
- **Reducers** — server-side functions callable from the client

### Python client (`examples/quickstart/client/main.py`)

Uses `SpacetimeDBAsyncClient` to:
1. Connect and authenticate (token persisted in `~/.spacetimedb-python-quickstart/`)
2. Subscribe to SQL queries for live data sync
3. Register row-update callbacks to print events
4. Schedule an input-polling loop to send messages/name changes

## Troubleshooting

**"SpacetimeDB is not running"**
```bash
spacetime start
```

**"Module not found" / connection errors**
Re-run setup to republish the module:
```bash
./demo/setup.sh
```

**"Cannot find module_bindings"**
Make sure you're running `run.sh` from the repo root, not from inside the `client/` directory.

**Build errors with the Rust module**
Ensure you have the wasm target installed:
```bash
rustup target add wasm32-unknown-unknown
```

**Slow first startup**
The first `spacetime publish` compiles Rust to WebAssembly — this is normal and takes ~1 minute. Subsequent publishes use incremental compilation.
