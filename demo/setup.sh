#!/usr/bin/env bash
# =============================================================================
# SpacetimeDB Python SDK — Quickstart Chat Demo Setup
# =============================================================================
# Runs against a local SpacetimeDB instance at localhost:3000
#
# Prerequisites:
#   - SpacetimeDB CLI installed  (https://spacetimedb.com/install)
#   - Rust toolchain installed   (https://rustup.rs)
#   - Python 3.8+
#   - pip
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SDK_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVER_DIR="$SDK_ROOT/examples/quickstart/server"
CLIENT_DIR="$SDK_ROOT/examples/quickstart/client"
MODULE_NAME="chat"
HOST="localhost:3000"

# ── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║    SpacetimeDB Python SDK — Quickstart Chat Demo Setup       ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── 1. Check prerequisites ────────────────────────────────────────────────────
info "Checking prerequisites..."

command -v python3 >/dev/null 2>&1 || error "Python 3 is required but not found."
command -v pip3 >/dev/null 2>&1 || command -v pip >/dev/null 2>&1 || error "pip is required but not found."

if command -v pip3 >/dev/null 2>&1; then
    PIP="$(command -v pip3)"
else
    PIP="$(command -v pip)"
fi

command -v spacetime >/dev/null 2>&1 || error \
  "SpacetimeDB CLI not found.\n  Install it from: https://spacetimedb.com/install\n  Then re-run this script."

command -v cargo >/dev/null 2>&1 || error \
  "Rust toolchain not found.\n  Install it from: https://rustup.rs\n  Then add the wasm32-unknown-unknown target:\n    rustup target add wasm32-unknown-unknown"

success "All prerequisites found."

# ── Helper: check if local SpacetimeDB is reachable ──────────────────────────
check_spacetime_running() {
    # Prefer the CLI's own ping command; fall back to a raw TCP probe via curl.
    if spacetime server ping local >/dev/null 2>&1; then
        return 0
    fi
    # Some CLI versions don't have "server ping"; try a bare TCP connection.
    if curl -s --connect-timeout 3 "http://$HOST" >/dev/null 2>&1; then
        return 0
    fi
    # Last resort: check if anything is listening on port 3000.
    if command -v nc >/dev/null 2>&1; then
        nc -z -w3 localhost 3000 >/dev/null 2>&1 && return 0
    fi
    return 1
}

# ── 2. Check local SpacetimeDB is running ────────────────────────────────────
info "Checking SpacetimeDB is running at $HOST..."

if ! check_spacetime_running; then
    echo ""
    warn "SpacetimeDB does not appear to be running at $HOST."
    echo "  Start it in another terminal with:"
    echo ""
    echo "    spacetime start"
    echo ""
    read -r -p "  Press Enter once SpacetimeDB is running, or Ctrl+C to abort... "
    # Re-check
    check_spacetime_running || error \
      "Still cannot reach SpacetimeDB at $HOST.\n  Make sure 'spacetime start' is running and try again."
fi
success "SpacetimeDB is reachable at $HOST."

# ── 3. Install the Python SDK ─────────────────────────────────────────────────
info "Installing SpacetimeDB Python SDK..."

# Check if maturin is available (needed for building the Rust BSATN extension)
if command -v maturin >/dev/null 2>&1; then
    info "Building and installing SDK with native BSATN extension (maturin)..."
    cd "$SDK_ROOT"
    $PIP install -e . --quiet
else
    warn "maturin not found — installing SDK in pure-Python fallback mode."
    warn "For best performance, install maturin: pip install maturin"
    # Install dependencies manually, then add SDK source to PYTHONPATH at runtime
    "$PIP" install "websocket-client" "websockets>=10.0" --quiet 2>/dev/null || true
    # We'll use PYTHONPATH to make the SDK importable without building
    export PYTHONPATH="$SDK_ROOT/src:${PYTHONPATH:-}"
    # Persist for run.sh
    printf 'export PYTHONPATH="%s/src:${PYTHONPATH:-}"\n' "$SDK_ROOT" > "$SCRIPT_DIR/.env"
    info "SDK will be loaded directly from source: $SDK_ROOT/src"
fi

success "Python SDK ready."

# ── 4. Deploy the server module ───────────────────────────────────────────────
info "Publishing server module '$MODULE_NAME' to $HOST..."
echo "  (This compiles the Rust module to WebAssembly — may take a minute the first time)"
echo ""

cd "$SERVER_DIR"

# The --server flag takes a server *nickname* (e.g. "local"), not a URL.
# Try "local" (the default nickname for spacetime start) first, then fall
# back to an explicit URL for older CLI versions that accepted URLs directly.
if spacetime publish --server local "$MODULE_NAME" 2>/dev/null; then
    : # success — modern CLI with "local" server nickname
elif spacetime publish --server "http://$HOST" "$MODULE_NAME" 2>/dev/null; then
    : # older CLI that accepted bare URLs
else
    # Last resort: no --server flag; relies on whatever the CLI has configured
    spacetime publish "$MODULE_NAME"
fi

success "Module '$MODULE_NAME' published successfully."

# ── 5. Done ───────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    Setup Complete!                           ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Start the chat demo with:"
echo ""
echo "    ./demo/run.sh"
echo ""
echo "  In the chat:"
echo "    /name <username>   — set your display name"
echo "    <message>          — send a message"
echo "    (empty line)       — exit"
echo ""
