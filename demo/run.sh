#!/usr/bin/env bash
# =============================================================================
# SpacetimeDB Python SDK — Quickstart Chat Demo Runner
# =============================================================================
# Run setup.sh first if you haven't already.
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SDK_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLIENT_DIR="$SDK_ROOT/examples/quickstart/client"

# Load environment set by setup.sh (e.g. PYTHONPATH for pure-Python mode)
if [ -f "$SCRIPT_DIR/.env" ]; then
    # shellcheck source=/dev/null
    source "$SCRIPT_DIR/.env"
fi

# Also ensure the SDK src is on PYTHONPATH as a fallback
export PYTHONPATH="$SDK_ROOT/src:${PYTHONPATH:-}"

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       SpacetimeDB Python SDK — Quickstart Chat Demo          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Commands:"
echo "    /name <username>   — set your display name"
echo "    <text>             — send a message"
echo "    (empty line)       — exit"
echo ""
echo "  Connecting to SpacetimeDB at localhost:3000, module: chat"
echo ""

# Verify SpacetimeDB is up — use the CLI's own ping, with TCP fallbacks
check_spacetime_running() {
    spacetime server ping local >/dev/null 2>&1 && return 0
    curl -s --connect-timeout 3 "http://localhost:3000" >/dev/null 2>&1 && return 0
    command -v nc >/dev/null 2>&1 && nc -z -w3 localhost 3000 >/dev/null 2>&1 && return 0
    return 1
}

if ! check_spacetime_running; then
    error "SpacetimeDB is not running at localhost:3000.\n  Start it with: spacetime start"
fi

# Check the module has been published
info "Connecting to module 'chat'..."

cd "$CLIENT_DIR"
python3 main.py
