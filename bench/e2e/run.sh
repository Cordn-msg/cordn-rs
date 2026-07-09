#!/usr/bin/env bash
# Layer B — end-to-end round-trip latency over a real relay (nak serve). Same TS
# client drives BOTH servers (one at a time), which share a private key ⇒ same
# server pubkey ⇒ identical client targeting. This is the path a real client
# feels: schnorr-signed request → relay → server → relay → response, including
# the transport/crypto floor that Layer A excludes.
#
# Prereqs:
#   - `nak serve` running on ws://localhost:10547 (start separately).
#   - Rust server built: cargo build --release -p cordn-server --features server
#     (produces target/release/cordn-server).
#
# Usage: bench/e2e/run.sh
# Knobs: CORDN_E2E_ITERATIONS (50), CORDN_E2E_WARMUP (10).
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NODE_PATH="$REPO/references/cordn/node_modules"
RELAY="ws://localhost:10547"
# Fixed test keys (seckey=1 / seckey=2). Both servers use sec=1 ⇒ same pubkey.
SERVER_SEC="0000000000000000000000000000000000000000000000000000000000000001"
CLIENT_SEC="0000000000000000000000000000000000000000000000000000000000000002"
SERVER_PUB="$(nak key public "$SERVER_SEC" 2>/dev/null)"
ITERATIONS="${CORDN_E2E_ITERATIONS:-50}"
WARMUP="${CORDN_E2E_WARMUP:-10}"

# Shared server config — identical for both impls so the comparison is fair.
export CORDN_SERVER_PRIVATE_KEY="$SERVER_SEC"
export CORDN_RELAY_URLS="$RELAY"
export CORDN_STORAGE_BACKEND="memory"
export CORDN_RATE_LIMIT_ENABLED="false"   # MUST: or you bench the limiter
export CORDN_ANNOUNCED="false"
export CORDN_MAX_KEY_PACKAGES_PER_IDENTITY="100000"

DRIVER_ENV=(
  "CORDN_E2E_SERVER_PUBKEY=$SERVER_PUB"
  "CORDN_E2E_CLIENT_PRIVATE_KEY=$CLIENT_SEC"
  "CORDN_E2E_RELAY=$RELAY"
  "CORDN_E2E_ITERATIONS=$ITERATIONS"
  "CORDN_E2E_WARMUP=$WARMUP"
  "NODE_PATH=$NODE_PATH"
)

# start_server <impl>  → echoes the server PID; logs to /tmp/cordn-e2e-<impl>.log
start_server() {
  local impl=$1 log="/tmp/cordn-e2e-${impl}.log"
  : > "$log"
  if [[ "$impl" == "ts" ]]; then
    # exec so $! is the node pid (killable directly).
    bash -c 'cd "$0" && exec node ./src/server/main.ts' "$REPO/references/cordn" >>"$log" 2>&1 &
  else
    "$REPO/target/release/cordn-server" >>"$log" 2>&1 &
  fi
  echo $!
}

# run_impl <impl> <label>
run_impl() {
  local impl=$1 label=$2 pid
  echo
  echo ">>> $label"
  pid="$(start_server "$impl")"
  # Give the server time to connect to the relay + subscribe. The driver's
  # warmup retries absorb any residual race.
  sleep 3
  env "${DRIVER_ENV[@]}" timeout 240 node "$REPO/bench/e2e/driver.ts" || {
    echo "    (driver failed or timed out; server log: /tmp/cordn-e2e-${impl}.log)"
  }
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

echo "================================================================"
echo " Layer B — end-to-end over $RELAY  (iterations=$ITERATIONS warmup=$WARMUP)"
echo " server pubkey: $SERVER_PUB"
echo "================================================================"

run_impl ts "TypeScript coordinator (references/cordn)"
run_impl rs "Rust coordinator (cordn-server, release)"

echo
echo "Done. Compare avg/p50/p99 across the two blocks."
