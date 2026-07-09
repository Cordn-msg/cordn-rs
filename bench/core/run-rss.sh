#!/usr/bin/env bash
# Peak RSS comparison (Layer A, memory backend). Runs the fan-out bench under
# GNU time at two scales — a tiny baseline (process + coordinator init only) and
# a retained-data load (N messages held in storage) — and reports the peak
# resident set (VmHWM) for each implementation.
#
# The "load" number includes the bench's transiently-buffered in-flight records
# (drained after posting), so read it as "peak RSS under this sustained post+
# retain load", not pure idle footprint — but both sides buffer the same logical
# records, so the relative comparison is fair and the per-record overhead
# difference (compact Rust vs V8 objects) shows up in the delta.
#
# Usage: bench/core/run-rss.sh
# Requires: /usr/bin/time (GNU time).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NODE_PATH="$REPO/references/cordn/node_modules"
BIN="$REPO/target/release/examples/bench_fanout"

rss_kb() { /usr/bin/time -v "$@" 2>&1 | awk '/Maximum resident set size/ { print $NF }'; }

run() { # label  impl  fanout  messages
  local label=$1 impl=$2 fanout=$3 messages=$4
  local kb
  if [[ "$impl" == "rust" ]]; then
    kb=$(CORDN_BENCH_BACKEND=memory CORDN_BENCH_FANOUT=$fanout CORDN_BENCH_MESSAGES=$messages rss_kb "$BIN")
  else
    kb=$(cd "$REPO" && CORDN_BENCH_BACKEND=memory CORDN_BENCH_FANOUT=$fanout CORDN_BENCH_MESSAGES=$messages \
      NODE_PATH="$NODE_PATH" rss_kb node bench/core/bench-fanout.ts)
  fi
  local mb
  mb=$(awk -v k="$kb" 'BEGIN { printf "%.1f", k/1024 }')
  printf '  %-10s %-6s fanout=%-4s messages=%-7s -> %8s MB\n' "$label" "$impl" "$fanout" "$messages" "$mb"
}

echo "================================================================"
echo " Peak RSS — cordn-core fan-out bench (GNU time VmHWM, memory backend)"
echo "================================================================"
run baseline rust 1 10
run baseline ts   1 10
run load     rust 1 100000
run load     ts   1 100000
echo
echo "(load retains 100k messages in storage + transient in-flight buffer)"
