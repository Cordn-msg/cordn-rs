#!/usr/bin/env bash
# Layer A (core, in-process) post→subscriber delivery LATENCY — the wall time
# from a post to the matching record arriving on a live subscription. This is
# the latency view of the fan-out path (run-fanout.sh reports it as throughput).
#
# Usage:
#     bench/core/run-stream-latency.sh
#     CORDN_BENCH_MESSAGES=5000 bench/core/run-stream-latency.sh
#
# Env (shared by both sides): CORDN_BENCH_MESSAGES (2000),
#      CORDN_BENCH_BACKEND (both | sqlite | memory).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NODE_PATH="$REPO/references/cordn/node_modules"

echo "================================================================"
echo " Layer A — post→subscriber delivery latency"
echo " messages=${CORDN_BENCH_MESSAGES:-2000} backend=${CORDN_BENCH_BACKEND:-both}"
echo "================================================================"

echo
echo ">>> [1/2] TypeScript coordinator"
echo "    ${REPO}/bench/core/bench-stream-latency.ts"
( cd "$REPO" && NODE_PATH="$NODE_PATH" node bench/core/bench-stream-latency.ts )

echo
echo ">>> [2/2] Rust coordinator (cordn-core, release)"
echo "    ${REPO}/crates/cordn-core/examples/bench_stream_latency.rs"
( cd "$REPO" && cargo run --release -q -p cordn-core --example bench_stream_latency )

echo
echo "Done. Compare avg/p50/p99 µs across the two blocks."
