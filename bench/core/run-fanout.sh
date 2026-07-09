#!/usr/bin/env bash
# Layer A (core, in-process) steady-state fan-out benchmark — the per-request
# cost a real serving loop pays: each post = 1 storage insert + fan-out to K live
# subscribers. Storage is opened ONCE (unlike run.sh's per-iteration seed), so
# this isolates steady-state serving cost, not bulk-seed insert throughput.
#
# Usage:
#     bench/core/run-fanout.sh                                  # defaults (8 / 1000)
#     CORDN_BENCH_FANOUT=64 CORDN_BENCH_MESSAGES=5000 bench/core/run-fanout.sh
#
# Env (shared by both sides): CORDN_BENCH_FANOUT, CORDN_BENCH_MESSAGES,
# CORDN_BENCH_BACKEND (both | sqlite | memory).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NODE_PATH="$REPO/references/cordn/node_modules"

echo "================================================================"
echo " Layer A — steady-state fan-out (open storage once; per-post cost)"
echo " fanout=${CORDN_BENCH_FANOUT:-8} messages=${CORDN_BENCH_MESSAGES:-1000} backend=${CORDN_BENCH_BACKEND:-both}"
echo "================================================================"

echo
echo ">>> [1/2] TypeScript coordinator"
echo "    ${REPO}/bench/core/bench-fanout.ts"
( cd "$REPO" && NODE_PATH="$NODE_PATH" node bench/core/bench-fanout.ts )

echo
echo ">>> [2/2] Rust coordinator (cordn-core, release)"
echo "    ${REPO}/crates/cordn-core/examples/bench_fanout.rs"
( cd "$REPO" && cargo run --release -p cordn-core --example bench_fanout )

echo
echo "Done. Compare avg µs/post and msgs/sec across the two blocks."
