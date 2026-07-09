#!/usr/bin/env bash
# Layer A (core, in-process) CONCURRENCY scaling — N workers each post M messages
# to a distinct group; report aggregate posts/sec + deliveries/sec as logical
# concurrency increases. Shows the single-writer ceiling: the coordinator
# serializes writes (mutex), so throughput is per-op-efficiency-bound, not
# parallelism-bound. See docs/benchmark-results.md.
#
# NOTE on runtime models: the Rust example uses the multi_thread runtime; TS is
# single-threaded. This is intentional (see the example/TS module docs) — the
# point is to surface each impl's concurrency-scaling ceiling.
#
# Usage:
#     bench/core/run-concurrency.sh
#     CORDN_BENCH_CONCURRENCY=1,8,32,128 CORDN_BENCH_MESSAGES=2000 bench/core/run-concurrency.sh
#
# Env (shared): CORDN_BENCH_CONCURRENCY ("1,4,16,64"),
#               CORDN_BENCH_MESSAGES (500), CORDN_BENCH_BACKEND (memory recommended).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NODE_PATH="$REPO/references/cordn/node_modules"

echo "================================================================"
echo " Layer A — concurrency scaling (N parallel posters)"
echo " concurrency=${CORDN_BENCH_CONCURRENCY:-1,4,16,64} messages/worker=${CORDN_BENCH_MESSAGES:-500} backend=${CORDN_BENCH_BACKEND:-memory}"
echo "================================================================"

echo
echo ">>> [1/2] TypeScript coordinator"
echo "    ${REPO}/bench/core/bench-concurrency.ts"
( cd "$REPO" && NODE_PATH="$NODE_PATH" node bench/core/bench-concurrency.ts )

echo
echo ">>> [2/2] Rust coordinator (cordn-core, release, multi_thread)"
echo "    ${REPO}/crates/cordn-core/examples/bench_concurrency.rs"
( cd "$REPO" && cargo run --release -q -p cordn-core --example bench_concurrency )

echo
echo "Done. Compare posts/sec across concurrency levels and impls."
