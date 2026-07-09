#!/usr/bin/env bash
# Layer A (core, in-process) benchmark harness — runs the TS coordinator core
# bench and its Rust mirror with the SAME scenario knobs, so the two outputs are
# directly comparable. No transport, no relay, no crypto; this isolates the
# language + storage-engine + concurrency model.
#
# Usage:
#     bench/core/run.sh                              # defaults (32/64/16/100)
#     CORDN_BENCH_GROUPS=64 CORDN_BENCH_ITERATIONS=50 bench/core/run.sh
#     bench/core/run.sh > my-run.txt 2>&1           # capture a run
#
# Env knobs (shared by both sides — the Rust example reads the same CORDN_BENCH_*
# names as the TS bench): CORDN_BENCH_GROUPS, CORDN_BENCH_BACKLOG,
# CORDN_BENCH_LIVE, CORDN_BENCH_ITERATIONS.
#
# What this does NOT do: merge the two outputs into a comparison table. That is
# the `bench/analyze` step; for now, read the two labeled blocks side by side.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "================================================================"
echo " Layer A — coordinator-core benchmark  (in-process, no transport)"
echo " scenario: groups=${CORDN_BENCH_GROUPS:-32} backlog=${CORDN_BENCH_BACKLOG:-64} live=${CORDN_BENCH_LIVE:-16} iterations=${CORDN_BENCH_ITERATIONS:-100}"
echo "================================================================"

echo
echo ">>> [1/2] TypeScript coordinator (references/cordn)"
echo "    ${REPO}/references/cordn/src/coordinator/storage/sqliteSubscriptionBenchmark.ts"
( cd "$REPO/references/cordn" && pnpm run bench:sqlite-subscriptions )

echo
echo ">>> [1b/2] TypeScript coordinator, in-memory backend (sqlite removed)"
echo "    ${REPO}/bench/core/bench-memory.ts"
NODE_PATH="$REPO/references/cordn/node_modules" node "$REPO/bench/core/bench-memory.ts"

echo
echo ">>> [2/2] Rust coordinator (cordn-core, release; both backends)"
echo "    ${REPO}/crates/cordn-core/examples/bench_subscriptions.rs"
( cd "$REPO" && CORDN_BENCH_BACKEND=both cargo run --release -p cordn-core --example bench_subscriptions )

echo
echo "Done. Compare the two blocks above (total/avg per scenario + speedup)."
