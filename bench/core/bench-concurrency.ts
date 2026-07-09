// Concurrency scaling (TS side), mirroring the Rust `bench_concurrency.rs`.
// N workers each post M messages to a DISTINCT group (one live subscriber
// draining per group, untimed); we report aggregate posts/sec + deliveries/sec
// across concurrency levels.
//
// Runtime-model caveat (see the Rust module docs): Node runs on a single
// thread, so these workers run their sync post loops sequentially regardless of
// `concurrency` — throughput is capped near one core. The Rust mirror uses the
// multi_thread runtime. So this comparison shows each impl's CONCURRENCY-SCALING
// ceiling, not an apples-to-apples runtime match. For writes the coordinator is
// single-writer by design (mutex), so even Rust serializes on the post path —
// the headline here is per-op efficiency, not parallelism. See
// docs/benchmark-results.md.
//
// Run:
//   NODE_PATH=<repo>/references/cordn/node_modules node bench/core/bench-concurrency.ts
// Env: CORDN_BENCH_CONCURRENCY ("1,4,16,64"), CORDN_BENCH_MESSAGES (500),
//      CORDN_BENCH_BACKEND (both | sqlite | memory; memory recommended).

import { Coordinator } from "../../references/cordn/src/coordinator/coordinator.ts";
import { InMemoryCoordinatorStorage } from "../../references/cordn/src/coordinator/storage/inMemoryStorage.ts";
import { SqliteCoordinatorStorage } from "../../references/cordn/src/coordinator/storage/sqliteStorage.ts";
import { createPrivateMessage } from "../../references/cordn/src/coordinator/testUtils.ts";

function readPositiveIntEnv(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Environment variable ${name} must be a positive integer`);
  }
  return parsed;
}

function readConcurrencyLevels() {
  const raw = process.env.CORDN_BENCH_CONCURRENCY;
  if (!raw) {
    return [1, 4, 16, 64];
  }
  return raw
    .split(",")
    .map((s) => Number.parseInt(s.trim(), 10))
    .filter((n) => Number.isFinite(n) && n > 0);
}

function readBackends() {
  switch (process.env.CORDN_BENCH_BACKEND) {
    case "sqlite":
      return ["sqlite"];
    case "memory":
      return ["memory"];
    default:
      return ["memory"]; // sqlite is single-writer; default to memory
  }
}

async function consumeRecords(messages, expectedCount) {
  const iterator = messages[Symbol.asyncIterator]();
  try {
    for (let index = 0; index < expectedCount; index += 1) {
      const result = await iterator.next();
      if (result.done) {
        throw new Error(`ended early after ${index}, expected ${expectedCount}`);
      }
    }
  } finally {
    await iterator.return?.();
  }
}

async function runLevel(backend, concurrency, messages) {
  const storage =
    backend === "memory"
      ? new InMemoryCoordinatorStorage()
      : new SqliteCoordinatorStorage({ path: ":memory:" });
  const coordinator = new Coordinator({ storage });

  const drainers = []; // untimed correctness checks (async, await iterator.next())
  const workerFns = []; // timed post loops (deferred so timing is honest)
  for (let w = 0; w < concurrency; w += 1) {
    const gid = `g${w}`;
    const subscription = coordinator.subscribeGroupMessages({ groupId: gid });
    drainers.push(consumeRecords(subscription.messages, messages));
    // Deferred (not IIFE'd): the post loop has no `await`, so on Node's single
    // thread these run sequentially regardless of `concurrency`. That is the
    // honest TS result — no write parallelism — which is the point of the
    // comparison with the multi_thread Rust mirror.
    workerFns.push(() => {
      for (let i = 0; i < messages; i += 1) {
        coordinator.postGroupMessage({
          groupId: gid,
          opaqueMessage: createPrivateMessage({
            groupId: gid,
            epoch: 1n,
            contentType: 1,
            bytes: [i % 251],
          }),
        });
      }
    });
  }

  const start = process.hrtime.bigint();
  for (const fn of workerFns) {
    fn();
  }
  const totalNs = Number(process.hrtime.bigint() - start);
  await Promise.all(drainers); // untimed: every delivery landed
  storage.close();

  const secs = totalNs / 1_000_000_000;
  const total = concurrency * messages;
  return { totalMs: totalNs / 1_000_000, postsPerSec: total / secs, deliveriesPerSec: total / secs };
}

async function main() {
  const messages = readPositiveIntEnv("CORDN_BENCH_MESSAGES", 500);
  const levels = readConcurrencyLevels();

  console.log("TypeScript cordn — concurrency scaling (single-threaded event loop)");
  console.log(`messages/worker=${messages} concurrency=${JSON.stringify(levels)}`);
  console.log("");

  for (const backend of readBackends()) {
    console.log(`── ${backend} ──────────────────────────────`);
    console.log(
      `  ${"workers".padStart(10)} ${"total_ms".padStart(12)} ${"posts/sec".padStart(14)} ${"deliv/sec".padStart(14)}`,
    );
    for (const c of levels) {
      const r = await runLevel(backend, c, messages);
      console.log(
        `  ${String(c).padStart(10)} ${r.totalMs.toFixed(2).padStart(12)} ${r.postsPerSec.toFixed(0).padStart(14)} ${r.deliveriesPerSec.toFixed(0).padStart(14)}`,
      );
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
