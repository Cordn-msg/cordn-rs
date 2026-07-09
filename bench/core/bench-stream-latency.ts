// Post→subscriber delivery latency (TS side), mirroring the Rust
// `bench_stream_latency.rs`. Times the wall clock from a `postGroupMessage`
// call to the matching record arriving on a live single-group subscription —
// the pub/sub hot-path handoff as per-message latency (avg/p50/p90/p99),
// complementing the fan-out bench's throughput view.
//
// Run:
//   NODE_PATH=<repo>/references/cordn/node_modules node bench/core/bench-stream-latency.ts
// Env: CORDN_BENCH_MESSAGES (2000), CORDN_BENCH_BACKEND (both | sqlite | memory)

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

function readBackends() {
  switch (process.env.CORDN_BENCH_BACKEND) {
    case "sqlite":
      return ["sqlite"];
    case "memory":
      return ["memory"];
    default:
      return ["sqlite", "memory"];
  }
}

function percentile(sorted, p) {
  return sorted[Math.min(sorted.length - 1, Math.round(p * (sorted.length - 1)))];
}

async function runBackend(backend, messages) {
  const storage =
    backend === "memory"
      ? new InMemoryCoordinatorStorage()
      : new SqliteCoordinatorStorage({ path: ":memory:" });
  const coordinator = new Coordinator({ storage });

  const subscription = coordinator.subscribeGroupMessages({ groupId: "g" });
  const iterator = subscription.messages[Symbol.asyncIterator]();

  const samples = []; // µs
  try {
    for (let index = 0; index < messages; index += 1) {
      const start = process.hrtime.bigint();
      coordinator.postGroupMessage({
        groupId: "g",
        opaqueMessage: createPrivateMessage({
          groupId: "g",
          epoch: 1n,
          contentType: 1,
          bytes: [index % 251],
        }),
      });
      await iterator.next();
      samples.push(Number(process.hrtime.bigint() - start) / 1000); // ns → µs
    }
  } finally {
    await iterator.return?.();
  }
  storage.close();

  samples.sort((a, b) => a - b);
  const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
  return {
    backend,
    avg,
    p50: percentile(samples, 0.5),
    p90: percentile(samples, 0.9),
    p99: percentile(samples, 0.99),
  };
}

async function main() {
  const messages = readPositiveIntEnv("CORDN_BENCH_MESSAGES", 2000);

  console.log("TypeScript cordn — post→subscriber delivery latency");
  console.log(`messages=${messages}`);
  console.log("");

  for (const backend of readBackends()) {
    const r = await runBackend(backend, messages);
    console.log(`── ${backend} ──────────────────────────────`);
    console.log(
      `  avg=${r.avg.toFixed(3)}µs  p50=${r.p50.toFixed(3)}µs  p90=${r.p90.toFixed(
        3,
      )}µs  p99=${r.p99.toFixed(3)}µs`,
    );
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
