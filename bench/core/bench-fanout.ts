// Steady-state per-request fan-out benchmark (TS side), mirroring the Rust
// `bench_fanout.rs`. Opens storage ONCE, registers K live subscribers, then
// times only the M `postGroupMessage` calls (1 insert + K fan-out pushes each)
// — the cost a real serving loop pays per post request. Subscribers drain
// concurrently (untimed) so the post loop measures insert+fan-out, not drain.
//
// Run:
//   NODE_PATH=<repo>/references/cordn/node_modules node bench/core/bench-fanout.ts
// Env: CORDN_BENCH_FANOUT (8), CORDN_BENCH_MESSAGES (1000),
//      CORDN_BENCH_BACKEND (both | sqlite | memory)

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

async function runBackend(backend, fanout, messages) {
  const storage =
    backend === "memory"
      ? new InMemoryCoordinatorStorage()
      : new SqliteCoordinatorStorage({ path: ":memory:" });
  const coordinator = new Coordinator({ storage });

  const subscriptions = Array.from({ length: fanout }, () =>
    coordinator.subscribeGroupMessages({ groupId: "g" }),
  );

  // K concurrent drainers; suspended at their first `await .next()` so they
  // don't run during the synchronous post loop below.
  const drainers = subscriptions.map((subscription) =>
    consumeRecords(subscription.messages, messages),
  );

  const start = process.hrtime.bigint();
  for (let index = 0; index < messages; index += 1) {
    coordinator.postGroupMessage({
      groupId: "g",
      opaqueMessage: createPrivateMessage({
        groupId: "g",
        epoch: 1n,
        contentType: 1,
        bytes: [index % 251],
      }),
    });
  }
  const totalNs = Number(process.hrtime.bigint() - start);

  // Untimed drain + correctness (each subscriber received all `messages`).
  await Promise.all(drainers);
  storage.close();

  const totalMs = totalNs / 1_000_000;
  const secs = totalNs / 1_000_000_000;
  return {
    backend,
    totalMs,
    avgUs: (totalMs * 1000) / messages,
    msgsPerSec: messages / secs,
    deliveriesPerSec: (messages * fanout) / secs,
  };
}

async function main() {
  const fanout = readPositiveIntEnv("CORDN_BENCH_FANOUT", 8);
  const messages = readPositiveIntEnv("CORDN_BENCH_MESSAGES", 1000);

  console.log("TypeScript cordn — steady-state fan-out benchmark");
  console.log(`fanout=${fanout} messages=${messages}`);
  console.log("");

  for (const backend of readBackends()) {
    const r = await runBackend(backend, fanout, messages);
    console.log(`── ${backend} ──────────────────────────────`);
    console.log(
      `  total=${r.totalMs.toFixed(2)}ms  avg=${r.avgUs.toFixed(
        3,
      )}µs/post  msgs/sec=${r.msgsPerSec.toFixed(0)}  deliveries/sec=${r.deliveriesPerSec.toFixed(
        0,
      )}`,
    );
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
