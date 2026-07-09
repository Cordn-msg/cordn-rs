// Layer B (end-to-end) CONCURRENT throughput / capacity driver. Same TS client
// drives both servers over a real relay (nak serve), but here it runs a
// CLOSED-LOOP load test: maintain W concurrent in-flight `PostGroupMessage`
// requests for a fixed duration, each completion firing the next. Reports
// sustained requests/sec (the headline capacity number) plus per-request
// latency percentiles (p50/p90/p99/p999) under load — the tails a real client
// sees when the server is busy. Sweeps a few concurrency levels.
//
// Single client, W concurrent in-flight calls: the SDK multiplexes by request
// id over one websocket, so this is genuine concurrent server load (N requests
// being processed at once) without N connections/keys.
//
// Run (the harness sets these):
//   NODE_PATH=<repo>/references/cordn/node_modules \
//   CORDN_E2E_SERVER_PUBKEY=<hex> CORDN_E2E_CLIENT_PRIVATE_KEY=<hex> \
//   CORDN_E2E_RELAY=ws://localhost:10547 node bench/e2e/driver-concurrent.ts

import { cordnClient } from "../../references/cordn/src/cli/coordinatorClient.ts";

const relay = process.env.CORDN_E2E_RELAY ?? "ws://localhost:10547";
const serverPubkey = process.env.CORDN_E2E_SERVER_PUBKEY;
const clientKey =
  process.env.CORDN_E2E_CLIENT_PRIVATE_KEY ??
  "0000000000000000000000000000000000000000000000000000000000000002";
const levels = (process.env.CORDN_E2E_CONCURRENCY ?? "1,8,32")
  .split(",")
  .map((s) => Number.parseInt(s.trim(), 10))
  .filter((n) => Number.isFinite(n) && n > 0);
const durationMs = Number.parseInt(process.env.CORDN_E2E_DURATION_MS ?? "8000", 10);
const warmup = Number.parseInt(process.env.CORDN_E2E_WARMUP ?? "5", 10);
const DEADLINE_MS = Number.parseInt(process.env.CORDN_E2E_DEADLINE_MS ?? "15000", 10);

if (!serverPubkey) {
  throw new Error("CORDN_E2E_SERVER_PUBKEY (hex) is required");
}

function percentile(sorted, p) {
  return sorted[Math.min(sorted.length - 1, Math.floor(p * (sorted.length - 1)))];
}

async function withRetry(label, fn, attempts) {
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      if (attempt === attempts) {
        throw new Error(`${label} failed after ${attempts} attempts: ${error}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 300 * attempt));
    }
  }
}

async function withDeadline(label, promise) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} timed out after ${DEADLINE_MS}ms`)),
          DEADLINE_MS,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

// Closed loop: `concurrency` workers, each serially fire-await-fire. Because
// there are `concurrency` of them in parallel, exactly `concurrency` requests
// stay in flight at all times → steady-state server load.
async function loadTest(client, concurrency, gid) {
  const samples = [];
  const deadline = Date.now() + durationMs;
  let counter = 0;

  const worker = async () => {
    while (Date.now() < deadline) {
      const i = counter;
      counter += 1;
      const msg64 = Buffer.from(`bench-load-${i}`).toString("base64");
      const start = process.hrtime.bigint();
      await withDeadline("PostGroupMessage", client.PostGroupMessage({ gid, msg_64: msg64 }));
      samples.push(Number(process.hrtime.bigint() - start) / 1_000_000); // ms
    }
  };

  const start = process.hrtime.bigint();
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  const elapsedSec = Number(process.hrtime.bigint() - start) / 1_000_000_000;

  samples.sort((a, b) => a - b);
  const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
  return {
    completed: samples.length,
    rps: samples.length / elapsedSec,
    avg,
    p50: percentile(samples, 0.5),
    p90: percentile(samples, 0.9),
    p99: percentile(samples, 0.99),
    p999: percentile(samples, 0.999),
  };
}

async function main() {
  const client = new cordnClient({ privateKey: clientKey, serverPubkey, relays: [relay] });
  const gid = "bench-concurrent";

  // Warmup: also retries, absorbing the server-still-connecting race.
  for (let i = 0; i < warmup; i += 1) {
    await withRetry(
      "warmup",
      () =>
        withDeadline(
          "warmup",
          client.PostGroupMessage({ gid, msg_64: Buffer.from(`warmup-${i}`).toString("base64") }),
        ),
      20,
    );
  }

  console.log(`relay=${relay} tool=PostGroupMessage duration=${durationMs}ms warmup=${warmup}`);
  console.log(
    `  ${"concurrency".padStart(11)} ${"completed".padStart(10)} ${"req/s".padStart(9)} ${"avg".padStart(7)} ${"p50".padStart(7)} ${"p90".padStart(7)} ${"p99".padStart(7)} ${"p99.9".padStart(7)}`,
  );

  for (const c of levels) {
    const r = await loadTest(client, c, gid);
    console.log(
      `  ${String(c).padStart(11)} ${String(r.completed).padStart(10)} ${r.rps.toFixed(1).padStart(9)} ${`${r.avg.toFixed(2)}ms`.padStart(7)} ${`${r.p50.toFixed(2)}ms`.padStart(7)} ${`${r.p90.toFixed(2)}ms`.padStart(7)} ${`${r.p99.toFixed(2)}ms`.padStart(7)} ${`${r.p999.toFixed(2)}ms`.padStart(7)}`,
    );
  }

  await client.disconnect();
}

main().catch((error) => {
  console.error("e2e concurrent driver failed:", error);
  process.exitCode = 1;
});
