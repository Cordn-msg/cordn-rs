// Layer B (end-to-end) driver. Connects to a coordinator over a real relay
// (nak serve) using the TS cordnClient — the SAME client for both the TS and
// Rust servers — and times N round-trips of three representative tools:
//   - ListAvailableKeyPackages  (read, auth-bound, empty result)
//   - PostGroupMessage          (write, 1 insert + ack)
//   - FetchGroupMessages        (read, returns queued rows)
// Every tool goes through the full path: schnorr-signed request event → relay →
// server → relay → response, so the latency includes the transport/crypto floor
// that Layer A excludes. This is "what a real client feels".
//
// Run (the harness sets these): the client awaits connection on its first call,
// so no explicit readiness handshake is needed.
//   NODE_PATH=<repo>/references/cordn/node_modules \
//   CORDN_E2E_SERVER_PUBKEY=<hex> CORDN_E2E_CLIENT_PRIVATE_KEY=<hex> \
//   CORDN_E2E_RELAY=ws://localhost:10547 node bench/e2e/driver.ts

import { cordnClient } from "../../references/cordn/src/cli/coordinatorClient.ts";

const relay = process.env.CORDN_E2E_RELAY ?? "ws://localhost:10547";
const serverPubkey = process.env.CORDN_E2E_SERVER_PUBKEY;
const clientKey =
  process.env.CORDN_E2E_CLIENT_PRIVATE_KEY ??
  "0000000000000000000000000000000000000000000000000000000000000002";
const iterations = Number.parseInt(process.env.CORDN_E2E_ITERATIONS ?? "50", 10);
const warmup = Number.parseInt(process.env.CORDN_E2E_WARMUP ?? "10", 10);
// Hard per-call cap. A request that gets no matched response would otherwise
// await forever (the SDK callTool has no default timeout); this guarantees the
// driver always makes progress. 15s is far above any healthy round-trip.
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

// Race a call against a hard deadline so no single round-trip can hang forever.
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

async function main() {
  const client = new cordnClient({
    privateKey: clientKey,
    serverPubkey,
    relays: [relay],
  });

  const gid = "bench-e2e";
  const msg64 = (i) => Buffer.from(`bench-message-${i}`).toString("base64");

  const tools = [
    { name: "ListAvailableKeyPackages", fn: () => client.ListAvailableKeyPackages({}) },
    { name: "PostGroupMessage", fn: (i) => client.PostGroupMessage({ gid, msg_64: msg64(i) }) },
    { name: "FetchGroupMessages", fn: () => client.FetchGroupMessages({ gid }) },
  ];

  // Warmup: also retries, which absorbs the server-still-connecting race.
  for (let i = 0; i < warmup; i += 1) {
    for (const tool of tools) {
      await withRetry(`warmup ${tool.name}`, () => withDeadline(tool.name, tool.fn(i)), 20);
    }
  }

  console.log(
    `relay=${relay} iterations=${iterations} warmup=${warmup}`,
  );

  for (const tool of tools) {
    const samples = [];
    for (let i = 0; i < iterations; i += 1) {
      const start = process.hrtime.bigint();
      await withDeadline(tool.name, tool.fn(i));
      samples.push(Number(process.hrtime.bigint() - start) / 1_000_000);
    }
    samples.sort((a, b) => a - b);
    const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
    const p50 = percentile(samples, 0.5);
    const p99 = percentile(samples, 0.99);
    console.log(
      `  ${tool.name.padEnd(26)} n=${iterations} avg=${avg.toFixed(2)}ms p50=${p50.toFixed(2)}ms p99=${p99.toFixed(2)}ms`,
    );
  }

  await client.disconnect();
}

main().catch((error) => {
  console.error("e2e driver failed:", error);
  process.exitCode = 1;
});
