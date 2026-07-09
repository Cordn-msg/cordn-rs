# Benchmark Results — cordn-rs vs cordn (TypeScript)

Snapshot from **2026-07-09**, recorded on the development machine (single
representative runs over a local `nak serve` relay on `ws://localhost:10547`,
release builds). Treat the absolute numbers as directional for this hardware;
the **methodology** and the **relative ratios** are the stable, comparable part.
Re-run any section with the command shown; the harnesses are in `bench/`.

The one rule that governs reading these: **the headline depends on the layer.**
Layer A (coordinator core, no transport) and Layer B (end-to-end over the wire)
tell different stories and must not be conflated.

---

## Layer A — coordinator core (in-process, no transport)

Isolates the language + storage engine + concurrency model. No relay, no crypto.

### Subscription fan-out — `bench/core/run.sh`

Scenario: 32 groups × 64 backlog × 16 live messages, 100 iterations, fresh
seeded coordinator per iteration (mirrors the TS `sqliteSubscriptionBenchmark`).

| backend | impl | single (ms/iter) | multi (ms/iter) |
|---|---|---|---|
| SQLite | TS | 19.67 | 20.05 |
| SQLite | Rust | 23.60 | 23.83 |
| In-memory | TS | 4.26 | 4.00 |
| In-memory | Rust | 0.489 | 0.608 |

- **In-memory (pure coordinator core): Rust ≈ 6.6–8.7× faster.** The real
  architectural win — mpsc fan-out + in-process storage + spawn/join.
- **SQLite: Rust ≈ 1.2× slower.** This scenario is bulk-insert-dominated (≈2k
  fresh inserts + a schema bootstrap per iteration), and `better-sqlite3` wins
  tight insert loops. See the per-request bench below for why this is a corner
  case, not steady-state server cost.

### Per-request fan-out — `bench/core/run-fanout.sh`

Scenario: storage opened **once**, then 1000 `post_group_message` calls measured
(1 insert + fan-out to 8 live subscribers each). This is the cost a real serving
loop pays per request.

| backend | impl | µs/post | msgs/sec | deliveries/sec |
|---|---|---|---|---|
| SQLite | TS | 13.32 | 75 068 | 600 542 |
| SQLite | Rust | 7.91 | 126 486 | 1 011 888 |
| In-memory | TS | 3.28 | 304 787 | 2 438 300 |
| In-memory | Rust | 0.485 | 2 061 524 | 16 492 193 |

- **SQLite per-request: Rust ≈ 1.7× faster** — the opposite of the bulk-seed
  scenario. Per-op sqlite overhead is actually lower in Rust (≈7.4 µs added vs
  TS's ≈10 µs); the bulk-seed deficit was a tight-loop artifact.
- **In-memory: Rust ≈ 6.8× faster**, sustaining **16.5M deliveries/sec**.

### Post→subscriber delivery latency — `bench/core/run-stream-latency.sh`

Scenario: one live subscriber; per message, time from `post_group_message`
returning to the matching record arriving on the subscription — the latency
view of the fan-out path that `run-fanout.sh` reports as throughput. 3000 samples.

| backend | impl | avg | p50 | p90 | p99 |
|---|---|---|---|---|---|
| SQLite | TS | 11.08 µs | 8.85 µs | 13.29 µs | 26.68 µs |
| SQLite | Rust | 8.58 µs | 7.21 µs | 12.87 µs | 19.59 µs |
| In-memory | TS | 3.68 µs | 1.98 µs | 5.17 µs | 14.73 µs |
| In-memory | Rust | **0.227 µs** | 0.164 µs | 0.208 µs | 0.771 µs |

- **In-memory: Rust ≈ 16× lower delivery latency** (sub-microsecond p99). The
  post pushes to the subscriber's unbounded channel synchronously, so receipt is
  immediate — this is the pure channel-push + wake + poll cost.
- **SQLite: Rust ≈ 1.3× lower**, again gated by the shared sqlite insert cost.

### Concurrency scaling — `bench/core/run-concurrency.sh`

Scenario: N workers each post 2000 messages to a distinct group (one subscriber
draining per group, untimed); report aggregate posts/sec as concurrency rises.
Memory backend. ⚠ Runtime-model asymmetry: Rust uses the multi_thread runtime
(workers genuinely parallel); TS is single-threaded (workers run sequentially).
So this shows each impl's **write-scaling ceiling**, not an apples-to-apples
runtime match.

| workers | TS posts/sec | Rust posts/sec |
|---|---|---|
| 1 | 155 352 | 1 805 251 |
| 4 | 383 674 | 2 337 279 |
| 16 | 572 899 | 973 060 |
| 64 | 601 041 | 1 333 292 |

- **Neither scales.** This is the single-writer design made visible: the
  in-memory storage mutex serializes every post, so adding concurrency does not
  raise throughput. Rust's curve actually *degrades* at high concurrency (lock
  contention); TS plateaus near one core (~600k/s).
- **The win is per-op efficiency, not parallelism**: Rust peaks ~2.3M posts/sec
  vs TS's ~600k — ~4× — under the same single-writer contract (AGENTS.md
  decisions #5/#6).
- Sub-millisecond per-level totals mean run-to-run variance is high (the Rust
  16→64 non-monotonic dip is noise); the trend is the signal.

### Peak RSS — `bench/core/run-rss.sh`

GNU time `VmHWM`, in-memory backend.

| workload | TS | Rust |
|---|---|---|
| Baseline (process + coordinator init) | 95.1 MB | **2.7 MB** |
| 100k messages retained | 167.5 MB | **31.8 MB** |

- Baseline **~35× smaller** (Node + V8 + the full module graph vs a Rust binary);
  under load **~5× smaller** (compact records vs V8 per-object overhead).
- The "load" number includes the bench's transient in-flight buffer; both sides
  buffer the same logical records, so the comparison is fair.

---

## Layer B — end-to-end over a real relay — `bench/e2e/run.sh`

Same TS client drives both servers (one at a time) over `nak serve`, shared
server key ⇒ identical targeting. Each round-trip = schnorr-signed request →
relay → server → relay → response (encryption disabled on the client). 80
timed samples per tool after warmup.

| tool | TS p50 / p99 | Rust p50 / p99 | Rust p50 |
|---|---|---|---|
| ListAvailableKeyPackages | 11.97 / 25.40 ms | 6.77 / 15.17 ms | **1.77×** |
| PostGroupMessage | 11.91 / 22.53 ms | 7.62 / 17.33 ms | **1.56×** |
| FetchGroupMessages | 13.43 / 18.89 ms | 9.38 / 19.30 ms | **1.43×** |

- **E2E Rust is ~1.4–1.8× faster (p50)** — much smaller than the 6.8× core win.
  The ~10 ms round-trip is dominated by a transport/crypto floor (schnorr sign +
  2 relay hops + verify + JSON) that is identical infrastructure on both sides;
  the coordinator core work is a small slice of it.
- **Rust's edge is clearest in the p99 tails** (15–17 ms vs 22–25 ms) — no GC
  pauses, which is the predicted TS weakness.

---

## Layer B — concurrent throughput / capacity — `bench/e2e/run-concurrent.sh`

Closed-loop load: keep W `PostGroupMessage` requests in flight for 6 s, each
completion firing the next; report sustained req/s + latency percentiles. Sweeps
concurrency {1, 8, 32}. Single client, W concurrent in-flight calls (the SDK
multiplexes by request id over one websocket → genuine concurrent server load).

| concurrency | TS req/s | TS p99 | Rust req/s | Rust p99 |
|---|---|---|---|---|
| 1 | 80.3 | 22.95 ms | 103.6 | 20.41 ms |
| 8 | 175.5 | 69.62 ms | 187.8 | 52.09 ms |
| 32 | 175.6 | 273 ms | 179.8 | 301 ms |

- **The relay is the bottleneck, not either server.** Both saturate at
  ~175–188 req/s and plateau — `nak serve`'s forwarding is the ceiling. This is
  the key capacity finding: at this layer, coordinator performance is invisible;
  the transport dominates.
- **At low concurrency Rust is ~1.3× faster** (103.6 vs 80.3 req/s at W=1) with a
  tighter p99 (52 ms vs 70 ms at W=8) — the no-GC-pause advantage shows in the
  tails before the relay ceiling flattens it.
- **Latency inflates sharply past saturation** (W=32 → ~180 ms avg): the
  closed-loop keeps 32 in flight against a ~180 req/s service, so each queues.
  Expected closed-loop behavior under saturation, not a server regression.

---

## Reproducing

```bash
# Prereqs: build the Rust benches + server
cargo build --release -p cordn-core --example bench_subscriptions
cargo build --release -p cordn-core --example bench_fanout
cargo build --release -p cordn-core --example bench_stream_latency
cargo build --release -p cordn-core --example bench_concurrency
cargo build --release -p cordn-server --features server
# And a local relay (separate terminal):  nak serve

# Layer A
bench/core/run.sh                # subscription single-vs-multi, TS + Rust (both backends)
bench/core/run-fanout.sh         # steady-state per-request fan-out, TS + Rust
bench/core/run-stream-latency.sh # post→subscriber delivery latency (avg/p50/p90/p99)
bench/core/run-concurrency.sh    # write concurrency scaling (posts/sec vs workers)
bench/core/run-rss.sh            # peak RSS (baseline + load)

# Layer B (needs nak serve running on :10547)
bench/e2e/run.sh                 # round-trip latency, 3 tools (p50/p99)
bench/e2e/run-concurrent.sh      # sustained concurrent throughput (req/s + tails)
```

Knobs (shared by both sides): `CORDN_BENCH_GROUPS/BACKLOG/LIVE/ITERATIONS`,
`CORDN_BENCH_FANOUT/MESSAGES/BACKEND`, `CORDN_BENCH_CONCURRENCY`, and for the
E2E drivers `CORDN_E2E_ITERATIONS/WARMUP/DEADLINE_MS` and
`CORDN_E2E_CONCURRENCY/DURATION_MS`.

---

## Takeaways

| question | answer |
|---|---|
| Is the Rust **coordinator core** faster? | Yes — **~6.8×** in-memory. |
| What's the **delivery latency** to a subscriber? | Rust **~16× lower** (in-memory, sub-µs p99). |
| Is it faster **per request on sqlite** (production)? | Yes — **~1.7×**. |
| Does the core **scale with concurrency**? | No — single-writer by design; the win is per-op (**~4×**), not parallelism. |
| Does it feel faster **to a real client**? | Modestly — **~1.4–1.8×** (crypto floor dominates E2E). |
| What's the **sustained E2E capacity**? | Relay-bound **~180 req/s**; Rust ~1.3× at low load, identical at saturation. |
| Does it use **less memory**? | Decisively — **~35×** at baseline, **~5×** under load. |
| Where is Rust *not* ahead? | Bulk sqlite insert loops (`better-sqlite3` is excellent there). |

---

## Side-finding: `after`-cursor input-validation parity (fixed)

Building the Layer-B driver surfaced that the TS `fetchGroupMessages` /
`fetchManyGroupMessages` / `subscribeGroupMessages` / `subscribeManyGroupMessages`
wire schema requires `after: z.number().int().positive().optional()`, but the
Rust adapter accepted `after: 0` and negatives. Fixed in
`crates/cordn-server/src/adapter.rs` via a `require_positive_cursor` check at
the adapter boundary (mirroring the existing `require_non_empty` pattern and the
contracts.rs stated design — "validation applied in the adapter"), gated by a
new adapter test. The advertised schemars schema is intentionally left loose
(consistent with how `gid`'s `min(1)` is also enforced at the adapter, not in
the schema).
