# cordn-rs vs cordn (TS) — Benchmark Proposal

Status: **proposal, not yet built.** Awaiting go-ahead.

## What we're measuring, and the one rule that governs this

There are **two layers** that tell two different stories, and the single most
common benchmarking mistake is to conflate them:

| Layer | What it isolates | What it answers |
|-------|------------------|-----------------|
| **A — Core (in-process)** | Language + storage engine + concurrency model. No transport, no relay, no crypto. | "How much faster is the coordinator *core* in Rust?" |
| **B — End-to-end (over relay)** | The whole path: client → relay → server → relay → client, incl. Nostr framing, gift-wrap/NIP-44 crypto, JSON, SDK plumbing. | "How much faster does it *feel* to a real client?" |

**Rule: never report one headline number.** Layer A will show a big Rust win;
Layer B will show a much smaller one because a large crypto/JSON floor sits in
front of the coordinator core and is identical on both sides (the client is TS
in both runs; the crypto is in `nostr-tools` / the SDK). Both numbers are true
and useful — only misleading if conflated. The benchmark must print both and
label them.

## Repo facts that shape the design

1. **The TS coordinator already ships an in-process core benchmark:**
   `references/cordn/src/coordinator/storage/sqliteSubscriptionBenchmark.ts`
   (`pnpm run bench:sqlite-subscriptions`). It seeds N groups, then measures
   N×single-subscribe vs 1×multi-subscribe, reporting total/avg ms and the
   speedup ratio. It reads `CORDN_BENCH_GROUPS/BACKLOG/LIVE/ITERATIONS` from
   env. **We mirror this exact scenario in Rust — we do not reinvent it.** That
   gives a true apples-to-apples Layer-A number from the same workload shape.
2. **The TS CLI client is already a cross-impl client.** `cordnClient`
   (`references/cordn/src/cli/coordinatorClient.ts`) speaks the ContextVM/Nostr
   protocol to *either* server. Its `DEFAULT_RELAYS` is already
   `["ws://localhost:10547"]` — i.e. `nak serve`. So Layer B needs **one**
   driver, pointed at server A then server B. No second client to write.
3. **`nak serve` is available** (v0.17.2). It is a single-process, in-memory,
   non-persistent reference relay on `ws://localhost:10547`. Because it is the
   *same* relay for both server targets, it cancels out of the comparison: any
   relay cost is paid identically by both. It also will not be the bottleneck
   (in-memory, no disk, no network), so Layer-B latency numbers reflect the
   server + SDK path. ⚠ Caveat surfaced by measurement: under *concurrent* load
   `nak serve` DOES become the ceiling (~180 req/s on this box) — see the
   Layer-B concurrent-throughput results — so concurrent *capacity* measures
   the relay, not the server impl.
4. **Config parity knobs already exist on both sides** and line up:
   `CORDN_SERVER_PRIVATE_KEY`, `CORDN_RELAY_URLS`, `CORDN_STORAGE_BACKEND`,
   `CORDN_RATE_LIMIT_*`, `CORDN_MAX_*_KEY_PACKAGES_*`, `CORDN_MAX_AGE_DAYS`.
   Both servers can be started byte-for-byte identically configured.

## Gotchas to bake in (these are load-bearing, not nice-to-haves)

- **Disable abuse protection for benchmark runs.** The rate limiter defaults to
  `refillPerMinute=500`, `burst=160`. Any throughput test that doesn't set
  `CORDN_RATE_LIMIT_ENABLED=false` (or raise burst/refill ~100×) will measure
  the rate limiter, not the coordinator. This is the #1 way benchmarks here lie.
- **Same private key ⇒ same server pubkey.** Good for apples-to-apples, but it
  means you run **one server at a time** against the relay (two servers with the
  same pubkey on one relay would both answer). The harness must stop server A
  before starting server B.
- **Crypto is the great equalizer in Layer B.** Schnorr signing + NIP-44 gift
  wrap happen on client and server regardless of language. Expect Layer-B win ≪
  Layer-A win. Measuring this gap is itself a result — it tells you the
  coordinator core is no longer the bottleneck and where (if anywhere) to push.
- **Warmup.** V8 JIT warms up; the TS core bench already loops `iterations`
  times in-process. For Layer B, drop the first N samples (cold start + JIT) and
  report p50/p99 of the warm tail.
- **Pin the scenario shape in env, not in code**, mirroring the existing
  `CORDN_BENCH_*` convention, so re-runs are reproducible and comparable.

## Proposed layout

```
bench/                         # NEW, sibling to crates/ and references/
  README.md                    # one-page: how to run each layer, expected output
  core/                        # Layer A — in-process, no transport
    README.md
    run.sh                     # runs TS bench (pnpm) + Rust bench (cargo),
                               #   emits results/core.{ts,rs}.json
    rust/                      # Rust mirror of sqliteSubscriptionBenchmark.ts
      ...                      # criterion benches against cordn-core directly
  e2e/                         # Layer B — over nak serve relay
    README.md
    run.sh                     # orchestrates: nak serve → server (A|B) →
                               #   driver → collect → teardown; repeats for B
    driver/                    # client harness; reuses cordnClient (TS CLI)
    scenarios/                 # one file per scenario (see below)
    results/                   # gitignored per-run json
  analyze/                     # ~50 lines: merges core+e2e json into a
                               #   comparison table (md) with speedup ratios
results/                       # gitignored; committed snapshots go in docs/
```

Why a top-level `bench/` and not `crates/cordn-core/benches/`: Layer A's Rust
half *does* belong under criterion, but the harness, the TS invocations, the
`nak serve` orchestration, and the comparison/analysis are repo-spanning and
language-mixed — they don't fit inside one crate. Top-level `bench/` keeps the
comparison first-class and keeps `references/` untouched (read-only). The Rust
core micro-benches themselves can still live as criterion `harness = false`
entries that the `bench/core/run.sh` invokes.

### Scenarios (start with these five; add more only when a number is surprising)

Layer A (core, in-process — mirrors + extends the existing TS bench):
1. **Subscription fan-out** (existing TS scenario, mirrored): N×single-subscribe
   vs 1×multi-subscribe over seeded backlog + live messages. This is where
   Rust's per-subscriber `mpsc` + zero-copy should beat TS's EventEmitter most.
2. **Storage write/read microbench**: `post_group_message` and
   `fetch_group_messages` throughput, memory vs sqlite, with a warmed DB at
   N rows. Isolates `rusqlite` vs `better-sqlite3`.

Layer B (end-to-end over `nak serve`):
3. **Tool round-trip latency**: publish / fetch / store-welcome, p50 + p99,
    one client. Captures the crypto/JSON floor.
4. **Sustained request rate**: ramp concurrent clients, find the requests/sec
    the server absorbs (abuse protection OFF). Captures backpressure behavior.
5. **Live stream delivery latency**: time from `post_group_message` return on a
    publisher client to receipt on a subscriber client's stream, plus
    time-to-first-message on subscribe. The end-to-end subscription number.

### Metrics to collect (the short list that actually informs decisions)

Per scenario, per implementation:
- **Latency:** p50, p99 (not mean — tails are where TS's GC pauses live).
- **Throughput:** ops/sec or msgs/sec sustained.
- **Peak RSS** of the server process over the run (cheapest high-signal metric;
  likely Rust's biggest absolute win). Capture via `/usr/bin/time -v` (GNU time)
  or sampling `/proc/<pid>/status`.
- **CPU time** (user + sys) for the same work — `getrusage` via `/usr/bin/time`.
- **Cold start:** process launch → "connected" log (Layer B only).
- **DB file size** growth per N messages (sqlite runs) — parity + overhead.

Report as a comparison table: scenario × impl × metric, with a **speedup
ratio** column (rs ÷ ts, or ts ÷ rs for "x faster"). Commit a snapshot to
`docs/benchmark-results.md` per meaningful change; raw runs go to gitignored
`results/`.

## How a run looks (the lazy version)

```bash
# Layer A — core, in-process, both languages, same scenario shape
cd bench/core && ./run.sh
#   → runs: cd references/cordn && pnpm run bench:sqlite-subscriptions
#   → runs: cargo bench -p cordn-core (criterion, mirrored scenario)
#   → writes results/core.{ts,rs}.json

# Layer B — end-to-end over nak serve
cd bench/e2e && ./run.sh ts     # starts nak serve + TS server, drives, stops
cd bench/e2e && ./run.sh rs     # same, Rust server
#   → both reuse identical .env (same key, same relay, abuse protection off)
#   → writes results/e2e.{ts,rs}.json

cd bench/analyze && ./run.sh    # → docs/benchmark-results.md table
```

Both servers run with a shared `.env`:
```
CORDN_SERVER_PRIVATE_KEY=<fixed hex>      # same pubkey for both ⇒ identical target
CORDN_RELAY_URLS=ws://localhost:10547     # nak serve
CORDN_STORAGE_BACKEND=sqlite              # or memory, per scenario
CORDN_RATE_LIMIT_ENABLED=false            # MUST: or you bench the limiter
CORDN_MAX_KEY_PACKAGES_PER_IDENTITY=100000
CORDN_MAX_AGE_DAYS=3650                   # avoid cleanup mid-run
```

## Recommendation (what to build first, in order)

1. **Layer A, scenario 1 only** — mirror `sqliteSubscriptionBenchmark.ts` in
   Rust criterion, run both, print the table. Smallest possible thing that
   produces a real "Rust is Nx faster at the core" number. ~1 file of Rust + a
   20-line shell script. This alone justifies most of the port.
2. **Peak RSS during a sustained Layer-A load run.** Near-free, high-signal.
3. **Layer B, scenario 3 (round-trip latency)** — proves (or disproves) the
   "crypto floor dominates E2E" hypothesis. If the E2E win is small, scenarios
   4–5 quantify where the remaining time goes; if it's large, ship it.
4. Everything else (sustained rate, stream latency, more storage scenarios) is
   "add when the first three numbers leave a question open." Don't pre-build.

## Explicitly out of scope for v1

- A Rust client driver (the TS CLI already covers both servers; a Rust client
  would only be needed to measure *client-side* CPU/RSS, which is not the
  coordinator comparison).
- Automated flamegraphs — manual `perf`/`cargo flamegraph` on demand is enough
  once a number is surprising.
- CI integration — benchmarks belong on a quiet, pinned machine, not a noisy
  CI runner, until the methodology is stable.
