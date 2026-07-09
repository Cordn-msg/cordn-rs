//! In-process core benchmark — a faithful mirror of the TS
//! `references/cordn/src/coordinator/storage/sqliteSubscriptionBenchmark.ts`,
//! for an apples-to-apples Rust-vs-TS comparison at the **coordinator-core
//! layer** (no transport, no relay, no crypto).
//!
//! Same scenario, same env knobs, same methodology (total/avg over N iterations,
//! each iteration over a fresh seeded in-memory sqlite coordinator), same output
//! shape — so the two numbers are directly comparable. We deliberately do NOT
//! use `criterion`: the TS bench is hand-rolled around `hrtime`, and matching
//! its methodology (not criterion's warmup/statistical model) is what keeps the
//! cross-language comparison honest.
//!
//! Run in release:
//!     cargo run --release -p cordn-core --example bench_subscriptions
//! Or via the harness: `bench/core/run.sh` (runs both TS and Rust sides).
//!
//! Env knobs mirror the TS bench exactly (same names, same defaults):
//!     CORDN_BENCH_GROUPS (32)
//!     CORDN_BENCH_BACKLOG (64)
//!     CORDN_BENCH_LIVE (16)
//!     CORDN_BENCH_ITERATIONS (100)
//!     CORDN_BENCH_BACKEND (both | sqlite | memory) — default `both`. The
//! memory backend removes sqlite entirely and isolates the pure coordinator +
//! channel fan-out; sqlite includes per-iteration schema bootstrap + inserts.

use std::env;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use cordn_core::{
    Coordinator, CoordinatorOptions, CoordinatorStorage, FetchGroupMessagesInput,
    InMemoryCoordinatorStorage, PostGroupMessageInput, SqliteCoordinatorStorage,
};

/// Which storage backend to bench. The memory backend isolates the coordinator
/// (fan-out, channel delivery) by removing sqlite write/schema cost entirely.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Backend {
    Sqlite,
    Memory,
}

impl Backend {
    fn label(self) -> &'static str {
        match self {
            Backend::Sqlite => "SQLite",
            Backend::Memory => "In-memory",
        }
    }

    fn build(self) -> Arc<dyn CoordinatorStorage> {
        match self {
            Backend::Sqlite => Arc::new(
                SqliteCoordinatorStorage::open_in_memory()
                    .expect("open in-memory sqlite for bench"),
            ),
            Backend::Memory => Arc::new(InMemoryCoordinatorStorage::new()),
        }
    }
}

fn read_backends() -> Vec<Backend> {
    match env::var("CORDN_BENCH_BACKEND").ok().as_deref() {
        Some("sqlite") => vec![Backend::Sqlite],
        Some("memory") => vec![Backend::Memory],
        // default: both, so one run shows sqlite vs memory side by side
        _ => vec![Backend::Sqlite, Backend::Memory],
    }
}

struct Config {
    group_count: usize,
    backlog_per_group: usize,
    live_per_group: usize,
    iterations: usize,
}

fn read_pos_env(name: &str, fallback: usize) -> usize {
    match env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|v| *v > 0)
            .unwrap_or_else(|| panic!("{name} must be a positive integer, got {raw:?}")),
        Err(_) => fallback,
    }
}

fn config() -> Config {
    Config {
        group_count: read_pos_env("CORDN_BENCH_GROUPS", 32),
        backlog_per_group: read_pos_env("CORDN_BENCH_BACKLOG", 64),
        live_per_group: read_pos_env("CORDN_BENCH_LIVE", 16),
        iterations: read_pos_env("CORDN_BENCH_ITERATIONS", 100),
    }
}

/// Per-message opaque payload. The TS bench encodes a minimal `ts-mls` private
/// message (~30-40 bytes); we don't link an MLS encoder (decision #1) and the
/// coordinator is opaque to payload content anyway, so a small fixed payload
/// stands in. The distinguishing trailing byte mirrors TS `bytes: [i % 251]`.
// ponytail: fixed-size synthetic payload; revisit if results look size-sensitive.
fn opaque_message(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; 32];
    bytes[31] = (index % 251) as u8;
    bytes
}

/// A fresh coordinator seeded with `group_count` groups, each with
/// `backlog_per_group` messages. `after_cursor` is `backlog / 2`, matching TS.
struct Seeded {
    coord: Arc<Coordinator>,
    groups: Vec<(String, i64)>, // (group_id, after_cursor)
}

fn seed(cfg: &Config, backend: Backend) -> Seeded {
    let coord = Coordinator::new(CoordinatorOptions {
        storage: Some(backend.build()),
        cleanup_interval_ms: Some(0), // disable the cleanup task; irrelevant to the scenario
        ..CoordinatorOptions::default()
    });

    let after_cursor = (cfg.backlog_per_group / 2) as i64;
    let mut groups = Vec::with_capacity(cfg.group_count);
    for g in 0..cfg.group_count {
        let group_id = format!("bench-group-{}", g + 1);
        for i in 0..cfg.backlog_per_group {
            coord
                .post_group_message(PostGroupMessageInput {
                    group_id: group_id.clone(),
                    opaque_message: opaque_message(i),
                })
                .expect("seed post");
        }
        groups.push((group_id, after_cursor));
    }

    Seeded { coord, groups }
}

fn expected_backlog_count(after_cursor: i64, backlog_per_group: usize) -> usize {
    ((backlog_per_group as i64) - after_cursor).max(0) as usize
}

/// N independent single-group streams, each consuming backlog (via fetch) + live
/// (via the live-tail subscription). Mirrors TS `benchmarkSingleSubscriptions`.
async fn benchmark_single_subscriptions(seeded: &Seeded, cfg: &Config) {
    let after_cursor = seeded.groups[0].1;
    let expected_per_group =
        expected_backlog_count(after_cursor, cfg.backlog_per_group) + cfg.live_per_group;

    // Live-tail subscriptions (single-group subscribe does NOT replay backlog).
    let subs = seeded
        .groups
        .iter()
        .map(|(gid, _)| seeded.coord.subscribe_group_messages(gid))
        .collect::<Vec<_>>();

    // Fetch backlog BEFORE posting live — TS does this synchronously before the
    // first `await`, so we match it: this keeps the measurement on the
    // subscription-delivery path (not a fetch-eats-live degenerate path).
    let consumed_per_group = seeded
        .groups
        .iter()
        .map(|(gid, after)| {
            seeded
                .coord
                .fetch_group_messages(gid, Some(*after))
                .expect("fetch backlog")
                .len()
        })
        .collect::<Vec<_>>();

    // One consumer task per subscription, reading the remaining live count.
    let handles = subs
        .into_iter()
        .zip(consumed_per_group)
        .map(|(mut sub, consumed)| {
            let to_read = expected_per_group - consumed;
            tokio::spawn(async move {
                for _ in 0..to_read {
                    sub.recv().await.expect("live message");
                }
                sub.unsubscribe();
            })
        })
        .collect::<Vec<_>>();

    // Post the live messages; they fan out to every subscriber.
    for (gid, _) in &seeded.groups {
        for i in 0..cfg.live_per_group {
            seeded
                .coord
                .post_group_message(PostGroupMessageInput {
                    group_id: gid.clone(),
                    opaque_message: opaque_message(i),
                })
                .expect("live post");
        }
    }

    for handle in handles {
        handle.await.expect("consumer task");
    }
}

/// One multi-group stream merging backlog + live across all groups. Mirrors TS
/// `benchmarkMultiSubscription`.
async fn benchmark_multi_subscription(seeded: &Seeded, cfg: &Config) {
    let after_cursor = seeded.groups[0].1;
    let expected_per_group =
        expected_backlog_count(after_cursor, cfg.backlog_per_group) + cfg.live_per_group;
    let expected_total = expected_per_group * seeded.groups.len();

    let inputs = seeded
        .groups
        .iter()
        .map(|(gid, after)| FetchGroupMessagesInput {
            group_id: gid.clone(),
            after_cursor: Some(*after),
        })
        .collect::<Vec<_>>();

    // Multi-subscribe replays backlog internally and buffers any live that races
    // in during setup, so one consumer draining `expected_total` covers both.
    let mut sub = seeded
        .coord
        .subscribe_many_group_messages(&inputs)
        .expect("multi subscribe");
    let consumer = tokio::spawn(async move {
        for _ in 0..expected_total {
            sub.recv().await.expect("streamed message");
        }
        sub.unsubscribe();
    });

    for (gid, _) in &seeded.groups {
        for i in 0..cfg.live_per_group {
            seeded
                .coord
                .post_group_message(PostGroupMessageInput {
                    group_id: gid.clone(),
                    opaque_message: opaque_message(i),
                })
                .expect("live post");
        }
    }

    consumer.await.expect("consumer task");
}

struct ScenarioResult {
    name: &'static str,
    iterations: usize,
    total_ms: f64,
    avg_ms: f64,
}

async fn measure<F, Fut>(name: &'static str, iterations: usize, run: F) -> ScenarioResult
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    let start = Instant::now();
    for _ in 0..iterations {
        run().await;
    }
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;
    ScenarioResult {
        name,
        iterations,
        total_ms,
        avg_ms: total_ms / iterations as f64,
    }
}

fn pad_to(s: &str, width: usize) -> String {
    let mut out = s.to_string();
    while out.chars().count() < width {
        out.push(' ');
    }
    out
}

fn format_result(result: &ScenarioResult) -> String {
    format!(
        "{}  total={:.2}ms  avg={:.4}ms  iterations={}",
        pad_to(result.name, 34),
        result.total_ms,
        result.avg_ms,
        result.iterations,
    )
}

// current_thread mirrors Node's single-threaded event loop (the model the TS
// bench runs under); `tokio::spawn` still schedules cooperatively on it.
// current_thread mirrors Node's single-threaded event loop (the model the TS
// bench runs under); `tokio::spawn` still schedules cooperatively on it.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cfg = config();
    let backends = read_backends();

    println!("Rust cordn-core — realistic subscription benchmark");
    println!(
        "groups={} backlogPerGroup={} livePerGroup={} iterations={}",
        cfg.group_count, cfg.backlog_per_group, cfg.live_per_group, cfg.iterations,
    );
    println!();

    for backend in &backends {
        let single = measure(
            "Equivalent N x single-group streams",
            cfg.iterations,
            || async {
                let seeded = seed(&cfg, *backend);
                benchmark_single_subscriptions(&seeded, &cfg).await;
                seeded.coord.close();
            },
        )
        .await;

        let multi = measure(
            "Equivalent 1 x multi-group stream",
            cfg.iterations,
            || async {
                let seeded = seed(&cfg, *backend);
                benchmark_multi_subscription(&seeded, &cfg).await;
                seeded.coord.close();
            },
        )
        .await;

        let speedup = single.total_ms / multi.total_ms;

        println!("── {} ──────────────────────────────", backend.label());
        println!("{}", format_result(&single));
        println!("{}", format_result(&multi));
        println!("multi-vs-single speedup: {:.2}x", speedup);
        println!();
    }
}
