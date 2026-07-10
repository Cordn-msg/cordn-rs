//! Steady-state per-request fan-out benchmark. Unlike `bench_subscriptions`,
//! the storage is opened ONCE and only the per-message server work is measured:
//! each `post_group_message` is 1 storage insert + a fan-out push to K live
//! subscribers — the cost a real serving loop pays per post request (not a
//! bulk-seed stress). K subscriber tasks drain concurrently (untimed) so the
//! post loop measures insert+fan-out, not drain.
//!
//! Run: cargo run --release -p cordn-core --example bench_fanout
//! Env: CORDN_BENCH_FANOUT (8)   — live subscribers per group
//!      CORDN_BENCH_MESSAGES (1000) — messages posted in the measured window
//!      CORDN_BENCH_BACKEND (both | sqlite | memory)

use std::env;
use std::sync::Arc;
use std::time::Instant;

use cordn_core::{
    Coordinator, CoordinatorOptions, CoordinatorStorage, InMemoryCoordinatorStorage,
    PostGroupMessageInput, SqliteCoordinatorStorage,
};

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
        _ => vec![Backend::Sqlite, Backend::Memory],
    }
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

// ponytail: fixed-size synthetic payload; revisit if results look size-sensitive.
fn opaque_message(index: usize) -> Vec<u8> {
    let size = std::env::var("CORDN_BENCH_MSG_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32)
        .max(1);
    let mut bytes = vec![0u8; size];
    let last = bytes.len() - 1;
    bytes[last] = (index % 251) as u8;
    bytes
}

struct FanoutResult {
    backend: Backend,
    total_ms: f64,
    avg_us: f64,
    msgs_per_sec: f64,
    deliveries_per_sec: f64,
}

/// Measure steady-state post+fan-out for one backend. Opens storage once,
/// registers K live-tail subscribers, then times only the M post calls.
async fn run_backend(backend: Backend, fanout: usize, messages: usize) -> FanoutResult {
    let coord = Coordinator::new(CoordinatorOptions {
        storage: Some(backend.build()),
        cleanup_interval_ms: Some(0),
        ..CoordinatorOptions::default()
    });

    let subs = (0..fanout)
        .map(|_| coord.subscribe_group_messages("g"))
        .collect::<Vec<_>>();

    // K concurrent drainers; on the current_thread runtime they only progress
    // once we await (after the timed post loop), so the post loop measures
    // insert + fan-out pushes, exactly like the TS side's synchronous post loop.
    let handles = subs
        .into_iter()
        .map(|mut sub| {
            tokio::spawn(async move {
                for _ in 0..messages {
                    sub.recv().await.expect("drain message");
                }
            })
        })
        .collect::<Vec<_>>();

    let start = Instant::now();
    for i in 0..messages {
        coord
            .post_group_message(PostGroupMessageInput {
                group_id: "g".into(),
                opaque_message: opaque_message(i),
            })
            .expect("post");
    }
    let elapsed = start.elapsed();

    // Untimed drain + correctness (each drainer received all `messages`).
    for handle in handles {
        handle.await.expect("drainer task");
    }
    coord.close();

    let secs = elapsed.as_secs_f64();
    FanoutResult {
        backend,
        total_ms: secs * 1000.0,
        avg_us: secs * 1_000_000.0 / messages as f64,
        msgs_per_sec: messages as f64 / secs,
        deliveries_per_sec: (messages * fanout) as f64 / secs,
    }
}

// current_thread mirrors Node's single-threaded event loop.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let fanout = read_pos_env("CORDN_BENCH_FANOUT", 8);
    let messages = read_pos_env("CORDN_BENCH_MESSAGES", 1000);

    println!("Rust cordn-core — steady-state fan-out benchmark");
    println!("fanout={fanout} messages={messages}");
    println!();

    for backend in read_backends() {
        let r = run_backend(backend, fanout, messages).await;
        println!("── {} ──────────────────────────────", r.backend.label());
        println!(
            "  total={:.2}ms  avg={:.3}µs/post  msgs/sec={:.0}  deliveries/sec={:.0}",
            r.total_ms, r.avg_us, r.msgs_per_sec, r.deliveries_per_sec,
        );
    }
}
