//! Concurrency scaling. N worker tasks each post M messages to a DISTINCT group
//! (one live subscriber draining per group), then we report aggregate
//! posts/sec + deliveries/sec. Unlike `bench_fanout` (a single sequential
//! poster), this measures how throughput scales as parallel load increases.
//!
//! Runtime model caveat (important): this uses the multi_thread tokio runtime
//! so N workers genuinely run in parallel across cores. The TS coordinator is
//! inherently single-threaded (Node event loop), so the TS mirror cannot
//! parallelize — its throughput plateaus near one core. The comparison therefore
//! shows each impl's CONCURRENCY-SCALING ceiling, NOT an apples-to-apples
//! runtime-model match. That asymmetry is the point: it is the dimension the
//! sequential benches cannot surface. See docs/benchmark-results.md.
//!
//! Run: cargo run --release -p cordn-core --example bench_concurrency
//! Env: CORDN_BENCH_CONCURRENCY ("1,4,16,64"), CORDN_BENCH_MESSAGES (500),
//!      CORDN_BENCH_BACKEND (both | sqlite | memory; memory recommended —
//!      sqlite is single-writer so it serializes regardless of concurrency).

use std::env;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Barrier;

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
        _ => vec![Backend::Memory], // sqlite is single-writer; default to memory
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

fn read_concurrency_levels() -> Vec<usize> {
    match env::var("CORDN_BENCH_CONCURRENCY") {
        Ok(raw) => raw
            .split(',')
            .filter_map(|s| s.trim().parse::<usize>().ok().filter(|v| *v > 0))
            .collect(),
        Err(_) => vec![1, 4, 16, 64],
    }
}

// ponytail: fixed-size synthetic payload; revisit if results look size-sensitive.
fn opaque_message(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; 32];
    bytes[31] = (index % 251) as u8;
    bytes
}

/// One concurrency level: N workers each post M messages to their own group;
/// one subscriber per group drains concurrently (untimed). Returns
/// (elapsed_ms, posts_per_sec, deliveries_per_sec).
async fn run_level(backend: Backend, concurrency: usize, messages: usize) -> (f64, f64, f64) {
    let coord = Coordinator::new(CoordinatorOptions {
        storage: Some(backend.build()),
        cleanup_interval_ms: Some(0),
        ..CoordinatorOptions::default()
    });

    // Barrier sized workers + main so timing starts exactly when all workers
    // are ready (no spawn-scheduling skew in the measurement).
    let barrier = Arc::new(Barrier::new(concurrency + 1));

    let mut drainer_handles = Vec::new();
    let mut worker_handles = Vec::new();
    for w in 0..concurrency {
        let gid = format!("g{w}");
        let mut sub = coord.subscribe_group_messages(&gid);

        // Drainer: untimed, just correctness (all messages received).
        drainer_handles.push(tokio::spawn(async move {
            for _ in 0..messages {
                sub.recv().await.expect("drain message");
            }
        }));

        // Worker: gated on the barrier, then posts its M messages.
        let coord_c = coord.clone();
        let barrier_c = barrier.clone();
        let gid_c = gid;
        worker_handles.push(tokio::spawn(async move {
            barrier_c.wait().await;
            for i in 0..messages {
                coord_c
                    .post_group_message(PostGroupMessageInput {
                        group_id: gid_c.clone(),
                        opaque_message: opaque_message(i),
                    })
                    .expect("post");
            }
        }));
    }

    // Release all workers together, time only the post loop.
    barrier.wait().await;
    let start = Instant::now();
    for handle in worker_handles {
        handle.await.expect("worker task");
    }
    let elapsed = start.elapsed();

    // Untimed correctness check: every delivery landed.
    for handle in drainer_handles {
        handle.await.expect("drainer task");
    }
    coord.close();

    let secs = elapsed.as_secs_f64();
    let total = (concurrency * messages) as f64;
    (secs * 1000.0, total / secs, total / secs)
}

// multi_thread so N workers run in parallel across cores (see module docs).
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let messages = read_pos_env("CORDN_BENCH_MESSAGES", 500);
    let levels = read_concurrency_levels();
    let backends = read_backends();

    println!("Rust cordn-core — concurrency scaling (multi_thread runtime)");
    println!(
        "messages/worker={messages} concurrency={:?} backend(s)={:?}",
        levels,
        backends.iter().map(|b| b.label()).collect::<Vec<_>>()
    );
    println!();

    for backend in backends {
        println!("── {} ──────────────────────────────", backend.label());
        println!(
            "  {:>10} {:>12} {:>14} {:>14}",
            "workers", "total_ms", "posts/sec", "deliv/sec"
        );
        for &c in &levels {
            let (ms, pps, dps) = run_level(backend, c, messages).await;
            println!("  {:>10} {:>12.2} {:>14.0} {:>14.0}", c, ms, pps, dps);
        }
    }
}
