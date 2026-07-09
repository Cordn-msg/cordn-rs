//! Post → subscriber delivery latency. Measures the wall time from a
//! `post_group_message` call to the matching record being received on a live
//! single-group subscription — the pub/sub hot-path handoff. The steady-state
//! fan-out bench (`bench_fanout`) reports this as throughput (msgs/sec); here
//! it is per-message LATENCY (avg/p50/p90/p99), which is what a streaming
//! subscriber actually feels between a post landing and the record arriving.
//!
//! current_thread runtime mirrors Node's single-threaded model. The post pushes
//! the record into the subscriber's unbounded channel synchronously, so the
//! following `recv().await` returns the already-buffered record on its first
//! poll (no real yield) — the sample is the channel-push + wake + poll cost.
//!
//! Run: cargo run --release -p cordn-core --example bench_stream_latency
//! Env: CORDN_BENCH_MESSAGES (2000), CORDN_BENCH_BACKEND (both | sqlite | memory)

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
    let mut bytes = vec![0u8; 32];
    bytes[31] = (index % 251) as u8;
    bytes
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    let idx = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let messages = read_pos_env("CORDN_BENCH_MESSAGES", 2000);

    println!("Rust cordn-core — post→subscriber delivery latency");
    println!("messages={messages}");
    println!();

    for backend in read_backends() {
        let coord = Coordinator::new(CoordinatorOptions {
            storage: Some(backend.build()),
            cleanup_interval_ms: Some(0),
            ..CoordinatorOptions::default()
        });
        let mut sub = coord.subscribe_group_messages("g");
        let mut samples: Vec<f64> = Vec::with_capacity(messages);
        for i in 0..messages {
            let t0 = Instant::now();
            coord
                .post_group_message(PostGroupMessageInput {
                    group_id: "g".into(),
                    opaque_message: opaque_message(i),
                })
                .expect("post");
            sub.recv().await.expect("recv");
            samples.push(t0.elapsed().as_nanos() as f64 / 1000.0); // µs
        }
        coord.close();

        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let avg = samples.iter().sum::<f64>() / samples.len() as f64;
        println!("── {} ──────────────────────────────", backend.label());
        println!(
            "  avg={avg:.3}µs  p50={:.3}µs  p90={:.3}µs  p99={:.3}µs",
            pct(&samples, 0.5),
            pct(&samples, 0.9),
            pct(&samples, 0.99),
        );
    }
}
