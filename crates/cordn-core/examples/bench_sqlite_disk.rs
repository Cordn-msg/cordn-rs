//! Persistent-sqlite write-path microbench: the REAL per-post cost on a file DB
//! (WAL + commit + fsync), measured through the actual
//! `SqliteCoordinatorStorage::append_group_message` path, under
//! `synchronous = FULL` vs `NORMAL`.
//!
//! This fills the gap that `bench_fanout` measures `:memory:` sqlite (no WAL,
//! no fsync) and so hides all persistence cost — production persistence is
//! ~100×–1000× the in-memory number and dominated by the `synchronous` setting.
//! `NORMAL` (the production default) skips the per-commit fsync; `FULL` (parity
//! with the TS default) fsyncs every commit.
//!
//!   cargo run --release -p cordn-core --example bench_sqlite_disk -- 3000 /tmp/cordn_disk.sqlite

use std::env;
use std::time::Instant;

use cordn_core::{
    AppendGroupMessageParams, CoordinatorStorage, SqliteCoordinatorStorage, Synchronous,
};

fn cleanup(base: &str) {
    for p in [base, &format!("{base}-wal"), &format!("{base}-shm")] {
        let _ = std::fs::remove_file(p);
    }
}

/// Open the file DB, post `n` messages one per transaction, return µs/post.
fn run(path: &str, sync: Synchronous, n: usize, msg_bytes: usize) -> f64 {
    cleanup(path);
    let storage = SqliteCoordinatorStorage::open(Some(path), sync).unwrap();
    let payload = vec![0u8; msg_bytes];
    let start = Instant::now();
    for i in 1..=n {
        storage
            .append_group_message(AppendGroupMessageParams {
                group_id: "g".into(),
                opaque_message: payload.clone(),
                created_at: i as i64,
                encrypted: true,
            })
            .unwrap();
    }
    let per = start.elapsed().as_secs_f64() / n as f64 * 1e6;
    drop(storage);
    cleanup(path);
    per
}

fn label(sync: Synchronous) -> &'static str {
    match sync {
        Synchronous::Full => "FULL",
        Synchronous::Normal => "NORMAL",
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(3000);
    let path = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "/tmp/cordn_disk.sqlite".into());
    let msg_bytes: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(256);

    println!("Persistent sqlite append_group_message: {n} posts, {msg_bytes}B each (best of 2)");
    for sync in [Synchronous::Full, Synchronous::Normal] {
        let best = run(&format!("{path}.a"), sync, n, msg_bytes).min(run(
            &format!("{path}.b"),
            sync,
            n,
            msg_bytes,
        ));
        println!(
            "  synchronous={:<6} {:8.1} µs/post  ({:>.0} posts/s)",
            label(sync),
            best,
            1e6 / best
        );
    }
}
