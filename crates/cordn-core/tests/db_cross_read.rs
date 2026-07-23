//! DB cross-read parity — AGENTS.md testing layer 4.
//!
//! Opens a sqlite DB written by the **TS** `SqliteCoordinatorStorage`
//! (`references/cordn/scripts/gen_ts_db.test.ts`, via `better-sqlite3`) with
//! the **Rust** `SqliteCoordinatorStorage` (`rusqlite`) and asserts every row
//! reads back identically to the TS-produced manifest.
//!
//! This is the literal proof of the drop-in guarantee: a DB written by the TS
//! coordinator is readable by the Rust port, byte-for-byte. It exercises the
//! full parity surface — the TS schema + migrations (RS migrations must be
//! idempotent over them), BLOB serialization, boolean↔INTEGER mapping, NULL
//! `join_after_cursor`, the `publication_event_json` column, and per-group
//! cursor allocation.
//!
//! Regenerate the fixture pair from `references/cordn`:
//! `npx vitest run scripts/gen_ts_db.test.ts`.

use std::collections::HashMap;

use serde::Deserialize;

use cordn_core::{CoordinatorStorage, SqliteCoordinatorStorage};

const DB: &str = "tests/fixtures/ts_written.db";
const MANIFEST: &str = "tests/fixtures/ts_written.manifest.json";

// The manifest mirrors exactly the columns Rust reads; `note` / `group_routing`
// are intentionally not modeled (serde ignores unknown fields, and per-group
// cursor density is asserted directly off `group_messages` below).
#[derive(Deserialize)]
struct Manifest {
    key_packages: Vec<Mkp>,
    welcomes: Vec<Mw>,
    join_requests: Vec<Mj>,
    group_messages: Vec<Mg>,
}
#[derive(Deserialize)]
struct Mkp {
    stable_pubkey: String,
    key_package_ref: String,
    key_package_bytes_hex: String,
    is_last_resort: i64,
    published_at: i64,
    publication_event: serde_json::Value,
}
#[derive(Deserialize)]
struct Mw {
    target_stable_pubkey: String,
    key_package_reference: String,
    welcome_bytes_hex: String,
    created_at: i64,
    join_after_cursor: Option<i64>,
}
#[derive(Deserialize)]
struct Mj {
    group_id: String,
    requester_stable_pubkey: String,
    key_package_ref: String,
    created_at: i64,
}
#[derive(Deserialize)]
struct Mg {
    cursor: i64,
    group_id: String,
    opaque_message_hex: String,
    created_at: i64,
}

fn load_manifest() -> Manifest {
    let raw = std::fs::read_to_string(MANIFEST).expect("manifest readable");
    serde_json::from_str(&raw).expect("manifest parses")
}

#[test]
fn rust_reads_ts_written_db() {
    let mut m = load_manifest();

    // Copy the fixture to a temp path first: RS `open` sets `journal_mode =
    // WAL`, which creates a `-wal`/`-shm` sidecar — we don't want to pollute the
    // committed fixtures dir, and the copy also guards against accidental
    // mutation by migration writes.
    let tmp = std::env::temp_dir().join("cordn_db_cross_read.db");
    std::fs::remove_file(&tmp).ok();
    std::fs::remove_file(tmp.with_extension("db-wal")).ok();
    std::fs::remove_file(tmp.with_extension("db-shm")).ok();
    std::fs::copy(DB, &tmp).expect("copy fixture to temp");

    // (1) Opening runs the RS migrations over a TS-written schema. The TS
    //     constructor already added `join_after_cursor` and dropped the legacy
    //     columns; RS migrations must therefore be no-ops here. If this errors,
    //     the migration branches have diverged.
    let storage = SqliteCoordinatorStorage::open(
        Some(tmp.to_str().expect("temp path is utf-8")),
        cordn_core::Synchronous::Normal,
    )
    .expect("Rust must open a TS-written sqlite DB");

    // (2) key_packages — raw bytes RS reads must equal TS's encodeKeyPackage
    //     output; is_last_resort maps INTEGER 0/1 → bool; publication_event_json
    //     round-trips to the same JSON value.
    let mut kps = storage
        .list_all_key_packages()
        .expect("list_all_key_packages");
    kps.sort_by(|a, b| a.key_package_ref.cmp(&b.key_package_ref));
    m.key_packages
        .sort_by(|a, b| a.key_package_ref.cmp(&b.key_package_ref));
    assert_eq!(kps.len(), m.key_packages.len(), "key package count");
    for (rs, exp) in kps.iter().zip(m.key_packages.iter()) {
        assert_eq!(rs.stable_pubkey, exp.stable_pubkey, "stable_pubkey");
        assert_eq!(rs.key_package_ref, exp.key_package_ref, "key_package_ref");
        assert_eq!(
            rs.key_package_bytes,
            hex::decode(&exp.key_package_bytes_hex).unwrap(),
            "key_package_bytes (RS raw == TS stored)"
        );
        assert_eq!(rs.is_last_resort, exp.is_last_resort != 0, "is_last_resort");
        assert_eq!(rs.published_at, exp.published_at, "published_at");
        assert_eq!(
            rs.publication_event, exp.publication_event,
            "publication_event_json"
        );
    }

    // (3) welcomes — fetched per target; covers NULL and non-NULL
    //     join_after_cursor in one fixture.
    for w in &m.welcomes {
        let fetched = storage
            .fetch_pending_welcomes(&w.target_stable_pubkey, &[])
            .expect("fetch_pending_welcomes");
        assert_eq!(fetched.len(), 1, "one welcome per target");
        let rs = &fetched[0];
        assert_eq!(rs.target_stable_pubkey, w.target_stable_pubkey);
        assert_eq!(rs.key_package_reference, w.key_package_reference);
        assert_eq!(
            rs.welcome_bytes,
            hex::decode(&w.welcome_bytes_hex).unwrap(),
            "welcome_bytes"
        );
        assert_eq!(rs.created_at, w.created_at);
        assert_eq!(
            rs.join_after_cursor, w.join_after_cursor,
            "join_after_cursor (NULL vs value)"
        );
    }

    // (4) join_requests — fetched per group.
    for j in &m.join_requests {
        let fetched = storage
            .fetch_pending_join_requests(&j.group_id, &[])
            .expect("fetch_pending_join_requests");
        assert_eq!(fetched.len(), 1, "one join request per group");
        let rs = &fetched[0];
        assert_eq!(rs.group_id, j.group_id);
        assert_eq!(rs.requester_stable_pubkey, j.requester_stable_pubkey);
        assert_eq!(rs.key_package_ref, j.key_package_ref);
        assert_eq!(rs.created_at, j.created_at);
    }

    // (5) group_messages — per-group fetch, asserting the opaque blob and
    //     per-group cursor density.
    let mut by_group: HashMap<String, Vec<&Mg>> = HashMap::new();
    for g in &m.group_messages {
        by_group.entry(g.group_id.clone()).or_default().push(g);
    }
    for (group, mut exp) in by_group {
        let mut fetched = storage
            .fetch_group_messages(&group, None)
            .expect("fetch_group_messages");
        fetched.sort_by_key(|r| r.cursor);
        exp.sort_by_key(|r| r.cursor);
        assert_eq!(fetched.len(), exp.len(), "message count for {group}");
        for (i, rs) in fetched.iter().enumerate() {
            assert_eq!(
                rs.cursor,
                i as i64 + 1,
                "per-group cursor must be dense from 1"
            );
        }
        for (rs, e) in fetched.iter().zip(exp.iter()) {
            assert_eq!(rs.group_id, e.group_id);
            assert_eq!(
                rs.opaque_message,
                hex::decode(&e.opaque_message_hex).unwrap(),
                "opaque_message"
            );
            assert_eq!(rs.created_at, e.created_at);
        }
    }

    // (6) Cross-group cursor independence: the fixture has 2 messages in
    //     group-A and 1 in group-B; a global sequence would yield {1,2,3}. The
    //     per-group allocation that AGENTS.md makes load-bearing must hold.
    let a = storage.fetch_group_messages("group-A", None).unwrap();
    let b = storage.fetch_group_messages("group-B", None).unwrap();
    assert_eq!(
        a.iter().map(|r| r.cursor).collect::<Vec<_>>(),
        vec![1, 2],
        "group-A cursors"
    );
    assert_eq!(
        b.iter().map(|r| r.cursor).collect::<Vec<_>>(),
        vec![1],
        "group-B cursor (independent of group-A)"
    );
}
