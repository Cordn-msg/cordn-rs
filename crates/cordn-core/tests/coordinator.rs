//! Coordinator tests — ported from
//! `references/cordn/src/coordinator/coordinator.test.ts` (and the subscription
//! cases in `coordinator.integration.test.ts`), adapted to drive the
//! coordinator with synthetic opaque bytes and explicit `is_last_resort`
//! (the adapter computes it via `mls_parse` — step 3). Storage-level parity is
//! covered by `storage_parity.rs`; these tests focus on what the coordinator
//! adds over storage: injected-clock timestamps, the cleanup timer, and the
//! pub/sub fan-out.

use std::sync::{Arc, Mutex};

use cordn_core::{
    Coordinator, CoordinatorOptions, FetchGroupMessagesInput, PostGroupMessageInput,
    PublishKeyPackageInput, StoreJoinRequestInput, StoreWelcomeInput,
};

/// A mutable tick so tests get deterministic, increasing timestamps.
fn ticking_clock() -> (Arc<dyn Fn() -> i64 + Send + Sync>, Arc<Mutex<i64>>) {
    let tick = Arc::new(Mutex::new(1_700_000_000_000_i64));
    let tick_clone = tick.clone();
    let now: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
        let mut t = tick_clone.lock().unwrap();
        *t += 1;
        *t
    });
    (now, tick)
}

fn advance(tick: &Arc<Mutex<i64>>, by: i64) {
    *tick.lock().unwrap() += by;
}

fn coord() -> (Arc<Coordinator>, Arc<Mutex<i64>>) {
    let (now, tick) = ticking_clock();
    let coord = Coordinator::new(CoordinatorOptions {
        now: Some(now),
        cleanup_interval_ms: Some(0),
        ..CoordinatorOptions::default()
    });
    (coord, tick)
}

fn publish(coord: &Coordinator, stable_pubkey: &str, kp_ref: &str, is_last_resort: bool) {
    coord
        .publish_key_package(PublishKeyPackageInput {
            stable_pubkey: stable_pubkey.to_string(),
            key_package_bytes: vec![0xde, 0xad],
            key_package_ref: kp_ref.to_string(),
            is_last_resort,
            publication_event: serde_json::json!({ "id": kp_ref, "pubkey": stable_pubkey }),
        })
        .unwrap();
}

fn post(coord: &Coordinator, group_id: &str, opaque: &[u8]) -> cordn_core::GroupMessageRecord {
    coord
        .post_group_message(PostGroupMessageInput {
            group_id: group_id.to_string(),
            opaque_message: opaque.to_vec(),
        })
        .unwrap()
}

// ── key packages: clock + is_last_resort forwarding ──────────────────

#[tokio::test]
async fn publish_sets_published_at_via_injected_clock_and_consumes_fifo() {
    let (coord, _tick) = coord();
    publish(&coord, "alice", "kp-1", false);
    publish(&coord, "alice", "kp-2", false);

    let listed = coord.list_key_packages_for_identity("alice").unwrap();
    assert_eq!(listed.len(), 2);
    // Injected clock produces increasing published_at.
    assert!(listed[1].published_at > listed[0].published_at);

    let first = coord.consume_key_package("alice").unwrap().unwrap();
    assert_eq!(first.key_package_ref, "kp-1");
    let second = coord.consume_key_package("alice").unwrap().unwrap();
    assert_eq!(second.key_package_ref, "kp-2");
    assert!(coord.consume_key_package("alice").unwrap().is_none());
}

#[tokio::test]
async fn last_resort_is_forwarded_and_kept_after_consume() {
    let (coord, _tick) = coord();
    publish(&coord, "alice", "regular", false);
    publish(&coord, "alice", "last-resort", true);

    let regular = coord.consume_key_package("alice").unwrap().unwrap();
    assert_eq!(regular.key_package_ref, "regular");

    let last_resort = coord.consume_key_package("alice").unwrap().unwrap();
    assert_eq!(last_resort.key_package_ref, "last-resort");
    assert!(last_resort.is_last_resort);

    // Last-resort is retained; a consume-by-ref returns it again.
    let again = coord.consume_key_package("last-resort").unwrap().unwrap();
    assert_eq!(again.key_package_ref, "last-resort");
}

#[tokio::test]
async fn consume_by_ref_returns_exact_published_package() {
    let (coord, _tick) = coord();
    publish(&coord, "alice", "kp-1", false);
    publish(&coord, "alice", "kp-2", false);

    let consumed = coord.consume_key_package("kp-2").unwrap().unwrap();
    assert_eq!(consumed.key_package_ref, "kp-2");
    let remaining: Vec<String> = coord
        .list_key_packages_for_identity("alice")
        .unwrap()
        .iter()
        .map(|r| r.key_package_ref.clone())
        .collect();
    assert_eq!(remaining, vec!["kp-1"]);
}

// ── welcomes / join requests: clock + cleanup ───────────────────────

#[tokio::test]
async fn store_welcome_sets_created_at_and_survives_fetch() {
    let (coord, _tick) = coord();
    let w = coord
        .store_welcome(StoreWelcomeInput {
            target_stable_pubkey: "bob".to_string(),
            key_package_reference: "kp-bob".to_string(),
            welcome_bytes: vec![0x00],
            join_after_cursor: Some(42),
        })
        .unwrap();
    assert!(w.created_at > 0);
    assert_eq!(w.join_after_cursor, Some(42));

    let fetched = coord.fetch_pending_welcomes("bob", &[]).unwrap();
    assert_eq!(fetched.len(), 1);
    // Non-destructive.
    assert_eq!(coord.fetch_pending_welcomes("bob", &[]).unwrap().len(), 1);
}

#[tokio::test]
async fn store_join_request_sets_created_at_and_dedups_with_refresh() {
    let (coord, _tick) = coord();
    let first = coord
        .store_join_request(StoreJoinRequestInput {
            group_id: "g".to_string(),
            requester_stable_pubkey: "alice".to_string(),
            key_package_ref: "kp-1".to_string(),
        })
        .unwrap();
    let second = coord
        .store_join_request(StoreJoinRequestInput {
            group_id: "g".to_string(),
            requester_stable_pubkey: "alice".to_string(),
            key_package_ref: "kp-2".to_string(),
        })
        .unwrap();
    assert_eq!(second.key_package_ref, "kp-2");
    assert!(second.created_at > first.created_at);

    let fetched = coord.fetch_pending_join_requests("g", &[]).unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].key_package_ref, "kp-2");
}

#[tokio::test]
async fn maxage_cleanup_deletes_expired_welcomes_only() {
    let (coord, tick) = coord();
    coord
        .store_welcome(StoreWelcomeInput {
            target_stable_pubkey: "bob".to_string(),
            key_package_reference: "kp-old".to_string(),
            welcome_bytes: vec![0x00],
            join_after_cursor: None,
        })
        .unwrap();
    let max_age = 7_200_000_i64;
    advance(&tick, max_age + 1);
    // Threshold = now - max_age; the welcome's created_at is now older.
    let now = { *tick.lock().unwrap() };
    assert_eq!(coord.delete_expired_welcomes(now - max_age).unwrap(), 1);
    assert_eq!(coord.fetch_pending_welcomes("bob", &[]).unwrap().len(), 0);
}

// ── group messages: routing + opaque storage ────────────────────────

#[tokio::test]
async fn post_routes_by_gid_and_assigns_per_group_cursors() {
    let (coord, _tick) = coord();
    let a1 = post(&coord, "group-alpha", &[1]);
    let b1 = post(&coord, "group-beta", &[2]);
    let a2 = post(&coord, "group-alpha", &[3]);

    assert_eq!(a1.cursor, 1);
    assert_eq!(b1.cursor, 1);
    assert_eq!(a2.cursor, 2);

    let after_a1 = coord.fetch_group_messages("group-alpha", Some(1)).unwrap();
    assert_eq!(after_a1.len(), 1);
    assert_eq!(after_a1[0].cursor, 2);
    assert!(coord
        .fetch_group_messages("group-beta", Some(1))
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn fetch_many_preserves_input_group_order() {
    let (coord, _tick) = coord();
    post(&coord, "group-alpha", &[1]);
    let beta_first = post(&coord, "group-beta", &[2]);
    let alpha_second = post(&coord, "group-alpha", &[3]);
    let beta_second = post(&coord, "group-beta", &[4]);

    let fetched = coord
        .fetch_many_group_messages(&[
            FetchGroupMessagesInput {
                group_id: "group-beta".to_string(),
                after_cursor: Some(beta_first.cursor),
            },
            FetchGroupMessagesInput {
                group_id: "group-alpha".to_string(),
                after_cursor: Some(1),
            },
        ])
        .unwrap();
    let order: Vec<(String, i64)> = fetched
        .iter()
        .map(|m| (m.group_id.clone(), m.cursor))
        .collect();
    assert_eq!(
        order,
        vec![
            ("group-beta".to_string(), beta_second.cursor),
            ("group-alpha".to_string(), alpha_second.cursor),
        ]
    );
}

// ── pub/sub: single-group (live-tail only) ──────────────────────────

#[tokio::test]
async fn single_subscribe_is_live_tail_only_no_backlog_replay() {
    // The coordinator's single-group subscribe does NOT replay backlog — the
    // adapter does that. A message posted before subscribe is not delivered.
    let (coord, _tick) = coord();
    let first = post(&coord, "group-live", &[1]);

    let mut sub = coord.subscribe_group_messages("group-live");

    let second = post(&coord, "group-live", &[2]);

    let received = sub.recv().await.unwrap();
    // First delivered message is the live `second`, not the pre-subscribe
    // `first` (which lives only in fetch backlog).
    assert_eq!(received.cursor, second.cursor);
    assert_ne!(received.cursor, first.cursor);

    sub.unsubscribe();
    assert_eq!(sub.recv().await, None);
}

#[tokio::test]
async fn unsubscribe_drops_the_subscriber_from_the_count() {
    let (coord, _tick) = coord();
    let mut sub = coord.subscribe_group_messages("g");
    assert_eq!(coord.get_active_subscription_count(), 1);
    sub.unsubscribe();
    assert_eq!(coord.get_active_subscription_count(), 0);
}

#[tokio::test]
async fn live_fan_out_delivers_to_all_subscribers() {
    let (coord, _tick) = coord();
    let mut a = coord.subscribe_group_messages("g");
    let mut b = coord.subscribe_group_messages("g");
    assert_eq!(coord.get_active_subscription_count(), 2);

    let posted = post(&coord, "g", &[1, 2, 3]);

    let ra = a.recv().await.unwrap();
    let rb = b.recv().await.unwrap();
    assert_eq!(ra.cursor, posted.cursor);
    assert_eq!(rb.cursor, posted.cursor);
}

#[tokio::test]
async fn live_messages_isolate_per_group() {
    let (coord, _tick) = coord();
    let mut alpha = coord.subscribe_group_messages("group-alpha");
    let mut _beta = coord.subscribe_group_messages("group-beta");

    post(&coord, "group-beta", &[9]);
    let alpha_posted = post(&coord, "group-alpha", &[1]);

    let received = alpha.recv().await.unwrap();
    assert_eq!(received.group_id, "group-alpha");
    assert_eq!(received.cursor, alpha_posted.cursor);
}

// ── pub/sub: multi-group (backlog + live merge) ─────────────────────

#[tokio::test]
async fn multi_subscribe_replays_backlog_then_streams_live_through_one_iterator() {
    let (coord, _tick) = coord();
    let alpha_backlog = post(&coord, "group-alpha", &[1]);
    let beta_skipped = post(&coord, "group-beta", &[2]); // will be skipped by after_cursor
    let beta_backlog = post(&coord, "group-beta", &[3]);

    let mut sub = coord
        .subscribe_many_group_messages(&[
            FetchGroupMessagesInput {
                group_id: "group-alpha".to_string(),
                after_cursor: Some(0),
            },
            FetchGroupMessagesInput {
                group_id: "group-beta".to_string(),
                after_cursor: Some(beta_skipped.cursor),
            },
        ])
        .unwrap();

    // Backlog first, in input-group order, with per-group cursor filtering.
    let r1 = sub.recv().await.unwrap();
    assert_eq!(r1.group_id, "group-alpha");
    assert_eq!(r1.cursor, alpha_backlog.cursor);
    let r2 = sub.recv().await.unwrap();
    assert_eq!(r2.group_id, "group-beta");
    assert_eq!(r2.cursor, beta_backlog.cursor);

    // Then live.
    let alpha_live = post(&coord, "group-alpha", &[4]);
    let r3 = sub.recv().await.unwrap();
    assert_eq!(r3.group_id, "group-alpha");
    assert_eq!(r3.cursor, alpha_live.cursor);

    assert_eq!(coord.get_active_subscription_count(), 1);
    sub.unsubscribe();
    assert_eq!(coord.get_active_subscription_count(), 0);
    assert_eq!(sub.recv().await, None);
}

#[tokio::test]
async fn multi_subscribe_dedups_repeated_group_registrations() {
    let (coord, _tick) = coord();
    let mut sub = coord
        .subscribe_many_group_messages(&[
            FetchGroupMessagesInput {
                group_id: "group-dup".to_string(),
                after_cursor: Some(0),
            },
            FetchGroupMessagesInput {
                group_id: "group-dup".to_string(),
                after_cursor: Some(0),
            },
        ])
        .unwrap();
    assert_eq!(coord.get_active_subscription_count(), 1);
    sub.unsubscribe();
    assert_eq!(coord.get_active_subscription_count(), 0);
}

#[tokio::test]
async fn multi_subscribe_counts_as_one_active_subscription() {
    let (coord, _tick) = coord();
    let mut sub = coord
        .subscribe_many_group_messages(&[
            FetchGroupMessagesInput {
                group_id: "a".to_string(),
                after_cursor: Some(0),
            },
            FetchGroupMessagesInput {
                group_id: "b".to_string(),
                after_cursor: Some(0),
            },
            FetchGroupMessagesInput {
                group_id: "c".to_string(),
                after_cursor: Some(0),
            },
        ])
        .unwrap();
    assert_eq!(coord.get_active_subscription_count(), 1);
    sub.unsubscribe();
    assert_eq!(coord.get_active_subscription_count(), 0);
}

#[tokio::test]
async fn multi_subscribe_buffers_live_during_backlog_fetch_so_order_is_preserved() {
    // Backlog exists; a live message posted after subscribe (during backlog
    // fetch) must arrive AFTER the backlog, not interleaved before it.
    let (coord, _tick) = coord();
    let backlog = post(&coord, "g", &[1]);

    // Register the subscription, but delay its backlog delivery by posting a
    // live message immediately after subscribe (before reading). The backlog
    // must still come out first.
    let mut sub = coord
        .subscribe_many_group_messages(&[FetchGroupMessagesInput {
            group_id: "g".to_string(),
            after_cursor: Some(0),
        }])
        .unwrap();

    let live = post(&coord, "g", &[2]);

    let r1 = sub.recv().await.unwrap();
    assert_eq!(r1.cursor, backlog.cursor);
    let r2 = sub.recv().await.unwrap();
    assert_eq!(r2.cursor, live.cursor);
}

// ── cleanup timer wiring ────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn cleanup_timer_fires_and_deletes_expired_records() {
    let (now, tick) = ticking_clock();
    // Short interval for the test; the runtime clock is paused so we control it.
    let coord = Coordinator::new(CoordinatorOptions {
        now: Some(now),
        cleanup_interval_ms: Some(1_000), // 1s
        max_age_ms: Some(60_000),         // 1min
        ..CoordinatorOptions::default()
    });

    coord
        .store_welcome(StoreWelcomeInput {
            target_stable_pubkey: "bob".to_string(),
            key_package_reference: "kp".to_string(),
            welcome_bytes: vec![0x00],
            join_after_cursor: None,
        })
        .unwrap();
    // Advance the injected clock past maxAge so the next cleanup reaps it.
    advance(&tick, 120_000);

    // Drive the paused runtime clock past the interval repeatedly until the
    // background cleanup task runs and reaps the expired welcome.
    for _ in 0..20 {
        tokio::time::advance(std::time::Duration::from_millis(1_500)).await;
        tokio::task::yield_now().await;
        if coord.fetch_pending_welcomes("bob", &[]).unwrap().is_empty() {
            coord.close();
            return;
        }
    }
    coord.close();
    panic!("cleanup timer did not delete the expired welcome");
}

#[tokio::test]
async fn close_is_idempotent_and_drops_cleanly() {
    let (coord, _tick) = coord();
    coord.close();
    coord.close(); // idempotent
                   // Drop happens at end of scope without panic.
}
