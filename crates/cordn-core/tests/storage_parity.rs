//! Storage parity tests — ported from
//! `references/cordn/src/coordinator/storage/storage.test.ts`, adapted to test
//! the storage layer directly. The TS tests route through the `Coordinator`
//! (which supplies `now()` and computes `isLastResort` from a parsed key
//! package); here we drive storage with explicit timestamps and flags, since
//! storage is opaque to MLS and takes both as input. The coordinator's wiring
//! of `now()`/`isLastResort` is covered by the coordinator tests (step 2).
//!
//! Each test runs against both the in-memory and the sqlite backend via
//! [`each_backend`], enforcing the parity mandate in `AGENTS.md`.

use cordn_core::{
    partition_consumed_join_requests, AppendGroupMessageParams, ConsumedJoinRequestRef,
    ConsumedJoinRequestWithGroupRef, ConsumedWelcomeRef, CoordinatorStorage,
    FetchGroupMessagesInput, InMemoryCoordinatorStorage, JoinRequestRecord,
    PublishedKeyPackageRecord, SqliteCoordinatorStorage, StorageError, WelcomeQueueRecord,
    MAX_PENDING_JOIN_REQUESTS_PER_GROUP,
};

/// Run a test body against every storage backend.
fn each_backend<F>(mut f: F)
where
    F: FnMut(&dyn CoordinatorStorage),
{
    let mem = InMemoryCoordinatorStorage::new();
    f(&mem);
    let sqlite = SqliteCoordinatorStorage::open_in_memory().expect("open in-memory sqlite");
    f(&sqlite);
}

// ── record builders ──────────────────────────────────────────────────

fn kp(
    stable_pubkey: &str,
    kp_ref: &str,
    is_last_resort: bool,
    published_at: i64,
) -> PublishedKeyPackageRecord {
    PublishedKeyPackageRecord {
        stable_pubkey: stable_pubkey.to_string(),
        // Opaque to storage; arbitrary non-empty bytes prove verbatim storage.
        key_package_bytes: vec![0xde, 0xad, 0xbe, 0xef],
        key_package_ref: kp_ref.to_string(),
        is_last_resort,
        published_at,
        publication_event: serde_json::json!({
            "id": kp_ref, "pubkey": stable_pubkey, "created_at": 1,
            "kind": 1111, "tags": [], "content": "kp", "sig": "s",
        }),
    }
}

fn welcome(
    target: &str,
    kp_ref: &str,
    created_at: i64,
    join_after_cursor: Option<i64>,
) -> WelcomeQueueRecord {
    WelcomeQueueRecord {
        target_stable_pubkey: target.to_string(),
        key_package_reference: kp_ref.to_string(),
        welcome_bytes: vec![0x00, 0x01, 0x02, 0x03],
        created_at,
        join_after_cursor,
    }
}

fn jr(group: &str, requester: &str, kp_ref: &str, created_at: i64) -> JoinRequestRecord {
    JoinRequestRecord {
        group_id: group.to_string(),
        requester_stable_pubkey: requester.to_string(),
        key_package_ref: kp_ref.to_string(),
        created_at,
    }
}

fn append(
    storage: &dyn CoordinatorStorage,
    group_id: &str,
    opaque_message: Vec<u8>,
    created_at: i64,
) -> cordn_core::GroupMessageRecord {
    storage
        .append_group_message(AppendGroupMessageParams {
            group_id: group_id.to_string(),
            opaque_message,
            created_at,
        })
        .unwrap()
}

// ── key packages ─────────────────────────────────────────────────────

#[test]
fn publishes_lists_consumes_in_fifo_order() {
    each_backend(|storage| {
        storage
            .publish_key_package(kp("alice", "kp-1", false, 100))
            .unwrap();
        storage
            .publish_key_package(kp("alice", "kp-2", false, 101))
            .unwrap();

        let for_identity = storage.list_key_packages_for_identity("alice").unwrap();
        assert_eq!(for_identity.len(), 2);
        assert_eq!(
            storage
                .list_all_key_packages()
                .unwrap()
                .iter()
                .map(|r| r.key_package_ref.clone())
                .collect::<Vec<_>>(),
            vec!["kp-1", "kp-2"]
        );

        let first = storage.consume_key_package("alice").unwrap();
        assert_eq!(
            first.as_ref().map(|r| r.key_package_ref.as_str()),
            Some("kp-1")
        );
        let second = storage.consume_key_package("kp-2").unwrap();
        assert_eq!(
            second.as_ref().map(|r| r.key_package_ref.as_str()),
            Some("kp-2")
        );
        let empty = storage.consume_key_package("alice").unwrap();
        assert!(empty.is_none());
    });
}

#[test]
fn keeps_last_resort_after_consume_and_supports_explicit_remove() {
    each_backend(|storage| {
        storage
            .publish_key_package(kp("alice", "regular", false, 100))
            .unwrap();
        storage
            .publish_key_package(kp("alice", "last-resort", true, 101))
            .unwrap();

        // Consume by identity prefers the regular (and removes it).
        let first = storage.consume_key_package("alice").unwrap();
        assert_eq!(
            first.as_ref().map(|r| r.key_package_ref.as_str()),
            Some("regular")
        );

        // With only a last-resort left, identity consume falls back to it
        // non-destructively.
        let second = storage.consume_key_package("alice").unwrap();
        assert_eq!(
            second.as_ref().map(|r| r.key_package_ref.as_str()),
            Some("last-resort")
        );

        let kept = storage.get_key_package("last-resort").unwrap();
        assert_eq!(kept.as_ref().map(|r| r.is_last_resort), Some(true));

        let removed = storage.remove_key_package("last-resort").unwrap();
        assert_eq!(
            removed.as_ref().map(|r| r.key_package_ref.as_str()),
            Some("last-resort")
        );
        assert!(storage.get_key_package("last-resort").unwrap().is_none());
    });
}

#[test]
fn consume_by_ref_keeps_last_resort_non_destructively() {
    each_backend(|storage| {
        storage
            .publish_key_package(kp("alice", "last-resort", true, 100))
            .unwrap();

        let consumed = storage.consume_key_package("last-resort").unwrap();
        assert_eq!(
            consumed.as_ref().map(|r| r.key_package_ref.as_str()),
            Some("last-resort")
        );
        // Still present — last-resort is never consumed away by ref.
        assert!(storage.get_key_package("last-resort").unwrap().is_some());

        // A second consume-by-ref of the same last-resort still returns it.
        let again = storage.consume_key_package("last-resort").unwrap();
        assert_eq!(
            again.as_ref().map(|r| r.key_package_ref.as_str()),
            Some("last-resort")
        );
    });
}

// ── welcomes ─────────────────────────────────────────────────────────

#[test]
fn stores_and_returns_welcomes_per_identity_without_draining() {
    each_backend(|storage| {
        storage
            .store_welcome(welcome("bob", "kp-bob", 100, None))
            .unwrap();
        storage
            .store_welcome(welcome("carol", "kp-carol", 101, None))
            .unwrap();

        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 1);
        assert_eq!(
            storage.fetch_pending_welcomes("carol", &[]).unwrap().len(),
            1
        );
        // Non-destructive: a second fetch still returns them.
        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 1);
        assert_eq!(
            storage.fetch_pending_welcomes("carol", &[]).unwrap().len(),
            1
        );
    });
}

#[test]
fn round_trips_welcome_join_after_cursor_and_defaults_to_none() {
    each_backend(|storage| {
        storage
            .store_welcome(welcome("bob", "kp-with-cursor", 100, Some(42)))
            .unwrap();
        storage
            .store_welcome(welcome("bob", "kp-no-cursor", 101, None))
            .unwrap();

        let fetched = storage.fetch_pending_welcomes("bob", &[]).unwrap();
        let with_cursor = fetched
            .iter()
            .find(|w| w.key_package_reference == "kp-with-cursor");
        let without = fetched
            .iter()
            .find(|w| w.key_package_reference == "kp-no-cursor");
        assert_eq!(with_cursor.and_then(|w| w.join_after_cursor), Some(42));
        assert_eq!(without.and_then(|w| w.join_after_cursor), None);
    });
}

#[test]
fn observation_never_deletes_welcomes_maxage_is_the_only_cleanup_clock() {
    each_backend(|storage| {
        let t0 = 1_700_000_000_000;
        let max_age = 7_200_000; // 2h
        storage
            .store_welcome(welcome("bob", "kp-1", t0, None))
            .unwrap();

        // Observe — non-destructive.
        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 1);

        // Advance past a hypothetical short read-TTL but stay within maxAge.
        assert_eq!(
            storage
                .delete_expired_welcomes(t0 + 3_700_000 - max_age)
                .unwrap(),
            0
        );
        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 1);

        // Crossing the maxAge ceiling removes it (created_at < threshold).
        assert_eq!(
            storage
                .delete_expired_welcomes(t0 + max_age + 1 - max_age)
                .unwrap(),
            1
        );
        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 0);
    });
}

#[test]
fn consumed_ack_retires_a_welcome_atomically_on_fetch() {
    each_backend(|storage| {
        storage
            .store_welcome(welcome("bob", "kp-1", 100, None))
            .unwrap();
        let observed = storage.fetch_pending_welcomes("bob", &[]).unwrap();
        assert_eq!(observed.len(), 1);
        let at = observed[0].created_at;

        let after = storage
            .fetch_pending_welcomes(
                "bob",
                &[ConsumedWelcomeRef {
                    key_package_reference: "kp-1".to_string(),
                    created_at: at,
                }],
            )
            .unwrap();
        assert!(after.is_empty());
    });
}

#[test]
fn delete_expired_welcomes_threshold_zero_is_a_noop() {
    each_backend(|storage| {
        storage
            .store_welcome(welcome("bob", "kp-1", 100, None))
            .unwrap();
        assert_eq!(storage.delete_expired_welcomes(0).unwrap(), 0);
        assert_eq!(storage.delete_expired_welcomes(-5).unwrap(), 0);
        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 1);
    });
}

#[test]
fn deletes_welcomes_older_than_maxage() {
    each_backend(|storage| {
        let t0 = 1_700_000_000_000;
        let max_age = 3_600_000;
        storage
            .store_welcome(welcome("bob", "kp-1", t0, None))
            .unwrap();

        let now = t0 + max_age + 60_000;
        assert_eq!(storage.delete_expired_welcomes(now - max_age).unwrap(), 1);
        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 0);
    });
}

#[test]
fn keeps_welcomes_younger_than_maxage() {
    each_backend(|storage| {
        let t0 = 1_700_000_000_000;
        let max_age = 3_600_000;
        storage
            .store_welcome(welcome("bob", "kp-1", t0, None))
            .unwrap();

        let now = t0 + max_age - 60_000;
        assert_eq!(storage.delete_expired_welcomes(now - max_age).unwrap(), 0);
        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 1);
    });
}

#[test]
fn delete_expired_welcomes_reaps_by_created_at_regardless_of_fetch() {
    each_backend(|storage| {
        let t0 = 1_700_000_000_000;
        let max_age = 7_200_000; // 2h
        storage
            .store_welcome(welcome("bob", "kp-1", t0, None))
            .unwrap();

        // Advance 2h then fetch — observation must not reset the clock.
        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 1);

        // createdAt is now ~2.5h old — past the 2h maxAge ceiling.
        let now = t0 + 7_200_000 + 1_800_000;
        assert_eq!(storage.delete_expired_welcomes(now - max_age).unwrap(), 1);
        assert_eq!(storage.fetch_pending_welcomes("bob", &[]).unwrap().len(), 0);
    });
}

// ── group messages ───────────────────────────────────────────────────

#[test]
fn stores_group_messages_opaquely_and_routes_per_group() {
    each_backend(|storage| {
        let first = append(storage, "group-local", vec![1, 2, 3], 100);
        let second = append(storage, "group-local", vec![4, 5, 6], 101);

        assert_eq!(
            storage
                .fetch_group_messages("group-local", None)
                .unwrap()
                .len(),
            2
        );
        let after_first = storage
            .fetch_group_messages("group-local", Some(first.cursor))
            .unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].cursor, second.cursor);
    });
}

#[test]
fn stores_and_round_trips_an_opaque_group_message() {
    each_backend(|storage| {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let posted = storage
            .append_group_message(AppendGroupMessageParams {
                group_id: "opaque-topic".to_string(),
                opaque_message: bytes.clone(),
                created_at: 100,
            })
            .unwrap();
        assert_eq!(posted.group_id, "opaque-topic");
        assert_eq!(posted.opaque_message, bytes);
        assert_eq!(posted.cursor, 1);

        let fetched = storage.fetch_group_messages("opaque-topic", None).unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].opaque_message, bytes);
    });
}

#[test]
fn interleaves_messages_on_a_shared_per_group_cursor_sequence() {
    each_backend(|storage| {
        let first = append(storage, "group-local", vec![0xde, 0xad], 100);
        let second = append(storage, "group-local", vec![0xc0, 0xff, 0xee], 101);
        assert_eq!(first.cursor, 1);
        assert_eq!(second.cursor, 2);

        let fetched = storage.fetch_group_messages("group-local", None).unwrap();
        assert_eq!(
            fetched.iter().map(|m| m.cursor).collect::<Vec<_>>(),
            vec![1, 2]
        );
    });
}

#[test]
fn assigns_monotonic_cursors_independently_per_group() {
    each_backend(|storage| {
        let alpha_first = append(storage, "group-alpha", vec![1, 2, 3], 100);
        let beta_first = append(storage, "group-beta", vec![4, 5, 6], 101);
        let alpha_second = append(storage, "group-alpha", vec![7, 8, 9], 102);

        assert_eq!(alpha_first.cursor, 1);
        assert_eq!(beta_first.cursor, 1);
        assert_eq!(alpha_second.cursor, 2);

        let alpha = storage.fetch_group_messages("group-alpha", None).unwrap();
        let beta = storage.fetch_group_messages("group-beta", None).unwrap();
        assert_eq!(
            alpha.iter().map(|m| m.cursor).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(beta.iter().map(|m| m.cursor).collect::<Vec<_>>(), vec![1]);
    });
}

#[test]
fn treats_after_cursor_as_group_scoped_across_groups_with_same_cursor_values() {
    each_backend(|storage| {
        let alpha_first = append(storage, "group-alpha", vec![1], 100);
        append(storage, "group-beta", vec![2], 101);
        let alpha_second = append(storage, "group-alpha", vec![3], 102);

        assert_eq!(alpha_first.cursor, 1);
        assert_eq!(alpha_second.cursor, 2);

        let alpha_after_1 = storage
            .fetch_group_messages("group-alpha", Some(1))
            .unwrap();
        assert_eq!(alpha_after_1.len(), 1);
        assert_eq!(alpha_after_1[0].cursor, 2);
        assert_eq!(alpha_after_1[0].group_id, "group-alpha");

        let beta_after_1 = storage.fetch_group_messages("group-beta", Some(1)).unwrap();
        assert!(beta_after_1.is_empty());
    });
}

#[test]
fn fetches_many_group_messages_with_independent_cursors_in_input_order() {
    each_backend(|storage| {
        append(storage, "group-alpha", vec![1], 100);
        let beta_first = append(storage, "group-beta", vec![2], 101);
        let alpha_second = append(storage, "group-alpha", vec![3], 102);
        let beta_second = append(storage, "group-beta", vec![4], 103);

        let fetched = storage
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
    });
}

#[test]
fn fetch_many_group_messages_empty_input_returns_empty() {
    each_backend(|storage| {
        append(storage, "group-alpha", vec![1], 100);
        let fetched = storage.fetch_many_group_messages(&[]).unwrap();
        assert!(fetched.is_empty());
    });
}

// ── join requests ────────────────────────────────────────────────────

#[test]
fn stores_and_returns_join_requests_per_group_without_draining() {
    each_backend(|storage| {
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-alice-1", 100))
            .unwrap();
        storage
            .store_join_request(jr("group-alpha", "bob", "kp-bob-1", 101))
            .unwrap();
        storage
            .store_join_request(jr("group-beta", "carol", "kp-carol-1", 102))
            .unwrap();

        let alpha = storage
            .fetch_pending_join_requests("group-alpha", &[])
            .unwrap();
        let beta = storage
            .fetch_pending_join_requests("group-beta", &[])
            .unwrap();
        assert_eq!(alpha.len(), 2);
        assert_eq!(alpha[0].requester_stable_pubkey, "alice");
        assert_eq!(alpha[0].key_package_ref, "kp-alice-1");
        assert_eq!(alpha[1].requester_stable_pubkey, "bob");
        assert_eq!(beta.len(), 1);
        assert_eq!(beta[0].requester_stable_pubkey, "carol");

        // Non-destructive.
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-alpha", &[])
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-beta", &[])
                .unwrap()
                .len(),
            1
        );
    });
}

#[test]
fn deduplicates_join_requests_refreshing_on_re_request() {
    each_backend(|storage| {
        let first = storage
            .store_join_request(jr("group-alpha", "alice", "kp-alice-1", 100))
            .unwrap();
        let first_created_at = first.created_at;

        let second = storage
            .store_join_request(jr("group-alpha", "alice", "kp-alice-2", 200))
            .unwrap();
        assert_eq!(second.key_package_ref, "kp-alice-2");
        assert!(second.created_at > first_created_at);

        let fetched = storage
            .fetch_pending_join_requests("group-alpha", &[])
            .unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].requester_stable_pubkey, "alice");
        assert_eq!(fetched[0].key_package_ref, "kp-alice-2");
        assert_eq!(fetched[0].created_at, second.created_at);
    });
}

#[test]
fn a_re_request_evades_a_consume_ref_recorded_against_the_original_created_at() {
    each_backend(|storage| {
        let first = storage
            .store_join_request(jr("group-alpha", "alice", "kp-alice-1", 100))
            .unwrap();
        let original_created_at = first.created_at;
        let consumed_ref = ConsumedJoinRequestRef {
            requester_stable_pubkey: "alice".to_string(),
            created_at: original_created_at,
        };

        // Re-request before the admin's consuming fetch.
        let refreshed = storage
            .store_join_request(jr("group-alpha", "alice", "kp-alice-2", 200))
            .unwrap();
        assert_eq!(refreshed.key_package_ref, "kp-alice-2");
        assert_ne!(refreshed.created_at, original_created_at);

        // The admin's fetch carries the stale consume ref — bumped createdAt
        // means it does not match and the refreshed request is returned.
        let fetched = storage
            .fetch_pending_join_requests("group-alpha", &[consumed_ref])
            .unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].key_package_ref, "kp-alice-2");
    });
}

#[test]
fn allows_a_new_join_request_only_after_the_previous_one_is_consumed() {
    each_backend(|storage| {
        let first = storage
            .store_join_request(jr("group-alpha", "alice", "kp-alice-1", 100))
            .unwrap();
        let _ = first;
        let deduped = storage
            .store_join_request(jr("group-alpha", "alice", "kp-alice-2", 200))
            .unwrap();
        // Observe without consuming does not insert another row.
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-alpha", &[])
                .unwrap()
                .len(),
            1
        );

        // Consume the refreshed request via its new createdAt.
        storage
            .fetch_pending_join_requests(
                "group-alpha",
                &[ConsumedJoinRequestRef {
                    requester_stable_pubkey: "alice".to_string(),
                    created_at: deduped.created_at,
                }],
            )
            .unwrap();

        let new_req = storage
            .store_join_request(jr("group-alpha", "alice", "kp-alice-3", 300))
            .unwrap();
        assert_eq!(new_req.key_package_ref, "kp-alice-3");

        let fetched = storage
            .fetch_pending_join_requests("group-alpha", &[])
            .unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].key_package_ref, "kp-alice-3");
    });
}

#[test]
fn observation_never_deletes_join_requests_maxage_is_the_only_cleanup_clock() {
    each_backend(|storage| {
        let t0 = 1_700_000_000_000;
        let max_age = 7_200_000;
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-1", t0))
            .unwrap();
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-alpha", &[])
                .unwrap()
                .len(),
            1
        );

        assert_eq!(
            storage
                .delete_expired_join_requests(t0 + 3_700_000 - max_age)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-alpha", &[])
                .unwrap()
                .len(),
            1
        );

        assert_eq!(
            storage
                .delete_expired_join_requests(t0 + max_age + 1 - max_age)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-alpha", &[])
                .unwrap()
                .len(),
            0
        );
    });
}

#[test]
fn delete_expired_join_requests_threshold_zero_is_a_noop() {
    each_backend(|storage| {
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-1", 100))
            .unwrap();
        assert_eq!(storage.delete_expired_join_requests(0).unwrap(), 0);
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-alpha", &[])
                .unwrap()
                .len(),
            1
        );
    });
}

#[test]
fn deletes_join_requests_older_than_maxage() {
    each_backend(|storage| {
        let t0 = 1_700_000_000_000;
        let max_age = 3_600_000;
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-1", t0))
            .unwrap();
        let now = t0 + max_age + 60_000;
        assert_eq!(
            storage.delete_expired_join_requests(now - max_age).unwrap(),
            1
        );
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-alpha", &[])
                .unwrap()
                .len(),
            0
        );
    });
}

#[test]
fn keeps_join_requests_younger_than_maxage() {
    each_backend(|storage| {
        let t0 = 1_700_000_000_000;
        let max_age = 3_600_000;
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-1", t0))
            .unwrap();
        let now = t0 + max_age - 60_000;
        assert_eq!(
            storage.delete_expired_join_requests(now - max_age).unwrap(),
            0
        );
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-alpha", &[])
                .unwrap()
                .len(),
            1
        );
    });
}

#[test]
fn rejects_join_requests_when_cap_is_reached() {
    each_backend(|storage| {
        for i in 0..MAX_PENDING_JOIN_REQUESTS_PER_GROUP {
            storage
                .store_join_request(jr(
                    "capped-group",
                    &format!("requester-{i}"),
                    &format!("kp-{i}"),
                    100 + i as i64,
                ))
                .unwrap();
        }

        let err = storage
            .store_join_request(jr("capped-group", "one-too-many", "kp-overflow", 999))
            .unwrap_err();
        assert!(
            matches!(err, StorageError::TooManyPendingJoinRequests),
            "got {err:?}"
        );
    });
}

#[test]
fn allows_join_requests_for_groups_with_no_messages_bootstrap() {
    each_backend(|storage| {
        let record = storage
            .store_join_request(jr("brand-new-group-no-messages", "alice", "kp-1", 100))
            .unwrap();
        assert_eq!(record.group_id, "brand-new-group-no-messages");
        assert_eq!(record.requester_stable_pubkey, "alice");

        let fetched = storage
            .fetch_pending_join_requests("brand-new-group-no-messages", &[])
            .unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].key_package_ref, "kp-1");
    });
}

#[test]
fn fetches_many_pending_join_requests_across_groups() {
    each_backend(|storage| {
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-alice-1", 100))
            .unwrap();
        storage
            .store_join_request(jr("group-alpha", "bob", "kp-bob-1", 101))
            .unwrap();
        storage
            .store_join_request(jr("group-beta", "carol", "kp-carol-1", 102))
            .unwrap();

        let results = storage
            .fetch_many_pending_join_requests(
                &["group-alpha".to_string(), "group-beta".to_string()],
                &[],
            )
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].group_id, "group-alpha");
        assert_eq!(results[0].requester_stable_pubkey, "alice");
        assert_eq!(results[1].group_id, "group-alpha");
        assert_eq!(results[1].requester_stable_pubkey, "bob");
        assert_eq!(results[2].group_id, "group-beta");
        assert_eq!(results[2].requester_stable_pubkey, "carol");

        // Non-destructive.
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-alpha", &[])
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            storage
                .fetch_pending_join_requests("group-beta", &[])
                .unwrap()
                .len(),
            1
        );
    });
}

#[test]
fn fetch_many_pending_join_requests_empty_for_groups_with_no_requests() {
    each_backend(|storage| {
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-1", 100))
            .unwrap();
        let results = storage
            .fetch_many_pending_join_requests(
                &[
                    "group-alpha".to_string(),
                    "group-empty".to_string(),
                    "group-beta".to_string(),
                ],
                &[],
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].group_id, "group-alpha");
        assert_eq!(results[0].requester_stable_pubkey, "alice");
    });
}

#[test]
fn fetch_many_pending_join_requests_empty_input_returns_empty() {
    each_backend(|storage| {
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-1", 100))
            .unwrap();
        let results = storage.fetch_many_pending_join_requests(&[], &[]).unwrap();
        assert!(results.is_empty());
    });
}

#[test]
fn consumed_ack_retires_single_group_join_requests_atomically_on_fetch() {
    each_backend(|storage| {
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-1", 100))
            .unwrap();
        storage
            .store_join_request(jr("group-alpha", "bob", "kp-2", 101))
            .unwrap();

        let observed = storage
            .fetch_pending_join_requests("group-alpha", &[])
            .unwrap();
        assert_eq!(observed.len(), 2);
        let alice_at = observed
            .iter()
            .find(|r| r.requester_stable_pubkey == "alice")
            .unwrap()
            .created_at;

        let after = storage
            .fetch_pending_join_requests(
                "group-alpha",
                &[ConsumedJoinRequestRef {
                    requester_stable_pubkey: "alice".to_string(),
                    created_at: alice_at,
                }],
            )
            .unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].requester_stable_pubkey, "bob");
    });
}

#[test]
fn consumed_ack_retires_join_requests_across_groups_via_fetch_many() {
    each_backend(|storage| {
        storage
            .store_join_request(jr("group-alpha", "alice", "kp-1", 100))
            .unwrap();
        storage
            .store_join_request(jr("group-beta", "carol", "kp-2", 101))
            .unwrap();

        let observed = storage
            .fetch_many_pending_join_requests(
                &["group-alpha".to_string(), "group-beta".to_string()],
                &[],
            )
            .unwrap();
        assert_eq!(observed.len(), 2);
        let carol_at = observed
            .iter()
            .find(|r| r.requester_stable_pubkey == "carol")
            .unwrap()
            .created_at;

        let after = storage
            .fetch_many_pending_join_requests(
                &["group-alpha".to_string(), "group-beta".to_string()],
                &[ConsumedJoinRequestWithGroupRef {
                    group_id: "group-beta".to_string(),
                    requester_stable_pubkey: "carol".to_string(),
                    created_at: carol_at,
                }],
            )
            .unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].group_id, "group-alpha");
        assert_eq!(after[0].requester_stable_pubkey, "alice");
    });
}

// ── shared helper parity ─────────────────────────────────────────────

#[test]
fn partition_consumed_join_requests_groups_by_group_id() {
    let map = partition_consumed_join_requests(&[
        ConsumedJoinRequestWithGroupRef {
            group_id: "g1".to_string(),
            requester_stable_pubkey: "a".to_string(),
            created_at: 1,
        },
        ConsumedJoinRequestWithGroupRef {
            group_id: "g2".to_string(),
            requester_stable_pubkey: "b".to_string(),
            created_at: 2,
        },
        ConsumedJoinRequestWithGroupRef {
            group_id: "g1".to_string(),
            requester_stable_pubkey: "c".to_string(),
            created_at: 3,
        },
    ]);
    assert_eq!(map.len(), 2);
    assert_eq!(map["g1"].len(), 2);
    assert_eq!(map["g1"][0].requester_stable_pubkey, "a");
    assert_eq!(map["g1"][1].requester_stable_pubkey, "c");
    assert_eq!(map["g2"].len(), 1);
}
