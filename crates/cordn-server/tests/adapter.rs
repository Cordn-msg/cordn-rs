//! Adapter logic tests — rmcp-free. Drives [`CoordinatorAdapter`] with an
//! in-memory coordinator, a synthetic publication [`NostrEvent`], a collecting
//! [`MessageSink`], and real `ts-mls` key-package fixtures, so the binding /
//! quota / rate-limit / output-mapping / streaming behavior is exercised
//! exactly as `methods.rs` will drive it in production.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cordn_core::contracts::*;
use cordn_core::{Coordinator, CoordinatorOptions, PublishKeyPackageInput as CorePublishInput};
use cordn_server::adapter::{CoordinatorAdapter, MessageSink, Now};
use cordn_server::config::AbuseProtectionConfig;

const ALICE: &str = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
const BOB: &str = "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321";

fn now_fn() -> (Now, Arc<Mutex<i64>>) {
    let tick = Arc::new(Mutex::new(1_700_000_000_000_i64));
    let t = tick.clone();
    let now: Now = Arc::new(move || {
        let mut g = t.lock().unwrap();
        *g += 1;
        *g
    });
    (now, tick)
}

fn abuse_default() -> AbuseProtectionConfig {
    AbuseProtectionConfig {
        rate_limit: cordn_server::config::RateLimitConfig {
            enabled: true,
            refill_per_minute: 1_000_000, // effectively unbounded for logic tests
            burst: 1_000_000,
            idle_ttl_ms: 3_600_000,
        },
        key_package_quota: cordn_server::config::KeyPackageQuotaConfig {
            max_per_identity: 50,
            max_last_resort_per_identity: 1,
        },
        log_rejections: false,
    }
}

fn adapter() -> (Arc<CoordinatorAdapter>, Arc<Mutex<i64>>) {
    let (now, tick) = now_fn();
    let coord = Coordinator::new(CoordinatorOptions {
        now: Some(now.clone()),
        cleanup_interval_ms: Some(0),
        ..CoordinatorOptions::default()
    });
    let adapter = Arc::new(CoordinatorAdapter::new(coord, abuse_default(), now));
    (adapter, tick)
}

/// Synthetic publication event signed by `pubkey` (the signer the transport
/// would have verified upstream). Mirrors what `methods.rs` builds from the
/// injected `InboundEvent`.
fn event(pubkey: &str) -> NostrEvent {
    NostrEvent {
        id: "evt-1".into(),
        pubkey: pubkey.into(),
        created_at: 1,
        kind: 1111,
        tags: vec![],
        content: String::new(),
        sig: "sig".into(),
    }
}

/// A sink that collects written JSON messages and can be toggled inactive.
struct CollectSink {
    active: Arc<AtomicBool>,
    messages: Mutex<Vec<String>>,
}

use std::sync::atomic::{AtomicBool, Ordering};

impl CollectSink {
    fn new() -> (Self, Arc<AtomicBool>) {
        let active = Arc::new(AtomicBool::new(true));
        let sink = Self {
            active: active.clone(),
            messages: Mutex::new(Vec::new()),
        };
        (sink, active)
    }
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.messages.lock().unwrap())
    }
}

#[async_trait]
impl MessageSink for CollectSink {
    async fn start(&self) -> bool {
        true
    }
    async fn write(&self, msg: String) -> bool {
        if !self.is_active() {
            return false;
        }
        self.messages.lock().unwrap().push(msg);
        true
    }
    fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
    async fn close(&self) {
        self.active.store(false, Ordering::SeqCst);
    }
}

// ── publish + binding ───────────────────────────────────────────────

#[tokio::test]
async fn publish_accepts_well_formed_binding_event() {
    let (a, _t) = adapter();
    let kp_bytes = hex::decode(include_fixture_hex("regular_alice")).unwrap();
    let kp_64 = base64_encode(&kp_bytes);
    let out = a
        .publish_key_package(
            PublishKeyPackageInput {
                kp_ref: "kp-1".into(),
                kp_64,
            },
            ALICE,
            Some(event(ALICE)),
        )
        .await
        .unwrap();
    assert_eq!(out.kp_ref, "kp-1");
    assert!(!out.last_resort);
    // Stored verbatim.
    let stored = a.coordinator().get_key_package("kp-1").unwrap().unwrap();
    assert_eq!(stored.key_package_bytes, kp_bytes);
}

#[tokio::test]
async fn publish_rejects_signer_mismatch() {
    let (a, _t) = adapter();
    let kp_64 = base64_encode(&hex::decode(include_fixture_hex("regular_alice")).unwrap());
    let err = a
        .publish_key_package(
            PublishKeyPackageInput {
                kp_ref: "kp-1".into(),
                kp_64,
            },
            ALICE,            // injected caller is ALICE
            Some(event(BOB)), // event signed by BOB
        )
        .await
        .unwrap_err();
    assert!(matches!(err, cordn_server::AdapterError::SignerMismatch));
}

#[tokio::test]
async fn publish_rejects_credential_identity_mismatch() {
    // Event signer == injected caller (ALICE), but the key package's credential
    // identity is BOB's (from the last_resort_bob fixture).
    let (a, _t) = adapter();
    let kp_64 = base64_encode(&hex::decode(include_fixture_hex("last_resort_bob")).unwrap());
    let err = a
        .publish_key_package(
            PublishKeyPackageInput {
                kp_ref: "kp-1".into(),
                kp_64,
            },
            ALICE,
            Some(event(ALICE)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, cordn_server::AdapterError::IdentityMismatch));
}

#[tokio::test]
async fn publish_without_event_returns_missing_publication_event() {
    // The transport always injects `InboundEvent` for real client requests.
    // `None` only occurs for synthetic transport-internal requests, which must
    // not be allowed to publish (no event to bind/store).
    let (a, _t) = adapter();
    let kp_64 = base64_encode(&hex::decode(include_fixture_hex("regular_alice")).unwrap());
    let err = a
        .publish_key_package(
            PublishKeyPackageInput {
                kp_ref: "kp-1".into(),
                kp_64,
            },
            ALICE,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        cordn_server::AdapterError::MissingPublicationEvent
    ));
}

#[tokio::test]
async fn publish_detects_last_resort() {
    // Event + caller + credential identity all BOB; last_resort_bob fixture.
    let (a, _t) = adapter();
    let kp_64 = base64_encode(&hex::decode(include_fixture_hex("last_resort_bob")).unwrap());
    let out = a
        .publish_key_package(
            PublishKeyPackageInput {
                kp_ref: "kp-bob".into(),
                kp_64,
            },
            BOB,
            Some(event(BOB)),
        )
        .await
        .unwrap();
    assert!(out.last_resort);
}

#[tokio::test]
async fn publish_rejects_invalid_kp_bytes() {
    let (a, _t) = adapter();
    let err = a
        .publish_key_package(
            PublishKeyPackageInput {
                kp_ref: "kp-1".into(),
                kp_64: "!!!!notbase64!!!!".into(),
            },
            ALICE,
            Some(event(ALICE)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, cordn_server::AdapterError::InvalidKeyPackage));
}

// ── quota ───────────────────────────────────────────────────────────

#[tokio::test]
async fn quota_rejects_beyond_max_per_identity() {
    // Tight quota: max 2 per identity. Adapter resolves ALICE's publication event.
    let (now, _t) = now_fn();
    let coord = Coordinator::new(CoordinatorOptions {
        now: Some(now.clone()),
        cleanup_interval_ms: Some(0),
        ..CoordinatorOptions::default()
    });
    let abuse = AbuseProtectionConfig {
        rate_limit: abuse_default().rate_limit,
        key_package_quota: cordn_server::config::KeyPackageQuotaConfig {
            max_per_identity: 2,
            max_last_resort_per_identity: 1,
        },
        log_rejections: false,
    };
    let a = Arc::new(CoordinatorAdapter::new(coord, abuse, now));
    let kp_64 = base64_encode(&hex::decode(include_fixture_hex("regular_alice")).unwrap());
    // Two publishes succeed.
    a.publish_key_package(
        PublishKeyPackageInput {
            kp_ref: "kp-1".into(),
            kp_64: kp_64.clone(),
        },
        ALICE,
        Some(event(ALICE)),
    )
    .await
    .unwrap();
    a.publish_key_package(
        PublishKeyPackageInput {
            kp_ref: "kp-2".into(),
            kp_64: kp_64.clone(),
        },
        ALICE,
        Some(event(ALICE)),
    )
    .await
    .unwrap();
    // Third is rejected by the per-identity quota.
    let err = a
        .publish_key_package(
            PublishKeyPackageInput {
                kp_ref: "kp-3".into(),
                kp_64,
            },
            ALICE,
            Some(event(ALICE)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, cordn_server::AdapterError::QuotaExceeded));
}

// ── welcomes / join requests / messages round-trip ──────────────────

#[tokio::test]
async fn store_then_fetch_welcome_round_trip() {
    let (a, _t) = adapter();
    a.store_welcome(
        StoreWelcomeInput {
            target_pk: ALICE.into(),
            kp_ref: "kp-1".into(),
            welcome_64: base64_encode(b"welcome-bytes"),
            after: Some(9),
        },
        BOB,
    )
    .unwrap();
    let out = a
        .fetch_pending_welcomes(FetchPendingWelcomesInput { consumed: None }, ALICE)
        .unwrap();
    assert_eq!(out.welcomes.len(), 1);
    assert_eq!(out.welcomes[0].kp_ref, "kp-1");
    assert_eq!(out.welcomes[0].welcome_64, base64_encode(b"welcome-bytes"));
    assert_eq!(out.welcomes[0].after, Some(9));
}

#[tokio::test]
async fn post_then_fetch_group_messages_round_trip() {
    let (a, _t) = adapter();
    let posted = a
        .post_group_message(
            PostGroupMessageInput {
                msg_64: base64_encode(b"hello"),
                gid: "g".into(),
            },
            ALICE,
        )
        .unwrap();
    assert_eq!(posted.cursor, 1);
    let fetched = a
        .fetch_group_messages(
            FetchGroupMessagesInput {
                gid: "g".into(),
                after: None,
            },
            ALICE,
        )
        .unwrap();
    assert_eq!(fetched.messages.len(), 1);
    assert_eq!(fetched.messages[0].cursor, 1);
    assert_eq!(fetched.messages[0].gid, "g");
    assert_eq!(fetched.messages[0].msg_64, base64_encode(b"hello"));
    assert_eq!(fetched.messages[0].encrypted, Some(true));
}

#[tokio::test]
async fn fetch_group_messages_rejects_non_positive_after_cursor() {
    // Parity with the TS wire schema `after: z.number().int().positive().optional()`:
    // 0 and negatives are rejected; absent (None) and positive cursors are accepted.
    let (a, _t) = adapter();
    a.post_group_message(
        PostGroupMessageInput {
            msg_64: base64_encode(b"hello"),
            gid: "g".into(),
        },
        ALICE,
    )
    .unwrap();

    for bad in [0, -3] {
        let err = a
            .fetch_group_messages(
                FetchGroupMessagesInput {
                    gid: "g".into(),
                    after: Some(bad),
                },
                ALICE,
            )
            .unwrap_err();
        assert!(
            matches!(err, cordn_server::AdapterError::InvalidInput(_)),
            "after={bad} should be rejected"
        );
    }

    // The multi-group path rejects a non-positive cursor in any element too.
    let err = a
        .fetch_many_group_messages(
            FetchManyGroupMessagesInput {
                groups: vec![FetchGroupMessagesInput {
                    gid: "g".into(),
                    after: Some(0),
                }],
            },
            ALICE,
        )
        .unwrap_err();
    assert!(matches!(err, cordn_server::AdapterError::InvalidInput(_)));

    // Sanity: None and a positive cursor still work.
    a.fetch_group_messages(
        FetchGroupMessagesInput {
            gid: "g".into(),
            after: None,
        },
        ALICE,
    )
    .unwrap();
    a.fetch_group_messages(
        FetchGroupMessagesInput {
            gid: "g".into(),
            after: Some(1),
        },
        ALICE,
    )
    .unwrap();
}

#[tokio::test]
async fn remove_key_packages_enforces_ownership() {
    let (a, _t) = adapter();
    a.coordinator()
        .publish_key_package(CorePublishInput {
            stable_pubkey: ALICE.into(),
            key_package_bytes: vec![1],
            key_package_ref: "kp-alice".into(),
            is_last_resort: false,
            publication_event: serde_json::Value::Null,
        })
        .unwrap();
    // BOB cannot remove ALICE's package.
    let err = a
        .remove_key_packages(
            RemoveKeyPackagesInput {
                kp_refs: vec!["kp-alice".into()],
            },
            BOB,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        cordn_server::AdapterError::UnauthorizedKeyPackageRef(_)
    ));
    // ALICE can.
    let out = a
        .remove_key_packages(
            RemoveKeyPackagesInput {
                kp_refs: vec!["kp-alice".into()],
            },
            ALICE,
        )
        .unwrap();
    assert_eq!(out.kp_refs, vec!["kp-alice"]);
}

// ── streaming subscribe ─────────────────────────────────────────────

#[tokio::test]
async fn subscribe_streams_backlog_then_live_then_closes() {
    let (a, _t) = adapter();
    // Backlog first.
    a.post_group_message(
        PostGroupMessageInput {
            msg_64: base64_encode(b"backlog"),
            gid: "g".into(),
        },
        ALICE,
    )
    .unwrap();

    let (sink, active) = CollectSink::new();
    let sink_ref: &dyn MessageSink = &sink;

    // Run the streaming subscribe concurrently with a driver that posts a live
    // message then deactivates the sink so the live loop exits. Same task (no
    // spawn) so the local sink borrow is fine.
    let a_clone = a.clone();
    let subscribe = a.subscribe_group_messages(
        SubscribeGroupMessagesInput {
            gid: "g".into(),
            after: None,
        },
        ALICE,
        sink_ref,
    );
    let driver = async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        a_clone
            .post_group_message(
                PostGroupMessageInput {
                    msg_64: base64_encode(b"live"),
                    gid: "g".into(),
                },
                ALICE,
            )
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        active.store(false, Ordering::SeqCst);
    };
    let (result, ()) = tokio::join!(subscribe, driver);
    result.unwrap();

    let messages = sink.take();
    assert_eq!(messages.len(), 2, "expected backlog + live: {messages:?}");
}

#[tokio::test]
async fn subscribe_many_streams_merged_groups() {
    let (a, _t) = adapter();
    a.post_group_message(
        PostGroupMessageInput {
            msg_64: base64_encode(b"a1"),
            gid: "ga".into(),
        },
        ALICE,
    )
    .unwrap();
    a.post_group_message(
        PostGroupMessageInput {
            msg_64: base64_encode(b"b1"),
            gid: "gb".into(),
        },
        ALICE,
    )
    .unwrap();

    let (sink, active) = CollectSink::new();
    let sink_ref: &dyn MessageSink = &sink;
    let subscribe = a.subscribe_many_group_messages(
        SubscribeManyGroupMessagesInput {
            groups: vec![
                FetchGroupMessagesInput {
                    gid: "ga".into(),
                    after: None,
                },
                FetchGroupMessagesInput {
                    gid: "gb".into(),
                    after: None,
                },
            ],
        },
        ALICE,
        sink_ref,
    );
    let driver = async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        active.store(false, Ordering::SeqCst);
    };
    let (result, ()) = tokio::join!(subscribe, driver);
    result.unwrap();

    let messages = sink.take();
    assert_eq!(messages.len(), 2, "both groups' backlog: {messages:?}");
}

// ── rate limiting ───────────────────────────────────────────────────

#[tokio::test]
async fn rate_limit_denies_when_bucket_empty() {
    let (now, _t) = now_fn();
    let coord = Coordinator::new(CoordinatorOptions {
        now: Some(now.clone()),
        cleanup_interval_ms: Some(0),
        ..CoordinatorOptions::default()
    });
    let abuse = AbuseProtectionConfig {
        rate_limit: cordn_server::config::RateLimitConfig {
            enabled: true,
            refill_per_minute: 0, // no refill
            burst: 1,
            idle_ttl_ms: 0,
        },
        key_package_quota: cordn_server::config::KeyPackageQuotaConfig {
            max_per_identity: 50,
            max_last_resort_per_identity: 1,
        },
        log_rejections: false,
    };
    let a = Arc::new(CoordinatorAdapter::new(coord, abuse, now));
    // First call consumes the single burst token.
    a.list_available_key_packages(ALICE).unwrap();
    // Second call is denied.
    let err = a.list_available_key_packages(ALICE).unwrap_err();
    assert!(matches!(err, cordn_server::AdapterError::RateLimitExceeded));
}

// ── helpers ─────────────────────────────────────────────────────────

fn base64_encode(b: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    STANDARD.encode(b)
}

/// Load a key-package fixture's hex from the cordn-core fixture file by name.
fn include_fixture_hex(name: &str) -> String {
    let json: serde_json::Value = serde_json::from_str(include_str!(
        "../../cordn-core/tests/fixtures/key_packages.json"
    ))
    .expect("fixture json");
    json["fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("fixture {name} not found"))["bytes_hex"]
        .as_str()
        .unwrap()
        .to_owned()
}
