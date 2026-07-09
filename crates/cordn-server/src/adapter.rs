//! `CoordinatorAdapter` — the server-layer logic between the rmcp wire
//! contracts and the [`cordn_core::Coordinator`]. Ports
//! `references/cordn/src/server/coordinatorMethods.ts`: rate limiting, key-
//! package quota enforcement, publication-event binding, MLS admission parsing,
//! output mapping, and the backlog-then-live subscription loops.
//!
//! This module is deliberately rmcp-free: the caller supplies the injected
//! caller pubkey, the (transport-resolved) publication Nostr event, and a
//! [`MessageSink`] for streaming tools. The thin rmcp glue that extracts those
//! from `RequestContext` lives in `methods.rs`.

use std::sync::Arc;

use async_trait::async_trait;

use cordn_core::contracts::*;
use cordn_core::mls_parse::parse_key_package;
use cordn_core::ratelimit::{TokenBucketRateLimitConfig, TokenBucketRateLimiter};
use cordn_core::types::FetchGroupMessagesInput as CoreFetchInput;
use cordn_core::{
    Coordinator, PostGroupMessageInput as CorePostInput,
    PublishKeyPackageInput as CorePublishInput, StoreJoinRequestInput as CoreStoreJoinInput,
    StoreWelcomeInput as CoreStoreWelcomeInput,
};

use crate::config::{AbuseProtectionConfig, RateLimitConfig};

/// How often the streaming loops poll the sink's `is_active` flag so a client
/// disconnect (which flips the writer inactive) breaks a blocked `recv`.
///
/// ponytail: poll-based because the rs-sdk `OpenStreamWriter` exposes no
/// signal / `closed()` future — only the `is_active()` `AtomicBool`. The flag
/// flips to false on: (a) an explicit client `abort` control frame (the
/// transport routes it to `writer.abort()`), (b) the SDK's writer keepalive
/// aborting after a probe timeout when the client goes silent (CEP-41
/// silent-disconnect fix — armed automatically from `OpenStreamConfig`'s
/// `idle_timeout_ms`/`probe_timeout_ms`, which `main.rs` leaves at their
/// 30s/20s defaults), or (c) this tool's own close/abort. This poll observes
/// that flip within `idle + probe + SINK_ACTIVE_POLL`.
///
/// `write()` is NOT a liveness signal: Nostr publishes to the relay, which
/// accepts the frame whether or not the client is connected, so without the SDK
/// keepalive a silently-dropped client would leave this loop (and the
/// coordinator subscriber + unbounded channel) leaked indefinitely. Full
/// transport teardown cancels the rmcp tool task, dropping the loop without
/// needing to observe the flag. Swap the poll for a signal future if the rs-sdk
/// ever adds one.
const SINK_ACTIVE_POLL: std::time::Duration = std::time::Duration::from_millis(25);

/// Milliseconds-since-epoch clock, shared with the coordinator so timestamps
/// stay consistent.
pub type Now = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Sink for streaming subscription tools. `methods.rs` adapts the rmcp
/// `OpenStreamWriter` to this; tests use a collecting sink.
#[async_trait]
pub trait MessageSink: Send + Sync {
    /// Publish the lazy stream `start` frame (idempotent).
    async fn start(&self) -> bool;
    /// Write one JSON-serialized group message. Returns false if the stream is
    /// no longer active.
    async fn write(&self, msg: String) -> bool;
    fn is_active(&self) -> bool;
    async fn close(&self);
}

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    #[error("Key package quota exceeded")]
    QuotaExceeded,
    #[error("Missing publication event")]
    MissingPublicationEvent,
    #[error("Publication event signer does not match injected client pubkey")]
    SignerMismatch,
    #[error("Key package credential identity does not match publication event signer")]
    IdentityMismatch,
    #[error("Invalid kp_64")]
    InvalidKeyPackage,
    #[error("Invalid welcome_64")]
    InvalidWelcome,
    #[error("Invalid msg_64")]
    InvalidMessage,
    #[error("Unknown key package ref: {0}")]
    UnknownKeyPackageRef(String),
    #[error("Unauthorized key package ref: {0}")]
    UnauthorizedKeyPackageRef(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error(transparent)]
    Storage(#[from] cordn_core::StorageError),
}

pub struct CoordinatorAdapter {
    coordinator: Arc<Coordinator>,
    rate_limiter: TokenBucketRateLimiter,
    abuse: AbuseProtectionConfig,
    now: Now,
}

impl CoordinatorAdapter {
    pub fn new(coordinator: Arc<Coordinator>, abuse: AbuseProtectionConfig, now: Now) -> Self {
        let rate_limiter = TokenBucketRateLimiter::new(to_rate_limit_config(abuse.rate_limit));
        Self {
            coordinator,
            rate_limiter,
            abuse,
            now,
        }
    }

    pub fn coordinator(&self) -> &Arc<Coordinator> {
        &self.coordinator
    }

    fn assert_within_rate_limit(
        &self,
        client_pubkey: &str,
        method: &str,
    ) -> Result<(), AdapterError> {
        if self.rate_limiter.check(client_pubkey, (self.now)()) {
            Ok(())
        } else {
            self.log_rejection("rate_limit", client_pubkey, method);
            Err(AdapterError::RateLimitExceeded)
        }
    }

    /// Log an abuse-protection rejection when `log_rejections` is enabled.
    /// Mirrors the TS adapter's structured warn (pubkey truncated to 12 chars).
    fn log_rejection(&self, kind: &str, client_pubkey: &str, reason: &str) {
        if !self.abuse.log_rejections {
            return;
        }
        let prefix = &client_pubkey[..client_pubkey.len().min(12)];
        tracing::warn!(
            target: "cordn_server::abuse",
            kind,
            client_pubkey = prefix,
            reason,
            "cordn abuse protection rejection"
        );
    }

    // ── key packages ─────────────────────────────────────────────────

    pub async fn publish_key_package(
        &self,
        input: PublishKeyPackageInput,
        client_pubkey: &str,
        publication_event: Option<NostrEvent>,
    ) -> Result<PublishKeyPackageOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "publishKeyPackage")?;
        require_non_empty(&input.kp_ref, "kp_ref")?;
        require_non_empty(&input.kp_64, "kp_64")?;

        let key_package_bytes =
            b64_decode(&input.kp_64).map_err(|_| AdapterError::InvalidKeyPackage)?;
        let parsed =
            parse_key_package(&key_package_bytes).map_err(|_| AdapterError::InvalidKeyPackage)?;

        // TS-style binding: the publication event signer must equal the injected
        // caller pubkey, and the key package's credential identity must equal both.
        // The event (with its client signature) is injected by the transport worker
        // as `InboundEvent`; `sig` is the client's Schnorr signature and cannot be
        // reconstructed server-side, so the real event must be threaded through.
        let publication_event = publication_event.ok_or(AdapterError::MissingPublicationEvent)?;
        if publication_event.pubkey != client_pubkey {
            return Err(AdapterError::SignerMismatch);
        }
        if parsed.credential_identity != publication_event.pubkey {
            return Err(AdapterError::IdentityMismatch);
        }

        self.enforce_key_package_quota(&parsed.credential_identity, parsed.is_last_resort)?;

        let record = self.coordinator.publish_key_package(CorePublishInput {
            stable_pubkey: parsed.credential_identity,
            key_package_bytes,
            key_package_ref: input.kp_ref,
            is_last_resort: parsed.is_last_resort,
            publication_event: serde_json::to_value(&publication_event)
                .unwrap_or(serde_json::Value::Null),
        })?;

        Ok(PublishKeyPackageOutput {
            kp_ref: record.key_package_ref,
            last_resort: record.is_last_resort,
            at: record.published_at,
        })
    }

    pub fn consume_key_package(
        &self,
        input: ConsumeKeyPackageInput,
        client_pubkey: &str,
    ) -> Result<ConsumeKeyPackageOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "consumeKeyPackage")?;
        require_non_empty(&input.id, "id")?;
        let record = self.coordinator.consume_key_package(&input.id)?;
        Ok(ConsumeKeyPackageOutput {
            key_package: record.map(|r| ConsumedKeyPackage {
                pk: r.stable_pubkey,
                kp_ref: r.key_package_ref,
                last_resort: r.is_last_resort,
                at: r.published_at,
                event: serde_json::from_value(r.publication_event).unwrap_or(NostrEvent {
                    id: String::new(),
                    pubkey: String::new(),
                    created_at: 0,
                    kind: 0,
                    tags: vec![],
                    content: String::new(),
                    sig: String::new(),
                }),
            }),
        })
    }

    pub fn list_available_key_packages(
        &self,
        client_pubkey: &str,
    ) -> Result<ListAvailableKeyPackagesOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "listAvailableKeyPackages")?;
        let records = self.coordinator.list_all_key_packages()?;
        Ok(ListAvailableKeyPackagesOutput {
            key_packages: records
                .iter()
                .map(|r| AvailableKeyPackage {
                    pk: r.stable_pubkey.clone(),
                    kp_ref: r.key_package_ref.clone(),
                    last_resort: r.is_last_resort,
                    at: r.published_at,
                })
                .collect(),
        })
    }

    pub fn remove_key_packages(
        &self,
        input: RemoveKeyPackagesInput,
        client_pubkey: &str,
    ) -> Result<RemoveKeyPackagesOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "removeKeyPackages")?;
        if input.kp_refs.is_empty() {
            return Err(AdapterError::InvalidInput(
                "kp_refs must be non-empty".into(),
            ));
        }
        let mut removed = Vec::with_capacity(input.kp_refs.len());
        for kp_ref in &input.kp_refs {
            let record = self
                .coordinator
                .get_key_package(kp_ref)?
                .ok_or_else(|| AdapterError::UnknownKeyPackageRef(kp_ref.clone()))?;
            if record.stable_pubkey != client_pubkey {
                return Err(AdapterError::UnauthorizedKeyPackageRef(kp_ref.clone()));
            }
            let gone = self.coordinator.remove_key_package(kp_ref)?;
            removed.push(
                gone.map(|r| r.key_package_ref)
                    .unwrap_or_else(|| kp_ref.clone()),
            );
        }
        Ok(RemoveKeyPackagesOutput { kp_refs: removed })
    }

    // ── welcomes ─────────────────────────────────────────────────────

    pub fn fetch_pending_welcomes(
        &self,
        input: FetchPendingWelcomesInput,
        client_pubkey: &str,
    ) -> Result<FetchPendingWelcomesOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "fetchPendingWelcomes")?;
        let consumed: Vec<cordn_core::ConsumedWelcomeRef> = input
            .consumed
            .unwrap_or_default()
            .iter()
            .map(|c| cordn_core::ConsumedWelcomeRef {
                key_package_reference: c.kp_ref.clone(),
                created_at: c.at,
            })
            .collect();
        let records = self
            .coordinator
            .fetch_pending_welcomes(client_pubkey, &consumed)?;
        Ok(FetchPendingWelcomesOutput {
            welcomes: records
                .iter()
                .map(|r| PendingWelcome {
                    kp_ref: r.key_package_reference.clone(),
                    welcome_64: b64_encode(&r.welcome_bytes),
                    at: r.created_at,
                    after: r.join_after_cursor,
                })
                .collect(),
        })
    }

    pub fn store_welcome(
        &self,
        input: StoreWelcomeInput,
        client_pubkey: &str,
    ) -> Result<StoreWelcomeOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "storeWelcome")?;
        require_non_empty(&input.target_pk, "target_pk")?;
        require_non_empty(&input.kp_ref, "kp_ref")?;
        require_non_empty(&input.welcome_64, "welcome_64")?;
        let welcome_bytes =
            b64_decode(&input.welcome_64).map_err(|_| AdapterError::InvalidWelcome)?;
        let record = self.coordinator.store_welcome(CoreStoreWelcomeInput {
            target_stable_pubkey: input.target_pk,
            key_package_reference: input.kp_ref,
            welcome_bytes,
            join_after_cursor: input.after,
        })?;
        Ok(StoreWelcomeOutput {
            at: record.created_at,
        })
    }

    // ── join requests ────────────────────────────────────────────────

    pub fn store_join_request(
        &self,
        input: StoreJoinRequestInput,
        client_pubkey: &str,
    ) -> Result<StoreJoinRequestOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "storeJoinRequest")?;
        require_non_empty(&input.gid, "gid")?;
        require_non_empty(&input.kp_ref, "kp_ref")?;
        let key_package_record = self
            .coordinator
            .get_key_package(&input.kp_ref)?
            .ok_or_else(|| AdapterError::UnknownKeyPackageRef(input.kp_ref.clone()))?;
        if key_package_record.stable_pubkey != client_pubkey {
            return Err(AdapterError::UnauthorizedKeyPackageRef(
                input.kp_ref.clone(),
            ));
        }
        let record = self.coordinator.store_join_request(CoreStoreJoinInput {
            group_id: input.gid,
            requester_stable_pubkey: client_pubkey.to_owned(),
            key_package_ref: input.kp_ref,
        })?;
        Ok(StoreJoinRequestOutput {
            at: record.created_at,
        })
    }

    pub fn fetch_pending_join_requests(
        &self,
        input: FetchPendingJoinRequestsInput,
        client_pubkey: &str,
    ) -> Result<FetchPendingJoinRequestsOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "fetchPendingJoinRequests")?;
        require_non_empty(&input.gid, "gid")?;
        let consumed: Vec<cordn_core::ConsumedJoinRequestRef> = input
            .consumed
            .unwrap_or_default()
            .iter()
            .map(|c| cordn_core::ConsumedJoinRequestRef {
                requester_stable_pubkey: c.pk.clone(),
                created_at: c.at,
            })
            .collect();
        let records = self
            .coordinator
            .fetch_pending_join_requests(&input.gid, &consumed)?;
        Ok(FetchPendingJoinRequestsOutput {
            requests: records
                .iter()
                .map(|r| JoinRequest {
                    pk: r.requester_stable_pubkey.clone(),
                    kp_ref: r.key_package_ref.clone(),
                    at: r.created_at,
                })
                .collect(),
        })
    }

    pub fn fetch_many_pending_join_requests(
        &self,
        input: FetchManyPendingJoinRequestsInput,
        client_pubkey: &str,
    ) -> Result<FetchManyPendingJoinRequestsOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "fetchManyPendingJoinRequests")?;
        if input.groups.is_empty() {
            return Err(AdapterError::InvalidInput(
                "groups must be non-empty".into(),
            ));
        }
        let group_ids: Vec<String> = input.groups.iter().map(|g| g.gid.clone()).collect();
        let consumed: Vec<cordn_core::ConsumedJoinRequestWithGroupRef> = input
            .consumed
            .unwrap_or_default()
            .iter()
            .map(|c| cordn_core::ConsumedJoinRequestWithGroupRef {
                group_id: c.gid.clone(),
                requester_stable_pubkey: c.pk.clone(),
                created_at: c.at,
            })
            .collect();
        let records = self
            .coordinator
            .fetch_many_pending_join_requests(&group_ids, &consumed)?;
        Ok(FetchManyPendingJoinRequestsOutput {
            requests: records
                .iter()
                .map(|r| JoinRequestWithGroup {
                    gid: r.group_id.clone(),
                    pk: r.requester_stable_pubkey.clone(),
                    kp_ref: r.key_package_ref.clone(),
                    at: r.created_at,
                })
                .collect(),
        })
    }

    // ── group messages ───────────────────────────────────────────────

    pub fn post_group_message(
        &self,
        input: PostGroupMessageInput,
        client_pubkey: &str,
    ) -> Result<PostGroupMessageOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "postGroupMessage")?;
        require_non_empty(&input.gid, "gid")?;
        require_non_empty(&input.msg_64, "msg_64")?;
        let opaque = b64_decode(&input.msg_64).map_err(|_| AdapterError::InvalidMessage)?;
        let record = self.coordinator.post_group_message(CorePostInput {
            group_id: input.gid,
            opaque_message: opaque,
        })?;
        Ok(PostGroupMessageOutput {
            cursor: record.cursor,
            gid: record.group_id,
            at: record.created_at,
        })
    }

    pub fn fetch_group_messages(
        &self,
        input: FetchGroupMessagesInput,
        client_pubkey: &str,
    ) -> Result<FetchGroupMessagesOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "fetchGroupMessages")?;
        require_non_empty(&input.gid, "gid")?;
        require_positive_cursor(input.after, "after")?;
        let records = self
            .coordinator
            .fetch_group_messages(&input.gid, input.after)?;
        Ok(map_group_messages(&records))
    }

    pub fn fetch_many_group_messages(
        &self,
        input: FetchManyGroupMessagesInput,
        client_pubkey: &str,
    ) -> Result<FetchGroupMessagesOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "fetchManyGroupMessages")?;
        if input.groups.is_empty() {
            return Err(AdapterError::InvalidInput(
                "groups must be non-empty".into(),
            ));
        }
        for g in &input.groups {
            require_positive_cursor(g.after, "after")?;
        }
        let groups: Vec<CoreFetchInput> = input
            .groups
            .iter()
            .map(|g| CoreFetchInput {
                group_id: g.gid.clone(),
                after_cursor: g.after,
            })
            .collect();
        let records = self.coordinator.fetch_many_group_messages(&groups)?;
        Ok(map_group_messages(&records))
    }

    // ── subscriptions ────────────────────────────────────────────────

    /// Replay backlog then stream live for a single group. The coordinator's
    /// single-group subscribe is live-tail only, so the backlog is fetched
    /// separately and streamed first (dedup by cursor against any live records
    /// that raced in).
    pub async fn subscribe_group_messages(
        &self,
        input: SubscribeGroupMessagesInput,
        client_pubkey: &str,
        sink: &dyn MessageSink,
    ) -> Result<SubscribeGroupMessagesOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "subscribeGroupMessages")?;
        require_non_empty(&input.gid, "gid")?;
        require_positive_cursor(input.after, "after")?;

        let mut subscription = self.coordinator.subscribe_group_messages(&input.gid);
        let backlog = self
            .coordinator
            .fetch_group_messages(&input.gid, input.after)?;
        let mut last_emitted = input.after.unwrap_or(0);

        sink.start().await;
        for record in &backlog {
            if !sink.is_active() {
                break;
            }
            sink.write(wire_message(record)).await;
            last_emitted = record.cursor;
        }

        loop {
            let recv = subscription.recv();
            tokio::pin!(recv);
            tokio::select! {
                record = &mut recv => {
                    let Some(record) = record else { break };
                    if record.cursor <= last_emitted {
                        continue;
                    }
                    last_emitted = record.cursor;
                    if !sink.write(wire_message(&record)).await {
                        break;
                    }
                }
                _ = tokio::time::sleep(SINK_ACTIVE_POLL) => {
                    if !sink.is_active() {
                        break;
                    }
                }
            }
        }

        if sink.is_active() {
            sink.close().await;
        }
        Ok(SubscribeGroupMessagesOutput { subscribed: true })
    }

    /// Stream backlog+live for multiple groups (the coordinator merges them).
    pub async fn subscribe_many_group_messages(
        &self,
        input: SubscribeManyGroupMessagesInput,
        client_pubkey: &str,
        sink: &dyn MessageSink,
    ) -> Result<SubscribeManyGroupMessagesOutput, AdapterError> {
        self.assert_within_rate_limit(client_pubkey, "subscribeManyGroupMessages")?;
        if input.groups.is_empty() {
            return Err(AdapterError::InvalidInput(
                "groups must be non-empty".into(),
            ));
        }
        for g in &input.groups {
            require_positive_cursor(g.after, "after")?;
        }
        let group_ids: Vec<String> = input.groups.iter().map(|g| g.gid.clone()).collect();
        let groups: Vec<CoreFetchInput> = input
            .groups
            .iter()
            .map(|g| CoreFetchInput {
                group_id: g.gid.clone(),
                after_cursor: g.after,
            })
            .collect();

        let mut subscription = self.coordinator.subscribe_many_group_messages(&groups)?;

        sink.start().await;
        loop {
            let recv = subscription.recv();
            tokio::pin!(recv);
            tokio::select! {
                record = &mut recv => {
                    let Some(record) = record else { break };
                    if !sink.write(wire_message(&record)).await {
                        break;
                    }
                }
                _ = tokio::time::sleep(SINK_ACTIVE_POLL) => {
                    if !sink.is_active() {
                        break;
                    }
                }
            }
        }
        if sink.is_active() {
            sink.close().await;
        }
        Ok(SubscribeManyGroupMessagesOutput {
            subscribed: true,
            groups: group_ids,
        })
    }

    // ── quota ────────────────────────────────────────────────────────

    fn enforce_key_package_quota(
        &self,
        stable_pubkey: &str,
        incoming_is_last_resort: bool,
    ) -> Result<(), AdapterError> {
        let records = self
            .coordinator
            .list_key_packages_for_identity(stable_pubkey)?;
        let max_per = self.abuse.key_package_quota.max_per_identity;
        let max_last_resort = self.abuse.key_package_quota.max_last_resort_per_identity;

        if incoming_is_last_resort {
            let existing_last_resort: Vec<_> =
                records.iter().filter(|r| r.is_last_resort).collect();

            if max_last_resort > 0 && existing_last_resort.len() >= max_last_resort {
                // Evict the oldest last-resort entries to make room.
                let to_remove = existing_last_resort.len() - max_last_resort + 1;
                for record in existing_last_resort.iter().take(to_remove) {
                    let _ = self
                        .coordinator
                        .remove_key_package(&record.key_package_ref)?;
                }
            }

            let non_last_resort_count = records.len() - existing_last_resort.len();
            if max_per > 0
                && non_last_resort_count
                    + existing_last_resort
                        .len()
                        .min(max_last_resort.saturating_sub(1))
                    + 1
                    > max_per
            {
                self.log_rejection(
                    "key_package_quota",
                    stable_pubkey,
                    "max key packages per identity exceeded",
                );
                return Err(AdapterError::QuotaExceeded);
            }
            return Ok(());
        }

        if max_per > 0 && records.len() >= max_per {
            self.log_rejection(
                "key_package_quota",
                stable_pubkey,
                "max key packages per identity exceeded",
            );
            return Err(AdapterError::QuotaExceeded);
        }
        Ok(())
    }
}

fn to_rate_limit_config(c: RateLimitConfig) -> TokenBucketRateLimitConfig {
    TokenBucketRateLimitConfig {
        enabled: c.enabled,
        refill_per_minute: c.refill_per_minute as f64,
        burst: c.burst as f64,
        idle_ttl_ms: c.idle_ttl_ms as f64,
    }
}

fn require_non_empty(value: &str, field: &str) -> Result<(), AdapterError> {
    if value.is_empty() {
        Err(AdapterError::InvalidInput(format!(
            "{field} must be non-empty"
        )))
    } else {
        Ok(())
    }
}

/// Reject non-positive cursors, matching the TS wire schema
/// `after: z.number().int().positive().optional()`. `None` is allowed (fetch
/// from the start); `Some(0)` and negatives are rejected. The advertised
/// schemars schema is intentionally loose (contracts are pure data shapes);
/// this enforces parity at the adapter boundary, like `require_non_empty`.
fn require_positive_cursor(after: Option<i64>, field: &str) -> Result<(), AdapterError> {
    if after.is_some_and(|c| c <= 0) {
        Err(AdapterError::InvalidInput(format!(
            "{field} must be a positive integer"
        )))
    } else {
        Ok(())
    }
}

fn map_group_messages(records: &[cordn_core::GroupMessageRecord]) -> FetchGroupMessagesOutput {
    FetchGroupMessagesOutput {
        messages: records.iter().map(wire_message_struct).collect(),
    }
}

fn wire_message_struct(record: &cordn_core::GroupMessageRecord) -> GroupMessage {
    GroupMessage {
        cursor: record.cursor,
        gid: record.group_id.clone(),
        msg_64: b64_encode(&record.opaque_message),
        at: record.created_at,
        encrypted: Some(record.encrypted),
    }
}

fn wire_message(record: &cordn_core::GroupMessageRecord) -> String {
    serde_json::to_string(&wire_message_struct(record)).unwrap_or_default()
}

fn b64_decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    STANDARD.decode(s)
}

fn b64_encode(b: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    STANDARD.encode(b)
}
