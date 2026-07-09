//! Domain records for the cordn coordinator storage layer.
//!
//! Ported from `references/cordn/src/coordinator/types.ts`. MLS payloads are
//! held as raw bytes — the coordinator never decodes them (spec
//! `references/cordn/spec/00.md` §7 "store verbatim").

use serde_json::Value;

/// A published MLS key package, stored verbatim.
///
/// `key_package_bytes` holds the exact incoming wire bytes — the coordinator
/// never re-encodes them. This diverges from the TS record's parsed
/// `keyPackage` field by design: storing raw bytes is simpler, more
/// spec-correct, and removes any encode-parity risk between Rust and TS.
/// `publication_event` is the signed Nostr publication payload, kept as opaque
/// JSON and returned verbatim on consume.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishedKeyPackageRecord {
    pub stable_pubkey: String,
    /// Raw opaque MLS key-package bytes (stored verbatim).
    pub key_package_bytes: Vec<u8>,
    pub key_package_ref: String,
    pub is_last_resort: bool,
    /// Unix milliseconds.
    pub published_at: i64,
    pub publication_event: Value,
}

/// A queued MLS welcome awaiting delivery to its target identity.
#[derive(Debug, Clone, PartialEq)]
pub struct WelcomeQueueRecord {
    pub target_stable_pubkey: String,
    pub key_package_reference: String,
    /// Raw opaque MLS welcome bytes (stored verbatim).
    pub welcome_bytes: Vec<u8>,
    /// Unix milliseconds.
    pub created_at: i64,
    /// Optional post-join sync cursor hint.
    pub join_after_cursor: Option<i64>,
}

/// Reference to a welcome the caller has joined locally and wants retired.
/// Scoped to the caller's own inbox, so `(key_package_reference, created_at)`
/// uniquely identifies the record.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumedWelcomeRef {
    pub key_package_reference: String,
    pub created_at: i64,
}

/// A pending join request awaiting admin handling.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinRequestRecord {
    pub group_id: String,
    pub requester_stable_pubkey: String,
    pub key_package_ref: String,
    /// Unix milliseconds.
    pub created_at: i64,
}

/// Reference to a join request an admin has handled and wants retired.
/// Scoped to a single group fetch, so `(requester_stable_pubkey, created_at)`
/// uniquely identifies the record.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumedJoinRequestRef {
    pub requester_stable_pubkey: String,
    pub created_at: i64,
}

/// Like [`ConsumedJoinRequestRef`] but carrying its own group id, for the
/// multi-group fetch where consumed items may span several groups.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumedJoinRequestWithGroupRef {
    pub group_id: String,
    pub requester_stable_pubkey: String,
    pub created_at: i64,
}

/// One opaque MLS group message in a per-group ordered log.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupMessageRecord {
    /// Per-group monotonic cursor. Never global across groups: different groups
    /// may each have a message with cursor 1.
    pub cursor: i64,
    pub group_id: String,
    /// Raw opaque MLS message bytes (stored verbatim).
    pub opaque_message: Vec<u8>,
    /// Unix milliseconds.
    pub created_at: i64,
    pub encrypted: bool,
}

/// Input to a single-group message fetch. `after_cursor` is scoped to
/// `group_id` only.
#[derive(Debug, Clone, PartialEq)]
pub struct FetchGroupMessagesInput {
    pub group_id: String,
    pub after_cursor: Option<i64>,
}
