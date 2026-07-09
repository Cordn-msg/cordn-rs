//! Wire contracts for the coordinator MCP tools — serde types mirroring the
//! Zod schemas in `references/cordn/src/contracts/index.ts`. Field names are
//! byte-identical to the TS schema (this is the on-the-wire JSON the clients
//! and the TS server already exchange), including the camelCase `keyPackage` /
//! `keyPackages` and the snake_case `kp_ref` / `msg_64` / `last_resort`.
//!
//! These are pure data shapes. Boundary validation (non-empty strings, positive
//! cursors) is applied in the adapter, matching where the TS Zod parses run.

use serde::{Deserialize, Serialize};

/// MCP tool names, matching `COORDINATOR_METHODS` exactly.
pub mod methods {
    pub const PUBLISH_KEY_PACKAGE: &str = "kp_publish";
    pub const LIST_AVAILABLE_KEY_PACKAGES: &str = "kp_list";
    pub const CONSUME_KEY_PACKAGE: &str = "kp_take";
    pub const REMOVE_KEY_PACKAGES: &str = "kp_remove";
    pub const FETCH_PENDING_WELCOMES: &str = "welcome_take";
    pub const STORE_WELCOME: &str = "welcome_store";
    pub const STORE_JOIN_REQUEST: &str = "join_request_store";
    pub const FETCH_PENDING_JOIN_REQUESTS: &str = "join_request_take";
    pub const FETCH_MANY_PENDING_JOIN_REQUESTS: &str = "join_request_take_many";
    pub const POST_GROUP_MESSAGE: &str = "msg_post";
    pub const FETCH_GROUP_MESSAGES: &str = "msg_fetch";
    pub const FETCH_MANY_GROUP_MESSAGES: &str = "msg_fetch_many";
    pub const SUBSCRIBE_GROUP_MESSAGES: &str = "msg_sub";
    pub const SUBSCRIBE_MANY_GROUP_MESSAGES: &str = "msg_sub_many";
}

/// A Nostr event, as embedded in consumed-key-package responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct NostrEvent {
    pub id: String,
    pub pubkey: String,
    pub created_at: i64,
    pub kind: i64,
    pub tags: Vec<Vec<String>>,
    pub content: String,
    pub sig: String,
}

// ── key packages ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PublishKeyPackageInput {
    pub kp_ref: String,
    pub kp_64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PublishKeyPackageOutput {
    pub kp_ref: String,
    pub last_resort: bool,
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ConsumeKeyPackageInput {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ConsumedKeyPackage {
    pub pk: String,
    pub kp_ref: String,
    pub last_resort: bool,
    pub at: i64,
    pub event: NostrEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ConsumeKeyPackageOutput {
    #[serde(rename = "keyPackage")]
    pub key_package: Option<ConsumedKeyPackage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct AvailableKeyPackage {
    pub pk: String,
    pub kp_ref: String,
    pub last_resort: bool,
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ListAvailableKeyPackagesOutput {
    #[serde(rename = "keyPackages")]
    pub key_packages: Vec<AvailableKeyPackage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RemoveKeyPackagesInput {
    pub kp_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RemoveKeyPackagesOutput {
    pub kp_refs: Vec<String>,
}

// ── welcomes ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PendingWelcome {
    pub kp_ref: String,
    pub welcome_64: String,
    pub at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ConsumedWelcomeRef {
    pub kp_ref: String,
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchPendingWelcomesInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumed: Option<Vec<ConsumedWelcomeRef>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchPendingWelcomesOutput {
    pub welcomes: Vec<PendingWelcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct StoreWelcomeInput {
    pub target_pk: String,
    pub kp_ref: String,
    pub welcome_64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct StoreWelcomeOutput {
    pub at: i64,
}

// ── join requests ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct StoreJoinRequestInput {
    pub gid: String,
    pub kp_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct StoreJoinRequestOutput {
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct JoinRequest {
    pub pk: String,
    pub kp_ref: String,
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ConsumedJoinRequestRef {
    pub pk: String,
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchPendingJoinRequestsInput {
    pub gid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumed: Option<Vec<ConsumedJoinRequestRef>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchPendingJoinRequestsOutput {
    pub requests: Vec<JoinRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchManyPendingJoinRequestsGroupInput {
    pub gid: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ConsumedJoinRequestWithGroupRef {
    pub pk: String,
    pub at: i64,
    pub gid: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchManyPendingJoinRequestsInput {
    pub groups: Vec<FetchManyPendingJoinRequestsGroupInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumed: Option<Vec<ConsumedJoinRequestWithGroupRef>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct JoinRequestWithGroup {
    pub pk: String,
    pub kp_ref: String,
    pub at: i64,
    pub gid: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchManyPendingJoinRequestsOutput {
    pub requests: Vec<JoinRequestWithGroup>,
}

// ── group messages ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PostGroupMessageInput {
    pub msg_64: String,
    pub gid: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PostGroupMessageOutput {
    pub cursor: i64,
    pub gid: String,
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchGroupMessagesInput {
    pub gid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct GroupMessage {
    pub cursor: i64,
    pub gid: String,
    pub msg_64: String,
    pub at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypted: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchGroupMessagesOutput {
    pub messages: Vec<GroupMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FetchManyGroupMessagesInput {
    pub groups: Vec<FetchGroupMessagesInput>,
}

// ── subscriptions ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SubscribeGroupMessagesInput {
    pub gid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SubscribeGroupMessagesOutput {
    pub subscribed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SubscribeManyGroupMessagesInput {
    pub groups: Vec<FetchGroupMessagesInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SubscribeManyGroupMessagesOutput {
    pub subscribed: bool,
    pub groups: Vec<String>,
}
