//! The storage contract. Ported from
//! `references/cordn/src/coordinator/storage/storage.ts`.

use std::collections::HashMap;

use crate::types::{
    ConsumedJoinRequestRef, ConsumedJoinRequestWithGroupRef, ConsumedWelcomeRef,
    FetchGroupMessagesInput, GroupMessageRecord, JoinRequestRecord, PublishedKeyPackageRecord,
    WelcomeQueueRecord,
};

pub mod in_memory;
pub mod sqlite;

pub use in_memory::InMemoryCoordinatorStorage;
pub use sqlite::{SqliteCoordinatorStorage, Synchronous};

/// Maximum pending join requests per group. Applies uniformly to all groups
/// (including those with no message history yet) so a freshly created group can
/// still accept requests.
pub const MAX_PENDING_JOIN_REQUESTS_PER_GROUP: usize = 100;

/// Error returned by storage operations.
///
/// `Backend` is a backend-agnostic string so the shared trait does not couple
/// every backend to a specific driver's error type. `TooManyPendingJoinRequests`
/// is the one expected operational error (from [`CoordinatorStorage::store_join_request`]).
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage backend error: {0}")]
    Backend(String),
    #[error("Too many pending join requests for this group")]
    TooManyPendingJoinRequests,
}

/// Partition multi-group consumed refs into per-group lists. Shared by both
/// storage backends' `fetch_many_pending_join_requests`.
pub fn partition_consumed_join_requests(
    consumed: &[ConsumedJoinRequestWithGroupRef],
) -> HashMap<String, Vec<ConsumedJoinRequestRef>> {
    let mut by_group: HashMap<String, Vec<ConsumedJoinRequestRef>> = HashMap::new();
    for item in consumed {
        by_group
            .entry(item.group_id.clone())
            .or_default()
            .push(ConsumedJoinRequestRef {
                requester_stable_pubkey: item.requester_stable_pubkey.clone(),
                created_at: item.created_at,
            });
    }
    by_group
}

/// Parameters for [`CoordinatorStorage::append_group_message`].
pub struct AppendGroupMessageParams {
    pub group_id: String,
    pub opaque_message: Vec<u8>,
    pub created_at: i64,
    pub encrypted: bool,
}

/// Storage instances are owned by a single coordinator instance.
///
/// The contract is domain-shaped and assumes a single-writer execution model,
/// which lets the coordinator perform read/decide/write flows without
/// optimistic-concurrency tokens.
///
/// Group message cursor invariants:
/// - cursors are monotonic within a group
/// - cursors are scoped to a group, not globally across all groups
/// - different groups may each have a message with cursor 1
/// - `fetch_group_messages(group_id, after_cursor)` interprets `after_cursor`
///   only within the specified group
pub trait CoordinatorStorage: Send + Sync {
    fn publish_key_package(
        &self,
        record: PublishedKeyPackageRecord,
    ) -> Result<PublishedKeyPackageRecord, StorageError>;

    fn list_key_packages_for_identity(
        &self,
        stable_pubkey: &str,
    ) -> Result<Vec<PublishedKeyPackageRecord>, StorageError>;

    fn list_all_key_packages(&self) -> Result<Vec<PublishedKeyPackageRecord>, StorageError>;

    fn get_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError>;

    fn remove_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError>;

    /// Consume by exact key-package ref first; if none matches, treat the
    /// identifier as a stable identity. Last-resort key packages are returned
    /// non-destructively (the spec §11 retrieval rule).
    fn consume_key_package(
        &self,
        identifier: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError>;

    fn store_welcome(&self, record: WelcomeQueueRecord)
        -> Result<WelcomeQueueRecord, StorageError>;

    /// Fetch all pending welcomes for a target identity. Observation never
    /// deletes; pass `consumed` to atomically retire welcomes the caller has
    /// joined (keyed by `key_package_reference` + `created_at`, scoped to the
    /// target identity). Consumed records are deleted before the fetch.
    fn fetch_pending_welcomes(
        &self,
        target_stable_pubkey: &str,
        consumed: &[ConsumedWelcomeRef],
    ) -> Result<Vec<WelcomeQueueRecord>, StorageError>;

    /// Delete welcomes with `created_at < max_age_threshold`. Pass `<= 0` to
    /// delete nothing (retention disabled). Returns the count deleted.
    fn delete_expired_welcomes(&self, max_age_threshold: i64) -> Result<usize, StorageError>;

    fn store_join_request(
        &self,
        record: JoinRequestRecord,
    ) -> Result<JoinRequestRecord, StorageError>;

    /// Fetch all pending join requests for a group. Observation never deletes;
    /// pass `consumed` to atomically retire requests the admin has handled
    /// (keyed by `requester_stable_pubkey` + `created_at`, scoped to the
    /// group). Consumed records are deleted before the fetch.
    fn fetch_pending_join_requests(
        &self,
        group_id: &str,
        consumed: &[ConsumedJoinRequestRef],
    ) -> Result<Vec<JoinRequestRecord>, StorageError>;

    /// Fetch pending join requests for multiple groups. Same read/consumed
    /// semantics as [`Self::fetch_pending_join_requests`], but consumed items
    /// carry their own group id. Results are ordered by input group order, then
    /// storage order within each group.
    fn fetch_many_pending_join_requests(
        &self,
        group_ids: &[String],
        consumed: &[ConsumedJoinRequestWithGroupRef],
    ) -> Result<Vec<JoinRequestRecord>, StorageError>;

    /// Delete join requests with `created_at < max_age_threshold`. Pass `<= 0`
    /// to delete nothing (retention disabled). Returns the count deleted.
    fn delete_expired_join_requests(&self, max_age_threshold: i64) -> Result<usize, StorageError>;

    /// Persist a group message and allocate the next per-group cursor.
    ///
    /// Implementations must never use a table-global cursor sequence here.
    fn append_group_message(
        &self,
        params: AppendGroupMessageParams,
    ) -> Result<GroupMessageRecord, StorageError>;

    /// Fetch messages for one group. If `after_cursor` is provided, it is a
    /// cursor previously returned for that same group.
    fn fetch_group_messages(
        &self,
        group_id: &str,
        after_cursor: Option<i64>,
    ) -> Result<Vec<GroupMessageRecord>, StorageError>;

    /// Fetch messages for multiple groups with independent per-group cursors.
    /// Results are ordered by input group order, then cursor ascending within
    /// each group.
    fn fetch_many_group_messages(
        &self,
        groups: &[FetchGroupMessagesInput],
    ) -> Result<Vec<GroupMessageRecord>, StorageError>;
}
