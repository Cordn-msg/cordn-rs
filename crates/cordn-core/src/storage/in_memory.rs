//! In-memory storage backend. Ported from
//! `references/cordn/src/coordinator/storage/inMemoryStorage.ts`.
//!
//! Uses `IndexMap` so iteration order matches the TS `Map` insertion order the
//! sqlite backend reproduces via `ORDER BY id ASC`. Returns owned clones (not
//! live references like the TS version), so callers always hold snapshots.

use std::sync::Mutex;

use indexmap::IndexMap;

use crate::storage::{
    partition_consumed_join_requests, AppendGroupMessageParams, CoordinatorStorage, StorageError,
    MAX_PENDING_JOIN_REQUESTS_PER_GROUP,
};
use crate::types::{
    ConsumedJoinRequestRef, ConsumedJoinRequestWithGroupRef, ConsumedWelcomeRef,
    FetchGroupMessagesInput, GroupMessageRecord, JoinRequestRecord, PublishedKeyPackageRecord,
    WelcomeQueueRecord,
};

struct GroupLog {
    next_cursor: i64,
    messages: Vec<GroupMessageRecord>,
}

impl GroupLog {
    fn new() -> Self {
        Self {
            next_cursor: 1,
            messages: Vec::new(),
        }
    }
}

struct InMemoryInner {
    key_packages_by_identity: IndexMap<String, Vec<PublishedKeyPackageRecord>>,
    welcomes_by_identity: IndexMap<String, Vec<WelcomeQueueRecord>>,
    join_requests_by_group: IndexMap<String, Vec<JoinRequestRecord>>,
    groups: IndexMap<String, GroupLog>,
}

impl Default for InMemoryInner {
    fn default() -> Self {
        Self {
            key_packages_by_identity: IndexMap::new(),
            welcomes_by_identity: IndexMap::new(),
            join_requests_by_group: IndexMap::new(),
            groups: IndexMap::new(),
        }
    }
}

pub struct InMemoryCoordinatorStorage {
    inner: Mutex<InMemoryInner>,
}

impl Default for InMemoryCoordinatorStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryCoordinatorStorage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(InMemoryInner::default()),
        }
    }
}

/// Result of locating a key package by ref: which identity holds it, at which
/// index. The record is cloned out so the lock can be mutated freely afterwards.
struct Located {
    stable_pubkey: String,
    index: usize,
    record: PublishedKeyPackageRecord,
}

fn locate_by_ref(
    key_packages_by_identity: &IndexMap<String, Vec<PublishedKeyPackageRecord>>,
    key_package_ref: &str,
) -> Option<Located> {
    for (stable_pubkey, records) in key_packages_by_identity.iter() {
        if let Some(index) = records
            .iter()
            .position(|r| r.key_package_ref == key_package_ref)
        {
            return Some(Located {
                stable_pubkey: stable_pubkey.clone(),
                index,
                record: records[index].clone(),
            });
        }
    }
    None
}

impl CoordinatorStorage for InMemoryCoordinatorStorage {
    fn publish_key_package(
        &self,
        record: PublishedKeyPackageRecord,
    ) -> Result<PublishedKeyPackageRecord, StorageError> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .key_packages_by_identity
            .entry(record.stable_pubkey.clone())
            .or_default()
            .push(record.clone());
        Ok(record)
    }

    fn list_key_packages_for_identity(
        &self,
        stable_pubkey: &str,
    ) -> Result<Vec<PublishedKeyPackageRecord>, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .key_packages_by_identity
            .get(stable_pubkey)
            .cloned()
            .unwrap_or_default())
    }

    fn list_all_key_packages(&self) -> Result<Vec<PublishedKeyPackageRecord>, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .key_packages_by_identity
            .values()
            .flatten()
            .cloned()
            .collect())
    }

    fn get_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(locate_by_ref(&inner.key_packages_by_identity, key_package_ref).map(|loc| loc.record))
    }

    fn remove_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        let mut inner = self.inner.lock().unwrap();
        let Some(loc) = locate_by_ref(&inner.key_packages_by_identity, key_package_ref) else {
            return Ok(None);
        };
        let records = inner
            .key_packages_by_identity
            .get_mut(&loc.stable_pubkey)
            // ponytail: expect is safe — locate_by_ref just found the ref under
            // this exact identity key.
            .expect("identity present immediately after locate_by_ref");
        records.remove(loc.index);
        if records.is_empty() {
            inner
                .key_packages_by_identity
                .shift_remove(&loc.stable_pubkey);
        }
        Ok(Some(loc.record))
    }

    fn consume_key_package(
        &self,
        identifier: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        let mut inner = self.inner.lock().unwrap();

        // 1. by exact key-package ref.
        if let Some(loc) = locate_by_ref(&inner.key_packages_by_identity, identifier) {
            if !loc.record.is_last_resort {
                let records = inner
                    .key_packages_by_identity
                    .get_mut(&loc.stable_pubkey)
                    .expect("identity present immediately after locate_by_ref");
                records.remove(loc.index);
                if records.is_empty() {
                    inner
                        .key_packages_by_identity
                        .shift_remove(&loc.stable_pubkey);
                }
            }
            return Ok(Some(loc.record));
        }

        // 2. fall back to treating the identifier as a stable identity.
        let first_regular = inner
            .key_packages_by_identity
            .get(identifier)
            .and_then(|records| records.iter().position(|r| !r.is_last_resort));

        if let Some(index) = first_regular {
            let stable_pubkey = identifier.to_string();
            let records = inner
                .key_packages_by_identity
                .get_mut(&stable_pubkey)
                .expect("identity present after position lookup");
            let record = records.remove(index);
            if records.is_empty() {
                inner.key_packages_by_identity.shift_remove(&stable_pubkey);
            }
            return Ok(Some(record));
        }

        // All remaining for that identity are last-resort (or none): return the
        // newest non-destructively. Matches ts `records.at(-1)`.
        Ok(inner
            .key_packages_by_identity
            .get(identifier)
            .and_then(|records| records.last())
            .cloned())
    }

    fn store_welcome(
        &self,
        record: WelcomeQueueRecord,
    ) -> Result<WelcomeQueueRecord, StorageError> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .welcomes_by_identity
            .entry(record.target_stable_pubkey.clone())
            .or_default()
            .push(record.clone());
        Ok(record)
    }

    fn fetch_pending_welcomes(
        &self,
        target_stable_pubkey: &str,
        consumed: &[ConsumedWelcomeRef],
    ) -> Result<Vec<WelcomeQueueRecord>, StorageError> {
        let mut inner = self.inner.lock().unwrap();
        let mut should_remove = false;
        let out = if let Some(records) = inner.welcomes_by_identity.get_mut(target_stable_pubkey) {
            if !consumed.is_empty() {
                records.retain(|r| {
                    !consumed.iter().any(|c| {
                        c.key_package_reference == r.key_package_reference
                            && c.created_at == r.created_at
                    })
                });
            }
            if records.is_empty() {
                should_remove = true;
            }
            records.clone()
        } else {
            Vec::new()
        };
        if should_remove {
            inner
                .welcomes_by_identity
                .shift_remove(target_stable_pubkey);
        }
        Ok(out)
    }

    fn delete_expired_welcomes(&self, max_age_threshold: i64) -> Result<usize, StorageError> {
        if max_age_threshold <= 0 {
            return Ok(0);
        }
        let mut inner = self.inner.lock().unwrap();
        let mut deleted = 0usize;
        let mut empty_keys = Vec::new();
        for (target_stable_pubkey, records) in inner.welcomes_by_identity.iter_mut() {
            let before = records.len();
            records.retain(|r| r.created_at >= max_age_threshold);
            deleted += before - records.len();
            if records.is_empty() {
                empty_keys.push(target_stable_pubkey.clone());
            }
        }
        for key in empty_keys {
            inner.welcomes_by_identity.shift_remove(&key);
        }
        Ok(deleted)
    }

    fn store_join_request(
        &self,
        record: JoinRequestRecord,
    ) -> Result<JoinRequestRecord, StorageError> {
        let mut inner = self.inner.lock().unwrap();
        let existing = inner
            .join_requests_by_group
            .get_mut(&record.group_id)
            .and_then(|records| {
                records
                    .iter_mut()
                    .find(|r| r.requester_stable_pubkey == record.requester_stable_pubkey)
            });

        if let Some(existing) = existing {
            // Re-request: refresh in place. The consume-ack model retires rows
            // by (group, requester, createdAt), so bumping createdAt evades an
            // admin's already-recorded consume ref and the updated keyPackageRef
            // makes the admin accept with the requester's current key package.
            existing.key_package_ref = record.key_package_ref.clone();
            existing.created_at = record.created_at;
            return Ok(record);
        }

        let records = inner
            .join_requests_by_group
            .entry(record.group_id.clone())
            .or_default();
        if records.len() >= MAX_PENDING_JOIN_REQUESTS_PER_GROUP {
            return Err(StorageError::TooManyPendingJoinRequests);
        }
        records.push(record.clone());
        Ok(record)
    }

    fn fetch_pending_join_requests(
        &self,
        group_id: &str,
        consumed: &[ConsumedJoinRequestRef],
    ) -> Result<Vec<JoinRequestRecord>, StorageError> {
        let mut inner = self.inner.lock().unwrap();
        let mut should_remove = false;
        let out = if let Some(records) = inner.join_requests_by_group.get_mut(group_id) {
            if !consumed.is_empty() {
                records.retain(|r| {
                    !consumed.iter().any(|c| {
                        c.requester_stable_pubkey == r.requester_stable_pubkey
                            && c.created_at == r.created_at
                    })
                });
            }
            if records.is_empty() {
                should_remove = true;
            }
            records.clone()
        } else {
            Vec::new()
        };
        if should_remove {
            inner.join_requests_by_group.shift_remove(group_id);
        }
        Ok(out)
    }

    fn fetch_many_pending_join_requests(
        &self,
        group_ids: &[String],
        consumed: &[ConsumedJoinRequestWithGroupRef],
    ) -> Result<Vec<JoinRequestRecord>, StorageError> {
        let consumed_by_group = partition_consumed_join_requests(consumed);
        let mut out = Vec::new();
        for group_id in group_ids {
            let per_group = consumed_by_group
                .get(group_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            out.extend(self.fetch_pending_join_requests(group_id, per_group)?);
        }
        Ok(out)
    }

    fn delete_expired_join_requests(&self, max_age_threshold: i64) -> Result<usize, StorageError> {
        if max_age_threshold <= 0 {
            return Ok(0);
        }
        let mut inner = self.inner.lock().unwrap();
        let mut deleted = 0usize;
        let mut empty_keys = Vec::new();
        for (group_id, records) in inner.join_requests_by_group.iter_mut() {
            let before = records.len();
            records.retain(|r| r.created_at >= max_age_threshold);
            deleted += before - records.len();
            if records.is_empty() {
                empty_keys.push(group_id.clone());
            }
        }
        for key in empty_keys {
            inner.join_requests_by_group.shift_remove(&key);
        }
        Ok(deleted)
    }

    fn append_group_message(
        &self,
        params: AppendGroupMessageParams,
    ) -> Result<GroupMessageRecord, StorageError> {
        let mut inner = self.inner.lock().unwrap();
        let AppendGroupMessageParams {
            group_id,
            opaque_message,
            created_at,
        } = params;
        let log = inner
            .groups
            .entry(group_id.clone())
            .or_insert_with(GroupLog::new);
        let cursor = log.next_cursor;
        log.next_cursor += 1;
        let record = GroupMessageRecord {
            cursor,
            group_id,
            opaque_message,
            created_at,
        };
        log.messages.push(record.clone());
        Ok(record)
    }

    fn fetch_group_messages(
        &self,
        group_id: &str,
        after_cursor: Option<i64>,
    ) -> Result<Vec<GroupMessageRecord>, StorageError> {
        let inner = self.inner.lock().unwrap();
        // Cursors start at 1, so `cursor > 0` is equivalent to "no filter".
        let ac = after_cursor.unwrap_or(0);
        let out = inner
            .groups
            .get(group_id)
            .map(|log| {
                log.messages
                    .iter()
                    .filter(|m| m.cursor > ac)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(out)
    }

    fn fetch_many_group_messages(
        &self,
        groups: &[FetchGroupMessagesInput],
    ) -> Result<Vec<GroupMessageRecord>, StorageError> {
        let inner = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for g in groups {
            let ac = g.after_cursor.unwrap_or(0);
            let messages = inner
                .groups
                .get(&g.group_id)
                .map(|log| {
                    log.messages
                        .iter()
                        .filter(|m| m.cursor > ac)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            out.extend(messages);
        }
        Ok(out)
    }
}
