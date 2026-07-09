//! The coordinator core: a clock, a storage backend, and a pub/sub fan-out
//! for live group-message delivery. Ported from
//! `references/cordn/src/coordinator/coordinator.ts`.
//!
//! The coordinator is opaque to MLS payload contents. It does NOT parse key
//! packages itself — `publish_key_package` receives `is_last_resort` already
//! computed by the adapter (which uses `mls_parse`). This keeps the core
//! MLS-free; see `AGENTS.md`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use indexmap::IndexMap;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::storage::{
    AppendGroupMessageParams, CoordinatorStorage, InMemoryCoordinatorStorage, StorageError,
};
use crate::types::{
    ConsumedJoinRequestRef, ConsumedJoinRequestWithGroupRef, ConsumedWelcomeRef,
    FetchGroupMessagesInput, GroupMessageRecord, JoinRequestRecord, PublishedKeyPackageRecord,
    WelcomeQueueRecord,
};

/// Default cleanup interval: 6 hours. Set `cleanup_interval_ms` to `0` to
/// disable the background task.
pub const DEFAULT_CLEANUP_INTERVAL_MS: i64 = 21_600_000;
/// Default max age for welcome/join-request records: 30 days. `0` or negative
/// disables age-based cleanup (keep forever).
pub const DEFAULT_MAX_AGE_MS: i64 = 2_592_000_000;

/// Clock injected for deterministic timestamps and cursor allocation.
pub type Now = Arc<dyn Fn() -> i64 + Send + Sync>;

#[derive(Default)]
pub struct CoordinatorOptions {
    pub storage: Option<Arc<dyn CoordinatorStorage>>,
    pub now: Option<Now>,
    /// Interval in ms between cleanup runs. `0` disables. Defaults to 6h.
    pub cleanup_interval_ms: Option<i64>,
    /// Max age in ms for welcome and join request records. `0`/negative keeps
    /// forever. Defaults to 30 days.
    pub max_age_ms: Option<i64>,
}

/// Input to [`Coordinator::publish_key_package`].
///
/// `is_last_resort` is supplied by the caller (the adapter, via `mls_parse`),
/// not computed here — the coordinator core stays MLS-free. `key_package_bytes`
/// are stored verbatim.
pub struct PublishKeyPackageInput {
    pub stable_pubkey: String,
    pub key_package_bytes: Vec<u8>,
    pub key_package_ref: String,
    pub is_last_resort: bool,
    pub publication_event: serde_json::Value,
}

pub struct StoreWelcomeInput {
    pub target_stable_pubkey: String,
    pub key_package_reference: String,
    pub welcome_bytes: Vec<u8>,
    pub join_after_cursor: Option<i64>,
}

pub struct StoreJoinRequestInput {
    pub group_id: String,
    pub requester_stable_pubkey: String,
    pub key_package_ref: String,
}

pub struct PostGroupMessageInput {
    pub group_id: String,
    pub opaque_message: Vec<u8>,
}

/// One live group-message subscription. Holds the receiving end of an
/// unbounded channel plus an unsubscribe guard.
///
/// Single-group [`Coordinator::subscribe_group_messages`] is **live-tail only**:
/// it does not replay backlog (the adapter fetches backlog separately and
/// dedups by cursor). Multi-group [`Coordinator::subscribe_many_group_messages`]
/// merges backlog+live into this one stream with per-group cursor dedup.
pub struct GroupMessageSubscription {
    receiver: mpsc::UnboundedReceiver<GroupMessageRecord>,
    coordinator: Weak<Coordinator>,
    subscriber_id: u64,
    group_ids: Vec<String>,
    unsubscribed: bool,
}

impl GroupMessageSubscription {
    /// Receive the next message. Returns `None` when the channel is closed
    /// (after [`Self::unsubscribe`] or when buffered messages have drained).
    pub async fn recv(&mut self) -> Option<GroupMessageRecord> {
        self.receiver.recv().await
    }

    /// Unsubscribe and close the stream. Idempotent. After this, `recv`
    /// drains any buffered messages then returns `None`.
    pub fn unsubscribe(&mut self) {
        if self.unsubscribed {
            return;
        }
        self.unsubscribed = true;
        if let Some(coord) = self.coordinator.upgrade() {
            coord.remove_subscriber(self.subscriber_id, &self.group_ids);
        }
    }
}

impl Drop for GroupMessageSubscription {
    fn drop(&mut self) {
        self.unsubscribe();
    }
}

/// Subscriber state held under the coordinator lock.
struct Subscriber {
    sender: mpsc::UnboundedSender<GroupMessageRecord>,
    /// When true, apply per-group cursor dedup (multi-group subs). Single-group
    /// subs set this false and send every live message directly.
    dedup: bool,
    cursors_by_group: HashMap<String, i64>,
    /// While true, live fan-out is buffered instead of emitted, so backlog
    /// replay (which happens during setup) goes to the channel first.
    buffering: bool,
    buffer: Vec<GroupMessageRecord>,
}

impl Subscriber {
    fn emit_if_new(&mut self, record: GroupMessageRecord) {
        if !self.dedup {
            let _ = self.sender.send(record);
            return;
        }
        let last = self
            .cursors_by_group
            .get(&record.group_id)
            .copied()
            .unwrap_or(0);
        if record.cursor > last {
            self.cursors_by_group
                .insert(record.group_id.clone(), record.cursor);
            let _ = self.sender.send(record);
        }
    }

    /// Called from live fan-out (`post_group_message`). Buffers while the
    /// multi-group setup is replaying backlog, otherwise emits.
    fn push(&mut self, record: GroupMessageRecord) {
        if self.buffering {
            self.buffer.push(record);
        } else {
            self.emit_if_new(record);
        }
    }

    /// Replay backlog straight to the channel with dedup, regardless of
    /// buffering mode (the buffer only holds live fan-out).
    fn replay_backlog(&mut self, records: Vec<GroupMessageRecord>) {
        for record in records {
            self.emit_if_new(record);
        }
    }

    /// End the buffering phase: emit any live records that arrived during
    /// backlog fetch, then switch to direct live delivery.
    fn finish_buffering(&mut self) {
        self.buffering = false;
        let buffered = std::mem::take(&mut self.buffer);
        for record in buffered {
            self.emit_if_new(record);
        }
    }
}

struct SubscriberRegistry {
    group_subscribers: HashMap<String, HashSet<u64>>,
    subscribers: HashMap<u64, Subscriber>,
    /// Refcount = number of distinct group sets this subscriber id is registered
    /// in. `get_active_subscription_count` is the number of distinct ids.
    refcounts: HashMap<u64, usize>,
    next_id: u64,
}

impl SubscriberRegistry {
    fn new() -> Self {
        Self {
            group_subscribers: HashMap::new(),
            subscribers: HashMap::new(),
            refcounts: HashMap::new(),
            next_id: 1,
        }
    }
}

pub struct Coordinator {
    storage: Arc<dyn CoordinatorStorage>,
    now: Now,
    inner: Mutex<SubscriberRegistry>,
    cleanup_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Coordinator {
    /// Construct a coordinator. Must be called within a tokio runtime context
    /// when `cleanup_interval_ms > 0` (the default), since it spawns a cleanup
    /// task. Pass `cleanup_interval_ms: Some(0)` to disable the task.
    pub fn new(options: CoordinatorOptions) -> Arc<Self> {
        let storage = options.storage.unwrap_or_else(|| {
            Arc::new(InMemoryCoordinatorStorage::new()) as Arc<dyn CoordinatorStorage>
        });
        let now = options.now.unwrap_or_else(default_now);

        let coordinator = Arc::new(Self {
            storage,
            now,
            inner: Mutex::new(SubscriberRegistry::new()),
            cleanup_handle: Mutex::new(None),
        });

        let interval_ms = options
            .cleanup_interval_ms
            .unwrap_or(DEFAULT_CLEANUP_INTERVAL_MS);
        if interval_ms > 0 {
            let max_age_ms = options.max_age_ms.unwrap_or(DEFAULT_MAX_AGE_MS);
            let weak = Arc::downgrade(&coordinator);
            let handle = tokio::spawn(async move {
                // ponytail: sleep-loop + JoinHandle::abort on close, replacing the
                // TS setInterval(...).unref(). The weak upgrade also lets the task
                // exit gracefully if the coordinator is dropped without close().
                let interval = Duration::from_millis(interval_ms as u64);
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(coord) = weak.upgrade() else {
                        break;
                    };
                    let threshold = if max_age_ms > 0 {
                        (coord.now)() - max_age_ms
                    } else {
                        0
                    };
                    let _ = coord.storage.delete_expired_welcomes(threshold);
                    let _ = coord.storage.delete_expired_join_requests(threshold);
                }
            });
            *coordinator.cleanup_handle.lock().unwrap() = Some(handle);
        }

        coordinator
    }

    pub fn close(&self) {
        if let Some(handle) = self.cleanup_handle.lock().unwrap().take() {
            handle.abort();
        }
    }

    // ── key packages ────────────────────────────────────────────────

    pub fn publish_key_package(
        &self,
        input: PublishKeyPackageInput,
    ) -> Result<PublishedKeyPackageRecord, StorageError> {
        let record = PublishedKeyPackageRecord {
            stable_pubkey: input.stable_pubkey,
            key_package_bytes: input.key_package_bytes,
            key_package_ref: input.key_package_ref,
            is_last_resort: input.is_last_resort,
            published_at: (self.now)(),
            publication_event: input.publication_event,
        };
        self.storage.publish_key_package(record)
    }

    pub fn list_key_packages_for_identity(
        &self,
        stable_pubkey: &str,
    ) -> Result<Vec<PublishedKeyPackageRecord>, StorageError> {
        self.storage.list_key_packages_for_identity(stable_pubkey)
    }

    pub fn list_all_key_packages(&self) -> Result<Vec<PublishedKeyPackageRecord>, StorageError> {
        self.storage.list_all_key_packages()
    }

    pub fn get_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        self.storage.get_key_package(key_package_ref)
    }

    pub fn remove_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        self.storage.remove_key_package(key_package_ref)
    }

    pub fn consume_key_package(
        &self,
        identifier: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        self.storage.consume_key_package(identifier)
    }

    // ── welcomes ────────────────────────────────────────────────────

    pub fn store_welcome(
        &self,
        input: StoreWelcomeInput,
    ) -> Result<WelcomeQueueRecord, StorageError> {
        let record = WelcomeQueueRecord {
            target_stable_pubkey: input.target_stable_pubkey,
            key_package_reference: input.key_package_reference,
            welcome_bytes: input.welcome_bytes,
            created_at: (self.now)(),
            join_after_cursor: input.join_after_cursor,
        };
        self.storage.store_welcome(record)
    }

    pub fn fetch_pending_welcomes(
        &self,
        target_stable_pubkey: &str,
        consumed: &[ConsumedWelcomeRef],
    ) -> Result<Vec<WelcomeQueueRecord>, StorageError> {
        self.storage
            .fetch_pending_welcomes(target_stable_pubkey, consumed)
    }

    pub fn delete_expired_welcomes(&self, max_age_threshold: i64) -> Result<usize, StorageError> {
        self.storage.delete_expired_welcomes(max_age_threshold)
    }

    // ── join requests ───────────────────────────────────────────────

    pub fn store_join_request(
        &self,
        input: StoreJoinRequestInput,
    ) -> Result<JoinRequestRecord, StorageError> {
        let record = JoinRequestRecord {
            group_id: input.group_id,
            requester_stable_pubkey: input.requester_stable_pubkey,
            key_package_ref: input.key_package_ref,
            created_at: (self.now)(),
        };
        self.storage.store_join_request(record)
    }

    pub fn fetch_pending_join_requests(
        &self,
        group_id: &str,
        consumed: &[ConsumedJoinRequestRef],
    ) -> Result<Vec<JoinRequestRecord>, StorageError> {
        self.storage.fetch_pending_join_requests(group_id, consumed)
    }

    pub fn fetch_many_pending_join_requests(
        &self,
        group_ids: &[String],
        consumed: &[ConsumedJoinRequestWithGroupRef],
    ) -> Result<Vec<JoinRequestRecord>, StorageError> {
        self.storage
            .fetch_many_pending_join_requests(group_ids, consumed)
    }

    pub fn delete_expired_join_requests(
        &self,
        max_age_threshold: i64,
    ) -> Result<usize, StorageError> {
        self.storage.delete_expired_join_requests(max_age_threshold)
    }

    // ── group messages ──────────────────────────────────────────────

    pub fn post_group_message(
        &self,
        input: PostGroupMessageInput,
    ) -> Result<GroupMessageRecord, StorageError> {
        let record = self
            .storage
            .append_group_message(AppendGroupMessageParams {
                group_id: input.group_id.clone(),
                opaque_message: input.opaque_message,
                created_at: (self.now)(),
                encrypted: true,
            })?;
        self.publish_live(&record);
        Ok(record)
    }

    pub fn fetch_group_messages(
        &self,
        group_id: &str,
        after_cursor: Option<i64>,
    ) -> Result<Vec<GroupMessageRecord>, StorageError> {
        self.storage.fetch_group_messages(group_id, after_cursor)
    }

    pub fn fetch_many_group_messages(
        &self,
        groups: &[FetchGroupMessagesInput],
    ) -> Result<Vec<GroupMessageRecord>, StorageError> {
        self.storage.fetch_many_group_messages(groups)
    }

    /// Live-tail subscription for a single group. Does NOT replay backlog — the
    /// adapter fetches backlog separately and dedups by cursor. Unsubscribes on
    /// drop.
    pub fn subscribe_group_messages(self: &Arc<Self>, group_id: &str) -> GroupMessageSubscription {
        let (tx, rx) = mpsc::unbounded_channel();
        let subscriber = Subscriber {
            sender: tx,
            dedup: false,
            cursors_by_group: HashMap::new(),
            buffering: false,
            buffer: Vec::new(),
        };
        let id = self.register_subscriber(group_id, subscriber);
        GroupMessageSubscription {
            receiver: rx,
            coordinator: Arc::downgrade(self),
            subscriber_id: id,
            group_ids: vec![group_id.to_string()],
            unsubscribed: false,
        }
    }

    /// Multi-group subscription that merges backlog replay and live delivery
    /// into one stream, with independent per-group cursors. Backlog is fetched
    /// after the live subscriber is registered; any live messages that arrive
    /// during the fetch are buffered and flushed after the backlog, so order is
    /// always backlog-then-live. Unsubscribes on drop.
    pub fn subscribe_many_group_messages(
        self: &Arc<Self>,
        groups: &[FetchGroupMessagesInput],
    ) -> Result<GroupMessageSubscription, StorageError> {
        // Dedup group ids preserving input order (TS builds a Map keyed by gid).
        let mut cursors: IndexMap<String, i64> = IndexMap::new();
        for g in groups {
            cursors.insert(g.group_id.clone(), g.after_cursor.unwrap_or(0));
        }
        let group_ids: Vec<String> = cursors.keys().cloned().collect();

        let (tx, rx) = mpsc::unbounded_channel();
        let subscriber = Subscriber {
            sender: tx,
            dedup: true,
            cursors_by_group: cursors.into_iter().collect(),
            buffering: true,
            buffer: Vec::new(),
        };
        let id = self.register_many(&group_ids, subscriber);

        // Fetch backlog without holding the coordinator lock; live fan-out that
        // races in is buffered on the subscriber. Propagate a storage failure
        // (TS throws here too) and unregister first so we don't leak a subscriber
        // whose `GroupMessageSubscription` (and its Drop unsubscribe) the caller
        // never receives.
        let backlog = match self.storage.fetch_many_group_messages(groups) {
            Ok(backlog) => backlog,
            Err(error) => {
                self.remove_subscriber(id, &group_ids);
                return Err(error);
            }
        };

        {
            let mut reg = self.inner.lock().unwrap();
            if let Some(sub) = reg.subscribers.get_mut(&id) {
                sub.replay_backlog(backlog);
                sub.finish_buffering();
            }
        }

        Ok(GroupMessageSubscription {
            receiver: rx,
            coordinator: Arc::downgrade(self),
            subscriber_id: id,
            group_ids,
            unsubscribed: false,
        })
    }

    pub fn get_active_subscription_count(&self) -> usize {
        self.inner.lock().unwrap().refcounts.len()
    }

    // ── internals ───────────────────────────────────────────────────

    fn register_subscriber(&self, group_id: &str, subscriber: Subscriber) -> u64 {
        let mut reg = self.inner.lock().unwrap();
        let id = reg.next_id;
        reg.next_id += 1;
        reg.subscribers.insert(id, subscriber);
        reg.group_subscribers
            .entry(group_id.to_string())
            .or_default()
            .insert(id);
        *reg.refcounts.entry(id).or_insert(0) += 1;
        id
    }

    fn register_many(&self, group_ids: &[String], subscriber: Subscriber) -> u64 {
        let mut reg = self.inner.lock().unwrap();
        let id = reg.next_id;
        reg.next_id += 1;
        reg.subscribers.insert(id, subscriber);
        for gid in group_ids {
            reg.group_subscribers
                .entry(gid.clone())
                .or_default()
                .insert(id);
            *reg.refcounts.entry(id).or_insert(0) += 1;
        }
        id
    }

    fn remove_subscriber(&self, id: u64, group_ids: &[String]) {
        let mut reg = self.inner.lock().unwrap();
        for gid in group_ids {
            if let Some(set) = reg.group_subscribers.get_mut(gid) {
                set.remove(&id);
                if set.is_empty() {
                    reg.group_subscribers.remove(gid);
                }
            }
            if let Some(count) = reg.refcounts.get_mut(&id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    reg.refcounts.remove(&id);
                }
            }
        }
        // Refcount gone → fully unsubscribed: drop the sender so the receiver's
        // `recv` returns None after draining buffered messages.
        if !reg.refcounts.contains_key(&id) {
            reg.subscribers.remove(&id);
        }
    }

    fn publish_live(&self, record: &GroupMessageRecord) {
        let mut reg = self.inner.lock().unwrap();
        let Some(ids) = reg.group_subscribers.get(&record.group_id) else {
            return;
        };
        // Collect ids first so we can mutably borrow `subscribers` next.
        let ids: Vec<u64> = ids.iter().copied().collect();
        for id in ids {
            if let Some(sub) = reg.subscribers.get_mut(&id) {
                sub.push(record.clone());
            }
        }
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        self.close();
    }
}

/// Wall-clock `now` in milliseconds since the UNIX epoch. Used as the default
/// `CoordinatorOptions::now`, and by `main.rs` so the adapter and coordinator
/// share one clock.
pub fn default_now() -> Now {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    })
}
