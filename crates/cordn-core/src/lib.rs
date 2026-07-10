//! `cordn-core` — core delivery-service state and storage for the cordn MLS
//! coordinator. A native Rust port of the TypeScript `cordn` coordinator core
//! (`references/cordn/src/coordinator/`).
//!
//! The coordinator is opaque to MLS payload contents: key packages, welcomes,
//! and group messages are stored and returned as raw bytes. Only the
//! key-package credential identity and the last-resort extension marker are
//! ever inspected (at publish time, by the adapter via `mls_parse` — not by
//! storage or the coordinator core).
//!
//! See `AGENTS.md` for the locked architectural decisions and parity contract.

pub mod contracts;
pub mod coordinator;
pub mod mls_parse;
pub mod ratelimit;
pub mod storage;
pub mod types;

pub use coordinator::{
    default_now, Coordinator, CoordinatorOptions, GroupMessageSubscription, Now,
    PostGroupMessageInput, PublishKeyPackageInput, StoreJoinRequestInput, StoreWelcomeInput,
    DEFAULT_CLEANUP_INTERVAL_MS, DEFAULT_MAX_AGE_MS,
};
pub use mls_parse::{
    parse_key_package, MlsParseError, ParsedKeyPackage, APP_DATA_DICTIONARY_EXTENSION_TYPE,
    LAST_RESORT_KEY_PACKAGE_COMPONENT_ID,
};
pub use ratelimit::{TokenBucketRateLimitConfig, TokenBucketRateLimiter};
pub use storage::{
    partition_consumed_join_requests, AppendGroupMessageParams, CoordinatorStorage,
    InMemoryCoordinatorStorage, SqliteCoordinatorStorage, StorageError, Synchronous,
    MAX_PENDING_JOIN_REQUESTS_PER_GROUP,
};
pub use types::{
    ConsumedJoinRequestRef, ConsumedJoinRequestWithGroupRef, ConsumedWelcomeRef,
    FetchGroupMessagesInput, GroupMessageRecord, JoinRequestRecord, PublishedKeyPackageRecord,
    WelcomeQueueRecord,
};
