//! SQLite storage backend. Ported from
//! `references/cordn/src/coordinator/storage/sqliteStorage.ts`.
//!
//! The schema, migrations, pragmas, and statement semantics mirror the TS
//! `better-sqlite3` implementation byte-for-byte so a database written by the
//! TS coordinator is a drop-in read here and vice-versa. MLS payloads are
//! stored as the raw incoming bytes (the TS code re-encodes via
//! `encodeKeyPackage`/`encodeWelcome`; we skip that round-trip — see
//! `AGENTS.md` decision #2).

use std::sync::Mutex;

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, Row};

use crate::storage::{
    partition_consumed_join_requests, AppendGroupMessageParams, CoordinatorStorage, StorageError,
    MAX_PENDING_JOIN_REQUESTS_PER_GROUP,
};
use crate::types::{
    ConsumedJoinRequestRef, ConsumedJoinRequestWithGroupRef, ConsumedWelcomeRef,
    FetchGroupMessagesInput, GroupMessageRecord, JoinRequestRecord, PublishedKeyPackageRecord,
    WelcomeQueueRecord,
};

/// `?`-ergonomic conversion: stringify the driver error into `Backend`, matching
/// the prior explicit `.map_err` at every site. Kept here (not in `mod.rs`) so
/// the trait module stays free of any driver coupling.
impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        StorageError::Backend(e.to_string())
    }
}

/// JSON (de)serialization of `publication_event_json` surfaces as a backend error.
impl From<serde_json::Error> for StorageError {
    fn from(e: serde_json::Error) -> Self {
        StorageError::Backend(e.to_string())
    }
}

/// SQLite `synchronous` durability mode for the write path.
///
/// `Normal` is the WAL production default: crash-safe (no DB corruption), but
/// a power loss can lose the last committed transaction — ~30–40× faster than
/// `Full` because it skips the per-commit `fsync`. `Full` fsyncs every commit
/// for maximum durability; choose it only when no committed message may be
/// lost. This is a runtime-pragma choice, not a schema/wire change, so the
/// TS/Rust DB cross-read guarantee is unaffected (TS leaves it unset/default).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Synchronous {
    Normal,
    Full,
}

impl Synchronous {
    fn pragma(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
        }
    }

    /// Parse the `CORDN_SQLITE_SYNCHRONOUS` value: "normal" | "full"
    /// (case-insensitive). Returns `None` for anything else so `config` can
    /// fail fast on a bad value.
    pub fn from_config(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "normal" => Some(Self::Normal),
            "full" => Some(Self::Full),
            _ => None,
        }
    }
}

const KEY_PACKAGE_WELCOME_JOIN_SCHEMA_SQL: &str = "
    CREATE TABLE IF NOT EXISTS key_packages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stable_pubkey TEXT NOT NULL,
        key_package_ref TEXT NOT NULL UNIQUE,
        key_package_bytes BLOB NOT NULL,
        is_last_resort INTEGER NOT NULL,
        published_at INTEGER NOT NULL,
        publication_event_json TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_key_packages_identity_order
        ON key_packages (stable_pubkey, id);
    CREATE INDEX IF NOT EXISTS idx_key_packages_identity_last_resort_order
        ON key_packages (stable_pubkey, is_last_resort, id);

    CREATE TABLE IF NOT EXISTS welcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        target_stable_pubkey TEXT NOT NULL,
        key_package_reference TEXT NOT NULL,
        welcome_bytes BLOB NOT NULL,
        created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_welcomes_target_order
        ON welcomes (target_stable_pubkey, id);

    CREATE TABLE IF NOT EXISTS join_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT NOT NULL,
        requester_stable_pubkey TEXT NOT NULL,
        key_package_ref TEXT NOT NULL,
        created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_join_requests_group_order
        ON join_requests (group_id, id);
";

const GROUP_SCHEMA_SQL: &str = "
    CREATE TABLE IF NOT EXISTS group_routing (
        group_id TEXT PRIMARY KEY,
        last_message_cursor INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS group_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cursor INTEGER NOT NULL,
        group_id TEXT NOT NULL,
        opaque_message BLOB NOT NULL,
        created_at INTEGER NOT NULL,
        encrypted INTEGER NOT NULL DEFAULT 0
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_group_messages_group_cursor_unique
        ON group_messages (group_id, cursor);
    CREATE INDEX IF NOT EXISTS idx_group_messages_group_cursor
        ON group_messages (group_id, cursor);
";

const KP_COLUMNS: &str =
    "stable_pubkey, key_package_ref, key_package_bytes, is_last_resort, published_at, publication_event_json";

const KP_SELECT_BY_REF_SQL: &str =
    "SELECT stable_pubkey, key_package_ref, key_package_bytes, is_last_resort, published_at, publication_event_json \
     FROM key_packages WHERE key_package_ref = ?1 LIMIT 1";

// Mirror the TS consume-by-identity ordering exactly: prefer non-last-resort
// (oldest first via id ASC), fall back to the newest last-resort (id DESC).
const KP_CONSUME_BY_IDENTITY_SQL: &str = "\
SELECT stable_pubkey, key_package_ref, key_package_bytes, is_last_resort, published_at, publication_event_json \
FROM key_packages WHERE stable_pubkey = ?1 \
ORDER BY is_last_resort ASC, \
         CASE WHEN is_last_resort = 0 THEN id END ASC, \
         CASE WHEN is_last_resort = 1 THEN id END DESC \
LIMIT 1";

pub struct SqliteCoordinatorStorage {
    conn: Mutex<Connection>,
}

impl SqliteCoordinatorStorage {
    /// Open a storage backend at `path`. `None` (or `":memory:"`) opens an
    /// in-memory database.
    pub fn open(path: Option<&str>, synchronous: Synchronous) -> Result<Self, StorageError> {
        let conn = Connection::open(path.unwrap_or(":memory:"))?;
        Self::init(&conn, synchronous)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Convenience for tests: an isolated in-memory database (`synchronous` is
    /// irrelevant for `:memory:`, so the `Normal` default is fine).
    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::open(None, Synchronous::Normal)
    }

    fn init(conn: &Connection, synchronous: Synchronous) -> Result<(), StorageError> {
        conn.execute_batch(&format!(
            "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; \
             PRAGMA busy_timeout = 5000; PRAGMA synchronous = {};",
            synchronous.pragma()
        ))?;
        conn.execute_batch(KEY_PACKAGE_WELCOME_JOIN_SCHEMA_SQL)?;

        // Migration: add join_after_cursor for efficient post-join sync.
        if !has_column(conn, "welcomes", "join_after_cursor")? {
            conn.execute(
                "ALTER TABLE welcomes ADD COLUMN join_after_cursor INTEGER",
                [],
            )?;
        }

        conn.execute_batch(GROUP_SCHEMA_SQL)?;

        // Migration: add encrypted column (0 = legacy/unencrypted, 1 = encrypted).
        if !has_column(conn, "group_messages", "encrypted")? {
            conn.execute(
                "ALTER TABLE group_messages ADD COLUMN encrypted INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        // Migration: drop ephemeral_sender_pubkey (session-scoped transport
        // handle the coordinator never read; routing is by gid).
        if has_column(conn, "group_messages", "ephemeral_sender_pubkey")? {
            conn.execute(
                "ALTER TABLE group_messages DROP COLUMN ephemeral_sender_pubkey",
                [],
            )?;
        }
        // Migration: drop epoch (sinceEpoch filtering moved to client-side).
        if has_column(conn, "group_messages", "epoch")? {
            conn.execute("ALTER TABLE group_messages DROP COLUMN epoch", [])?;
        }
        // Migration: drop latest_handshake_epoch (stale-handshake rejection
        // removed when the coordinator became fully opaque).
        if has_column(conn, "group_routing", "latest_handshake_epoch")? {
            conn.execute(
                "ALTER TABLE group_routing DROP COLUMN latest_handshake_epoch",
                [],
            )?;
        }

        Ok(())
    }
}

fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool, StorageError> {
    Ok(table_columns(conn, table)?.iter().any(|c| c == column))
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, StorageError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>("name"))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn row_to_key_package(row: &Row<'_>) -> rusqlite::Result<PublishedKeyPackageRecord> {
    let pub_json: String = row.get("publication_event_json")?;
    let publication_event = serde_json::from_str(&pub_json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;
    Ok(PublishedKeyPackageRecord {
        stable_pubkey: row.get("stable_pubkey")?,
        key_package_ref: row.get("key_package_ref")?,
        key_package_bytes: row.get("key_package_bytes")?,
        is_last_resort: row.get::<_, i64>("is_last_resort")? != 0,
        published_at: row.get("published_at")?,
        publication_event,
    })
}

fn row_to_welcome(row: &Row<'_>) -> rusqlite::Result<WelcomeQueueRecord> {
    Ok(WelcomeQueueRecord {
        target_stable_pubkey: row.get("target_stable_pubkey")?,
        key_package_reference: row.get("key_package_reference")?,
        welcome_bytes: row.get("welcome_bytes")?,
        created_at: row.get("created_at")?,
        join_after_cursor: row.get("join_after_cursor")?,
    })
}

fn row_to_join_request(row: &Row<'_>) -> rusqlite::Result<JoinRequestRecord> {
    Ok(JoinRequestRecord {
        group_id: row.get("group_id")?,
        requester_stable_pubkey: row.get("requester_stable_pubkey")?,
        key_package_ref: row.get("key_package_ref")?,
        created_at: row.get("created_at")?,
    })
}

fn row_to_group_message(row: &Row<'_>) -> rusqlite::Result<GroupMessageRecord> {
    Ok(GroupMessageRecord {
        cursor: row.get("cursor")?,
        group_id: row.get("group_id")?,
        opaque_message: row.get("opaque_message")?,
        created_at: row.get("created_at")?,
        encrypted: row.get::<_, i64>("encrypted")? != 0,
    })
}

impl CoordinatorStorage for SqliteCoordinatorStorage {
    fn publish_key_package(
        &self,
        record: PublishedKeyPackageRecord,
    ) -> Result<PublishedKeyPackageRecord, StorageError> {
        let conn = self.conn.lock().unwrap();
        let pub_json = serde_json::to_string(&record.publication_event)?;
        {
            let mut stmt = conn.prepare_cached(
                "INSERT INTO key_packages (stable_pubkey, key_package_ref, key_package_bytes, is_last_resort, published_at, publication_event_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            stmt.execute(params![
                &record.stable_pubkey,
                &record.key_package_ref,
                &record.key_package_bytes,
                record.is_last_resort as i64,
                record.published_at,
                pub_json,
            ])?;
        }
        Ok(record)
    }

    fn list_key_packages_for_identity(
        &self,
        stable_pubkey: &str,
    ) -> Result<Vec<PublishedKeyPackageRecord>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {KP_COLUMNS} FROM key_packages WHERE stable_pubkey = ?1 ORDER BY id ASC"
        ))?;
        let rows = stmt.query_map(params![stable_pubkey], row_to_key_package)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    fn list_all_key_packages(&self) -> Result<Vec<PublishedKeyPackageRecord>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {KP_COLUMNS} FROM key_packages ORDER BY id ASC"
        ))?;
        let rows = stmt.query_map([], row_to_key_package)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    fn get_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(KP_SELECT_BY_REF_SQL)?;
        let record = stmt
            .query_row(params![key_package_ref], row_to_key_package)
            .optional()?;
        Ok(record)
    }

    fn remove_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let record: Option<PublishedKeyPackageRecord> = {
            let mut stmt = conn.prepare_cached(KP_SELECT_BY_REF_SQL)?;
            stmt.query_row(params![key_package_ref], row_to_key_package)
                .optional()?
        };
        let Some(record) = record else {
            return Ok(None);
        };
        // key_package_ref is UNIQUE, so deleting by ref is equivalent to the TS
        // delete-by-id without exposing the row id on the record.
        {
            let mut stmt =
                conn.prepare_cached("DELETE FROM key_packages WHERE key_package_ref = ?1")?;
            stmt.execute(params![key_package_ref])?;
        }
        Ok(Some(record))
    }

    fn consume_key_package(
        &self,
        identifier: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        // 1. by exact key-package ref.
        let by_ref: Option<PublishedKeyPackageRecord> = {
            let mut stmt = tx.prepare_cached(KP_SELECT_BY_REF_SQL)?;
            stmt.query_row(params![identifier], row_to_key_package)
                .optional()?
        };
        if let Some(record) = by_ref {
            if !record.is_last_resort {
                let mut stmt =
                    tx.prepare_cached("DELETE FROM key_packages WHERE key_package_ref = ?1")?;
                stmt.execute(params![identifier])?;
            }
            tx.commit()?;
            return Ok(Some(record));
        }

        // 2. fall back to treating the identifier as a stable identity.
        let by_identity: Option<PublishedKeyPackageRecord> = {
            let mut stmt = tx.prepare_cached(KP_CONSUME_BY_IDENTITY_SQL)?;
            stmt.query_row(params![identifier], row_to_key_package)
                .optional()?
        };
        if let Some(record) = by_identity {
            if !record.is_last_resort {
                let mut stmt =
                    tx.prepare_cached("DELETE FROM key_packages WHERE key_package_ref = ?1")?;
                stmt.execute(params![&record.key_package_ref])?;
            }
            tx.commit()?;
            return Ok(Some(record));
        }

        tx.commit()?;
        Ok(None)
    }

    fn store_welcome(
        &self,
        record: WelcomeQueueRecord,
    ) -> Result<WelcomeQueueRecord, StorageError> {
        let conn = self.conn.lock().unwrap();
        {
            let mut stmt = conn.prepare_cached(
                "INSERT INTO welcomes (target_stable_pubkey, key_package_reference, welcome_bytes, created_at, join_after_cursor) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            stmt.execute(params![
                &record.target_stable_pubkey,
                &record.key_package_reference,
                &record.welcome_bytes,
                record.created_at,
                record.join_after_cursor,
            ])?;
        }
        Ok(record)
    }

    fn fetch_pending_welcomes(
        &self,
        target_stable_pubkey: &str,
        consumed: &[ConsumedWelcomeRef],
    ) -> Result<Vec<WelcomeQueueRecord>, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "DELETE FROM welcomes WHERE target_stable_pubkey = ?1 AND key_package_reference = ?2 AND created_at = ?3",
            )?;
            for c in consumed {
                stmt.execute(params![
                    target_stable_pubkey,
                    &c.key_package_reference,
                    c.created_at
                ])?;
            }
        }
        let out = {
            let mut stmt = tx.prepare_cached(
                "SELECT target_stable_pubkey, key_package_reference, welcome_bytes, created_at, join_after_cursor \
                 FROM welcomes WHERE target_stable_pubkey = ?1 ORDER BY id ASC",
            )?;
            let rows = stmt.query_map(params![target_stable_pubkey], row_to_welcome)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            out
        };
        tx.commit()?;
        Ok(out)
    }

    fn delete_expired_welcomes(&self, max_age_threshold: i64) -> Result<usize, StorageError> {
        if max_age_threshold <= 0 {
            return Ok(0);
        }
        let conn = self.conn.lock().unwrap();
        let n = {
            let mut stmt = conn.prepare_cached("DELETE FROM welcomes WHERE created_at < ?1")?;
            stmt.execute(params![max_age_threshold])?
        };
        Ok(n)
    }

    fn store_join_request(
        &self,
        record: JoinRequestRecord,
    ) -> Result<JoinRequestRecord, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let existing: Option<()> = {
            let mut stmt = tx.prepare_cached(
                "SELECT 1 FROM join_requests WHERE group_id = ?1 AND requester_stable_pubkey = ?2 LIMIT 1",
            )?;
            stmt.query_row(
                params![&record.group_id, &record.requester_stable_pubkey],
                |_| Ok(()),
            )
            .optional()?
        };

        if existing.is_some() {
            // Re-request: refresh in place (see updateJoinRequestOnReRequestStatement).
            {
                let mut stmt = tx.prepare_cached(
                    "UPDATE join_requests SET key_package_ref = ?1, created_at = ?2 \
                     WHERE group_id = ?3 AND requester_stable_pubkey = ?4",
                )?;
                stmt.execute(params![
                    &record.key_package_ref,
                    record.created_at,
                    &record.group_id,
                    &record.requester_stable_pubkey,
                ])?;
            }
            tx.commit()?;
            return Ok(record);
        }

        // New row — enforce the per-group cap only on the insert path. A refresh
        // above doesn't add a row, so it must not hit the cap.
        let count: i64 = {
            let mut stmt =
                tx.prepare_cached("SELECT COUNT(*) FROM join_requests WHERE group_id = ?1")?;
            stmt.query_row(params![&record.group_id], |row| row.get(0))?
        };
        if (count as usize) >= MAX_PENDING_JOIN_REQUESTS_PER_GROUP {
            // tx drops → rollback.
            return Err(StorageError::TooManyPendingJoinRequests);
        }

        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO join_requests (group_id, requester_stable_pubkey, key_package_ref, created_at) \
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            stmt.execute(params![
                &record.group_id,
                &record.requester_stable_pubkey,
                &record.key_package_ref,
                record.created_at,
            ])?;
        }
        tx.commit()?;
        Ok(record)
    }

    fn fetch_pending_join_requests(
        &self,
        group_id: &str,
        consumed: &[ConsumedJoinRequestRef],
    ) -> Result<Vec<JoinRequestRecord>, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "DELETE FROM join_requests WHERE group_id = ?1 AND requester_stable_pubkey = ?2 AND created_at = ?3",
            )?;
            for c in consumed {
                stmt.execute(params![group_id, &c.requester_stable_pubkey, c.created_at])?;
            }
        }
        let out = {
            let mut stmt = tx.prepare_cached(
                "SELECT group_id, requester_stable_pubkey, key_package_ref, created_at \
                 FROM join_requests WHERE group_id = ?1 ORDER BY id ASC",
            )?;
            let rows = stmt.query_map(params![group_id], row_to_join_request)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            out
        };
        tx.commit()?;
        Ok(out)
    }

    fn fetch_many_pending_join_requests(
        &self,
        group_ids: &[String],
        consumed: &[ConsumedJoinRequestWithGroupRef],
    ) -> Result<Vec<JoinRequestRecord>, StorageError> {
        if group_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        // Retire consumed requests for each group before the fetch.
        let consumed_by_group = partition_consumed_join_requests(consumed);
        {
            let mut stmt = tx.prepare_cached(
                "DELETE FROM join_requests WHERE group_id = ?1 AND requester_stable_pubkey = ?2 AND created_at = ?3",
            )?;
            for group_id in group_ids {
                if let Some(list) = consumed_by_group.get(group_id) {
                    for c in list {
                        stmt.execute(params![group_id, &c.requester_stable_pubkey, c.created_at])?;
                    }
                }
            }
        }

        let placeholders = (0..group_ids.len())
            .map(|_| "(?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "WITH requested(group_order, group_id) AS (VALUES {placeholders}) \
             SELECT jr.group_id, jr.requester_stable_pubkey, jr.key_package_ref, jr.created_at \
             FROM requested r JOIN join_requests jr ON jr.group_id = r.group_id \
             ORDER BY r.group_order ASC, jr.id ASC"
        );
        let out = {
            let mut params_vec: Vec<SqlValue> = Vec::with_capacity(group_ids.len() * 2);
            for (i, gid) in group_ids.iter().enumerate() {
                params_vec.push(SqlValue::Integer(i as i64));
                params_vec.push(SqlValue::Text(gid.clone()));
            }
            let mut stmt = tx.prepare_cached(&sql)?;
            let rows = stmt.query_map(params_from_iter(params_vec.iter()), row_to_join_request)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            out
        };
        tx.commit()?;
        Ok(out)
    }

    fn delete_expired_join_requests(&self, max_age_threshold: i64) -> Result<usize, StorageError> {
        if max_age_threshold <= 0 {
            return Ok(0);
        }
        let conn = self.conn.lock().unwrap();
        let n = {
            let mut stmt =
                conn.prepare_cached("DELETE FROM join_requests WHERE created_at < ?1")?;
            stmt.execute(params![max_age_threshold])?
        };
        Ok(n)
    }

    fn append_group_message(
        &self,
        params: AppendGroupMessageParams,
    ) -> Result<GroupMessageRecord, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        // Cached prepared statements: the three write-path statements are
        // prepared once per connection and reused across posts, saving the
        // reparse/replan that `execute` pays on every call.
        let last: Option<i64> = {
            let mut stmt = tx.prepare_cached(
                "SELECT last_message_cursor FROM group_routing WHERE group_id = ?1",
            )?;
            stmt.query_row(params![&params.group_id], |row| row.get(0))
                .optional()?
        };
        let cursor = last.unwrap_or(0) + 1;
        // ponytail: i64 ceiling is far beyond any group's lifetime; the TS
        // Number.isSafeInteger guard has no practical analogue here.
        if cursor <= 0 {
            return Err(StorageError::Backend(
                "Unable to allocate per-group message cursor".to_string(),
            ));
        }

        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO group_messages (cursor, group_id, opaque_message, created_at, encrypted) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            stmt.execute(params![
                cursor,
                &params.group_id,
                &params.opaque_message,
                params.created_at,
                params.encrypted as i64,
            ])?;
        }
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO group_routing (group_id, last_message_cursor) VALUES (?1, ?2) \
                 ON CONFLICT(group_id) DO UPDATE SET last_message_cursor = excluded.last_message_cursor",
            )?;
            stmt.execute(params![&params.group_id, cursor])?;
        }
        tx.commit()?;

        Ok(GroupMessageRecord {
            cursor,
            group_id: params.group_id,
            opaque_message: params.opaque_message,
            created_at: params.created_at,
            encrypted: params.encrypted,
        })
    }

    fn fetch_group_messages(
        &self,
        group_id: &str,
        after_cursor: Option<i64>,
    ) -> Result<Vec<GroupMessageRecord>, StorageError> {
        let conn = self.conn.lock().unwrap();
        // Cursors start at 1, so `cursor > 0` is equivalent to "no filter" — one
        // statement serves both the Some- and None-cursor cases.
        let ac = after_cursor.unwrap_or(0);
        let mut stmt = conn.prepare_cached(
            "SELECT cursor, group_id, opaque_message, created_at, encrypted \
                 FROM group_messages WHERE group_id = ?1 AND cursor > ?2 ORDER BY cursor ASC",
        )?;
        let rows = stmt.query_map(params![group_id, ac], row_to_group_message)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    fn fetch_many_group_messages(
        &self,
        groups: &[FetchGroupMessagesInput],
    ) -> Result<Vec<GroupMessageRecord>, StorageError> {
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap();
        let placeholders = (0..groups.len())
            .map(|_| "(?, ?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "WITH requested(group_order, group_id, after_cursor) AS (VALUES {placeholders}) \
             SELECT gm.cursor, gm.group_id, gm.opaque_message, gm.created_at, gm.encrypted \
             FROM requested r JOIN group_messages gm \
               ON gm.group_id = r.group_id AND gm.cursor > r.after_cursor \
             ORDER BY r.group_order ASC, gm.cursor ASC"
        );
        let mut params_vec: Vec<SqlValue> = Vec::with_capacity(groups.len() * 3);
        for (i, g) in groups.iter().enumerate() {
            params_vec.push(SqlValue::Integer(i as i64));
            params_vec.push(SqlValue::Text(g.group_id.clone()));
            params_vec.push(SqlValue::Integer(g.after_cursor.unwrap_or(0)));
        }
        let mut stmt = conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(params_from_iter(params_vec.iter()), row_to_group_message)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_db_path() -> String {
        let n = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir()
            .join(format!(
                "cordn-sqlite-test-{}-{n}.sqlite",
                std::process::id()
            ))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn applies_production_pragmas_and_indexes() {
        let storage = SqliteCoordinatorStorage::open_in_memory().unwrap();
        let conn = storage.conn.lock().unwrap();
        let busy_timeout: i64 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(busy_timeout, 5000);

        let foreign_keys: i64 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1);

        let names: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT name FROM pragma_index_list('key_packages')")
                .unwrap();
            stmt.query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert!(names
            .iter()
            .any(|n| n == "idx_key_packages_identity_last_resort_order"));
    }

    #[test]
    fn migrates_legacy_group_messages_and_routing_columns() {
        let path = temp_db_path();
        let _ = fs::remove_file(&path);

        // Create a legacy schema resembling a pre-encrypted, pre-opaque DB.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE group_routing (
                    group_id TEXT PRIMARY KEY,
                    last_message_cursor INTEGER NOT NULL,
                    latest_handshake_epoch INTEGER
                );
                CREATE TABLE group_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cursor INTEGER NOT NULL,
                    group_id TEXT NOT NULL,
                    opaque_message BLOB NOT NULL,
                    created_at INTEGER NOT NULL,
                    epoch INTEGER,
                    ephemeral_sender_pubkey TEXT
                );",
            )
            .unwrap();
        }

        // Opening the storage runs the migrations.
        let storage = SqliteCoordinatorStorage::open(Some(&path), Synchronous::Normal).unwrap();
        let conn = storage.conn.lock().unwrap();

        let gm_cols = table_columns(&conn, "group_messages").unwrap();
        assert!(
            gm_cols.iter().any(|c| c == "encrypted"),
            "encrypted column added"
        );
        assert!(!gm_cols.iter().any(|c| c == "epoch"), "epoch dropped");
        assert!(
            !gm_cols.iter().any(|c| c == "ephemeral_sender_pubkey"),
            "ephemeral_sender_pubkey dropped"
        );

        let gr_cols = table_columns(&conn, "group_routing").unwrap();
        assert!(
            !gr_cols.iter().any(|c| c == "latest_handshake_epoch"),
            "latest_handshake_epoch dropped"
        );

        drop(conn);
        drop(storage);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{path}-wal"));
        let _ = fs::remove_file(format!("{path}-shm"));
    }

    #[test]
    fn fresh_schema_has_join_after_cursor_on_welcomes() {
        let storage = SqliteCoordinatorStorage::open_in_memory().unwrap();
        let conn = storage.conn.lock().unwrap();
        let cols = table_columns(&conn, "welcomes").unwrap();
        assert!(cols.iter().any(|c| c == "join_after_cursor"));
    }
}
