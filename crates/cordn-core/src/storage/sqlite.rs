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
    pub fn open(path: Option<&str>) -> Result<Self, StorageError> {
        let conn = Connection::open(path.unwrap_or(":memory:"))
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Convenience for tests: an isolated in-memory database.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::open(None)
    }

    fn init(conn: &Connection) -> Result<(), StorageError> {
        let backend = |e: rusqlite::Error| StorageError::Backend(e.to_string());

        conn.execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;",
        )
        .map_err(backend)?;
        conn.execute_batch(KEY_PACKAGE_WELCOME_JOIN_SCHEMA_SQL)
            .map_err(backend)?;

        // Migration: add join_after_cursor for efficient post-join sync.
        if !has_column(conn, "welcomes", "join_after_cursor")? {
            conn.execute(
                "ALTER TABLE welcomes ADD COLUMN join_after_cursor INTEGER",
                [],
            )
            .map_err(backend)?;
        }

        conn.execute_batch(GROUP_SCHEMA_SQL).map_err(backend)?;

        // Migration: add encrypted column (0 = legacy/unencrypted, 1 = encrypted).
        if !has_column(conn, "group_messages", "encrypted")? {
            conn.execute(
                "ALTER TABLE group_messages ADD COLUMN encrypted INTEGER NOT NULL DEFAULT 0",
                [],
            )
            .map_err(backend)?;
        }
        // Migration: drop ephemeral_sender_pubkey (session-scoped transport
        // handle the coordinator never read; routing is by gid).
        if has_column(conn, "group_messages", "ephemeral_sender_pubkey")? {
            conn.execute(
                "ALTER TABLE group_messages DROP COLUMN ephemeral_sender_pubkey",
                [],
            )
            .map_err(backend)?;
        }
        // Migration: drop epoch (sinceEpoch filtering moved to client-side).
        if has_column(conn, "group_messages", "epoch")? {
            conn.execute("ALTER TABLE group_messages DROP COLUMN epoch", [])
                .map_err(backend)?;
        }
        // Migration: drop latest_handshake_epoch (stale-handshake rejection
        // removed when the coordinator became fully opaque).
        if has_column(conn, "group_routing", "latest_handshake_epoch")? {
            conn.execute(
                "ALTER TABLE group_routing DROP COLUMN latest_handshake_epoch",
                [],
            )
            .map_err(backend)?;
        }

        Ok(())
    }
}

fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool, StorageError> {
    Ok(table_columns(conn, table)?.iter().any(|c| c == column))
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, StorageError> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(|e| StorageError::Backend(e.to_string()))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>("name"))
        .map_err(|e| StorageError::Backend(e.to_string()))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| StorageError::Backend(e.to_string()))?);
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
        let pub_json = serde_json::to_string(&record.publication_event)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        conn.execute(
            "INSERT INTO key_packages (stable_pubkey, key_package_ref, key_package_bytes, is_last_resort, published_at, publication_event_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                &record.stable_pubkey,
                &record.key_package_ref,
                &record.key_package_bytes,
                record.is_last_resort as i64,
                record.published_at,
                pub_json,
            ],
        )
        .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(record)
    }

    fn list_key_packages_for_identity(
        &self,
        stable_pubkey: &str,
    ) -> Result<Vec<PublishedKeyPackageRecord>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare_cached(&format!(
                "SELECT {KP_COLUMNS} FROM key_packages WHERE stable_pubkey = ?1 ORDER BY id ASC"
            ))
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let rows = stmt
            .query_map(params![stable_pubkey], row_to_key_package)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| StorageError::Backend(e.to_string()))?);
        }
        Ok(out)
    }

    fn list_all_key_packages(&self) -> Result<Vec<PublishedKeyPackageRecord>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare_cached(&format!(
                "SELECT {KP_COLUMNS} FROM key_packages ORDER BY id ASC"
            ))
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let rows = stmt
            .query_map([], row_to_key_package)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| StorageError::Backend(e.to_string()))?);
        }
        Ok(out)
    }

    fn get_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare_cached(KP_SELECT_BY_REF_SQL)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let record = stmt
            .query_row(params![key_package_ref], row_to_key_package)
            .optional()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(record)
    }

    fn remove_key_package(
        &self,
        key_package_ref: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let record: Option<PublishedKeyPackageRecord> = {
            let mut stmt = conn
                .prepare_cached(KP_SELECT_BY_REF_SQL)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            stmt.query_row(params![key_package_ref], row_to_key_package)
                .optional()
                .map_err(|e| StorageError::Backend(e.to_string()))?
        };
        let Some(record) = record else {
            return Ok(None);
        };
        // key_package_ref is UNIQUE, so deleting by ref is equivalent to the TS
        // delete-by-id without exposing the row id on the record.
        conn.execute(
            "DELETE FROM key_packages WHERE key_package_ref = ?1",
            params![key_package_ref],
        )
        .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(Some(record))
    }

    fn consume_key_package(
        &self,
        identifier: &str,
    ) -> Result<Option<PublishedKeyPackageRecord>, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        // 1. by exact key-package ref.
        let by_ref: Option<PublishedKeyPackageRecord> = {
            let mut stmt = tx
                .prepare(KP_SELECT_BY_REF_SQL)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            stmt.query_row(params![identifier], row_to_key_package)
                .optional()
                .map_err(|e| StorageError::Backend(e.to_string()))?
        };
        if let Some(record) = by_ref {
            if !record.is_last_resort {
                tx.execute(
                    "DELETE FROM key_packages WHERE key_package_ref = ?1",
                    params![identifier],
                )
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            }
            tx.commit()
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            return Ok(Some(record));
        }

        // 2. fall back to treating the identifier as a stable identity.
        let by_identity: Option<PublishedKeyPackageRecord> = {
            let mut stmt = tx
                .prepare(KP_CONSUME_BY_IDENTITY_SQL)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            stmt.query_row(params![identifier], row_to_key_package)
                .optional()
                .map_err(|e| StorageError::Backend(e.to_string()))?
        };
        if let Some(record) = by_identity {
            if !record.is_last_resort {
                tx.execute(
                    "DELETE FROM key_packages WHERE key_package_ref = ?1",
                    params![&record.key_package_ref],
                )
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            }
            tx.commit()
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            return Ok(Some(record));
        }

        tx.commit()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(None)
    }

    fn store_welcome(
        &self,
        record: WelcomeQueueRecord,
    ) -> Result<WelcomeQueueRecord, StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO welcomes (target_stable_pubkey, key_package_reference, welcome_bytes, created_at, join_after_cursor) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                &record.target_stable_pubkey,
                &record.key_package_reference,
                &record.welcome_bytes,
                record.created_at,
                record.join_after_cursor,
            ],
        )
        .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(record)
    }

    fn fetch_pending_welcomes(
        &self,
        target_stable_pubkey: &str,
        consumed: &[ConsumedWelcomeRef],
    ) -> Result<Vec<WelcomeQueueRecord>, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        for c in consumed {
            tx.execute(
                "DELETE FROM welcomes WHERE target_stable_pubkey = ?1 AND key_package_reference = ?2 AND created_at = ?3",
                params![target_stable_pubkey, &c.key_package_reference, c.created_at],
            )
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        }
        let out = {
            let mut stmt = tx
                .prepare(
                    "SELECT target_stable_pubkey, key_package_reference, welcome_bytes, created_at, join_after_cursor \
                     FROM welcomes WHERE target_stable_pubkey = ?1 ORDER BY id ASC",
                )
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let rows = stmt
                .query_map(params![target_stable_pubkey], row_to_welcome)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| StorageError::Backend(e.to_string()))?);
            }
            out
        };
        tx.commit()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(out)
    }

    fn delete_expired_welcomes(&self, max_age_threshold: i64) -> Result<usize, StorageError> {
        if max_age_threshold <= 0 {
            return Ok(0);
        }
        let conn = self.conn.lock().unwrap();
        let n = conn
            .execute(
                "DELETE FROM welcomes WHERE created_at < ?1",
                params![max_age_threshold],
            )
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(n)
    }

    fn store_join_request(
        &self,
        record: JoinRequestRecord,
    ) -> Result<JoinRequestRecord, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        let existing: Option<()> = tx
            .query_row(
                "SELECT 1 FROM join_requests WHERE group_id = ?1 AND requester_stable_pubkey = ?2 LIMIT 1",
                params![&record.group_id, &record.requester_stable_pubkey],
                |_| Ok(()),
            )
            .optional()
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        if existing.is_some() {
            // Re-request: refresh in place (see updateJoinRequestOnReRequestStatement).
            tx.execute(
                "UPDATE join_requests SET key_package_ref = ?1, created_at = ?2 \
                 WHERE group_id = ?3 AND requester_stable_pubkey = ?4",
                params![
                    &record.key_package_ref,
                    record.created_at,
                    &record.group_id,
                    &record.requester_stable_pubkey,
                ],
            )
            .map_err(|e| StorageError::Backend(e.to_string()))?;
            tx.commit()
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            return Ok(record);
        }

        // New row — enforce the per-group cap only on the insert path. A refresh
        // above doesn't add a row, so it must not hit the cap.
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM join_requests WHERE group_id = ?1",
                params![&record.group_id],
                |row| row.get(0),
            )
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        if (count as usize) >= MAX_PENDING_JOIN_REQUESTS_PER_GROUP {
            // tx drops → rollback.
            return Err(StorageError::TooManyPendingJoinRequests);
        }

        tx.execute(
            "INSERT INTO join_requests (group_id, requester_stable_pubkey, key_package_ref, created_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                &record.group_id,
                &record.requester_stable_pubkey,
                &record.key_package_ref,
                record.created_at,
            ],
        )
        .map_err(|e| StorageError::Backend(e.to_string()))?;
        tx.commit()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(record)
    }

    fn fetch_pending_join_requests(
        &self,
        group_id: &str,
        consumed: &[ConsumedJoinRequestRef],
    ) -> Result<Vec<JoinRequestRecord>, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        for c in consumed {
            tx.execute(
                "DELETE FROM join_requests WHERE group_id = ?1 AND requester_stable_pubkey = ?2 AND created_at = ?3",
                params![group_id, &c.requester_stable_pubkey, c.created_at],
            )
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        }
        let out = {
            let mut stmt = tx
                .prepare(
                    "SELECT group_id, requester_stable_pubkey, key_package_ref, created_at \
                     FROM join_requests WHERE group_id = ?1 ORDER BY id ASC",
                )
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let rows = stmt
                .query_map(params![group_id], row_to_join_request)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| StorageError::Backend(e.to_string()))?);
            }
            out
        };
        tx.commit()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
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
        let tx = conn
            .transaction()
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        // Retire consumed requests for each group before the fetch.
        let consumed_by_group = partition_consumed_join_requests(consumed);
        for group_id in group_ids {
            if let Some(list) = consumed_by_group.get(group_id) {
                for c in list {
                    tx.execute(
                        "DELETE FROM join_requests WHERE group_id = ?1 AND requester_stable_pubkey = ?2 AND created_at = ?3",
                        params![group_id, &c.requester_stable_pubkey, c.created_at],
                    )
                    .map_err(|e| StorageError::Backend(e.to_string()))?;
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
            let mut stmt = tx
                .prepare(&sql)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let rows = stmt
                .query_map(params_from_iter(params_vec.iter()), row_to_join_request)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| StorageError::Backend(e.to_string()))?);
            }
            out
        };
        tx.commit()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(out)
    }

    fn delete_expired_join_requests(&self, max_age_threshold: i64) -> Result<usize, StorageError> {
        if max_age_threshold <= 0 {
            return Ok(0);
        }
        let conn = self.conn.lock().unwrap();
        let n = conn
            .execute(
                "DELETE FROM join_requests WHERE created_at < ?1",
                params![max_age_threshold],
            )
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(n)
    }

    fn append_group_message(
        &self,
        params: AppendGroupMessageParams,
    ) -> Result<GroupMessageRecord, StorageError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction()
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        let last: Option<i64> = tx
            .query_row(
                "SELECT last_message_cursor FROM group_routing WHERE group_id = ?1",
                params![&params.group_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let cursor = last.unwrap_or(0) + 1;
        // ponytail: i64 ceiling is far beyond any group's lifetime; the TS
        // Number.isSafeInteger guard has no practical analogue here.
        if cursor <= 0 {
            return Err(StorageError::Backend(
                "Unable to allocate per-group message cursor".to_string(),
            ));
        }

        tx.execute(
            "INSERT INTO group_messages (cursor, group_id, opaque_message, created_at, encrypted) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                cursor,
                &params.group_id,
                &params.opaque_message,
                params.created_at,
                params.encrypted as i64,
            ],
        )
        .map_err(|e| StorageError::Backend(e.to_string()))?;
        tx.execute(
            "INSERT INTO group_routing (group_id, last_message_cursor) VALUES (?1, ?2) \
             ON CONFLICT(group_id) DO UPDATE SET last_message_cursor = excluded.last_message_cursor",
            params![&params.group_id, cursor],
        )
        .map_err(|e| StorageError::Backend(e.to_string()))?;
        tx.commit()
            .map_err(|e| StorageError::Backend(e.to_string()))?;

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
        let out = if let Some(ac) = after_cursor {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT cursor, group_id, opaque_message, created_at, encrypted \
                     FROM group_messages WHERE group_id = ?1 AND cursor > ?2 ORDER BY cursor ASC",
                )
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let rows = stmt
                .query_map(params![group_id, ac], row_to_group_message)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| StorageError::Backend(e.to_string()))?);
            }
            out
        } else {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT cursor, group_id, opaque_message, created_at, encrypted \
                     FROM group_messages WHERE group_id = ?1 ORDER BY cursor ASC",
                )
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let rows = stmt
                .query_map(params![group_id], row_to_group_message)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| StorageError::Backend(e.to_string()))?);
            }
            out
        };
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
        let mut stmt = conn
            .prepare_cached(&sql)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let rows = stmt
            .query_map(params_from_iter(params_vec.iter()), row_to_group_message)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| StorageError::Backend(e.to_string()))?);
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
        let storage = SqliteCoordinatorStorage::open(Some(&path)).unwrap();
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
