use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};

use herald_core::{EndpointId, Message};

/// SQLite-backed store for pending (undelivered) messages.
pub struct MessageStore {
    conn: Connection,
}

impl MessageStore {
    /// Opens or creates the database at the given path.
    pub fn open(db_path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(db_path)?;
        Self::init(conn)
    }

    /// Creates an in-memory database (for testing).
    #[cfg(test)]
    pub(crate) fn in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> rusqlite::Result<Self> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pending_messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                recipient   TEXT NOT NULL,
                message_json TEXT NOT NULL,
                created_at  INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pending_recipient
                ON pending_messages (recipient);",
        )?;
        Ok(Self { conn })
    }

    /// Persists a message for an offline recipient.
    pub fn save_pending(&self, recipient: &EndpointId, message: &Message) -> rusqlite::Result<i64> {
        let json = serde_json::to_string(message).map_err(|e| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            )))
        })?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.conn.execute(
            "INSERT INTO pending_messages (recipient, message_json, created_at) VALUES (?1, ?2, ?3)",
            params![recipient.as_str(), json, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Retrieves all pending messages for a recipient, returning (id, message) pairs.
    pub fn get_pending(&self, recipient: &EndpointId) -> rusqlite::Result<Vec<(i64, Message)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, message_json FROM pending_messages WHERE recipient = ?1 ORDER BY id",
        )?;
        let rows = stmt.query_map(params![recipient.as_str()], |row| {
            let id: i64 = row.get(0)?;
            let json: String = row.get(1)?;
            let message: Message = serde_json::from_str(&json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
            Ok((id, message))
        })?;
        rows.collect()
    }

    /// Deletes a delivered message by id.
    pub fn mark_delivered(&self, id: i64) -> rusqlite::Result<()> {
        self.conn
            .execute("DELETE FROM pending_messages WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Removes messages older than `max_age_secs`.
    pub fn cleanup_old(&self, max_age_secs: u64) -> rusqlite::Result<usize> {
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(max_age_secs) as i64;

        self.conn.execute(
            "DELETE FROM pending_messages WHERE created_at <= ?1",
            params![cutoff],
        )
    }
}

#[cfg(test)]
mod tests {
    use herald_core::{Address, EndpointId, Message};
    use serde_json::json;

    use super::*;

    fn make_message(from: &str, to: &str) -> Message {
        Message {
            from: EndpointId::new(from).unwrap(),
            to: Address::Direct(EndpointId::new(to).unwrap()),
            payload: json!({"text": "hello"}),
            metadata: None,
            timestamp: 1700000000000,
        }
    }

    #[test]
    fn save_and_retrieve_pending() {
        let store = MessageStore::in_memory().unwrap();
        let recipient = EndpointId::new("bob").unwrap();
        let msg = make_message("alice", "bob");

        let id = store.save_pending(&recipient, &msg).unwrap();
        assert!(id > 0);

        let pending = store.get_pending(&recipient).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, id);
        assert_eq!(pending[0].1.payload, msg.payload);
    }

    #[test]
    fn mark_delivered_removes_message() {
        let store = MessageStore::in_memory().unwrap();
        let recipient = EndpointId::new("bob").unwrap();
        let msg = make_message("alice", "bob");

        let id = store.save_pending(&recipient, &msg).unwrap();
        store.mark_delivered(id).unwrap();

        let pending = store.get_pending(&recipient).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn get_pending_returns_empty_for_unknown_recipient() {
        let store = MessageStore::in_memory().unwrap();
        let recipient = EndpointId::new("nobody").unwrap();
        let pending = store.get_pending(&recipient).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn multiple_pending_messages_ordered_by_id() {
        let store = MessageStore::in_memory().unwrap();
        let recipient = EndpointId::new("bob").unwrap();

        let id1 = store
            .save_pending(&recipient, &make_message("alice", "bob"))
            .unwrap();
        let id2 = store
            .save_pending(&recipient, &make_message("charlie", "bob"))
            .unwrap();

        let pending = store.get_pending(&recipient).unwrap();
        assert_eq!(pending.len(), 2);
        assert!(id1 < id2);
        assert_eq!(pending[0].0, id1);
        assert_eq!(pending[1].0, id2);
    }

    #[test]
    fn cleanup_old_removes_expired_messages() {
        let store = MessageStore::in_memory().unwrap();
        let recipient = EndpointId::new("bob").unwrap();
        let msg = make_message("alice", "bob");

        store.save_pending(&recipient, &msg).unwrap();

        // Cleanup with max_age of 0 should remove everything.
        let removed = store.cleanup_old(0).unwrap();
        assert_eq!(removed, 1);

        let pending = store.get_pending(&recipient).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn pending_messages_are_per_recipient() {
        let store = MessageStore::in_memory().unwrap();
        let bob = EndpointId::new("bob").unwrap();
        let charlie = EndpointId::new("charlie").unwrap();

        store
            .save_pending(&bob, &make_message("alice", "bob"))
            .unwrap();
        store
            .save_pending(&charlie, &make_message("alice", "charlie"))
            .unwrap();

        assert_eq!(store.get_pending(&bob).unwrap().len(), 1);
        assert_eq!(store.get_pending(&charlie).unwrap().len(), 1);
    }
}
