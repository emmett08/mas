//! Native repository declarations and exact, literal-preserving compatibility.
use rusqlite::Connection;
use std::sync::OnceLock;

pub(crate) const V1: &str = "CREATE TABLE runs(id TEXT PRIMARY KEY,config TEXT NOT NULL,deadline REAL NOT NULL,message_count INTEGER NOT NULL DEFAULT 0,byte_count INTEGER NOT NULL DEFAULT 0,call_count INTEGER NOT NULL DEFAULT 0,active TEXT NOT NULL DEFAULT '1');
    CREATE TABLE messages(seq INTEGER PRIMARY KEY AUTOINCREMENT,run TEXT NOT NULL REFERENCES runs(id),id TEXT NOT NULL,sender TEXT NOT NULL,recipient TEXT NOT NULL,content TEXT NOT NULL,received INTEGER NOT NULL DEFAULT 0,acked INTEGER NOT NULL DEFAULT 0,UNIQUE(run,id));
    CREATE INDEX mailbox ON messages(run,recipient,acked,seq);
    CREATE TABLE calls(run TEXT NOT NULL REFERENCES runs(id),id TEXT NOT NULL,actor TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'reserved',result TEXT,UNIQUE(run,id));
    PRAGMA application_id=1296126769; PRAGMA user_version=1;";

pub(crate) const V2_MIGRATION: &str = "ALTER TABLE runs ADD COLUMN expired INTEGER NOT NULL DEFAULT 0 CHECK(expired IN (0,1));
    CREATE TABLE tasks(run TEXT NOT NULL REFERENCES runs(id),id TEXT NOT NULL,request_id TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','completed','unresolved','failed','cancelled','expired_outcome_unknown')),terminal_id TEXT,closed_at REAL,PRIMARY KEY(run,id),UNIQUE(run,request_id),FOREIGN KEY(run,request_id) REFERENCES messages(run,id),FOREIGN KEY(run,terminal_id) REFERENCES messages(run,id));
    CREATE TABLE admissions(scope TEXT PRIMARY KEY,policy_fingerprint TEXT NOT NULL,limit_count INTEGER NOT NULL CHECK(limit_count>0),attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0 AND attempts<=limit_count));";

type Layout = Vec<(String, String, String, Vec<String>)>;

fn tokens(source: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut result = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let start = offset;
        if bytes[offset].is_ascii_whitespace() {
            offset += 1;
            continue;
        }
        if source[offset..].starts_with("--") {
            offset += source[offset..].find('\n').unwrap_or(bytes.len() - offset);
            continue;
        }
        if source[offset..].starts_with("/*") {
            offset += source[offset + 2..].find("*/").map(|n| n + 4).unwrap_or(bytes.len() - offset);
            continue;
        }
        match bytes[offset] {
            quote @ (b'\'' | b'"' | b'`' | b'[') => {
                let end = if quote == b'[' { b']' } else { quote };
                offset += 1;
                while offset < bytes.len() {
                    let byte = bytes[offset];
                    offset += 1;
                    if byte == end {
                        if quote != b'[' && bytes.get(offset) == Some(&end) {
                            offset += 1;
                        } else { break; }
                    }
                }
                result.push(source[start..offset].to_owned());
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                offset += 1;
                while bytes.get(offset).is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$')) {
                    offset += 1;
                }
                result.push(source[start..offset].to_ascii_lowercase());
            }
            byte if byte.is_ascii_digit() => {
                offset += 1;
                while bytes.get(offset).is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.')) {
                    offset += 1;
                }
                result.push(source[start..offset].to_owned());
            }
            _ => {
                // Keep punctuation token boundaries: >= and > = must differ.
                let operator = ["->>", "->", "||", "<<", ">>", "<=", ">=", "<>", "!=", "=="]
                    .iter().find(|token| source[offset..].starts_with(**token));
                offset += operator.map_or_else(|| source[offset..].chars().next().unwrap().len_utf8(), |s| s.len());
                result.push(source[start..offset].to_owned());
            }
        }
    }
    // SQLite normally removes this guard from sqlite_schema, but tolerate its
    // documented spelling without altering text inside a quoted token.
    let mut index = 0;
    while index + 2 < result.len() {
        if result[index..index + 3] == ["if", "not", "exists"] {
            result.drain(index..index + 3);
        } else { index += 1; }
    }
    result
}

fn layout(db: &Connection) -> Result<Layout, String> {
    let mut statement = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
        .map_err(|e| e.to_string())?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?,
        tokens(&row.get::<_, Option<String>>(3)?.unwrap_or_default())))).map_err(|e| e.to_string())?;
    rows.collect::<Result<_, _>>().map_err(|e| e.to_string())
}

fn expected(version: i64) -> Result<Layout, String> {
    let db = Connection::open_in_memory().map_err(|e| e.to_string())?;
    db.execute_batch(V1).map_err(|e| e.to_string())?;
    if version == 2 {
        db.execute_batch(V2_MIGRATION).map_err(|e| e.to_string())?;
    }
    layout(&db)
}

pub(crate) fn validate(db: &Connection, version: i64) -> Result<(), String> {
    static V1_LAYOUT: OnceLock<Result<Layout, String>> = OnceLock::new();
    static V2_LAYOUT: OnceLock<Result<Layout, String>> = OnceLock::new();
    let cache = match version {
        1 => &V1_LAYOUT,
        2 => &V2_LAYOUT,
        _ => return Err("incompatible database schema: unsupported native version".into()),
    };
    let expected = cache.get_or_init(|| expected(version)).as_ref().map_err(Clone::clone)?;
    if &layout(db)? != expected {
        return Err("incompatible database schema: constraints, indices or declarations differ from MAS native schema".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT: AtomicUsize = AtomicUsize::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("mas-schema-{}-{}.sqlite", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)))
    }

    #[test]
    fn quoted_literals_and_token_boundaries_are_not_normalised_away() {
        assert_ne!(tokens("CHECK(value='A B')"), tokens("CHECK(value='ab')"));
        assert_ne!(tokens("CHECK(value='A B')"), tokens("CHECK(value='AB')"));
        assert_ne!(tokens("DEFAULT 'IF NOT EXISTS'"), tokens("DEFAULT ''"));
        assert_ne!(tokens("value NOT NULL"), tokens("value NOTNULL"));
        assert_ne!(tokens("CHECK(value>=12)"), tokens("CHECK(value> =1 2)"));
        assert_eq!(tokens("CHECK(value='It''s A B')"), tokens("check ( value /* comment */ = 'It''s A B' )"));
    }

    #[test]
    fn native_v1_constraints_defaults_and_index_definitions_are_exact() {
        for source in [
            V1.replace(" REFERENCES runs(id)", ""),
            V1.replace(",UNIQUE(run,id)", ""),
            V1.replace("DEFAULT 'reserved'", "DEFAULT 'RESERVED'"),
            V1.replace("ON messages(run,recipient,acked,seq)", "ON messages(run,sender,acked,seq)"),
        ] {
            let path = path();
            let db = Connection::open(&path).unwrap();
            db.execute_batch(&source).unwrap();
            drop(db);
            let before = std::fs::read(&path).unwrap();
            assert!(matches!(Store::open(&path), Err(error) if error.contains("incompatible database schema")));
            assert_eq!(before, std::fs::read(&path).unwrap());
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn native_v2_checks_and_unknown_triggers_are_rejected_before_mutation() {
        for migration in [
            V2_MIGRATION.replace("CHECK(expired IN (0,1))", "CHECK(expired IN (0,1,2))"),
            V2_MIGRATION.replace("attempts<=limit_count", "attempts<=limit_count+1"),
            V2_MIGRATION.replace("'open'", "'OPEN'"),
            format!("{V2_MIGRATION} CREATE TRIGGER unknown AFTER INSERT ON calls BEGIN DELETE FROM calls; END;"),
        ] {
            let path = path();
            let db = Connection::open(&path).unwrap();
            db.execute_batch(V1).unwrap();
            db.execute_batch(&migration).unwrap();
            db.execute_batch("PRAGMA user_version=2;").unwrap();
            drop(db);
            let before = std::fs::read(&path).unwrap();
            assert!(matches!(Store::open(&path), Err(error) if error.contains("incompatible database schema")));
            assert_eq!(before, std::fs::read(&path).unwrap());
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn native_v1_spelling_compatibility_migrates_without_losing_prior_allocations() {
        let path = path();
        let db = Connection::open(&path).unwrap();
        db.execute_batch(&V1.replace("CREATE TABLE", "create  table\nIF NOT EXISTS")
            .replace("CREATE INDEX", "create /* index */ index")).unwrap();
        db.execute("INSERT INTO runs(id,config,deadline,call_count) VALUES('r',?,9999999999,1)",
                   [r#"{"agents":["a"],"max_calls":1}"#]).unwrap();
        db.execute("INSERT INTO calls(run,id,actor) VALUES('r','call','a')", []).unwrap();
        drop(db);
        let mut store = Store::open(&path).unwrap();
        let state = store.inspect("r").unwrap();
        assert_eq!(state["usage"]["calls"], 1);
        assert_eq!(state["calls"][0]["id"], "call");
        assert!(store.reserve_call("r", "a", "new").is_err());
        drop(store);
        let db = Connection::open(&path).unwrap();
        assert_eq!(db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0)).unwrap(), 2);
        validate(&db, 2).unwrap();
        drop(db);
        Store::open(&path).unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
