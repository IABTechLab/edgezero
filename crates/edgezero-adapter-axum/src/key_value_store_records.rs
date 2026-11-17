//! Explicit persistent record format. Ordinary-only tables are not reinterpreted.

use std::ops::Bound;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvHandle, KvPage};
use edgezero_core::record_store::{
    Record, RecordBinding, RecordGeneration, RecordPage, RecordPartition, RecordWrite,
    RecordWriteMode, RecordWriteOutcome,
};
use redb::{ReadableDatabase as _, ReadableTable as _, TableDefinition, TableError};
use serde::{Deserialize, Serialize};

use super::PersistentKvStore;

const ROWS: TableDefinition<&str, RecordRow<'static>> = TableDefinition::new("edgezero-records-v1");
const STATE: TableDefinition<&str, (u64, &str, u32)> =
    TableDefinition::new("edgezero-records-state-v1");
const STATE_KEY: &str = "state";
pub(super) const FORMAT: &str = "redb-records-v1";

type RecordRow<'row> = (&'row [u8], Option<&'row str>, u64, Option<u128>);

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    after: String,
    layout: String,
    mode: String,
    prefix: String,
    scope: String,
    version: u32,
}

fn binding(store: &PersistentKvStore) -> Result<&RecordBinding, KvError> {
    store
        .record_binding
        .as_ref()
        .ok_or_else(|| KvError::Unsupported {
            operation: "records".to_owned(),
        })
}

pub(super) fn initialize(store: &PersistentKvStore) -> Result<(), KvError> {
    let binding = binding(store)?;
    let read = store
        .db
        .begin_read()
        .map_err(|_error| KvError::Unavailable)?;
    let rows_exist = match read.open_table(ROWS) {
        Ok(_) => true,
        Err(TableError::TableDoesNotExist(_)) => false,
        Err(_) => return Err(KvError::Unavailable),
    };
    let state_exists = match read.open_table(STATE) {
        Ok(_) => true,
        Err(TableError::TableDoesNotExist(_)) => false,
        Err(_) => return Err(KvError::Unavailable),
    };
    if rows_exist != state_exists {
        return Err(KvError::Unavailable);
    }
    if state_exists {
        let state = read
            .open_table(STATE)
            .map_err(|_error| KvError::Unavailable)?;
        let saved = state
            .get(STATE_KEY)
            .map_err(|_error| KvError::Unavailable)?
            .ok_or(KvError::Unavailable)?;
        let (_, id, version) = saved.value();
        if id != binding.layout.id() || version != binding.layout.version() {
            return Err(invalid("persistent record layout does not match"));
        }
        return Ok(());
    }
    drop(read);
    let write = store
        .db
        .begin_write()
        .map_err(|_error| KvError::Unavailable)?;
    {
        let _rows = write
            .open_table(ROWS)
            .map_err(|_error| KvError::Unavailable)?;
        let mut state = write
            .open_table(STATE)
            .map_err(|_error| KvError::Unavailable)?;
        let _previous_state = state
            .insert(
                STATE_KEY,
                (0, binding.layout.id(), binding.layout.version()),
            )
            .map_err(|_error| KvError::Unavailable)?;
    }
    write.commit().map_err(|_error| KvError::MutationUncertain)
}

fn invalid(message: &str) -> KvError {
    KvError::Validation(message.to_owned())
}

fn validate_partition(
    store: &PersistentKvStore,
    key: &str,
    partition: &RecordPartition,
) -> Result<(), KvError> {
    if binding(store)?.layout.resolve(key)? != *partition {
        return Err(invalid("record partition does not match the key"));
    }
    Ok(())
}

pub(super) fn lookup(
    store: &PersistentKvStore,
    key: &str,
    partition: &RecordPartition,
) -> Result<Option<Record>, KvError> {
    validate_partition(store, key, partition)?;
    let read = store
        .db
        .begin_read()
        .map_err(|_error| KvError::Unavailable)?;
    let table = read
        .open_table(ROWS)
        .map_err(|_error| KvError::Unavailable)?;
    let result = match table.get(key).map_err(|_error| KvError::Unavailable)? {
        None => return Ok(None),
        Some(row) => {
            let (body, metadata, generation, expiry) = row.value();
            if generation == 0 {
                return Err(KvError::Unavailable);
            }
            if PersistentKvStore::is_expired(expiry) {
                None
            } else {
                Some(Record {
                    body: Bytes::copy_from_slice(body),
                    generation: RecordGeneration::from_u64(generation),
                    metadata: metadata.filter(|text| !text.is_empty()).map(str::to_owned),
                })
            }
        }
    };
    drop(table);
    drop(read);
    if result.is_none() {
        cleanup_expired(store, &[key.to_owned()])?;
    }
    Ok(result)
}

pub(super) fn exists(
    store: &PersistentKvStore,
    key: &str,
    partition: &RecordPartition,
) -> Result<bool, KvError> {
    validate_partition(store, key, partition)?;
    let read = store
        .db
        .begin_read()
        .map_err(|_error| KvError::Unavailable)?;
    let table = read
        .open_table(ROWS)
        .map_err(|_error| KvError::Unavailable)?;
    // Authoritative existence uses the same native redb TTL visibility as
    // lookup, conditional writes and partition listing, not mere disk presence.
    Ok(table
        .get(key)
        .map_err(|_error| KvError::Unavailable)?
        .is_some_and(|row| !PersistentKvStore::is_expired(row.value().3)))
}

pub(super) fn insert(
    store: &PersistentKvStore,
    key: &str,
    partition: &RecordPartition,
    value: RecordWrite<'_>,
) -> Result<RecordWriteOutcome, KvError> {
    validate_partition(store, key, partition)?;
    if value.body.len() > KvHandle::MAX_VALUE_SIZE
        || value.metadata.is_some_and(|text| {
            text.len() > binding(store).map_or(0, |capability| capability.max_metadata_bytes)
        })
    {
        return Err(invalid("record exceeds the provider byte limit"));
    }
    let replacement_expiry = value
        .ttl
        .map(|ttl| {
            if ttl.is_zero() {
                return Err(invalid("record TTL must be positive"));
            }
            SystemTime::now()
                .checked_add(ttl)
                .map(PersistentKvStore::system_time_to_millis)
                .ok_or_else(|| invalid("record TTL cannot be represented"))
        })
        .transpose()?;
    let write = store
        .db
        .begin_write()
        .map_err(|_error| KvError::Unavailable)?;
    {
        let mut rows = write
            .open_table(ROWS)
            .map_err(|_error| KvError::Unavailable)?;
        let current = rows
            .get(key)
            .map_err(|_error| KvError::Unavailable)?
            .map(|row| {
                let (_, _, generation, expiry) = row.value();
                (generation, expiry)
            });
        let live = current.filter(|(_, expiry)| !PersistentKvStore::is_expired(*expiry));
        match value.mode {
            RecordWriteMode::Add if live.is_some() => {
                return Ok(RecordWriteOutcome::PreconditionFailed);
            }
            RecordWriteMode::IfGenerationMatch(expected)
                if live.map(|(generation, _)| generation) != Some(expected.as_u64()) =>
            {
                return Ok(RecordWriteOutcome::PreconditionFailed);
            }
            RecordWriteMode::Add
            | RecordWriteMode::Overwrite
            | RecordWriteMode::IfGenerationMatch(_) => {}
        }
        let mut state = write
            .open_table(STATE)
            .map_err(|_error| KvError::Unavailable)?;
        let counter = {
            let saved = state
                .get(STATE_KEY)
                .map_err(|_error| KvError::Unavailable)?
                .ok_or(KvError::Unavailable)?;
            let (counter, id, version) = saved.value();
            let binding = binding(store)?;
            if id != binding.layout.id()
                || version != binding.layout.version()
                || current.is_some_and(|(generation, _)| generation == 0 || generation > counter)
            {
                return Err(KvError::Unavailable);
            }
            counter
        };
        let next = counter.checked_add(1).ok_or(KvError::Unavailable)?;
        let binding = binding(store)?;
        state
            .insert(
                STATE_KEY,
                (next, binding.layout.id(), binding.layout.version()),
            )
            .map_err(|_error| KvError::Unavailable)?;
        let _previous_row = rows
            .insert(
                key,
                (
                    value.body,
                    value.metadata.filter(|text| !text.is_empty()),
                    next,
                    replacement_expiry,
                ),
            )
            .map_err(|_error| KvError::Unavailable)?;
    }
    write
        .commit()
        .map_err(|_error| KvError::MutationUncertain)?;
    Ok(RecordWriteOutcome::Written)
}

pub(super) fn delete(
    store: &PersistentKvStore,
    key: &str,
    partition: &RecordPartition,
) -> Result<(), KvError> {
    validate_partition(store, key, partition)?;
    let write = store
        .db
        .begin_write()
        .map_err(|_error| KvError::Unavailable)?;
    {
        let mut rows = write
            .open_table(ROWS)
            .map_err(|_error| KvError::Unavailable)?;
        if rows
            .remove(key)
            .map_err(|_error| KvError::Unavailable)?
            .is_none()
        {
            return Ok(());
        }
    }
    write.commit().map_err(|_error| KvError::MutationUncertain)
}

pub(super) fn ordinary_lookup(
    store: &PersistentKvStore,
    key: &str,
) -> Result<Option<Bytes>, KvError> {
    let partition = binding(store)?.layout.resolve(key)?;
    Ok(lookup(store, key, &partition)?.map(|record| record.body))
}

pub(super) fn ordinary_put(
    store: &PersistentKvStore,
    key: &str,
    body: &[u8],
    ttl: Option<Duration>,
) -> Result<(), KvError> {
    let partition = binding(store)?.layout.resolve(key)?;
    match insert(
        store,
        key,
        &partition,
        RecordWrite {
            body,
            metadata: None,
            ttl,
            mode: RecordWriteMode::Overwrite,
        },
    )? {
        RecordWriteOutcome::Written => Ok(()),
        RecordWriteOutcome::PreconditionFailed => Err(KvError::Unavailable),
    }
}

pub(super) fn ordinary_delete(store: &PersistentKvStore, key: &str) -> Result<(), KvError> {
    let partition = binding(store)?.layout.resolve(key)?;
    delete(store, key, &partition)
}

fn cleanup_expired(store: &PersistentKvStore, keys: &[String]) -> Result<(), KvError> {
    if keys.is_empty() {
        return Ok(());
    }
    let write = store
        .db
        .begin_write()
        .map_err(|_error| KvError::Unavailable)?;
    {
        let mut table = write
            .open_table(ROWS)
            .map_err(|_error| KvError::Unavailable)?;
        for key in keys {
            let expired = table
                .get(key.as_str())
                .map_err(|_error| KvError::Unavailable)?
                .is_some_and(|row| PersistentKvStore::is_expired(row.value().3));
            if expired {
                table
                    .remove(key.as_str())
                    .map_err(|_error| KvError::Unavailable)?;
            }
        }
    }
    write.commit().map_err(|_error| KvError::Unavailable)
}

pub(super) fn page(
    store: &PersistentKvStore,
    prefix: &str,
    cursor: Option<&str>,
    limit: usize,
    mode: &str,
) -> Result<RecordPage, KvError> {
    if prefix.len() > KvHandle::MAX_KEY_SIZE
        || prefix.chars().any(char::is_control)
        || limit == 0
        || limit > KvHandle::MAX_LIST_PAGE_SIZE
    {
        return Err(invalid("invalid record listing arguments"));
    }
    let binding = binding(store)?;
    let after = cursor
        .map(|text| {
            let token: Cursor = serde_json::from_str(text)
                .map_err(|_error| invalid("invalid persistent record cursor"))?;
            if token.scope != binding.scope
                || token.layout != binding.layout.id()
                || token.version != binding.layout.version()
                || token.prefix != prefix
                || token.mode != mode
                || !token.after.starts_with(prefix)
            {
                return Err(invalid("persistent record cursor does not match the query"));
            }
            binding.layout.resolve(&token.after)?;
            Ok(token.after)
        })
        .transpose()?;
    let read = store
        .db
        .begin_read()
        .map_err(|_error| KvError::Unavailable)?;
    let table = read
        .open_table(ROWS)
        .map_err(|_error| KvError::Unavailable)?;
    let mut range = match after.as_deref() {
        Some(position) => table.range::<&str>((Bound::Excluded(position), Bound::Unbounded)),
        None => table.range(prefix..),
    }
    .map_err(|_error| KvError::Unavailable)?;
    let mut keys = Vec::with_capacity(limit.saturating_add(1));
    let mut expired = Vec::new();
    let mut scanned = 0;
    let mut last = None;
    let mut complete = false;
    let scan_limit = PersistentKvStore::LIST_SCAN_BATCH_SIZE * PersistentKvStore::MAX_SCAN_BATCHES;
    while scanned < scan_limit && keys.len() <= limit {
        let Some(entry) = range.next() else {
            complete = true;
            break;
        };
        let (stored_key, row) = entry.map_err(|_error| KvError::Unavailable)?;
        let key = stored_key.value();
        if !key.starts_with(prefix) {
            complete = true;
            break;
        }
        binding.layout.resolve(key)?;
        last = Some(key.to_owned());
        scanned = scanned.saturating_add(1);
        if PersistentKvStore::is_expired(row.value().3) {
            expired.push(key.to_owned());
        } else {
            keys.push(key.to_owned());
        }
    }
    drop(range);
    drop(table);
    drop(read);
    let next = if keys.len() > limit {
        keys.truncate(limit);
        keys.last().cloned()
    } else if complete {
        None
    } else {
        last
    };
    let next_cursor = next
        .map(|last_key| {
            serde_json::to_string(&Cursor {
                after: last_key,
                layout: binding.layout.id().to_owned(),
                mode: mode.to_owned(),
                prefix: prefix.to_owned(),
                scope: binding.scope.clone(),
                version: binding.layout.version(),
            })
            .map_err(|_error| KvError::Unavailable)
        })
        .transpose()?;
    cleanup_expired(store, &expired)?;
    Ok(RecordPage {
        keys,
        cursor: next_cursor,
    })
}

pub(super) fn ordinary_page(
    store: &PersistentKvStore,
    prefix: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<KvPage, KvError> {
    let page = page(store, prefix, cursor, limit, "ordinary")?;
    Ok(KvPage {
        keys: page.keys,
        cursor: page.cursor,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        sync::{Arc, Barrier},
        thread::{sleep, spawn},
    };

    use edgezero_core::key_value_store::KvStore as _;
    use edgezero_core::record_store::{
        RecordKeyLayout, RecordLayout, contract::assert_record_contract,
    };
    use futures::executor::block_on;

    use super::*;

    struct Layout;

    impl RecordKeyLayout for Layout {
        fn id(&self) -> &'static str {
            "persistent-test"
        }
        fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError> {
            key.find('/')
                .and_then(|split| key.get(..=split))
                .ok_or_else(|| invalid("missing group"))
        }
        fn validate(&self) -> Result<(), KvError> {
            Ok(())
        }
        fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
            match prefix.strip_suffix('/') {
                Some(group) if !group.is_empty() && !group.contains('/') => Ok(()),
                _ => Err(invalid("invalid group")),
            }
        }
        fn version(&self) -> u32 {
            1
        }
    }

    fn layout() -> RecordLayout {
        RecordLayout::new(Layout).expect("should validate layout")
    }

    fn open(path: &Path) -> KvHandle {
        KvHandle::new(Arc::new(
            PersistentKvStore::new_records(path, layout()).expect("should open records"),
        ))
    }

    #[test]
    fn ordinary_overwrite_shares_row_clears_metadata_and_invalidates_generation() {
        let dir = tempfile::tempdir().expect("should create directory");
        let kv = open(&dir.path().join("records.redb"));
        let records = kv.require_records().expect("should require records");
        block_on(async {
            assert_eq!(
                records
                    .insert(
                        "a/key",
                        RecordWrite {
                            body: b"one",
                            metadata: Some("json"),
                            ttl: None,
                            mode: RecordWriteMode::Add
                        }
                    )
                    .await
                    .expect("should insert"),
                RecordWriteOutcome::Written
            );
            let old = records
                .lookup("a/key")
                .await
                .expect("should read")
                .expect("should find row");
            kv.put_bytes("a/key", Bytes::from_static(b"two"))
                .await
                .expect("should overwrite");
            let new = records
                .lookup("a/key")
                .await
                .expect("should read")
                .expect("should find row");
            assert_eq!(new.body, Bytes::from_static(b"two"));
            assert_eq!(new.metadata, None);
            assert_ne!(old.generation, new.generation);
            assert_eq!(
                records
                    .insert(
                        "a/key",
                        RecordWrite {
                            body: b"stale",
                            metadata: None,
                            ttl: None,
                            mode: RecordWriteMode::IfGenerationMatch(old.generation)
                        }
                    )
                    .await
                    .expect("should reject stale token"),
                RecordWriteOutcome::PreconditionFailed
            );
            assert_eq!(
                kv.get_bytes("a/key").await.expect("should read ordinary"),
                Some(Bytes::from_static(b"two"))
            );
        });
    }

    #[test]
    fn generation_history_survives_delete_and_reopen() {
        let dir = tempfile::tempdir().expect("should create directory");
        let path = dir.path().join("records.redb");
        let old = {
            let kv = open(&path);
            let records = kv.require_records().expect("should require records");
            block_on(async {
                kv.put_bytes("a/key", Bytes::from_static(b"one"))
                    .await
                    .expect("should write");
                let old = records
                    .lookup("a/key")
                    .await
                    .expect("should read")
                    .expect("should find row")
                    .generation;
                records.delete("a/key").await.expect("should delete");
                old
            })
        };
        let kv = open(&path);
        let records = kv.require_records().expect("should require records");
        block_on(async {
            kv.put_bytes("a/key", Bytes::from_static(b"two"))
                .await
                .expect("should recreate");
            assert_ne!(
                records
                    .lookup("a/key")
                    .await
                    .expect("should read")
                    .expect("should find row")
                    .generation,
                old
            );
            assert_eq!(
                records
                    .insert(
                        "a/key",
                        RecordWrite {
                            body: b"stale",
                            metadata: None,
                            ttl: None,
                            mode: RecordWriteMode::IfGenerationMatch(old)
                        }
                    )
                    .await
                    .expect("should reject stale token"),
                RecordWriteOutcome::PreconditionFailed
            );
        });
    }

    #[test]
    fn record_and_global_pages_have_complete_separate_continuation() {
        let dir = tempfile::tempdir().expect("should create directory");
        let kv = open(&dir.path().join("records.redb"));
        let records = kv.require_records().expect("should require records");
        block_on(async {
            for key in ["a/one", "a/two", "b/three", "b/four"] {
                kv.put_bytes(key, Bytes::from_static(b"value"))
                    .await
                    .expect("should insert");
            }
            let partition = records.partition("a/").expect("should resolve group");
            let mut keys = Vec::new();
            let mut cursor = None;
            loop {
                let page = records
                    .list_partition(&partition, cursor.as_deref(), 1)
                    .await
                    .expect("should list group");
                keys.extend(page.keys);
                cursor = page.cursor;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(keys, vec!["a/one", "a/two"]);
            let first = kv
                .list_keys_page("", None, 1)
                .await
                .expect("should list globally");
            records
                .list_partition(&partition, first.cursor.as_deref(), 1)
                .await
                .expect_err("global cursor must not cross into partition traversal");
            let mut global_keys = first.keys;
            let mut global_cursor = first.cursor;
            while global_cursor.is_some() {
                let page = kv
                    .list_keys_page("", global_cursor.as_deref(), 1)
                    .await
                    .expect("should continue globally");
                global_keys.extend(page.keys);
                global_cursor = page.cursor;
            }
            global_keys.sort();
            assert_eq!(global_keys, vec!["a/one", "a/two", "b/four", "b/three"]);
        });
    }

    #[test]
    fn delayed_expiry_cleanup_preserves_a_fresh_overwrite() {
        let dir = tempfile::tempdir().expect("should create directory");
        let store = PersistentKvStore::new_records(dir.path().join("records.redb"), layout())
            .expect("should open store");
        ordinary_put(&store, "a/key", b"old", None).expect("should write original");
        let partition = layout().resolve("a/key").expect("should resolve key");
        let generation = lookup(&store, "a/key", &partition)
            .expect("should read original")
            .expect("should find original")
            .generation;
        let write = store.db.begin_write().expect("should begin expiry fixture");
        let mut table = write.open_table(ROWS).expect("should open rows");
        table
            .insert(
                "a/key",
                (b"old".as_slice(), None, generation.as_u64(), Some(0)),
            )
            .expect("should make original row expired");
        drop(table);
        write.commit().expect("should commit expiry fixture");
        // A reader has observed the expired key but has not acquired its cleanup
        // transaction yet. A different writer replaces it in that interval.
        let observed_expired = vec!["a/key".to_owned()];
        ordinary_put(&store, "a/key", b"fresh", None).expect("should overwrite old expiry");
        cleanup_expired(&store, &observed_expired).expect("should recheck current expiry");
        assert_eq!(
            ordinary_lookup(&store, "a/key").expect("should read fresh value"),
            Some(Bytes::from_static(b"fresh"))
        );
    }

    #[test]
    fn public_handle_shared_record_contract() {
        let dir = tempfile::tempdir().expect("should create directory");
        let kv = open(&dir.path().join("records.redb"));
        block_on(assert_record_contract(&kv, "a/"));
    }

    fn race(kv: &KvHandle, mode: RecordWriteMode) -> Vec<RecordWriteOutcome> {
        let barrier = Arc::new(Barrier::new(3));
        let writers: Vec<_> = [b"left".as_slice(), b"right".as_slice()]
            .into_iter()
            .map(|body| {
                let writer_kv = kv.clone();
                let writer_barrier = Arc::clone(&barrier);
                spawn(move || {
                    let records = writer_kv.require_records().expect("should require records");
                    writer_barrier.wait();
                    block_on(records.insert(
                        "a/race",
                        RecordWrite {
                            body,
                            metadata: None,
                            ttl: None,
                            mode,
                        },
                    ))
                    .expect("should complete conditional write")
                })
            })
            .collect();
        barrier.wait();
        writers
            .into_iter()
            .map(|writer| writer.join().expect("should join writer"))
            .collect()
    }

    #[test]
    fn independent_writers_have_one_add_and_one_cas_winner() {
        let dir = tempfile::tempdir().expect("should create directory");
        let kv = open(&dir.path().join("records.redb"));
        let records = kv.require_records().expect("should require records");
        let outcomes = race(&kv, RecordWriteMode::Add);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == RecordWriteOutcome::Written)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == RecordWriteOutcome::PreconditionFailed)
                .count(),
            1
        );
        let generation = block_on(records.lookup("a/race"))
            .expect("should read")
            .expect("should find row")
            .generation;
        let cas_outcomes = race(&kv, RecordWriteMode::IfGenerationMatch(generation));
        assert_eq!(
            cas_outcomes
                .iter()
                .filter(|outcome| **outcome == RecordWriteOutcome::Written)
                .count(),
            1
        );
        assert_eq!(
            cas_outcomes
                .iter()
                .filter(|outcome| **outcome == RecordWriteOutcome::PreconditionFailed)
                .count(),
            1
        );
    }

    #[test]
    fn missing_allocator_state_is_not_recreated_after_all_values_were_deleted() {
        let dir = tempfile::tempdir().expect("should create directory");
        let path = dir.path().join("records.redb");
        let store = PersistentKvStore::new_records(&path, layout()).expect("should open");
        ordinary_put(&store, "a/key", b"one", None).expect("should write");
        ordinary_delete(&store, "a/key").expect("should delete value only");
        let write = store
            .db
            .begin_write()
            .expect("should begin corruption fixture");
        assert!(
            write
                .delete_table(STATE)
                .expect("should remove allocator fixture")
        );
        write.commit().expect("should commit corruption fixture");
        drop(store);
        assert!(PersistentKvStore::new_records(&path, layout()).is_err());
    }

    #[test]
    fn explicit_record_format_does_not_convert_ordinary_rows() {
        let dir = tempfile::tempdir().expect("should create directory");
        let path = dir.path().join("records.redb");
        let ordinary = PersistentKvStore::new(&path).expect("should open ordinary store");
        block_on(ordinary.put_bytes("a/key", Bytes::from_static(b"ordinary")))
            .expect("should write ordinary row");
        drop(ordinary);
        let records = PersistentKvStore::new_records(&path, layout())
            .expect("should open explicit record format");
        assert_eq!(
            ordinary_lookup(&records, "a/key").expect("should read fresh record format"),
            None
        );
        ordinary_put(&records, "a/key", b"record", None).expect("should write record row");
        drop(records);
        let reopened = PersistentKvStore::new(&path).expect("should reopen ordinary format");
        assert_eq!(
            block_on(reopened.get_bytes("a/key")).expect("should preserve ordinary row"),
            Some(Bytes::from_static(b"ordinary"))
        );
    }

    #[test]
    fn empty_expired_page_keeps_continuation_and_cleanup_cannot_reset_generation() {
        let dir = tempfile::tempdir().expect("should create directory");
        let store = PersistentKvStore::new_records(dir.path().join("records.redb"), layout())
            .expect("should open");
        let partition = layout().partition("a/").expect("should group");
        for index in 0_usize..40_usize {
            insert(
                &store,
                &format!("a/{index:03}"),
                &partition,
                RecordWrite {
                    body: b"old",
                    metadata: None,
                    ttl: Some(Duration::from_millis(1)),
                    mode: RecordWriteMode::Add,
                },
            )
            .expect("should insert expired fixture");
        }
        let old_generation = {
            let read = store
                .db
                .begin_read()
                .expect("should inspect expiry fixture");
            let table = read.open_table(ROWS).expect("should open expired rows");
            let row = table
                .get("a/000")
                .expect("should read old row")
                .expect("should retain expired row until cleanup");
            RecordGeneration::from_u64(row.value().2)
        };
        sleep(Duration::from_millis(20));
        assert!(
            !exists(&store, "a/000", &partition).expect("should check native expiry visibility"),
            "strong exact existence must agree with this provider's expiry-aware partition listing"
        );
        ordinary_put(&store, "a/zzz", b"live", None).expect("should insert live tail");
        let first = page(&store, "a/", None, 1, "partition").expect("should scan first page");
        assert!(first.keys.is_empty());
        assert!(first.cursor.is_some());
        let second =
            page(&store, "a/", first.cursor.as_deref(), 1, "partition").expect("should continue");
        assert_eq!(second.keys, vec!["a/zzz"]);
        assert!(
            second.cursor.is_none(),
            "the live tail completes this traversal"
        );
        let kv = KvHandle::new(Arc::new(store));
        let records = kv.require_records().expect("should require records");
        block_on(async {
            assert_eq!(
                records
                    .insert(
                        "a/000",
                        RecordWrite {
                            body: b"recreated",
                            metadata: None,
                            ttl: None,
                            mode: RecordWriteMode::Add,
                        }
                    )
                    .await
                    .expect("should recreate expired value"),
                RecordWriteOutcome::Written
            );
            let recreated = records
                .lookup("a/000")
                .await
                .expect("should read recreated value")
                .expect("should find recreated value");
            assert_ne!(recreated.generation, old_generation);
            assert_eq!(
                records
                    .insert(
                        "a/000",
                        RecordWrite {
                            body: b"stale",
                            metadata: None,
                            ttl: None,
                            mode: RecordWriteMode::IfGenerationMatch(old_generation),
                        }
                    )
                    .await
                    .expect("should reject stale generation after expiry"),
                RecordWriteOutcome::PreconditionFailed
            );
        });
    }
}
