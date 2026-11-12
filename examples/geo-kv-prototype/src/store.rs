//! Local artifact transport only. The resolver itself knows only KvHandle.
//! Synchronous filesystem calls here are NOT a production async KV backend.
use std::{
    fs,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvPage, KvStore};
use serde::Serialize;

#[derive(Default)]
pub struct ArtifactStore {
    root: PathBuf,
    reads: AtomicU64,
    read_bytes: AtomicU64,
    writes: AtomicU64,
    write_bytes: AtomicU64,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct StoreStats {
    pub reads: u64,
    pub read_bytes: u64,
    pub writes: u64,
    pub write_bytes: u64,
}

impl ArtifactStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            ..Self::default()
        }
    }

    fn path(&self, key: &str) -> Result<PathBuf, KvError> {
        if !key.starts_with("geo/p1/")
            || Path::new(key)
                .components()
                .any(|p| !matches!(p, Component::Normal(_)))
        {
            return Err(KvError::Validation("invalid artifact key".into()));
        }
        Ok(self.root.join(key))
    }

    pub fn stats(&self) -> StoreStats {
        StoreStats {
            reads: self.reads.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
        }
    }

    pub fn reset_stats(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.read_bytes.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
        self.write_bytes.store(0, Ordering::Relaxed);
    }
}

fn unsupported(operation: &str) -> KvError {
    KvError::Unsupported {
        operation: operation.into(),
    }
}

#[async_trait(?Send)]
impl KvStore for ArtifactStore {
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let value = match fs::read(self.path(key)?) {
            Ok(value) => value,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(KvError::Internal(e.into())),
        };
        self.read_bytes
            .fetch_add(value.len() as u64, Ordering::Relaxed);
        Ok(Some(value.into()))
    }

    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        let path = self.path(key)?;
        fs::create_dir_all(
            path.parent()
                .ok_or_else(|| KvError::Validation("missing parent".into()))?,
        )
        .map_err(|e| KvError::Internal(e.into()))?;
        // A rerun may repeat the same immutable publication, never replace different bytes.
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut file) => {
                use std::io::Write;
                file.write_all(&value)
                    .map_err(|e| KvError::Internal(e.into()))?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let old = fs::read(path).map_err(|e| KvError::Internal(e.into()))?;
                if old != value {
                    return Err(KvError::Validation("immutable artifact differs".into()));
                }
            }
            Err(e) => return Err(KvError::Internal(e.into())),
        }
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.write_bytes
            .fetch_add(value.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn delete(&self, _: &str) -> Result<(), KvError> {
        Err(unsupported("delete"))
    }
    async fn list_keys_page(&self, _: &str, _: Option<&str>, _: usize) -> Result<KvPage, KvError> {
        Err(unsupported("list_keys_page"))
    }
    async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
        Err(unsupported("put_bytes_with_ttl"))
    }
}
