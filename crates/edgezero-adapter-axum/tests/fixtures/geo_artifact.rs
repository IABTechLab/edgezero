//! Files as exact-key KV fixtures for the managed listener test, not a backend.

use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use edgezero_core::geo::kv::{digest, hex};
use edgezero_core::key_value_store::{KvError, KvPage, KvStore};

pub static READS: AtomicUsize = AtomicUsize::new(0);

pub struct Artifact(pub PathBuf);

impl Artifact {
    pub fn path(&self, key: &str) -> PathBuf {
        self.0.join(hex(&digest(key.as_bytes())))
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "Geo fixture needs no record operations"
)]
#[async_trait(?Send)]
impl KvStore for Artifact {
    async fn delete(&self, _: &str) -> Result<(), KvError> {
        Err(KvError::Unavailable)
    }

    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        READS.fetch_add(1, Ordering::Relaxed);
        match fs::read(self.path(key)) {
            Ok(bytes) => Ok(Some(bytes.into())),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(_error) => Err(KvError::Unavailable),
        }
    }

    async fn list_keys_page(&self, _: &str, _: Option<&str>, _: usize) -> Result<KvPage, KvError> {
        Err(KvError::Unavailable)
    }

    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        fs::write(self.path(key), &value).map_err(|_error| KvError::Unavailable)
    }

    async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
        Err(KvError::Unavailable)
    }
}
