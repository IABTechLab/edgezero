//! Immutable publication. One operator owns each generation; read-before-write is not a lock.
#[cfg(test)]
#[path = "publication_tests.rs"]
mod tests;
use std::{collections::HashMap, fs, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvHandle, KvPage, KvStore};
#[cfg(any(test, feature = "publisher", feature = "celld", feature = "dynamodb"))]
use serde::Serialize;

use crate::{
    index::{self, InventoryEntry, Receipt, Resolver, digest, hex},
    store::ArtifactStore,
};

pub struct Artifact {
    pub receipt: Receipt,
    pub inventory: Vec<InventoryEntry>,
    #[cfg(any(test, feature = "publisher", feature = "celld", feature = "dynamodb"))]
    kv: KvHandle,
}

// Verification is restricted to the inventory, so an omitted descendant cannot be
// hidden by a leftover file from an earlier build. Each read rechecks artifact bytes.
struct InventoryStore {
    inner: ArtifactStore,
    entries: HashMap<String, (usize, String)>,
}

#[async_trait(?Send)]
impl KvStore for InventoryStore {
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        let (size, hash) = self
            .entries
            .get(key)
            .ok_or_else(|| KvError::Validation("object absent from inventory".into()))?;
        let bytes = self.inner.get_bytes(key).await?;
        if let Some(bytes) = &bytes
            && (bytes.len() != *size || hex(&digest(bytes)) != *hash)
        {
            return Err(KvError::Validation("artifact digest mismatch".into()));
        }
        Ok(bytes)
    }
    async fn put_bytes(&self, _: &str, _: Bytes) -> Result<(), KvError> {
        Err(read_only())
    }
    async fn delete(&self, _: &str) -> Result<(), KvError> {
        Err(read_only())
    }
    async fn list_keys_page(&self, _: &str, _: Option<&str>, _: usize) -> Result<KvPage, KvError> {
        Err(read_only())
    }
    async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
        Err(read_only())
    }
}
fn read_only() -> KvError {
    KvError::Unsupported {
        operation: "artifact mutation or listing".into(),
    }
}

impl Artifact {
    pub async fn open(out: &Path) -> Result<Self> {
        let receipt: Receipt = serde_json::from_slice(&fs::read(out.join("receipt.json"))?)?;
        let inventory: Vec<InventoryEntry> =
            serde_json::from_slice(&fs::read(out.join("inventory.json"))?)?;
        let prefix = receipt
            .manifest_key
            .strip_suffix("manifest")
            .context("invalid manifest key")?;
        let mut entries = HashMap::new();
        let mut manifests = 0;
        for item in &inventory {
            ensure!(
                item.bytes > 0 && item.bytes <= index::limits(0).max_object_bytes,
                "invalid inventory size"
            );
            ensure!(
                item.key.len() <= KvHandle::MAX_KEY_SIZE,
                "invalid inventory key length"
            );
            if item.key == receipt.manifest_key {
                manifests += 1;
                ensure!(
                    item.sha256 == hex(&receipt.manifest_hash),
                    "inventory manifest mismatch"
                );
            } else {
                let suffix = item
                    .key
                    .strip_prefix(prefix)
                    .context("inventory generation mismatch")?;
                let hash = suffix
                    .strip_prefix("v4/")
                    .or_else(|| suffix.strip_prefix("v6/"))
                    .context("invalid inventory object key")?;
                ensure!(
                    hash.len() == 64
                        && hash
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                        && hash == item.sha256,
                    "invalid inventory object digest"
                );
            }
            ensure!(
                entries
                    .insert(item.key.clone(), (item.bytes, item.sha256.clone()))
                    .is_none(),
                "duplicate inventory key"
            );
        }
        ensure!(manifests == 1, "inventory must contain one manifest");
        let kv = KvHandle::new(Arc::new(InventoryStore {
            inner: ArtifactStore::new(out),
            entries,
        }));
        let resolver = Resolver::open(kv.clone(), &receipt, index::limits(0)).await?;
        ensure!(
            resolver.verify_tree().await? == inventory.len(),
            "inventory contains unreachable objects"
        );
        Ok(Self {
            receipt,
            inventory,
            #[cfg(any(test, feature = "publisher", feature = "celld", feature = "dynamodb"))]
            kv,
        })
    }
}

#[cfg(any(test, feature = "publisher", feature = "celld", feature = "dynamodb"))]
#[derive(Debug, Default, Serialize)]
pub struct PublishReport {
    pub objects: usize,
    pub written: usize,
    pub identical: usize,
}

#[cfg(any(test, feature = "publisher", feature = "celld", feature = "dynamodb"))]
pub async fn publish(artifact: &Artifact, remote: &KvHandle) -> Result<PublishReport> {
    let mut report = PublishReport::default();
    // Inventory ordering is not trusted. Only the manifest is a publication marker.
    for item in artifact
        .inventory
        .iter()
        .filter(|i| i.key != artifact.receipt.manifest_key)
        .chain(
            artifact
                .inventory
                .iter()
                .filter(|i| i.key == artifact.receipt.manifest_key),
        )
    {
        let bytes = artifact
            .kv
            .get_bytes(&item.key)
            .await?
            .context("missing artifact object")?;
        match remote
            .get_bytes(&item.key)
            .await
            .context("check existing object")?
        {
            Some(old) => {
                ensure!(old == bytes, "immutable remote object conflict");
                report.identical += 1;
            }
            None => {
                remote
                    .put_bytes(&item.key, bytes.clone())
                    .await
                    .context("write absent object")?;
                let read_back = remote
                    .get_bytes(&item.key)
                    .await
                    .context("read back written object")?
                    .context("remote read-back missing")?;
                ensure!(read_back == bytes, "remote read-back mismatch");
                report.written += 1;
            }
        }
        report.objects += 1;
    }
    Ok(report)
}

pub async fn verify(artifact: &Artifact, serving: KvHandle) -> Result<Resolver> {
    let resolver = Resolver::open(serving, &artifact.receipt, index::limits(0)).await?;
    ensure!(
        resolver.verify_tree().await? == artifact.inventory.len(),
        "remote inventory mismatch"
    );
    Ok(resolver)
}
