use super::*;
use crate::index::{Location, Range, Source};
use futures::executor::block_on;
use std::sync::Mutex;

#[derive(Default)]
struct State {
    data: HashMap<String, Bytes>,
    puts: Vec<String>,
    uncertain_after: Option<usize>,
    deny_writes: bool,
    corrupt_after_write: bool,
}
#[derive(Default)]
struct Remote(Mutex<State>);
#[async_trait(?Send)]
impl KvStore for Remote {
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        Ok(self.0.lock().unwrap().data.get(key).cloned())
    }
    async fn put_bytes(&self, key: &str, bytes: Bytes) -> Result<(), KvError> {
        let mut state = self.0.lock().unwrap();
        assert!(
            !state.deny_writes,
            "serving verification must issue no writes"
        );
        state.puts.push(key.into());
        let corrupt = state.corrupt_after_write;
        state.data.insert(
            key.into(),
            if corrupt {
                Bytes::from_static(b"bad read-back")
            } else {
                bytes
            },
        );
        if state.uncertain_after == Some(state.puts.len()) {
            return Err(KvError::Validation(
                "uncertain write, may have committed".into(),
            ));
        }
        Ok(())
    }
    async fn delete(&self, _: &str) -> Result<(), KvError> {
        panic!("must not delete")
    }
    async fn list_keys_page(&self, _: &str, _: Option<&str>, _: usize) -> Result<KvPage, KvError> {
        panic!("must not list")
    }
    async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
        panic!("must not use TTL")
    }
}

async fn fixture() -> (tempfile::TempDir, Artifact) {
    let dir = tempfile::tempdir().unwrap();
    let local = KvHandle::new(Arc::new(ArtifactStore::new(dir.path())));
    let ranges: Vec<_> = (0..300)
        .map(|i| Range {
            start: i * 4,
            end: i * 4 + 1,
            location: Location::new("US", "CA").unwrap(),
        })
        .collect();
    let (receipt, _, mut inventory) = index::build(
        &local,
        Source {
            edition: "synthetic".into(),
            files: vec![],
        },
        &ranges,
        &ranges,
        1024,
    )
    .await
    .unwrap();
    // Deliberately put the manifest first, as publication must not trust input order.
    inventory.reverse();
    fs::write(
        dir.path().join("receipt.json"),
        serde_json::to_vec(&receipt).unwrap(),
    )
    .unwrap();
    fs::write(
        dir.path().join("inventory.json"),
        serde_json::to_vec(&inventory).unwrap(),
    )
    .unwrap();
    let artifact = Artifact::open(dir.path()).await.unwrap();
    (dir, artifact)
}

#[test]
fn manifest_last_identical_resume_and_serving_read_only() {
    block_on(async {
        let (_dir, artifact) = fixture().await;
        let raw = Arc::new(Remote::default());
        let remote = KvHandle::new(raw.clone());
        raw.0.lock().unwrap().uncertain_after = Some(2);
        assert!(publish(&artifact, &remote).await.is_err());
        assert!(
            !raw.0
                .lock()
                .unwrap()
                .data
                .contains_key(&artifact.receipt.manifest_key)
        );
        raw.0.lock().unwrap().uncertain_after = None;
        let report = publish(&artifact, &remote).await.unwrap();
        assert_eq!(report.identical, 2);
        assert_eq!(
            raw.0.lock().unwrap().puts.last(),
            Some(&artifact.receipt.manifest_key)
        );
        let before = raw.0.lock().unwrap().puts.len();
        let report = publish(&artifact, &remote).await.unwrap();
        assert_eq!(report.written, 0);
        assert_eq!(report.identical, artifact.inventory.len());
        assert_eq!(before, raw.0.lock().unwrap().puts.len());
        raw.0.lock().unwrap().deny_writes = true;
        let resolver = verify(&artifact, remote).await.unwrap();
        assert_eq!(
            resolver.lookup("0.0.0.1".parse().unwrap()).await.unwrap(),
            Some(Location::new("US", "CA").unwrap())
        );
        assert_eq!(
            resolver.lookup("0.0.0.2".parse().unwrap()).await.unwrap(),
            None
        );
    });
}

#[test]
fn resume_after_every_write_including_manifest() {
    block_on(async {
        let (_dir, artifact) = fixture().await;
        for stop in 1..=artifact.inventory.len() {
            let raw = Arc::new(Remote::default());
            let remote = KvHandle::new(raw.clone());
            raw.0.lock().unwrap().uncertain_after = Some(stop);
            assert!(publish(&artifact, &remote).await.is_err());
            raw.0.lock().unwrap().uncertain_after = None;
            publish(&artifact, &remote).await.unwrap();
            verify(&artifact, remote).await.unwrap();
            assert_eq!(raw.0.lock().unwrap().puts.len(), artifact.inventory.len());
        }
    });
}

#[test]
fn remote_conflict_and_bad_readback_do_not_publish_manifest() {
    block_on(async {
        let (_dir, artifact) = fixture().await;
        for corrupt_readback in [false, true] {
            let raw = Arc::new(Remote::default());
            let remote = KvHandle::new(raw.clone());
            {
                let mut state = raw.0.lock().unwrap();
                state.corrupt_after_write = corrupt_readback;
                if !corrupt_readback {
                    let key = &artifact
                        .inventory
                        .iter()
                        .find(|i| i.key != artifact.receipt.manifest_key)
                        .unwrap()
                        .key;
                    state
                        .data
                        .insert(key.clone(), Bytes::from_static(b"conflict"));
                }
            }
            assert!(publish(&artifact, &remote).await.is_err());
            assert!(
                !raw.0
                    .lock()
                    .unwrap()
                    .data
                    .contains_key(&artifact.receipt.manifest_key)
            );
        }
    });
}

#[test]
fn serving_verification_detects_incomplete_visibility_and_corruption() {
    block_on(async {
        let (_dir, artifact) = fixture().await;
        let raw = Arc::new(Remote::default());
        let remote = KvHandle::new(raw.clone());
        publish(&artifact, &remote).await.unwrap();
        raw.0.lock().unwrap().deny_writes = true;
        let key = &artifact
            .inventory
            .iter()
            .find(|i| i.key != artifact.receipt.manifest_key)
            .unwrap()
            .key;
        let bytes = raw.0.lock().unwrap().data.remove(key).unwrap();
        assert!(verify(&artifact, remote.clone()).await.is_err());
        raw.0
            .lock()
            .unwrap()
            .data
            .insert(key.clone(), Bytes::from_static(b"corrupt"));
        assert!(verify(&artifact, remote.clone()).await.is_err());
        raw.0.lock().unwrap().data.insert(key.clone(), bytes);
        verify(&artifact, remote).await.unwrap();
    });
}

#[test]
fn artifact_rejects_tamper_duplicates_omissions_extras_and_cross_generation() {
    block_on(async {
        for mode in [
            "tamper",
            "duplicate",
            "omit",
            "extra",
            "cross-generation",
            "pin",
        ] {
            let (dir, artifact) = fixture().await;
            let mut items: Vec<InventoryEntry> =
                serde_json::from_slice(&fs::read(dir.path().join("inventory.json")).unwrap())
                    .unwrap();
            let item = items
                .iter()
                .find(|i| i.key != artifact.receipt.manifest_key)
                .unwrap();
            match mode {
                "tamper" => fs::write(dir.path().join(&item.key), b"bad").unwrap(),
                "duplicate" => items.push(InventoryEntry {
                    key: item.key.clone(),
                    bytes: item.bytes,
                    sha256: item.sha256.clone(),
                }),
                "omit" => {
                    items.remove(1);
                }
                "extra" => items.push(InventoryEntry {
                    key: item.key.replace(&item.sha256, &"0".repeat(64)),
                    sha256: "0".repeat(64),
                    bytes: 1,
                }),
                "cross-generation" => {
                    items[1].key = items[1].key.replacen("geo/v1/", "geo/v1/wrong/", 1)
                }
                "pin" => {
                    let mut receipt = artifact.receipt;
                    receipt.manifest_hash[0] ^= 1;
                    fs::write(
                        dir.path().join("receipt.json"),
                        serde_json::to_vec(&receipt).unwrap(),
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            fs::write(
                dir.path().join("inventory.json"),
                serde_json::to_vec(&items).unwrap(),
            )
            .unwrap();
            assert!(Artifact::open(dir.path()).await.is_err(), "accepted {mode}");
        }
    });
}
