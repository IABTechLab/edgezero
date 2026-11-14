//! Explicit real-service proof against a disposable, loopback-only celld listener.
#![cfg(all(not(target_arch = "wasm32"), feature = "records"))]

#[cfg(test)]
mod tests {
    use std::env;
    use std::time::Duration;

    use edgezero_core::time::MonotonicClock;
    use edgezero_store_celld::CelldPreparation;
    use edgezero_store_celld::records::{
        CelldRecordStore, RecordWrite, RecordWriteMode, RecordWriteOutcome,
    };
    use edgezero_store_celld::settings::{CelldAccess, CelldKvSettings, CelldPreparationLimits};
    use tokio::time::sleep;
    use url::Url;

    fn write(body: &[u8], mode: RecordWriteMode) -> RecordWrite<'_> {
        RecordWrite {
            body,
            metadata: Some(&[0, 255]),
            ttl: Duration::from_secs(120),
            mode,
        }
    }

    async fn record_generation(store: &CelldRecordStore, key: &str) -> u64 {
        store
            .lookup(key)
            .await
            .expect("should look up record")
            .expect("should find live record")
            .generation
    }

    async fn conditional_round_trip(store: &CelldRecordStore) {
        let key = "interop:root";
        store
            .delete(key)
            .await
            .expect("should clear only fixture key");
        assert!(
            !store
                .key_exists(key)
                .await
                .expect("should establish absence")
        );
        assert_eq!(
            store
                .insert(key, write(b"first", RecordWriteMode::Add))
                .await
                .expect("should create"),
            RecordWriteOutcome::Written
        );
        let first = store
            .lookup(key)
            .await
            .expect("should read bytes")
            .expect("should find row");
        assert_eq!(first.body, b"first");
        assert_eq!(first.metadata, Some(vec![0, 255]));
        assert_eq!(
            store
                .insert(key, write(b"wrong", RecordWriteMode::Add))
                .await
                .expect("should reject collision"),
            RecordWriteOutcome::PreconditionFailed
        );
        assert_eq!(
            store
                .insert(
                    key,
                    write(
                        b"second",
                        RecordWriteMode::IfGenerationMatch(first.generation)
                    )
                )
                .await
                .expect("should apply CAS"),
            RecordWriteOutcome::Written
        );
        let second = record_generation(store, key).await;
        assert!(second > first.generation);
        store.delete(key).await.expect("should hard delete");
        assert_eq!(
            store
                .insert(key, write(b"recreated", RecordWriteMode::Add))
                .await
                .expect("should recreate"),
            RecordWriteOutcome::Written
        );
        assert!(record_generation(store, key).await > second);
        assert_eq!(
            store
                .insert(
                    key,
                    write(
                        b"stale",
                        RecordWriteMode::IfGenerationMatch(first.generation)
                    )
                )
                .await
                .expect("should reject stale generation"),
            RecordWriteOutcome::PreconditionFailed
        );
        assert!(
            store
                .list_keys_with_prefix("interop:root", 1)
                .await
                .expect("should observe completed write")
                .iter()
                .any(|entry| entry == key)
        );
        store.delete(key).await.expect("should remove fixture row");
    }

    async fn concurrent_updates(store: &CelldRecordStore) {
        let key = "interop:race";
        store.delete(key).await.expect("should clear race fixture");
        store
            .insert(key, write(b"initial", RecordWriteMode::Add))
            .await
            .expect("should seed race");
        let generation = record_generation(store, key).await;
        let (left, right) = tokio::join!(
            store.insert(
                key,
                write(b"left", RecordWriteMode::IfGenerationMatch(generation))
            ),
            store.insert(
                key,
                write(b"right", RecordWriteMode::IfGenerationMatch(generation))
            )
        );
        let outcomes = [
            left.expect("should classify left"),
            right.expect("should classify right"),
        ];
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
        store.delete(key).await.expect("should remove race fixture");
    }

    async fn expiry_and_full_size(store: &CelldRecordStore) {
        let key = "interop:expiry";
        store
            .delete(key)
            .await
            .expect("should clear expiry fixture");
        store
            .insert(
                key,
                RecordWrite {
                    ttl: Duration::from_millis(200),
                    ..write(b"expiring", RecordWriteMode::Add)
                },
            )
            .await
            .expect("should write short TTL");
        let generation = record_generation(store, key).await;
        sleep(Duration::from_millis(300)).await;
        assert!(
            store
                .lookup(key)
                .await
                .expect("should check logical expiry")
                .is_none()
        );
        assert!(
            store
                .list_keys_with_prefix(key, 1)
                .await
                .expect("should list live rows")
                .is_empty()
        );
        assert!(
            store
                .list_physical_keys_with_prefix(key, 1)
                .await
                .expect("should enumerate held expired row")
                .iter()
                .any(|entry| entry == key)
        );
        assert_eq!(
            store
                .insert(
                    key,
                    write(b"delayed", RecordWriteMode::IfGenerationMatch(generation))
                )
                .await
                .expect("should reject expired generation"),
            RecordWriteOutcome::PreconditionFailed
        );
        store
            .insert(key, write(b"new", RecordWriteMode::Add))
            .await
            .expect("should recreate after expiry");
        assert!(record_generation(store, key).await > generation);
        store
            .delete(key)
            .await
            .expect("should remove recreated row");
        let large = "interop:large";
        let body = vec![255; 0x0010_0000];
        let metadata = vec![255; 1_024];
        store
            .insert(
                large,
                RecordWrite {
                    body: &body,
                    metadata: Some(&metadata),
                    ttl: Duration::from_secs(120),
                    mode: RecordWriteMode::Overwrite,
                },
            )
            .await
            .expect("should accept full-size record envelope");
        let row = store
            .lookup(large)
            .await
            .expect("should read full envelope")
            .expect("should find full-size row");
        assert_eq!(row.body, body);
        assert_eq!(row.metadata, Some(metadata));
        store
            .delete(large)
            .await
            .expect("should remove large fixture row");
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires a disposable modified celld listener in CELLD_RECORDS_ENDPOINT"]
    async fn real_celld_conditional_records() {
        let endpoint = env::var("CELLD_RECORDS_ENDPOINT")
            .expect("should explicitly supply a disposable listener");
        let url = Url::parse(&endpoint).expect("should parse fixture endpoint");
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert!(url.port().is_some());
        let mut preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .expect("should prepare session");
        let store = preparation
            .prepare_records(
                &CelldKvSettings {
                    endpoint,
                    store: "identity".to_owned(),
                    access: CelldAccess::TrustedNetwork {
                        allow_remote_http: false,
                    },
                },
                None,
            )
            .await
            .expect("should connect to real records API");
        conditional_round_trip(&store).await;
        concurrent_updates(&store).await;
        expiry_and_full_size(&store).await;
    }
}
