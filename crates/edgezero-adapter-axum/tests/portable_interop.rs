//! Real services only, ignored by default. Run `scripts/run_portable_record_contract.mjs`.
//! `EDGEZERO_PORTABLE_RECORDS_BINDINGS` names the CLI-pinned TOML document containing
//! `dynamodb_records` and `celld_records`. It supplies endpoints, table, namespace,
//! store alias and pinned table/instance identities. `AWS_ENDPOINT_URL_DYNAMODB` must
//! equal the document endpoint; `AWS_REGION=us-east-1` and `AWS_ACCESS_KEY_ID` /
//! `AWS_SECRET_ACCESS_KEY` supply fake Local credentials. `KV__CELLD_RECORDS__CELLD_TOKEN`
//! supplies the authenticated celld token. `SSL_CERT_FILE` names the TLS test CA.
//! Both native reqwest and `AxumOutboundClient` use reqwest's rustls platform verifier,
//! which loads rustls-native-certs on Linux and honors `SSL_CERT_FILE` at client creation.
//! The rejection case runs in a separate process with an unrelated `SSL_CERT_FILE`.
//! No verifier is disabled. The native seed test deliberately leaves `native/seed`
//! for the Viceroy suite to read and delete. All other writes use unique prefixes.

#![cfg(all(not(target_arch = "wasm32"), feature = "axum"))]
#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep service setup, shared assertions and ignored entry points together"
)]

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::env;
    use std::fs;
    use std::process;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use edgezero_adapter_axum::outbound::AxumOutboundClient;
    use edgezero_adapter_axum::outbound_executor::AxumOutboundExecutor;
    use edgezero_core::key_value_store::{KvError, KvHandle};
    use edgezero_core::outbound::HttpClient;
    use edgezero_core::record_store::{
        RecordKeyLayout, RecordLayout, RecordWrite, RecordWriteMode, RecordWriteOutcome,
    };
    use edgezero_core::secret_store::{InMemorySecretStore, SecretHandle};
    use edgezero_core::store_registry::BoundSecretStore;
    use edgezero_core::time::MonotonicClock;
    use edgezero_store_aws::AwsPreparation;
    use edgezero_store_aws::portable_dynamodb::PreparedDynamoDbRecords;
    use edgezero_store_aws::settings::{
        AwsPreparationLimits, DynamoDbKvLimits, DynamoDbRecordsSettings,
    };
    use edgezero_store_celld::CelldPreparation;
    use edgezero_store_celld::portable::PreparedCelldRecords;
    use edgezero_store_celld::settings::{
        CelldAccess, CelldAccessToken, CelldKvSettings, CelldPreparationLimits,
        CelldRecordsSettings,
    };
    use serde::de::DeserializeOwned;

    struct Groups;
    impl RecordKeyLayout for Groups {
        fn id(&self) -> &'static str {
            "fastly-record-contract"
        }
        fn version(&self) -> u32 {
            1
        }
        fn validate(&self) -> Result<(), KvError> {
            Ok(())
        }
        fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError> {
            key.find('/')
                .filter(|index| *index > 0 && index.saturating_add(1) < key.len())
                .and_then(|index| key.get(..=index))
                .ok_or_else(|| KvError::Validation("expected group/remainder".to_owned()))
        }
        fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
            if prefix.len() > 1 && prefix.ends_with('/') && prefix.matches('/').count() == 1 {
                Ok(())
            } else {
                Err(KvError::Validation("expected group/".to_owned()))
            }
        }
    }

    fn layout() -> RecordLayout {
        RecordLayout::new(Groups).unwrap()
    }

    fn binding<T>(id: &str) -> T
    where
        T: DeserializeOwned,
    {
        let path =
            env::var("EDGEZERO_PORTABLE_RECORDS_BINDINGS").expect("pinned bindings required");
        let document: toml::Value = toml::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        let mut entry = document["kv"][id].clone();
        entry.as_table_mut().unwrap().remove("provider");
        entry
            .try_into()
            .unwrap_or_else(|_| panic!("invalid {id} binding"))
    }

    fn secrets() -> BoundSecretStore {
        let entries = [
            (
                "KV__DYNAMODB_RECORDS__AWS_ACCESS_KEY_ID",
                "AWS_ACCESS_KEY_ID",
            ),
            (
                "KV__DYNAMODB_RECORDS__AWS_SECRET_ACCESS_KEY",
                "AWS_SECRET_ACCESS_KEY",
            ),
            (
                "KV__CELLD_RECORDS__CELLD_TOKEN",
                "KV__CELLD_RECORDS__CELLD_TOKEN",
            ),
        ]
        .map(|(key, variable)| {
            (
                format!("stores/{key}"),
                env::var(variable).expect("fixture credential required"),
            )
        });
        BoundSecretStore::new(
            SecretHandle::new(Arc::new(InMemorySecretStore::new(entries))),
            "stores".to_owned(),
        )
    }

    fn client(executor: &AxumOutboundExecutor) -> HttpClient {
        HttpClient::with_client(AxumOutboundClient::try_new(executor.handle()).unwrap())
    }

    async fn dynamodb(executor: &AxumOutboundExecutor) -> (KvHandle, KvHandle) {
        let settings: DynamoDbRecordsSettings = binding("dynamodb_records");
        assert_eq!(
            env::var("AWS_ENDPOINT_URL_DYNAMODB").unwrap(),
            settings.endpoint.as_ref().unwrap().as_str()
        );
        let mut preparation =
            AwsPreparation::new(MonotonicClock::default(), AwsPreparationLimits::default())
                .unwrap();
        let native = preparation
            .prepare_dynamodb_records_pinned(
                &settings.kv_settings(DynamoDbKvLimits::default()),
                &layout(),
                settings.pinned.as_ref(),
            )
            .await
            .unwrap();
        let portable =
            PreparedDynamoDbRecords::new("dynamodb_records", &settings, &layout(), secrets())
                .unwrap();
        (
            native,
            KvHandle::new(Arc::new(portable.bind(client(executor)))),
        )
    }

    fn celld_settings() -> (CelldRecordsSettings, CelldKvSettings, CelldAccessToken) {
        let settings: CelldRecordsSettings = binding("celld_records");
        assert!(settings.endpoint.starts_with("https://127.0.0.1:"));
        let native = CelldKvSettings {
            endpoint: settings.endpoint.clone(),
            store: settings.store.clone(),
            access: CelldAccess::Authenticated {
                token_env: "KV__CELLD_RECORDS__CELLD_TOKEN".to_owned(),
            },
        };
        let token =
            CelldAccessToken::new(env::var("KV__CELLD_RECORDS__CELLD_TOKEN").unwrap()).unwrap();
        (settings, native, token)
    }

    async fn celld(executor: &AxumOutboundExecutor) -> (KvHandle, KvHandle) {
        let (settings, native_settings, token) = celld_settings();
        let mut preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        let native = preparation
            .prepare_record_store_pinned(
                &native_settings,
                &layout(),
                Some(&token),
                settings.pinned.as_ref(),
            )
            .await
            .unwrap();
        let portable =
            PreparedCelldRecords::new("celld_records", &settings, &layout(), secrets()).unwrap();
        (
            native,
            KvHandle::new(Arc::new(portable.bind(client(executor)))),
        )
    }

    fn write(body: &[u8], mode: RecordWriteMode) -> RecordWrite<'_> {
        RecordWrite {
            body,
            metadata: Some("interop m\u{e9}tadata"),
            ttl: None,
            mode,
        }
    }

    async fn interop(native: &KvHandle, portable: &KvHandle, scan: bool) {
        let prefix = format!(
            "interop-{}-{}/",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let keys: Vec<_> = ["native", "portable", "third"]
            .map(|suffix| format!("{prefix}{suffix}"))
            .into();
        let first = native.require_records().unwrap();
        let second = portable.require_records().unwrap();
        round_trip(native, portable, &keys[0], b"native\0\xff").await;
        round_trip(portable, native, &keys[1], b"portable\0\xff").await;
        assert_eq!(
            first
                .insert(&keys[2], write(b"third", RecordWriteMode::Add))
                .await
                .unwrap(),
            RecordWriteOutcome::Written
        );
        cursors(native, portable, &prefix, &keys, scan).await;
        for key in &keys {
            first.delete(key).await.unwrap();
            assert!(second.lookup(key).await.unwrap().is_none());
        }
    }

    async fn round_trip(writer_kv: &KvHandle, reader_kv: &KvHandle, key: &str, body: &[u8]) {
        let writer = writer_kv.require_records().unwrap();
        let reader = reader_kv.require_records().unwrap();
        assert_eq!(
            writer
                .insert(key, write(body, RecordWriteMode::Add))
                .await
                .unwrap(),
            RecordWriteOutcome::Written
        );
        let row = reader.lookup(key).await.unwrap().unwrap();
        assert_eq!(row.body.as_ref(), body);
        assert_eq!(row.metadata.as_deref(), Some("interop m\u{e9}tadata"));
        assert_eq!(
            reader.lookup(key).await.unwrap().unwrap().generation,
            writer.lookup(key).await.unwrap().unwrap().generation
        );
        assert_eq!(
            reader
                .insert(
                    key,
                    write(b"cas", RecordWriteMode::IfGenerationMatch(row.generation))
                )
                .await
                .unwrap(),
            RecordWriteOutcome::Written
        );
        assert_eq!(
            writer
                .insert(
                    key,
                    write(b"stale", RecordWriteMode::IfGenerationMatch(row.generation))
                )
                .await
                .unwrap(),
            RecordWriteOutcome::PreconditionFailed
        );
        assert_eq!(
            writer.lookup(key).await.unwrap().unwrap().body.as_ref(),
            b"cas"
        );
        assert_eq!(
            writer_kv.get_bytes(key).await.unwrap().unwrap().as_ref(),
            b"cas"
        );
        assert_eq!(
            reader_kv.get_bytes(key).await.unwrap().unwrap().as_ref(),
            b"cas"
        );
    }

    async fn cursors(
        native: &KvHandle,
        portable: &KvHandle,
        prefix: &str,
        keys: &[String],
        scan: bool,
    ) {
        let first = native.require_records().unwrap();
        let second = portable.require_records().unwrap();
        // Begin in either transport, continue in the other, including allocator-only pages.
        for (start, continuation) in [(&first, &second), (&second, &first)] {
            let partition = start.partition(prefix).unwrap();
            let page = start.list_partition(&partition, None, 1).await.unwrap();
            assert!(page.cursor.is_some());
            let mut found: BTreeSet<_> = page.keys.into_iter().collect();
            let mut cursor = page.cursor;
            let mut seen = BTreeSet::new();
            while let Some(after) = cursor {
                assert!(seen.insert(after.clone()), "repeated partition cursor");
                let next = continuation
                    .list_partition(&partition, Some(&after), 1)
                    .await
                    .unwrap();
                found.extend(next.keys);
                cursor = next.cursor;
            }
            assert_eq!(found, keys.iter().cloned().collect());
        }
        if scan {
            for (start, continuation) in [(native, portable), (portable, native)] {
                let page = start.list_keys_page(prefix, None, 1).await.unwrap();
                assert!(page.cursor.is_some());
                let mut found: BTreeSet<_> = page.keys.into_iter().collect();
                let mut cursor = page.cursor;
                let mut seen = BTreeSet::new();
                while let Some(after) = cursor {
                    assert!(seen.insert(after.clone()), "repeated Scan cursor");
                    let next = continuation
                        .list_keys_page(prefix, Some(&after), 1)
                        .await
                        .unwrap();
                    found.extend(next.keys);
                    cursor = next.cursor;
                }
                assert_eq!(found, keys.iter().cloned().collect());
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires pinned bindings and real DynamoDB Local; run portable orchestrator"]
    async fn portable_dynamodb_real_service_interop() {
        let mut executor = AxumOutboundExecutor::new();
        let (native, portable) = dynamodb(&executor).await;
        executor.run_until(interop(&native, &portable, true)).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires pinned bindings and authenticated HTTPS stock celld; run portable orchestrator"]
    async fn portable_celld_real_service_interop() {
        let mut executor = AxumOutboundExecutor::new();
        let (native, portable) = celld(&executor).await;
        executor.run_until(interop(&native, &portable, false)).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires an unrelated SSL_CERT_FILE and real celld TLS endpoint"]
    async fn portable_celld_rejects_untrusted_certificate() {
        let mut executor = AxumOutboundExecutor::new();
        let (settings, native_settings, token) = celld_settings();
        let mut preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        preparation
            .prepare_record_store_pinned(
                &native_settings,
                &layout(),
                Some(&token),
                settings.pinned.as_ref(),
            )
            .await
            .unwrap_err();
        let portable =
            PreparedCelldRecords::new("celld_records", &settings, &layout(), secrets()).unwrap();
        let store = KvHandle::new(Arc::new(portable.bind(client(&executor))));
        executor
            .run_until(async {
                assert!(matches!(
                    store.require_records().unwrap().lookup("native/seed").await,
                    Err(KvError::Unavailable)
                ));
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "writes native/seed for the portable Viceroy suite to consume and delete"]
    async fn portable_native_seed_rows() {
        let executor = AxumOutboundExecutor::new();
        for (native, _) in [dynamodb(&executor).await, celld(&executor).await] {
            assert_eq!(
                native
                    .require_records()
                    .unwrap()
                    .insert(
                        "native/seed",
                        RecordWrite {
                            body: b"native-seed",
                            metadata: Some("native"),
                            ttl: None,
                            mode: RecordWriteMode::Add,
                        }
                    )
                    .await
                    .unwrap(),
                RecordWriteOutcome::Written
            );
        }
    }
}
