//! Public API and boundary tests. Scripted providers here are not native-provider
//! conformance evidence for atomicity, generation lifetime, storage or TTL.

#[cfg(test)]
mod tests {
    #![expect(
        clippy::assertions_on_result_states,
        reason = "boundary rejection matrices check error presence; classification is asserted separately"
    )]

    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use edgezero_core::key_value_store::{KvError, KvHandle, KvPage, KvStore};
    use edgezero_core::{
        Record, RecordBinding, RecordGeneration, RecordKeyLayout, RecordLayout, RecordPage,
        RecordPartition, RecordWrite, RecordWriteMode, RecordWriteOutcome,
    };
    use futures::executor::block_on;

    struct Groups(u32);

    impl RecordKeyLayout for Groups {
        fn id(&self) -> &'static str {
            "test.groups"
        }

        fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError> {
            let (group, suffix) = key
                .split_once('/')
                .ok_or_else(|| KvError::Validation("missing group".to_owned()))?;
            if group.is_empty() || suffix.is_empty() {
                return Err(KvError::Validation("missing group/key".to_owned()));
            }
            key.strip_suffix(suffix)
                .ok_or_else(|| KvError::Validation("missing group prefix".to_owned()))
        }

        fn validate(&self) -> Result<(), KvError> {
            Ok(())
        }

        fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
            if prefix.len() > 1 && prefix.ends_with('/') && prefix.matches('/').count() == 1 {
                Ok(())
            } else {
                Err(KvError::Validation("incomplete group".to_owned()))
            }
        }

        fn version(&self) -> u32 {
            self.0
        }
    }

    struct Ordinary;

    #[expect(
        clippy::missing_trait_methods,
        reason = "ordinary provider intentionally uses unsupported record defaults"
    )]
    #[async_trait(?Send)]
    impl KvStore for Ordinary {
        async fn delete(&self, _: &str) -> Result<(), KvError> {
            panic!("unexpected I/O")
        }

        async fn get_bytes(&self, _: &str) -> Result<Option<Bytes>, KvError> {
            panic!("unexpected I/O")
        }

        async fn list_keys_page(
            &self,
            _: &str,
            _: Option<&str>,
            _: usize,
        ) -> Result<KvPage, KvError> {
            panic!("unexpected I/O")
        }

        async fn put_bytes(&self, _: &str, _: Bytes) -> Result<(), KvError> {
            panic!("unexpected I/O")
        }

        async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
            panic!("unexpected I/O")
        }
    }

    struct Scripted {
        binding: RecordBinding,
        calls: AtomicUsize,
        cursors: Mutex<Vec<Option<String>>>,
        outcome: RecordWriteOutcome,
        pages: Mutex<VecDeque<RecordPage>>,
        uncertain: bool,
        writes: Mutex<Vec<Option<String>>>,
    }

    impl Scripted {
        fn handle(self) -> (Arc<Self>, KvHandle) {
            let store = Arc::new(self);
            let handle = KvHandle::new(Arc::<Self>::clone(&store));
            (store, handle)
        }

        fn new() -> Self {
            Self {
                binding: RecordBinding {
                    layout: RecordLayout::new(Groups(1)).unwrap(),
                    scope: "test-resource-sensitive".to_owned(),
                    format: "test-v1".to_owned(),
                    max_metadata_bytes: 4,
                },
                calls: AtomicUsize::new(0),
                cursors: Mutex::new(Vec::new()),
                outcome: RecordWriteOutcome::Written,
                pages: Mutex::new(VecDeque::new()),
                uncertain: false,
                writes: Mutex::new(Vec::new()),
            }
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "ordinary existence default is unrelated to scripted record operations"
    )]
    #[async_trait(?Send)]
    impl KvStore for Scripted {
        async fn delete(&self, _: &str) -> Result<(), KvError> {
            panic!("unexpected ordinary I/O")
        }

        async fn get_bytes(&self, _: &str) -> Result<Option<Bytes>, KvError> {
            panic!("unexpected ordinary I/O")
        }

        async fn list_keys_page(
            &self,
            _: &str,
            _: Option<&str>,
            _: usize,
        ) -> Result<KvPage, KvError> {
            panic!("unexpected ordinary I/O")
        }

        async fn put_bytes(&self, _: &str, _: Bytes) -> Result<(), KvError> {
            panic!("unexpected ordinary I/O")
        }

        async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
            panic!("unexpected ordinary I/O")
        }

        fn record_binding(&self) -> Option<RecordBinding> {
            Some(self.binding.clone())
        }

        async fn record_delete(&self, _: &str, _: &RecordPartition) -> Result<(), KvError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.uncertain {
                Err(KvError::MutationUncertain)
            } else {
                Ok(())
            }
        }

        async fn record_exists_strong(
            &self,
            _: &str,
            _: &RecordPartition,
        ) -> Result<bool, KvError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }

        async fn record_insert(
            &self,
            _: &str,
            partition: &RecordPartition,
            write: RecordWrite<'_>,
        ) -> Result<RecordWriteOutcome, KvError> {
            assert_eq!(partition.prefix(), "a/");
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.writes
                .lock()
                .unwrap()
                .push(write.metadata.map(str::to_owned));
            if self.uncertain {
                Err(KvError::MutationUncertain)
            } else {
                Ok(self.outcome)
            }
        }

        async fn record_list_partition(
            &self,
            _: &RecordPartition,
            cursor: Option<&str>,
            _: usize,
        ) -> Result<RecordPage, KvError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.cursors.lock().unwrap().push(cursor.map(str::to_owned));
            Ok(self
                .pages
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted page"))
        }

        async fn record_lookup(
            &self,
            _: &str,
            partition: &RecordPartition,
        ) -> Result<Option<Record>, KvError> {
            assert_eq!(partition.prefix(), "a/");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(Record {
                body: Bytes::from_static(b"\x00\xff"),
                metadata: Some(String::new()),
                generation: RecordGeneration::from_u64(u64::MAX),
            }))
        }
    }

    fn write(metadata: Option<&str>, mode: RecordWriteMode) -> RecordWrite<'_> {
        RecordWrite {
            body: b"\x00\xff",
            metadata,
            ttl: None,
            mode,
        }
    }

    #[test]
    fn ordinary_only_acquisition_and_defaults_are_unsupported_without_io() {
        let store: Arc<dyn KvStore> = Arc::new(Ordinary);
        assert!(matches!(
            KvHandle::new(Arc::clone(&store)).require_records(),
            Err(KvError::Unsupported { .. })
        ));
        let partition = RecordLayout::new(Groups(1))
            .unwrap()
            .resolve("a/k")
            .unwrap();
        block_on(async {
            assert!(matches!(
                store.record_lookup("a/k", &partition).await,
                Err(KvError::Unsupported { .. })
            ));
            assert!(matches!(
                store
                    .record_insert("a/k", &partition, write(None, RecordWriteMode::Add))
                    .await,
                Err(KvError::Unsupported { .. })
            ));
            assert!(matches!(
                store.record_exists_strong("a/k", &partition).await,
                Err(KvError::Unsupported { .. })
            ));
            assert!(matches!(
                store.record_delete("a/k", &partition).await,
                Err(KvError::Unsupported { .. })
            ));
            assert!(matches!(
                store.record_list_partition(&partition, None, 1).await,
                Err(KvError::Unsupported { .. })
            ));
        });
    }

    #[test]
    fn acquisition_is_pure_and_debug_does_not_expose_binding_scope() {
        let (store, kv) = Scripted::new().handle();
        let records = kv.require_records().unwrap();
        let _again = kv.clone().require_records().unwrap();
        let partition = records.partition("a/").unwrap();
        assert_eq!(store.calls.load(Ordering::SeqCst), 0);
        for debug in [
            format!("{records:?}"),
            format!("{:?}", records.binding()),
            format!("{partition:?}"),
        ] {
            assert!(!debug.contains("sensitive"));
            assert!(!debug.contains("a/"));
        }
    }

    #[test]
    fn generation_preserves_all_u64_bits_and_only_promises_equality() {
        let token = RecordGeneration::from_u64(u64::MAX);
        assert_eq!(token.as_u64(), u64::MAX);
        assert_eq!(token, RecordGeneration::from_u64(u64::MAX));
        assert_ne!(token, RecordGeneration::from_u64(0));
    }

    #[test]
    fn layout_requires_complete_partition_and_preserves_full_key() {
        let layout = RecordLayout::new(Groups(1)).unwrap();
        assert_eq!(
            layout.resolve("\u{3b1}/\u{00e9}.x").unwrap().prefix(),
            "\u{3b1}/"
        );
        assert!(layout.partition("").is_err());
        assert!(layout.partition("\u{3b1}").is_err());
        assert!(layout.partition("\u{3b1}/nested/").is_err());
        assert!(layout.resolve("ungrouped").is_err());
        assert!(layout.resolve("/empty").is_err());
        assert!(layout.resolve("a/\n").is_err());
    }

    #[test]
    fn invalid_binding_is_not_unsupported_or_availability() {
        let mut provider = Scripted::new();
        provider.binding.scope.clear();
        assert!(matches!(
            provider.handle().1.require_records(),
            Err(KvError::Validation(_))
        ));
    }

    #[test]
    fn input_validation_precedes_io_and_metadata_counts_utf8_bytes() {
        let (store, kv) = Scripted::new().handle();
        let records = kv.require_records().unwrap();
        block_on(async {
            assert!(
                records
                    .insert("ungrouped", write(None, RecordWriteMode::Add))
                    .await
                    .is_err()
            );
            assert!(
                records
                    .insert(
                        "a/k",
                        write(Some("\u{3b1}\u{3b1}\u{3b1}"), RecordWriteMode::Add)
                    )
                    .await
                    .is_err()
            );
            let mut invalid_ttl = write(None, RecordWriteMode::Add);
            invalid_ttl.ttl = Some(Duration::from_secs(1));
            assert!(records.insert("a/k", invalid_ttl).await.is_err());
            assert!(
                records
                    .list_partition(&records.partition("a/").unwrap(), None, 0)
                    .await
                    .is_err()
            );
            assert_eq!(store.calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                records
                    .insert("a/k", write(Some("\u{3b1}\u{3b1}"), RecordWriteMode::Add))
                    .await
                    .unwrap(),
                RecordWriteOutcome::Written
            );
        });
        assert_eq!(
            store.writes.lock().unwrap().as_slice(),
            &[Some("\u{3b1}\u{3b1}".to_owned())]
        );
    }

    #[test]
    fn empty_metadata_normalizes_and_lookup_keeps_binary_body_and_generation() {
        let (store, kv) = Scripted::new().handle();
        let records = kv.require_records().unwrap();
        block_on(async {
            records
                .insert("a/k", write(Some(""), RecordWriteMode::Overwrite))
                .await
                .unwrap();
            records
                .insert("a/k", write(None, RecordWriteMode::Overwrite))
                .await
                .unwrap();
            let record = records.lookup("a/k").await.unwrap().unwrap();
            assert_eq!(record.body.as_ref(), b"\x00\xff");
            assert_eq!(record.metadata, None);
            assert_eq!(record.generation.as_u64(), u64::MAX);
            assert!(records.exists_strong("a/k").await.unwrap());
        });
        assert_eq!(store.writes.lock().unwrap().as_slice(), &[None, None]);
    }

    #[test]
    fn definitive_preconditions_are_not_unconditional_failures_or_uncertainty() {
        let mut provider = Scripted::new();
        provider.outcome = RecordWriteOutcome::PreconditionFailed;
        let (_, kv) = provider.handle();
        let records = kv.require_records().unwrap();
        block_on(async {
            for mode in [
                RecordWriteMode::Add,
                RecordWriteMode::IfGenerationMatch(RecordGeneration::from_u64(5)),
            ] {
                assert_eq!(
                    records.insert("a/k", write(None, mode)).await.unwrap(),
                    RecordWriteOutcome::PreconditionFailed
                );
            }
            assert!(matches!(
                records
                    .insert("a/k", write(None, RecordWriteMode::Overwrite))
                    .await,
                Err(KvError::Internal(_))
            ));
        });
    }

    #[test]
    fn ambiguous_insert_and_delete_are_never_replayed() {
        let mut provider = Scripted::new();
        provider.uncertain = true;
        let (store, kv) = provider.handle();
        let records = kv.require_records().unwrap();
        block_on(async {
            assert!(matches!(
                records
                    .insert("a/k", write(None, RecordWriteMode::Add))
                    .await,
                Err(KvError::MutationUncertain)
            ));
            assert_eq!(store.calls.load(Ordering::SeqCst), 1);
            assert!(matches!(
                records.delete("a/k").await,
                Err(KvError::MutationUncertain)
            ));
            assert_eq!(store.calls.load(Ordering::SeqCst), 2);
        });
        assert_eq!(
            KvError::MutationUncertain.to_string(),
            "kv mutation may have committed"
        );
    }

    #[test]
    fn empty_page_continues_and_provider_receives_only_native_cursor() {
        let provider = Scripted::new();
        provider.pages.lock().unwrap().extend([
            RecordPage {
                keys: vec![],
                cursor: Some("native-next".to_owned()),
            },
            RecordPage {
                keys: vec!["a/one".to_owned()],
                cursor: None,
            },
        ]);
        let (store, kv) = provider.handle();
        let records = kv.require_records().unwrap();
        let partition = records.partition("a/").unwrap();
        block_on(async {
            let first = records.list_partition(&partition, None, 1).await.unwrap();
            assert!(first.keys.is_empty());
            assert!(first.cursor.is_some());
            let second = records
                .list_partition(&partition, first.cursor.as_deref(), 1)
                .await
                .unwrap();
            assert_eq!(second.keys, ["a/one"]);
            assert!(second.cursor.is_none());
        });
        assert_eq!(
            store.cursors.lock().unwrap().as_slice(),
            &[None, Some("native-next".to_owned())]
        );
    }

    #[test]
    fn foreign_cursor_scope_layout_partition_and_operation_are_rejected_before_io() {
        let provider = Scripted::new();
        provider.pages.lock().unwrap().push_back(RecordPage {
            keys: vec![],
            cursor: Some("native".to_owned()),
        });
        let (store, kv) = provider.handle();
        let records = kv.require_records().unwrap();
        let partition = records.partition("a/").unwrap();
        block_on(async {
            let cursor = records
                .list_partition(&partition, None, 1)
                .await
                .unwrap()
                .cursor
                .unwrap();
            for (field, value) in [
                ("scope", serde_json::json!("another-scope")),
                ("format", serde_json::json!("another-format")),
                ("layout_id", serde_json::json!("another-layout")),
                ("layout_version", serde_json::json!(2_u32)),
                ("partition", serde_json::json!("b/")),
                ("operation", serde_json::json!("scan")),
                ("cursor", serde_json::json!("")),
            ] {
                let mut foreign: serde_json::Value = serde_json::from_str(&cursor).unwrap();
                foreign[field] = value;
                assert!(matches!(
                    records
                        .list_partition(&partition, Some(&foreign.to_string()), 1)
                        .await,
                    Err(KvError::Validation(_))
                ));
            }
            assert!(
                records
                    .list_partition(&partition, Some("not a cursor"), 1)
                    .await
                    .is_err()
            );
            assert!(
                records
                    .list_partition(&partition, Some(r#"{"cursor":"x","prefix":"a/"}"#), 1)
                    .await
                    .is_err()
            );
            let foreign_partition = RecordLayout::new(Groups(2))
                .unwrap()
                .partition("a/")
                .unwrap();
            assert!(
                records
                    .list_partition(&foreign_partition, None, 1)
                    .await
                    .is_err()
            );
        });
        assert_eq!(store.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn invalid_provider_pages_and_nonadvancing_continuation_are_errors() {
        for keys in [
            vec!["b/foreign".to_owned()],
            vec!["a/one".to_owned(), "a/two".to_owned()],
            vec!["ungrouped".to_owned()],
        ] {
            let provider = Scripted::new();
            provider
                .pages
                .lock()
                .unwrap()
                .push_back(RecordPage { keys, cursor: None });
            let (_, kv) = provider.handle();
            let records = kv.require_records().unwrap();
            block_on(async {
                assert!(matches!(
                    records
                        .list_partition(&records.partition("a/").unwrap(), None, 1)
                        .await,
                    Err(KvError::Internal(_))
                ));
            });
        }
        let provider = Scripted::new();
        provider.pages.lock().unwrap().extend([
            RecordPage {
                keys: vec![],
                cursor: Some("same".to_owned()),
            },
            RecordPage {
                keys: vec![],
                cursor: Some("same".to_owned()),
            },
        ]);
        let (_, kv) = provider.handle();
        let records = kv.require_records().unwrap();
        let partition = records.partition("a/").unwrap();
        block_on(async {
            let first = records.list_partition(&partition, None, 1).await.unwrap();
            assert!(matches!(
                records
                    .list_partition(&partition, first.cursor.as_deref(), 1)
                    .await,
                Err(KvError::Internal(_))
            ));
        });
    }
}
