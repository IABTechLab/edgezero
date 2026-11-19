//! Configuration and supplied-handle validation only. No provider I/O is needed.

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use edgezero_core::app::{App, Hooks};
    use edgezero_core::key_value_store::{KvError, KvHandle, KvPage, KvStore};
    use edgezero_core::platform::PlatformMetadata;
    use edgezero_core::router::RouterService;
    use edgezero_core::store_registry::KvRegistry;
    use edgezero_core::{
        EdgeError, RecordBinding, RecordKeyLayout, RecordLayout, RecordStoreRequirements,
    };

    struct Layout(u32);

    impl RecordKeyLayout for Layout {
        fn id(&self) -> &'static str {
            "configuration.test"
        }

        fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError> {
            key.strip_prefix("group/")
                .and_then(|suffix| key.strip_suffix(suffix))
                .ok_or_else(|| KvError::Validation("unknown group".to_owned()))
        }

        fn validate(&self) -> Result<(), KvError> {
            Ok(())
        }

        fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
            if prefix == "group/" {
                Ok(())
            } else {
                Err(KvError::Validation("unknown partition".to_owned()))
            }
        }

        fn version(&self) -> u32 {
            self.0
        }
    }

    struct Configured;

    #[expect(
        clippy::missing_trait_methods,
        reason = "configuration fixture intentionally retains unrelated Hooks defaults"
    )]
    impl Hooks for Configured {
        fn configure(app: &mut App) -> Result<(), EdgeError> {
            app.require_record_store("identity", Layout(1))?;
            Ok(())
        }

        fn routes() -> Result<RouterService, EdgeError> {
            Ok(RouterService::builder().build())
        }
    }

    struct Unconfigured;
    #[expect(
        clippy::missing_trait_methods,
        reason = "configuration fixture intentionally retains Hooks defaults"
    )]
    impl Hooks for Unconfigured {
        fn routes() -> Result<RouterService, EdgeError> {
            Ok(RouterService::builder().build())
        }
    }

    struct Prepared(Option<RecordBinding>);

    #[expect(
        clippy::missing_trait_methods,
        reason = "prepared binding fixture intentionally retains unsupported operation defaults"
    )]
    #[async_trait(?Send)]
    impl KvStore for Prepared {
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

        fn record_binding(&self) -> Option<RecordBinding> {
            self.0.clone()
        }
    }

    fn handle(version: u32) -> KvHandle {
        KvHandle::new(Arc::new(Prepared(Some(RecordBinding {
            layout: RecordLayout::new(Layout(version)).unwrap(),
            scope: "test".to_owned(),
            format: "test-v1".to_owned(),
            max_metadata_bytes: 1024,
        }))))
    }

    #[test]
    fn configure_installs_immutable_per_id_requirements_before_preparation() {
        let mut app = App::build::<Configured>(PlatformMetadata::default()).unwrap();
        let requirements = app.record_store_requirements().clone();
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements["identity"].id(), "configuration.test");
        assert_eq!(requirements["identity"].version(), 1);
        assert!(app.require_record_store("another", Layout(1)).is_err());
        assert!(app.require_record_store("identity", Layout(2)).is_err());
        KvRegistry::single_id("identity".to_owned(), handle(1))
            .validate_record_requirements(&requirements)
            .unwrap();
    }

    #[test]
    fn low_level_declarations_cannot_replace_existing_layout() {
        let mut app = App::new(RouterService::builder().build());
        assert!(app.require_record_store("", Layout(1)).is_err());
        app.require_record_store("identity", Layout(1)).unwrap();
        assert!(app.require_record_store("identity", Layout(2)).is_err());
        assert_eq!(app.record_store_requirements()["identity"].version(), 1);
    }

    #[test]
    fn defaults_preserve_ordinary_only_construction() {
        let app = App::build::<Unconfigured>(PlatformMetadata::default()).unwrap();
        assert!(app.record_store_requirements().is_empty());
        let ordinary = KvHandle::new(Arc::new(Prepared(None)));
        KvRegistry::single_id("ordinary".to_owned(), ordinary)
            .validate_record_requirements(app.record_store_requirements())
            .unwrap();
    }

    #[test]
    fn supplied_handles_must_already_have_matching_capability_and_layout() {
        let requirements = RecordStoreRequirements::from([(
            "identity".to_owned(),
            RecordLayout::new(Layout(1)).unwrap(),
        )]);
        let wrong_layout = KvRegistry::single_id("identity".to_owned(), handle(2));
        assert!(matches!(
            wrong_layout.validate_record_requirements(&requirements),
            Err(KvError::Validation(_))
        ));
        let missing_id = KvRegistry::single_id("another".to_owned(), handle(1));
        assert!(matches!(
            missing_id.validate_record_requirements(&requirements),
            Err(KvError::Validation(_))
        ));
        let ordinary = KvRegistry::single_id(
            "identity".to_owned(),
            KvHandle::new(Arc::new(Prepared(None))),
        );
        assert!(matches!(
            ordinary.validate_record_requirements(&requirements),
            Err(KvError::Unsupported { .. })
        ));
    }
}
