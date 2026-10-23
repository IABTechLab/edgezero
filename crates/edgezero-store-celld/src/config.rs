//! Immutable config: there is deliberately no client in a published snapshot.

use std::collections::BTreeMap;

use edgezero_core::config_store::{
    BoundedStoreRead, ConfigStore, ConfigStoreError, ConfigValue, finish_bounded_config_read,
};
use edgezero_core::time::{Deadline, MonotonicClock};

pub(crate) struct ConfigSnapshot {
    entries: BTreeMap<String, ConfigValue>,
}

impl ConfigSnapshot {
    pub(crate) const fn new(entries: BTreeMap<String, ConfigValue>) -> Self {
        Self { entries }
    }
}

#[async_trait::async_trait(?Send)]
impl ConfigStore for ConfigSnapshot {
    async fn get(&self, key: &str) -> Result<Option<ConfigValue>, ConfigStoreError> {
        Ok(self.entries.get(key).cloned())
    }

    async fn get_bounded(
        &self,
        key: &str,
        clock: &MonotonicClock,
        deadline: Deadline,
        max_backend_bytes: u64,
        max_value_bytes: u64,
    ) -> Result<BoundedStoreRead<ConfigValue>, ConfigStoreError> {
        if deadline.is_expired_at(clock.now()) {
            return Err(ConfigStoreError::DeadlineExceeded);
        }
        let result = finish_bounded_config_read(
            Ok(self.entries.get(key).cloned()),
            clock,
            deadline,
            max_backend_bytes,
            max_value_bytes,
        );
        // Check even if the helper returned a size failure: deadline wins.
        if deadline.is_expired_at(clock.now()) {
            return Err(ConfigStoreError::DeadlineExceeded);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use edgezero_core::config_store::{ConfigStore as _, ConfigStoreError, ConfigValue};
    use edgezero_core::time::{Deadline, MonotonicClock, MonotonicInstant};

    use super::ConfigSnapshot;

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_reads_count_exact_utf8_and_independent_caps() {
        let snapshot = ConfigSnapshot::new(BTreeMap::from([(
            "k".to_owned(),
            ConfigValue::from("\u{e9}"),
        )]));
        let clock = MonotonicClock::default();
        for (backend, value) in [(1, 2), (2, 1)] {
            assert!(matches!(
                snapshot
                    .get_bounded(
                        "k",
                        &clock,
                        clock.deadline_after(Duration::from_secs(1)),
                        backend,
                        value
                    )
                    .await,
                Err(ConfigStoreError::ValueTooLarge)
            ));
        }
        let result = snapshot
            .get_bounded(
                "k",
                &clock,
                clock.deadline_after(Duration::from_secs(1)),
                2,
                2,
            )
            .await
            .unwrap();
        assert_eq!(result.backend_bytes, 2);
        assert_eq!(result.value.unwrap().as_str(), "\u{e9}");
        assert_eq!(
            snapshot
                .get_bounded(
                    "missing",
                    &clock,
                    clock.deadline_after(Duration::from_secs(1)),
                    0,
                    0
                )
                .await
                .unwrap()
                .backend_bytes,
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deadline_wins_even_after_size_failure() {
        let snapshot = ConfigSnapshot::new(BTreeMap::from([(
            "k".to_owned(),
            ConfigValue::from("bytes"),
        )]));
        let now = MonotonicInstant::now();
        let later = now.checked_add(Duration::from_secs(2)).unwrap();
        let ticks = Arc::new(AtomicUsize::new(0));
        let clock = MonotonicClock::new(move || {
            if ticks.fetch_add(1, Ordering::SeqCst) < 2 {
                now
            } else {
                later
            }
        });
        let result = snapshot
            .get_bounded(
                "k",
                &clock,
                Deadline::at_instant(now.checked_add(Duration::from_secs(1)).unwrap()),
                0,
                0,
            )
            .await;
        assert!(matches!(result, Err(ConfigStoreError::DeadlineExceeded)));
    }
}
