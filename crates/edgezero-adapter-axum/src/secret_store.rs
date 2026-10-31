//! Environment variable secret store for local development.
//!
//! Reads secret `key` from the environment variable `EDGEZERO__SECRETS__`
//! followed by `key` in ASCII uppercase. Set secrets before starting the dev
//! server:
//!
//! ```bash
//! EDGEZERO__SECRETS__API_KEY=mysecret edgezero serve --adapter axum
//! ```

use std::env;

use async_trait::async_trait;
use bytes::Bytes;
use edgezero_core::secret_store::{SecretError, SecretStore};
use edgezero_core::{BoundedStoreRead, Deadline, MonotonicClock};

use crate::secret_env::secret_env_var;

/// Secret store for local development that reads secrets from environment variables.
///
/// When `[stores.secrets]` is declared in `edgezero.toml`, the dev server
/// creates an `EnvSecretStore` that reads secret `key` from
/// `EDGEZERO__SECRETS__<KEY>` (see [`secret_env_var`]), e.g.
/// `EDGEZERO__SECRETS__API_KEY=mysecret edgezero serve --adapter axum`.
pub struct EnvSecretStore;

impl EnvSecretStore {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self
    }
}

impl Default for EnvSecretStore {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl SecretStore for EnvSecretStore {
    #[inline]
    async fn get_bytes(&self, _store_name: &str, key: &str) -> Result<Option<Bytes>, SecretError> {
        let var = secret_env_var(key).map_err(SecretError::Validation)?;

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt as _;

            match env::var_os(&var) {
                Some(value) => Ok(Some(Bytes::from(value.into_vec()))),
                None => Ok(None),
            }
        }

        #[cfg(not(unix))]
        {
            use std::env::VarError;

            match env::var(&var) {
                Ok(value) => Ok(Some(Bytes::from(value.into_bytes()))),
                Err(VarError::NotPresent) => Ok(None),
                Err(VarError::NotUnicode(_)) => Err(SecretError::Internal(anyhow::anyhow!(
                    "secret store returned an invalid Unicode value"
                ))),
            }
        }
    }

    #[inline]
    async fn get_bytes_bounded(
        &self,
        _store_name: &str,
        key: &str,
        clock: &MonotonicClock,
        deadline: Deadline,
        max_backend_bytes: u64,
        max_value_bytes: u64,
    ) -> Result<BoundedStoreRead<Bytes>, SecretError> {
        if deadline.is_expired_at(clock.now()) {
            return Err(SecretError::DeadlineExceeded);
        }

        let var = secret_env_var(key).map_err(SecretError::Validation)?;

        #[cfg(unix)]
        let stored_value = env::var_os(&var);

        #[cfg(not(unix))]
        let stored_value = env::var(&var);

        if deadline.is_expired_at(clock.now()) {
            return Err(SecretError::DeadlineExceeded);
        }

        #[cfg(not(unix))]
        let stored_value = match stored_value {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(SecretError::Internal(anyhow::anyhow!(
                    "secret store returned an invalid Unicode value"
                )));
            }
        };

        #[cfg(unix)]
        let backend_bytes = {
            use std::os::unix::ffi::OsStrExt as _;

            stored_value.as_ref().map_or(Ok(0_u64), |value| {
                u64::try_from(value.as_os_str().as_bytes().len())
                    .map_err(|_length_error| SecretError::ValueTooLarge)
            })?
        };

        #[cfg(not(unix))]
        let backend_bytes = stored_value.as_ref().map_or(Ok(0_u64), |value| {
            u64::try_from(value.len()).map_err(|_length_error| SecretError::ValueTooLarge)
        })?;

        if backend_bytes > max_backend_bytes || backend_bytes > max_value_bytes {
            return Err(SecretError::ValueTooLarge);
        }

        #[cfg(unix)]
        let value = {
            use std::os::unix::ffi::OsStringExt as _;

            stored_value.map(|value| Bytes::from(value.into_vec()))
        };

        #[cfg(not(unix))]
        let value = stored_value.map(|value| Bytes::from(value.into_bytes()));

        if deadline.is_expired_at(clock.now()) {
            return Err(SecretError::DeadlineExceeded);
        }

        Ok(BoundedStoreRead {
            backend_bytes,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    // Contract tests: use InMemorySecretStoreProvider since EnvSecretStore needs
    // real env vars, which are unsafe in parallel tests.
    // The EnvSecretStore is tested individually above.
    secret_store_contract_tests!(env_secret_contract, {
        InMemorySecretStore::new([
            ("mystore/contract_key", Bytes::from("contract_value")),
            ("mystore/contract_key_2", Bytes::from("another_value")),
        ])
    });

    use super::*;
    use crate::test_utils::env_guard;
    use bytes::Bytes;
    use edgezero_core::secret_store::{InMemorySecretStore, SecretHandle};
    use edgezero_core::secret_store_contract_tests;
    use edgezero_core::test_env::EnvOverride;
    use edgezero_core::{Deadline, MonotonicClock, MonotonicInstant};
    use futures::executor::block_on;
    #[cfg(unix)]
    use std::ffi::OsString;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_get_bytes_accepts_exact_caps_and_reports_exact_backend_bytes() {
        let _guard = env_guard().lock().await;
        let _env = EnvOverride::set(
            "EDGEZERO__SECRETS____EDGEZERO_TEST_BOUNDED_SECRET__",
            "hello",
        );
        let handle = SecretHandle::new(Arc::new(EnvSecretStore::new()));
        let clock = MonotonicClock::default();

        let read = handle
            .get_bytes_bounded(
                "env",
                "__EDGEZERO_TEST_BOUNDED_SECRET__",
                &clock,
                Deadline::after(Duration::from_secs(1)),
                5,
                5,
            )
            .await
            .expect("exact cap must succeed");

        assert_eq!(read.backend_bytes, 5);
        assert_eq!(read.value, Some(Bytes::from_static(b"hello")));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_get_bytes_rejects_either_cap() {
        let _guard = env_guard().lock().await;
        let _env = EnvOverride::set(
            "EDGEZERO__SECRETS____EDGEZERO_TEST_OVERSIZE_SECRET__",
            "hello",
        );
        let handle = SecretHandle::new(Arc::new(EnvSecretStore::new()));
        let clock = MonotonicClock::default();

        for (max_backend_bytes, max_value_bytes) in [(4, 5), (5, 4)] {
            let error = handle
                .get_bytes_bounded(
                    "env",
                    "__EDGEZERO_TEST_OVERSIZE_SECRET__",
                    &clock,
                    Deadline::after(Duration::from_secs(1)),
                    max_backend_bytes,
                    max_value_bytes,
                )
                .await
                .expect_err("over-cap secret must fail");

            assert!(matches!(error, SecretError::ValueTooLarge));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_get_bytes_rejects_an_expired_deadline() {
        let _guard = env_guard().lock().await;
        let _env = EnvOverride::set(
            "EDGEZERO__SECRETS____EDGEZERO_TEST_EXPIRED_SECRET__",
            "hello",
        );
        let handle = SecretHandle::new(Arc::new(EnvSecretStore::new()));
        let expired = Deadline::at_instant(MonotonicInstant::now());
        let clock = MonotonicClock::default();

        let error = handle
            .get_bytes_bounded(
                "env",
                "__EDGEZERO_TEST_EXPIRED_SECRET__",
                &clock,
                expired,
                5,
                5,
            )
            .await
            .expect_err("expired deadline must fail");

        assert!(matches!(error, SecretError::DeadlineExceeded));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_get_bytes_preserves_handle_name_validation() {
        let handle = SecretHandle::new(Arc::new(EnvSecretStore::new()));
        let clock = MonotonicClock::default();

        let error = handle
            .get_bytes_bounded(
                "",
                "key",
                &clock,
                Deadline::after(Duration::from_secs(1)),
                5,
                5,
            )
            .await
            .expect_err("empty store name must be rejected");

        assert!(matches!(error, SecretError::Validation(_)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_secret_read_uses_injected_clock() {
        let _guard = env_guard().lock().await;
        let _env = EnvOverride::set(
            "EDGEZERO__SECRETS____EDGEZERO_TEST_CLOCKED_SECRET__",
            "hello",
        );
        let handle = SecretHandle::new(Arc::new(EnvSecretStore::new()));
        let process_now = MonotonicInstant::now();
        let injected_now = process_now
            .checked_sub(Duration::from_mins(1))
            .expect("injected instant");
        let clock = MonotonicClock::new(move || injected_now);
        let deadline = Deadline::at_instant(
            injected_now
                .checked_add(Duration::from_secs(1))
                .expect("deadline instant"),
        );

        let read = handle
            .get_bytes_bounded(
                "env",
                "__EDGEZERO_TEST_CLOCKED_SECRET__",
                &clock,
                deadline,
                5,
                5,
            )
            .await
            .expect("the application clock is still before its deadline");

        assert_eq!(read.value, Some(Bytes::from_static(b"hello")));
    }

    fn read(key: &str) -> Result<Option<Bytes>, SecretError> {
        block_on(EnvSecretStore::new().get_bytes("env", key))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_and_unbounded_reads_share_prefixed_secret_mapping() {
        let _guard = env_guard().lock().await;
        let _bare = EnvOverride::set("EDGEZERO_TEST_BOUNDED_MAPPING", "bare");
        let prefixed_guard = EnvOverride::set(
            "EDGEZERO__SECRETS__EDGEZERO_TEST_BOUNDED_MAPPING",
            "prefixed",
        );
        let store = EnvSecretStore::new();
        let clock = MonotonicClock::default();
        let deadline = clock.deadline_after(Duration::from_secs(1));
        let bounded = store
            .get_bytes_bounded(
                "env",
                "edgezero_test_bounded_mapping",
                &clock,
                deadline,
                8,
                8,
            )
            .await
            .unwrap();
        assert_eq!(bounded.value, Some(Bytes::from_static(b"prefixed")));
        assert_eq!(bounded.backend_bytes, 8);
        assert_eq!(
            bounded.value,
            store
                .get_bytes("env", "edgezero_test_bounded_mapping")
                .await
                .unwrap()
        );

        drop(prefixed_guard);
        let _missing = EnvOverride::remove("EDGEZERO__SECRETS__EDGEZERO_TEST_BOUNDED_MAPPING");
        let missing = store
            .get_bytes_bounded(
                "env",
                "EDGEZERO_TEST_BOUNDED_MAPPING",
                &clock,
                deadline,
                8,
                8,
            )
            .await
            .unwrap();
        assert!(missing.value.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn get_bytes_preserves_non_utf8_secret_values() {
        use std::os::unix::ffi::OsStringExt as _;

        let _guard = block_on(env_guard().lock());
        let _env = EnvOverride::set(
            "EDGEZERO__SECRETS__EDGEZERO_TEST_BINARY_SECRET",
            OsString::from_vec(vec![0xff, 0x61]),
        );
        let result = read("EDGEZERO_TEST_BINARY_SECRET").unwrap();
        assert_eq!(result, Some(Bytes::from_static(&[0xff, 0x61])));
    }

    #[test]
    fn get_bytes_returns_none_when_var_not_set() {
        let _guard = block_on(env_guard().lock());
        let _env = EnvOverride::remove("EDGEZERO__SECRETS__EDGEZERO_TEST_MISSING_SECRET");
        assert!(read("EDGEZERO_TEST_MISSING_SECRET").unwrap().is_none());
    }

    #[test]
    fn get_bytes_returns_value_when_var_set() {
        let _guard = block_on(env_guard().lock());
        let _env = EnvOverride::set("EDGEZERO__SECRETS__EDGEZERO_TEST_SECRET", "test_value_123");
        let result = read("EDGEZERO_TEST_SECRET").unwrap();
        assert_eq!(result, Some(Bytes::from("test_value_123")));
    }

    #[test]
    fn get_bytes_reads_uppercased_prefixed_var_for_lowercase_key() {
        let _guard = block_on(env_guard().lock());
        let _env = EnvOverride::set(
            "EDGEZERO__SECRETS__EDGEZERO_TEST_LOWER_SECRET",
            "lower_value",
        );
        let result = read("edgezero_test_lower_secret").unwrap();
        assert_eq!(result, Some(Bytes::from("lower_value")));
    }

    #[test]
    fn get_bytes_does_not_read_var_named_exactly_like_key() {
        let _guard = block_on(env_guard().lock());
        let _bare = EnvOverride::set("EDGEZERO_TEST_BARE_SECRET", "bare_value");
        let _prefixed = EnvOverride::remove("EDGEZERO__SECRETS__EDGEZERO_TEST_BARE_SECRET");
        assert!(read("EDGEZERO_TEST_BARE_SECRET").unwrap().is_none());
    }

    #[test]
    fn get_bytes_rejects_key_that_cannot_name_a_var() {
        let err = read("api-token").unwrap_err();
        assert!(
            matches!(&err, SecretError::Validation(msg) if msg.contains("api-token")),
            "{err:?}"
        );
    }
}
