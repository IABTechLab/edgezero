//! Cloudflare Workers KV adapter.
//!
//! Wraps `worker::kv::KvStore` to implement the `edgezero_core::key_value_store::KvStore` trait.
//!
//! # Note
//!
//! This module is only compiled when the `cloudflare` feature is enabled
//! and the target is `wasm32`.

use async_trait::async_trait;
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvPage, KvStore, KvStoreOpenFailure};
use edgezero_core::logging::BOOT_LOG_TARGET;
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;
use worker::js_sys::Reflect;
use worker::kv::{KvError as WorkerKvError, KvStore as WorkerKvStore};
use worker::wasm_bindgen::JsValue;

/// KV store backed by Cloudflare Workers KV.
///
/// Opens compiled declarations lazily per request, or wraps an explicitly opened handle.
pub struct CloudflareKvStore {
    source: CloudflareKvStoreSource,
}

enum CloudflareKvStoreSource {
    Declared {
        binding: String,
        env: worker::Env,
        id: &'static str,
        opened: OnceLock<Result<WorkerKvStore, KvStoreOpenFailure>>,
    },
    Opened(WorkerKvStore),
}

impl CloudflareKvStore {
    pub(crate) fn declared(env: worker::Env, id: &'static str, binding: String) -> Self {
        Self {
            source: CloudflareKvStoreSource::Declared {
                binding,
                env,
                id,
                opened: OnceLock::new(),
            },
        }
    }

    /// Create a new Cloudflare KV store from the environment binding name.
    ///
    /// The `binding` must match a KV namespace binding in `wrangler.toml`.
    /// Validates the namespace's callable methods before constructing the SDK binding.
    ///
    /// # Errors
    /// Returns [`KvError::OpenFailed`] with a payload-free open reason.
    #[inline]
    pub fn from_env(env: &worker::Env, binding: &str) -> Result<Self, KvError> {
        let store = open_store(env, binding).map_err(|reason| KvError::OpenFailed { reason })?;
        Ok(Self {
            source: CloudflareKvStoreSource::Opened(store),
        })
    }

    fn store(&self) -> Result<&WorkerKvStore, KvError> {
        match &self.source {
            CloudflareKvStoreSource::Opened(store) => Ok(store),
            CloudflareKvStoreSource::Declared {
                binding,
                env,
                id,
                opened,
            } => opened
                .get_or_init(|| {
                    open_store(env, binding)
                        .inspect_err(|reason| warn_declared_open_once(id, *reason))
                })
                .as_ref()
                .map_err(|reason| KvError::OpenFailed { reason: *reason }),
        }
    }
}

#[async_trait(?Send)]
impl KvStore for CloudflareKvStore {
    #[inline]
    async fn delete(&self, key: &str) -> Result<(), KvError> {
        self.store()?
            .delete(key)
            .await
            .map_err(|err| KvError::Internal(anyhow::anyhow!("delete failed: {err}")))
    }

    #[inline]
    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        Ok(self.get_bytes(key).await?.is_some())
    }

    #[inline]
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        let result = self
            .store()?
            .get(key)
            .bytes()
            .await
            .map_err(|err| KvError::Internal(anyhow::anyhow!("get failed: {err}")))?;
        Ok(result.map(Bytes::from))
    }

    #[inline]
    async fn list_keys_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        let limit_u64 = u64::try_from(limit)
            .map_err(|err| KvError::Validation(format!("list limit exceeds u64: {err}")))?;
        let mut request = self.store()?.list().limit(limit_u64);

        if !prefix.is_empty() {
            request = request.prefix(prefix.to_owned());
        }
        if let Some(cursor_str) = cursor.filter(|value| !value.is_empty()) {
            request = request.cursor(cursor_str.to_owned());
        }

        let response = request
            .execute()
            .await
            .map_err(|err| KvError::Internal(anyhow::anyhow!("list execute failed: {err}")))?;

        Ok(KvPage {
            keys: response.keys.into_iter().map(|key| key.name).collect(),
            cursor: (!response.list_complete)
                .then_some(response.cursor)
                .flatten()
                .filter(|value| !value.is_empty()),
        })
    }

    #[inline]
    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        self.store()?
            .put_bytes(key, value.as_ref())
            .map_err(|err| KvError::Internal(anyhow::anyhow!("put failed: {err}")))?
            .execute()
            .await
            .map_err(|err| KvError::Internal(anyhow::anyhow!("put execute failed: {err}")))
    }

    #[inline]
    async fn put_bytes_with_ttl(
        &self,
        key: &str,
        value: Bytes,
        ttl: Duration,
    ) -> Result<(), KvError> {
        // `KvHandle::validate_ttl` enforces a minimum of 60s, so sub-second
        // truncation via `as_secs()` cannot produce a zero TTL here.
        let ttl_secs = ttl.as_secs();

        self.store()?
            .put_bytes(key, value.as_ref())
            .map_err(|err| KvError::Internal(anyhow::anyhow!("put failed: {err}")))?
            .expiration_ttl(ttl_secs)
            .execute()
            .await
            .map_err(|err| KvError::Internal(anyhow::anyhow!("put with ttl execute failed: {err}")))
    }
}

fn open_store(env: &worker::Env, binding: &str) -> Result<WorkerKvStore, KvStoreOpenFailure> {
    let namespace = Reflect::get(env.as_ref(), &JsValue::from_str(binding))
        .map_err(|_error| KvStoreOpenFailure::Unavailable)?;
    if namespace.is_undefined() {
        return Err(KvStoreOpenFailure::MissingBinding);
    }
    if namespace.is_null() || !namespace.is_object() {
        return Err(KvStoreOpenFailure::InvalidConfiguration);
    }
    // The SDK casts these methods unchecked; reject malformed bindings before use.
    for method in ["get", "getWithMetadata", "put", "list", "delete"] {
        let value = Reflect::get(&namespace, &JsValue::from_str(method))
            .map_err(|_error| KvStoreOpenFailure::Unavailable)?;
        if !value.is_function() {
            return Err(KvStoreOpenFailure::InvalidConfiguration);
        }
    }
    WorkerKvStore::from_this(env.as_ref(), binding).map_err(|error| match error {
        WorkerKvError::InvalidKvStore(_) => KvStoreOpenFailure::MissingBinding,
        WorkerKvError::Serialization(_) => KvStoreOpenFailure::InvalidConfiguration,
        WorkerKvError::JavaScript(_) => KvStoreOpenFailure::Unavailable,
    })
}

fn warn_declared_open_once(id: &'static str, reason: KvStoreOpenFailure) {
    // Membership is bounded by compiled metadata IDs, never runtime-selected bindings.
    static WARNED: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let first = WARNED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(id);
    if first {
        log::warn!(target: BOOT_LOG_TARGET, "declared Cloudflare KV store '{id}' failed to open: {reason}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;
    use worker::js_sys::{Function, Object, Reflect};
    use worker::wasm_bindgen::{JsCast as _, JsValue};

    fn bind(env: &worker::Env, value: &JsValue) {
        Reflect::set(env.as_ref(), &JsValue::from_str("physical"), value).expect("binding");
    }

    fn namespace() -> Object {
        let object = Object::new();
        for method in ["get", "getWithMetadata", "put", "list", "delete"] {
            Reflect::set(
                &object,
                &JsValue::from_str(method),
                &Function::new_no_args(""),
            )
            .expect("method");
        }
        object
    }

    #[wasm_bindgen_test]
    async fn declared_kv_open_failure_retains_binding_and_recovers() {
        let env: worker::Env = Object::new().unchecked_into();
        let store = CloudflareKvStore::declared(env.clone(), "declared", "physical".to_owned());
        assert!(
            matches!(&store.source, CloudflareKvStoreSource::Declared { opened, .. } if opened.get().is_none())
        );
        let failures = [
            store.get_bytes("key").await.err(),
            store.exists("key").await.err(),
            store.put_bytes("key", Bytes::new()).await.err(),
            store
                .put_bytes_with_ttl("key", Bytes::new(), Duration::from_secs(60))
                .await
                .err(),
            store.delete("key").await.err(),
            store.list_keys_page("", None, 1).await.err(),
        ];
        for error in failures {
            assert!(matches!(
                error,
                Some(KvError::OpenFailed {
                    reason: KvStoreOpenFailure::MissingBinding
                })
            ));
        }
        bind(&env, &namespace());
        assert!(
            store.store().is_err(),
            "request-local failure stays settled"
        );
        let restored = CloudflareKvStore::declared(env, "declared", "physical".to_owned());
        assert!(restored.store().is_ok(), "next request retries the binding");
    }

    #[wasm_bindgen_test]
    fn malformed_and_throwing_bindings_have_payload_free_reasons() {
        let env: worker::Env = Object::new().unchecked_into();
        for value in [
            JsValue::NULL,
            JsValue::from_str("provider-token"),
            Object::new().into(),
        ] {
            bind(&env, &value);
            assert!(matches!(
                CloudflareKvStore::from_env(&env, "physical"),
                Err(KvError::OpenFailed {
                    reason: KvStoreOpenFailure::InvalidConfiguration
                })
            ));
        }
        let getter = Object::new();
        Reflect::set(
            &getter,
            &JsValue::from_str("get"),
            &Function::new_no_args("throw new Error('provider-token')"),
        )
        .expect("getter");
        Object::define_property(
            env.unchecked_ref::<Object>(),
            &JsValue::from_str("physical"),
            &getter,
        );
        let error = CloudflareKvStore::from_env(&env, "physical")
            .err()
            .expect("throwing binding");
        assert!(matches!(
            error,
            KvError::OpenFailed {
                reason: KvStoreOpenFailure::Unavailable
            }
        ));
        assert!(!error.to_string().contains("provider-token"));
    }
}
