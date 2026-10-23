//! Live byte-oriented KV. Each method performs at most one bounded exchange.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "group the live store and its operations before private wire models and cursor helpers"
)]

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvPage, KvStore};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use url::Url;

use crate::PreparationError;
use crate::client::{ResponseKind, ScopedClient};
use crate::settings::{CelldPreparationLimits, valid_key};

const MAX_PROVIDER_CURSOR: usize = 8_192;
const MAX_SERVICE_CURSOR: usize = 2_048;

pub(crate) struct CelldKvStore {
    client: Arc<ScopedClient>,
    endpoint: Url,
    limits: CelldPreparationLimits,
    store: String,
}

impl CelldKvStore {
    pub(crate) const fn new(
        client: Arc<ScopedClient>,
        endpoint: Url,
        store: String,
        limits: CelldPreparationLimits,
    ) -> Self {
        Self {
            client,
            endpoint,
            limits,
            store,
        }
    }

    fn value_url(&self, key: &str) -> Result<Url, PreparationError> {
        if !valid_key(key, false) {
            return Err(PreparationError::InvalidResource);
        }
        let mut url = self.client.url(&self.store, "value")?;
        url.query_pairs_mut().append_pair("key", key);
        Ok(url)
    }

    #[expect(
        clippy::big_endian_bytes,
        reason = "cursor fingerprints frame each field with a canonical big-endian u64 length"
    )]
    fn fingerprint(&self, prefix: &str) -> Result<String, PreparationError> {
        let mut digest = Sha256::new();
        for part in [
            "celld-store-v1",
            self.endpoint.as_str(),
            &self.store,
            prefix,
        ] {
            let size = u64::try_from(part.len()).map_err(|_error| PreparationError::SizeLimit)?;
            digest.update(size.to_be_bytes());
            digest.update(part.as_bytes());
        }
        Ok(URL_SAFE_NO_PAD.encode(digest.finalize()))
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), PreparationError> {
        let deadline = self.client.deadline()?;
        let mut url = self.value_url(key)?;
        if u64::try_from(value.len()).map_err(|_error| PreparationError::SizeLimit)?
            > self.limits.max_value_bytes
        {
            return Err(PreparationError::SizeLimit);
        }
        if let Some(duration) = ttl {
            url.query_pairs_mut()
                .append_pair("ttl_seconds", &ttl_seconds(duration)?.to_string());
        }
        self.client
            .execute(
                Method::PUT,
                url,
                Some(value),
                0,
                deadline,
                ResponseKind::Mutation,
            )
            .await?;
        Ok(())
    }

    async fn page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, PreparationError> {
        let deadline = self.client.deadline()?;
        if !valid_key(prefix, true) || !(1..=1_000).contains(&limit) {
            return Err(PreparationError::InvalidResource);
        }
        let fingerprint = self.fingerprint(prefix)?;
        let prior = cursor
            .map(|encoded| decode_cursor(encoded, &fingerprint, prefix))
            .transpose()?;
        let mut url = self.client.url(&self.store, "keys")?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("prefix", prefix)
                .append_pair("limit", &limit.to_string());
            if let Some(previous) = &prior {
                query.append_pair("cursor", &previous.service);
            }
        }
        let cap = usize::try_from(self.limits.max_page_bytes)
            .map_err(|_error| PreparationError::InvalidLimits)?;
        let body = self
            .client
            .execute(Method::GET, url, None, cap, deadline, ResponseKind::Page)
            .await?
            .ok_or(PreparationError::Malformed)?;
        let result = (|| {
            let page: WirePage =
                serde_json::from_slice(&body).map_err(|_error| PreparationError::Malformed)?;
            if page.keys.len() > limit {
                return Err(PreparationError::SizeLimit);
            }
            let mut last = prior.as_ref().and_then(|previous| previous.last.as_deref());
            for key in &page.keys {
                if !valid_key(key, false)
                    || !key.starts_with(prefix)
                    || last.is_some_and(|previous_key| key.as_str() <= previous_key)
                {
                    return Err(PreparationError::Malformed);
                }
                last = Some(key);
            }
            let next = if let Some(service) = page.cursor {
                if service.is_empty()
                    || service.len() > MAX_SERVICE_CURSOR
                    || prior
                        .as_ref()
                        .is_some_and(|previous| service == previous.service)
                {
                    return Err(PreparationError::Malformed);
                }
                let envelope = Cursor {
                    version: 1,
                    fingerprint,
                    last: last.map(str::to_owned),
                    service,
                };
                let bytes =
                    serde_json::to_vec(&envelope).map_err(|_error| PreparationError::Malformed)?;
                let encoded = URL_SAFE_NO_PAD.encode(bytes);
                if encoded.len() > MAX_PROVIDER_CURSOR {
                    return Err(PreparationError::SizeLimit);
                }
                Some(encoded)
            } else {
                None
            };
            Ok(KvPage {
                keys: page.keys,
                cursor: next,
            })
        })();
        // Includes parsing/validation time and gives deadline precedence.
        self.client.check(deadline)?;
        result
    }
}

#[async_trait::async_trait(?Send)]
impl KvStore for CelldKvStore {
    async fn delete(&self, key: &str) -> Result<(), KvError> {
        let deadline = self.client.deadline().map_err(kv_error)?;
        let url = self.value_url(key).map_err(kv_error)?;
        self.client
            .execute(
                Method::DELETE,
                url,
                None,
                0,
                deadline,
                ResponseKind::Mutation,
            )
            .await
            .map_err(kv_error)?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        let deadline = self.client.deadline().map_err(kv_error)?;
        let url = self.value_url(key).map_err(kv_error)?;
        self.client
            .execute(Method::HEAD, url, None, 0, deadline, ResponseKind::Exists)
            .await
            .map(|body| body.is_some())
            .map_err(kv_error)
    }

    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        let deadline = self.client.deadline().map_err(kv_error)?;
        let url = self.value_url(key).map_err(kv_error)?;
        let cap = usize::try_from(self.limits.max_value_bytes)
            .map_err(|_error| kv_error(PreparationError::InvalidLimits))?;
        self.client
            .execute(Method::GET, url, None, cap, deadline, ResponseKind::Value)
            .await
            .map(|body| body.map(Bytes::from))
            .map_err(kv_error)
    }

    async fn list_keys_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        self.page(prefix, cursor, limit).await.map_err(kv_error)
    }

    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        self.put(key, value, None).await.map_err(kv_error)
    }

    async fn put_bytes_with_ttl(
        &self,
        key: &str,
        value: Bytes,
        ttl: Duration,
    ) -> Result<(), KvError> {
        self.put(key, value, Some(ttl)).await.map_err(kv_error)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePage {
    keys: Vec<String>,
    #[serde(deserialize_with = "optional_string")]
    cursor: Option<String>,
}

fn optional_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    fingerprint: String,
    last: Option<String>,
    service: String,
}

fn decode_cursor(text: &str, fingerprint: &str, prefix: &str) -> Result<Cursor, PreparationError> {
    if text.is_empty() || text.len() > MAX_PROVIDER_CURSOR {
        return Err(PreparationError::InvalidResource);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_error| PreparationError::InvalidResource)?;
    let cursor: Cursor =
        serde_json::from_slice(&bytes).map_err(|_error| PreparationError::InvalidResource)?;
    if cursor.version != 1
        || cursor.fingerprint != fingerprint
        || cursor.service.is_empty()
        || cursor.service.len() > MAX_SERVICE_CURSOR
        || cursor
            .last
            .as_deref()
            .is_some_and(|last| !valid_key(last, false) || !last.starts_with(prefix))
    {
        return Err(PreparationError::InvalidResource);
    }
    Ok(cursor)
}

fn ttl_seconds(ttl: Duration) -> Result<u64, PreparationError> {
    // Validate the direct trait input BEFORE rounding. A sub-minimum or
    // maximum-plus-one-nanosecond duration must not become a valid wire TTL.
    if ttl < Duration::from_mins(1) || ttl > Duration::from_hours(8_760) {
        return Err(PreparationError::InvalidResource);
    }
    ttl.as_secs()
        .checked_add(u64::from(ttl.subsec_nanos() != 0))
        .ok_or(PreparationError::InvalidResource)
}

fn kv_error(error: PreparationError) -> KvError {
    match error {
        PreparationError::InvalidBinding
        | PreparationError::InvalidEndpoint
        | PreparationError::InvalidLimits
        | PreparationError::InvalidResource => KvError::Validation(error.to_string()),
        PreparationError::SizeLimit => KvError::LimitExceeded {
            message: "celld size limit".to_owned(),
        },
        // Core has no typed auth/protocol/deadline variants. Do not smuggle
        // remote JSON or transport source chains into its generic error variants.
        PreparationError::Credentials
        | PreparationError::Unauthenticated
        | PreparationError::Forbidden
        | PreparationError::Missing
        | PreparationError::Malformed
        | PreparationError::UnsupportedVersion
        | PreparationError::Unavailable
        | PreparationError::Deadline => KvError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use edgezero_core::key_value_store::KvStore as _;
    use edgezero_core::time::{MonotonicClock, MonotonicInstant};
    use url::Url;

    use super::{CelldKvStore, ttl_seconds};
    use crate::CelldPreparation;
    use crate::client::ScopedClient;
    use crate::settings::{CelldAccess, CelldKvSettings, CelldPreparationLimits};
    use crate::test_support::{Fixture, Reply};

    fn store(fixture: &Fixture, alias: &str) -> CelldKvStore {
        let endpoint = Url::parse(fixture.endpoint()).unwrap();
        let limits = CelldPreparationLimits::default();
        let client = Arc::new(
            ScopedClient::new(endpoint.clone(), None, MonotonicClock::default(), limits).unwrap(),
        );
        CelldKvStore::new(client, endpoint, alias.to_owned(), limits)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_setup_is_probe_free_and_runtime_deadlines_are_fresh() {
        let fixture = Fixture::new(vec![]).await;
        let now = MonotonicInstant::now();
        let elapsed = Arc::new(AtomicU64::new(0));
        let ticks = Arc::clone(&elapsed);
        let clock = MonotonicClock::new(move || {
            now.checked_add(Duration::from_millis(ticks.load(Ordering::SeqCst)))
                .unwrap()
        });
        let settings = CelldKvSettings {
            endpoint: fixture.endpoint().to_owned(),
            access: CelldAccess::TrustedNetwork {
                allow_remote_http: false,
            },
            store: "state".to_owned(),
        };
        let mut preparation =
            CelldPreparation::new(clock, CelldPreparationLimits::default()).unwrap();
        let kv = preparation.prepare_kv(&settings, None).await.unwrap();
        assert_eq!(fixture.count().await, 0);
        assert_eq!(preparation.stats().mappings, 1);
        assert_eq!(preparation.stats().unique_retained_payload_bytes, 0);
        elapsed.store(60_000, Ordering::SeqCst);
        kv.put_bytes("k", Bytes::from_static(&[0, 255, 7]))
            .await
            .unwrap();
        assert_eq!(
            kv.get_bytes("k").await.unwrap().unwrap(),
            Bytes::from_static(&[0, 255, 7])
        );
        assert!(kv.exists("k").await.unwrap());
        kv.put_bytes("k", Bytes::new()).await.unwrap();
        assert_eq!(kv.get_bytes("k").await.unwrap(), Some(Bytes::new()));
        kv.delete("k").await.unwrap();
        kv.delete("k").await.unwrap();
        assert!(!kv.exists("k").await.unwrap());
        assert_eq!(kv.get_bytes("k").await.unwrap(), None);
        assert_eq!(preparation.stats().unique_retained_payload_bytes, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aliases_isolate_and_explicit_sharing_preserves_exact_unicode_keys() {
        let fixture = Fixture::new(vec![]).await;
        let first = store(&fixture, "first");
        let shared = store(&fixture, "first");
        let second = store(&fixture, "second");
        for key in ["\u{e9}", "e\u{301}", &"\u{e9}".repeat(256)] {
            first
                .put_bytes(key, Bytes::copy_from_slice(key.as_bytes()))
                .await
                .unwrap();
            assert_eq!(
                shared.get_bytes(key).await.unwrap().unwrap().as_ref(),
                key.as_bytes()
            );
            assert_eq!(second.get_bytes(key).await.unwrap(), None);
        }
        let count = fixture.count().await;
        first
            .get_bytes(&"x".repeat(513))
            .await
            .expect_err("overlong key must be rejected before transport");
        first
            .get_bytes("control\n")
            .await
            .expect_err("control character in key must be rejected before transport");
        assert_eq!(fixture.count().await, count);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pagination_is_one_page_and_cursors_bind_origin_store_prefix_and_progress() {
        let fixture = Fixture::new(vec![]).await;
        for key in ["app/a", "app/b", "app/c", "other"] {
            fixture.seed("state", key, b"v").await;
        }
        let kv = store(&fixture, "state");
        let page = kv.list_keys_page("app/", None, 1).await.unwrap();
        assert_eq!(page.keys, ["app/a"]);
        let cursor = page.cursor.unwrap();
        let next = kv.list_keys_page("app/", Some(&cursor), 1).await.unwrap();
        assert_eq!(next.keys, ["app/b"]);
        assert_eq!(fixture.count().await, 2);
        let other_fixture = Fixture::new(vec![]).await;
        store(&other_fixture, "state")
            .list_keys_page("app/", Some(&cursor), 1)
            .await
            .expect_err("cursor from another origin must be rejected");
        assert_eq!(other_fixture.count().await, 0);
        store(&fixture, "other")
            .list_keys_page("app/", Some(&cursor), 1)
            .await
            .expect_err("cursor from another store must be rejected");
        kv.list_keys_page("different", Some(&cursor), 1)
            .await
            .expect_err("cursor from another prefix must be rejected");
        kv.list_keys_page("app/", Some(&"x".repeat(8_193)), 1)
            .await
            .expect_err("overlong provider cursor must be rejected");
        assert_eq!(fixture.count().await, 2);
        let last = kv
            .list_keys_page("app/", next.cursor.as_deref(), 1)
            .await
            .unwrap();
        assert_eq!(last.keys, ["app/c"]);
        assert_eq!(last.cursor, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn empty_changed_continuation_is_returned_once_and_retains_last_key() {
        let fixture = Fixture::new(vec![
            Reply::response(
                200,
                "application/json",
                br#"{"keys":["p/a"],"cursor":"one"}"#,
            ),
            Reply::response(200, "application/json", br#"{"keys":[],"cursor":"two"}"#),
            Reply::response(
                200,
                "application/json",
                br#"{"keys":["p/a"],"cursor":null}"#,
            ),
        ])
        .await;
        let kv = store(&fixture, "state");
        let first = kv.list_keys_page("p/", None, 10).await.unwrap();
        let empty = kv
            .list_keys_page("p/", first.cursor.as_deref(), 10)
            .await
            .unwrap();
        assert!(empty.keys.is_empty());
        assert!(empty.cursor.is_some());
        assert_eq!(fixture.count().await, 2);
        kv.list_keys_page("p/", empty.cursor.as_deref(), 10)
            .await
            .expect_err("continuation must not repeat the previous page's last key");
        assert_eq!(fixture.count().await, 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pages_reject_unknown_duplicate_missing_malformed_and_unordered_fields() {
        for bytes in [
            br#"{"keys":["z"],"cursor":null}"#.as_slice(),
            br#"{"keys":["p/b","p/a"],"cursor":null}"#,
            br#"{"keys":["p/a","p/a"],"cursor":null}"#,
            br#"{"keys":[],"cursor":""}"#,
            br#"{"keys":[],"cursor":null,"extra":1}"#,
            br#"{"keys":[],"keys":[],"cursor":null}"#,
            br#"{"keys":[]}"#,
            b"not json",
        ] {
            let fixture = Fixture::new(vec![Reply::response(200, "application/json", bytes)]).await;
            store(&fixture, "state")
                .list_keys_page("p/", None, 100)
                .await
                .expect_err("malformed or unordered page must be rejected");
        }
        let too_large_cursor =
            serde_json::to_vec(&serde_json::json!({"keys":[], "cursor":"x".repeat(2_049)}))
                .unwrap();
        let fixture = Fixture::new(vec![Reply::response(
            200,
            "application/json",
            &too_large_cursor,
        )])
        .await;
        store(&fixture, "state")
            .list_keys_page("", None, 1_000)
            .await
            .expect_err("overlong service cursor must be rejected");
        let repeated_cursor_fixture = Fixture::new(vec![
            Reply::response(200, "application/json", br#"{"keys":[],"cursor":"same"}"#),
            Reply::response(200, "application/json", br#"{"keys":[],"cursor":"same"}"#),
        ])
        .await;
        let kv = store(&repeated_cursor_fixture, "state");
        let first = kv.list_keys_page("", None, 1).await.unwrap();
        kv.list_keys_page("", first.cursor.as_deref(), 1)
            .await
            .expect_err("unchanged service cursor must be rejected");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ttl_wire_rounding_renewal_and_plain_reset_are_explicit() {
        let fixture = Fixture::new(vec![]).await;
        let kv = store(&fixture, "state");
        kv.put_bytes_with_ttl("k", Bytes::new(), Duration::from_millis(1))
            .await
            .expect_err("sub-minimum TTL must be rejected before transport");
        assert_eq!(fixture.count().await, 0);
        kv.put_bytes_with_ttl(
            "k",
            Bytes::new(),
            Duration::from_mins(1) + Duration::from_nanos(1),
        )
        .await
        .unwrap();
        kv.put_bytes_with_ttl("k", Bytes::new(), Duration::from_mins(2))
            .await
            .unwrap();
        kv.put_bytes("k", Bytes::new()).await.unwrap();
        let requests = fixture.requests().await;
        assert!(requests[0].target.contains("ttl_seconds=61"));
        assert!(requests[1].target.contains("ttl_seconds=120"));
        assert!(!requests[2].target.contains("ttl_seconds"));
        // These are wire assertions, not proof of real service elapsed expiry.
    }

    #[tokio::test(flavor = "current_thread")]
    async fn applied_then_lost_put_and_delete_are_not_replayed_fresh_or_reused() {
        for reused in [false, true] {
            for delete in [false, true] {
                let replies = if reused {
                    vec![Reply::response(404, "application/json", b""), Reply::Drop]
                } else {
                    vec![Reply::Drop]
                };
                let fixture = Fixture::new(replies).await;
                fixture.seed("state", "mut", b"original").await;
                let kv = store(&fixture, "state");
                if reused {
                    assert!(!kv.exists("warm").await.unwrap());
                }
                let result = if delete {
                    kv.delete("mut").await
                } else {
                    kv.put_bytes("mut", Bytes::from_static(b"applied")).await
                };
                result.expect_err("lost mutation reply must return an error without replay");
                assert_eq!(
                    fixture.value("state", "mut").await,
                    if delete {
                        None
                    } else {
                        Some(b"applied".to_vec())
                    }
                );
                assert_eq!(fixture.count().await, if reused { 2 } else { 1 });
                let requests = fixture.requests().await;
                if reused {
                    assert_eq!(requests[0].connection, requests[1].connection);
                }
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn timed_out_write_may_have_applied_and_is_not_replayed() {
        let fixture = Fixture::new(vec![Reply::Stall]).await;
        let limits = CelldPreparationLimits {
            operation_timeout_ms: 40,
            connect_timeout_ms: 10,
            ..CelldPreparationLimits::default()
        };
        let endpoint = Url::parse(fixture.endpoint()).unwrap();
        let client = Arc::new(
            ScopedClient::new(endpoint.clone(), None, MonotonicClock::default(), limits).unwrap(),
        );
        let kv = CelldKvStore::new(client, endpoint, "state".to_owned(), limits);
        kv.put_bytes("mut", Bytes::from_static(b"applied"))
            .await
            .expect_err("stalled mutation reply must reach the operation deadline");
        assert_eq!(fixture.value("state", "mut").await.unwrap(), b"applied");
        assert_eq!(fixture.count().await, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_caps_and_direct_trait_input_checks_precede_requests() {
        let fixture = Fixture::new(vec![]).await;
        let endpoint = Url::parse(fixture.endpoint()).unwrap();
        let limits = CelldPreparationLimits {
            max_value_bytes: 2,
            max_config_document_bytes: 2,
            max_page_bytes: 4,
            ..CelldPreparationLimits::default()
        };
        let client = Arc::new(
            ScopedClient::new(endpoint.clone(), None, MonotonicClock::default(), limits).unwrap(),
        );
        let kv = CelldKvStore::new(client, endpoint, "state".to_owned(), limits);
        kv.put_bytes("k", Bytes::from_static(b"123"))
            .await
            .expect_err("value above the provider cap must be rejected before transport");
        kv.get_bytes("")
            .await
            .expect_err("empty value key must be rejected before transport");
        kv.list_keys_page("", None, 0)
            .await
            .expect_err("zero page size must be rejected before transport");
        kv.list_keys_page("", None, 1_001)
            .await
            .expect_err("page size above the service maximum must be rejected before transport");
        assert_eq!(fixture.count().await, 0);
        fixture.seed("state", "k", b"123").await;
        kv.get_bytes("k")
            .await
            .expect_err("response value above the provider cap must be rejected");
        kv.list_keys_page("", None, 1)
            .await
            .expect_err("response page above the provider cap must be rejected");
        assert_eq!(fixture.count().await, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn head_and_mutations_require_version_and_their_exact_success_status() {
        for reply in [
            Reply::custom(404, "application/json", "", b""),
            Reply::response(204, "application/octet-stream", b""),
        ] {
            let fixture = Fixture::new(vec![reply]).await;
            store(&fixture, "state")
                .exists("k")
                .await
                .expect_err("HEAD with invalid version or status must be rejected");
        }
        for reply in [
            Reply::custom(204, "application/octet-stream", "", b""),
            Reply::response(200, "application/octet-stream", b""),
            Reply::response(201, "application/octet-stream", b""),
        ] {
            let fixture = Fixture::new(vec![reply]).await;
            store(&fixture, "state")
                .delete("k")
                .await
                .expect_err("mutation with invalid version or status must be rejected");
        }
    }

    #[test]
    fn ttl_validates_then_rounds_up_without_shortening() {
        assert_eq!(ttl_seconds(Duration::from_mins(1)).unwrap(), 60);
        assert_eq!(
            ttl_seconds(Duration::from_mins(1) + Duration::from_nanos(1)).unwrap(),
            61
        );
        assert_eq!(
            ttl_seconds(Duration::from_hours(8_760)).unwrap(),
            31_536_000
        );
        for ttl in [
            Duration::ZERO,
            Duration::from_millis(59_999),
            Duration::from_hours(8_760) + Duration::from_nanos(1),
            Duration::MAX,
        ] {
            ttl_seconds(ttl).expect_err("out-of-range TTL must be rejected before rounding");
        }
    }
}
