//! One bounded HTTP exchange. No discovery, redirects, proxies or mutation replay.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep transport construction, request lifecycle and response validators together"
)]

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use edgezero_core::time::{Deadline, MonotonicClock};
use futures_util::StreamExt as _;
#[cfg(feature = "kv")]
use reqwest::header::CONTENT_LENGTH;
use reqwest::header::{
    ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderValue,
};
use reqwest::redirect::Policy;
use reqwest::retry::never;
use reqwest::{Client, Method, StatusCode};
use serde::Deserialize;
use tokio::time::timeout;
use url::Url;

use crate::PreparationError;
use crate::settings::{CelldAccessToken, CelldPreparationLimits};

const ERROR_CAP: usize = 1_024;
const VERSION_HEADER: &str = "x-celld-store-version";

pub(crate) struct ScopedClient {
    authorization: Option<HeaderValue>,
    client: Client,
    clock: MonotonicClock,
    endpoint: Url,
    limits: CelldPreparationLimits,
}

impl fmt::Debug for ScopedClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedClient").finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ResponseKind {
    #[cfg(feature = "kv")]
    Exists,
    #[cfg(feature = "kv")]
    Mutation,
    #[cfg(feature = "kv")]
    Page,
    #[cfg(feature = "config")]
    Document,
    #[cfg(feature = "kv")]
    Value,
}

impl ScopedClient {
    pub(crate) fn new(
        endpoint: Url,
        token: Option<&CelldAccessToken>,
        clock: MonotonicClock,
        limits: CelldPreparationLimits,
    ) -> Result<Self, PreparationError> {
        Self::build(endpoint, token, clock, limits, None)
    }

    // A root-only seam keeps tests on the production transport policy. Production
    // always passes None; no arbitrary client or insecure TLS switch is exposed.
    fn build(
        endpoint: Url,
        token: Option<&CelldAccessToken>,
        clock: MonotonicClock,
        limits: CelldPreparationLimits,
        root: Option<reqwest::Certificate>,
    ) -> Result<Self, PreparationError> {
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .retry(never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connection_verbose(false)
            .http1_only()
            .connect_timeout(Duration::from_millis(limits.connect_timeout_ms))
            .timeout(Duration::from_millis(limits.operation_timeout_ms));
        if let Some(certificate) = root {
            builder = builder.tls_certs_only([certificate]);
        }
        let authorization = token
            .map(|credential| {
                let mut header = HeaderValue::from_str(&format!("Bearer {}", credential.expose()))
                    .map_err(|_error| PreparationError::Credentials)?;
                header.set_sensitive(true);
                Ok(header)
            })
            .transpose()?;
        Ok(Self {
            authorization,
            client: builder
                .build()
                .map_err(|_error| PreparationError::Unavailable)?,
            clock,
            endpoint,
            limits,
        })
    }

    pub(crate) fn deadline(&self) -> Result<Deadline, PreparationError> {
        self.clock
            .now()
            .checked_add(Duration::from_millis(self.limits.operation_timeout_ms))
            .map(Deadline::at_instant)
            .ok_or(PreparationError::Deadline)
    }

    pub(crate) fn check(&self, deadline: Deadline) -> Result<Duration, PreparationError> {
        deadline
            .remaining_at(self.clock.now())
            .ok_or(PreparationError::Deadline)
    }

    pub(crate) fn url(&self, store: &str, resource: &str) -> Result<Url, PreparationError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_error| PreparationError::InvalidEndpoint)?
            .clear()
            .extend(["v1", "stores", store, resource]);
        Ok(url)
    }

    /// Covers sending, streaming and decoding with one real timer and injected
    /// monotonic checks. None means a validated miss, never a protocol failure.
    pub(crate) async fn execute(
        &self,
        method: Method,
        url: Url,
        body: Option<Bytes>,
        cap: usize,
        deadline: Deadline,
        #[cfg_attr(
            not(feature = "kv"),
            expect(
                unused_variables,
                reason = "config-only builds have one request kind and no optional misses"
            )
        )]
        kind: ResponseKind,
    ) -> Result<Option<Vec<u8>>, PreparationError> {
        let budget = self
            .check(deadline)?
            .min(Duration::from_millis(self.limits.operation_timeout_ms));
        let mut request = self
            .client
            .request(method.clone(), url)
            .header(ACCEPT_ENCODING, "identity")
            .timeout(budget);
        if let Some(authorization) = &self.authorization {
            request = request.header(AUTHORIZATION, authorization.clone());
        }
        if let Some(payload) = body {
            request = request
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(payload);
        }
        let result = timeout(budget, async {
            let response = request.send().await.map_err(http_error)?;
            let headers = response.headers();
            validate_headers(headers)?;
            let status = response.status();
            #[cfg(feature = "kv")]
            let success = status
                == if matches!(kind, ResponseKind::Mutation) {
                    StatusCode::NO_CONTENT
                } else {
                    StatusCode::OK
                };
            #[cfg(not(feature = "kv"))]
            let success = status == StatusCode::OK;
            #[cfg(feature = "kv")]
            if matches!(kind, ResponseKind::Mutation)
                && success
                && headers
                    .get(CONTENT_LENGTH)
                    .is_some_and(|length| length.as_bytes() != b"0")
            {
                return Err(PreparationError::Malformed);
            }
            let missing = status == StatusCode::NOT_FOUND;
            // The wire HEAD contract has no error body, including non-404 errors.
            if method == Method::HEAD {
                return if success {
                    Ok(Some(Vec::new()))
                } else if missing {
                    Ok(None)
                } else {
                    Err(status_error(status)
                        .map_or(PreparationError::Malformed, |(_, error)| error))
                };
            }
            #[cfg(feature = "kv")]
            let expected_type = if !success || matches!(kind, ResponseKind::Page) {
                "application/json"
            } else {
                "application/octet-stream"
            };
            #[cfg(not(feature = "kv"))]
            let expected_type = if success {
                "application/octet-stream"
            } else {
                "application/json"
            };
            validate_content_type(headers, expected_type)?;
            let response_cap = if success { cap } else { ERROR_CAP };
            let bytes = collect(response, response_cap, &self.clock, deadline).await?;
            if success {
                return Ok(Some(bytes));
            }
            let (expected, error) = status_error(status).ok_or(PreparationError::Malformed)?;
            let parsed: ErrorBody =
                serde_json::from_slice(&bytes).map_err(|_error| PreparationError::Malformed)?;
            if parsed.error != expected {
                return Err(PreparationError::Malformed);
            }
            #[cfg(feature = "kv")]
            if missing && matches!(kind, ResponseKind::Value) {
                return Ok(None);
            }
            Err(error)
        })
        .await
        .map_err(|_elapsed| PreparationError::Deadline);
        self.check(deadline)?;
        result?
    }
}

fn validate_headers(headers: &HeaderMap) -> Result<(), PreparationError> {
    let mut versions = headers.get_all(VERSION_HEADER).iter();
    if versions.next().is_none_or(|value| value.as_bytes() != b"1") || versions.next().is_some() {
        return Err(PreparationError::UnsupportedVersion);
    }
    let mut encodings = headers.get_all(CONTENT_ENCODING).iter();
    if encodings
        .next()
        .is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
        || encodings.next().is_some()
    {
        return Err(PreparationError::Malformed);
    }
    Ok(())
}

fn validate_content_type(headers: &HeaderMap, expected: &str) -> Result<(), PreparationError> {
    let mut types = headers.get_all(CONTENT_TYPE).iter();
    if types
        .next()
        .is_none_or(|value| value.as_bytes() != expected.as_bytes())
        || types.next().is_some()
    {
        return Err(PreparationError::Malformed);
    }
    Ok(())
}

async fn collect(
    response: reqwest::Response,
    cap: usize,
    clock: &MonotonicClock,
    deadline: Deadline,
) -> Result<Vec<u8>, PreparationError> {
    let cap_u64 = u64::try_from(cap).map_err(|_error| PreparationError::SizeLimit)?;
    if response
        .content_length()
        .is_some_and(|length| length > cap_u64)
    {
        return Err(PreparationError::SizeLimit);
    }
    let mut bytes = Vec::with_capacity(cap.min(0x4000));
    let mut stream = response.bytes_stream();
    while let Some(next_chunk) = stream.next().await {
        let chunk = next_chunk.map_err(http_error)?;
        if deadline.is_expired_at(clock.now()) {
            return Err(PreparationError::Deadline);
        }
        if chunk.len() > cap.saturating_sub(bytes.len()) {
            return Err(PreparationError::SizeLimit);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorBody {
    error: String,
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "consume transport errors at the redaction boundary so no source chain escapes"
)]
fn http_error(error: reqwest::Error) -> PreparationError {
    if error.is_timeout() {
        PreparationError::Deadline
    } else {
        PreparationError::Unavailable
    }
}

fn status_error(status: StatusCode) -> Option<(&'static str, PreparationError)> {
    Some(match status.as_u16() {
        400 => ("invalid_input", PreparationError::InvalidResource),
        401 => ("unauthenticated", PreparationError::Unauthenticated),
        403 => ("forbidden", PreparationError::Forbidden),
        404 => ("not_found", PreparationError::Missing),
        413 => ("limit_exceeded", PreparationError::SizeLimit),
        426 => ("unsupported_version", PreparationError::UnsupportedVersion),
        503 => ("unavailable", PreparationError::Unavailable),
        504 => ("deadline", PreparationError::Deadline),
        _ => return None,
    })
}

#[cfg(test)]
#[cfg(any(feature = "config", feature = "kv"))]
mod tests {
    use std::env::{current_exe, var};
    use std::io::ErrorKind;
    use std::net::TcpListener;
    use std::process::Command;

    use edgezero_core::time::MonotonicClock;
    use reqwest::Method;
    use url::Url;

    use super::{ResponseKind, ScopedClient};
    use crate::PreparationError;
    use crate::settings::{CelldAccessToken, CelldPreparationLimits};
    use crate::test_support::{Fixture, Reply};

    #[tokio::test(flavor = "current_thread")]
    async fn native_tls_requires_trust_matching_identity_and_valid_dates() {
        for (certificate, trusted, accepted) in [
            (
                include_bytes!("fixtures/tls-valid.der").as_slice(),
                true,
                true,
            ),
            (
                include_bytes!("fixtures/tls-valid.der").as_slice(),
                false,
                false,
            ),
            (
                include_bytes!("fixtures/tls-wrong.der").as_slice(),
                true,
                false,
            ),
            (
                include_bytes!("fixtures/tls-expired.der").as_slice(),
                true,
                false,
            ),
        ] {
            let fixture = Fixture::tls(certificate, vec![]).await;
            fixture.seed("config", "key", b"TLS").await;
            let token =
                CelldAccessToken::new("test-tls-bearer-credential-at-least-32-bytes".to_owned())
                    .unwrap();
            let root = trusted.then(|| {
                reqwest::Certificate::from_der(include_bytes!("fixtures/tls-ca.der")).unwrap()
            });
            let client = ScopedClient::build(
                Url::parse(fixture.endpoint()).unwrap(),
                Some(&token),
                MonotonicClock::default(),
                CelldPreparationLimits::default(),
                root,
            )
            .unwrap();
            let mut url = client.url("config", "value").unwrap();
            url.query_pairs_mut().append_pair("key", "key");
            let result = client
                .execute(
                    Method::GET,
                    url,
                    None,
                    16,
                    client.deadline().unwrap(),
                    kind(),
                )
                .await;
            if accepted {
                assert_eq!(result.unwrap(), Some(b"TLS".to_vec()));
                assert_eq!(
                    fixture.requests().await[0].authorization.as_deref(),
                    Some(format!("Bearer {}", token.expose()).as_str())
                );
            } else {
                assert_eq!(result.unwrap_err(), PreparationError::Unavailable);
                assert_eq!(
                    fixture.count().await,
                    0,
                    "TLS failure must never fall back to plaintext"
                );
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_tls_keeps_distinct_credentials_on_reused_connections() {
        let fixture = Fixture::tls(include_bytes!("fixtures/tls-valid.der"), vec![]).await;
        fixture.seed("config", "key", b"TLS").await;
        let mut clients = Vec::new();
        for token_text in [
            "first-tls-credential-at-least-32-bytes",
            "second-tls-credential-at-least-32-bytes",
        ] {
            let token = CelldAccessToken::new(token_text.to_owned()).unwrap();
            clients.push(
                ScopedClient::build(
                    Url::parse(fixture.endpoint()).unwrap(),
                    Some(&token),
                    MonotonicClock::default(),
                    CelldPreparationLimits::default(),
                    Some(
                        reqwest::Certificate::from_der(include_bytes!("fixtures/tls-ca.der"))
                            .unwrap(),
                    ),
                )
                .unwrap(),
            );
        }
        for index in [0, 1, 0] {
            let client = &clients[index];
            let mut url = client.url("config", "value").unwrap();
            url.query_pairs_mut().append_pair("key", "key");
            assert_eq!(
                client
                    .execute(
                        Method::GET,
                        url,
                        None,
                        16,
                        client.deadline().unwrap(),
                        kind()
                    )
                    .await
                    .unwrap(),
                Some(b"TLS".to_vec())
            );
        }
        let requests = fixture.requests().await;
        assert_eq!(requests[0].connection, requests[2].connection);
        assert_ne!(requests[0].connection, requests[1].connection);
        assert_eq!(requests[0].authorization, requests[2].authorization);
        assert_ne!(requests[0].authorization, requests[1].authorization);
    }

    #[test]
    fn ambient_proxy_is_not_used_by_the_production_client() {
        let sentinel = TcpListener::bind("127.0.0.1:0").unwrap();
        sentinel.set_nonblocking(true).unwrap();
        let proxy = format!("http://{}", sentinel.local_addr().unwrap());
        let mut command =
            Command::new(current_exe().expect("locate proxy fixture test executable"));
        command
            .env_clear()
            .env("CELLD_PROXY_FIXTURE_CHILD", "selected");
        for variable in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            command.env(variable, &proxy);
        }
        let output = command
            .env("NO_PROXY", "")
            .env("no_proxy", "")
            .args([
                "--exact",
                "client::tests::ambient_proxy_child",
                "--nocapture",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("CELLD_DIRECT_TRANSPORT_PROVEN"));
        assert!(matches!(sentinel.accept(), Err(error) if error.kind() == ErrorKind::WouldBlock));
    }

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::print_stdout,
        reason = "the parent requires an explicit child execution marker"
    )]
    async fn ambient_proxy_child() {
        if var("CELLD_PROXY_FIXTURE_CHILD").as_deref() != Ok("selected") {
            return;
        }
        let fixture = Fixture::new(vec![]).await;
        fixture.seed("config", "key", b"direct").await;
        let client = ScopedClient::new(
            Url::parse(fixture.endpoint()).unwrap(),
            None,
            MonotonicClock::default(),
            CelldPreparationLimits::default(),
        )
        .unwrap();
        let mut url = client.url("config", "value").unwrap();
        url.query_pairs_mut().append_pair("key", "key");
        assert_eq!(
            client
                .execute(
                    Method::GET,
                    url,
                    None,
                    16,
                    client.deadline().unwrap(),
                    kind()
                )
                .await
                .unwrap(),
            Some(b"direct".to_vec())
        );
        assert_eq!(fixture.count().await, 1);
        println!("CELLD_DIRECT_TRANSPORT_PROVEN");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn oversized_response_headers_fail_at_the_http_parser_bound() {
        let header = format!(
            "x-celld-store-version: 1\r\nx-excess: {}\r\n",
            "x".repeat(512 * 1024)
        );
        let fixture = Fixture::new(vec![Reply::custom(
            200,
            "application/octet-stream",
            &header,
            b"x",
        )])
        .await;
        let client = ScopedClient::new(
            Url::parse(fixture.endpoint()).expect("fixture origin"),
            None,
            MonotonicClock::default(),
            CelldPreparationLimits::default(),
        )
        .expect("bounded client");
        let url = client.url("config", "value").expect("value URL");
        assert_eq!(
            client
                .execute(
                    Method::GET,
                    url,
                    None,
                    16,
                    client.deadline().expect("deadline"),
                    kind()
                )
                .await
                .expect_err("oversized headers"),
            PreparationError::Unavailable
        );
        assert_eq!(fixture.count().await, 1);
    }

    fn kind() -> ResponseKind {
        #[cfg(feature = "kv")]
        {
            ResponseKind::Value
        }
        #[cfg(not(feature = "kv"))]
        {
            ResponseKind::Document
        }
    }

    async fn read(fixture: &Fixture, cap: usize) -> Result<Option<Vec<u8>>, PreparationError> {
        let client = ScopedClient::new(
            Url::parse(fixture.endpoint()).expect("parse read fixture endpoint"),
            None,
            MonotonicClock::default(),
            CelldPreparationLimits::default(),
        )
        .expect("construct read fixture client");
        let mut url = client
            .url("state", "value")
            .expect("construct read fixture value URL");
        url.query_pairs_mut().append_pair("key", "k");
        client
            .execute(
                Method::GET,
                url,
                None,
                cap,
                client.deadline().expect("construct read fixture deadline"),
                kind(),
            )
            .await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn raw_binary_empty_and_encoded_keys_use_real_transport() {
        let fixture = Fixture::new(vec![]).await;
        let key = " /+?%&#=\u{e9}\u{65e5}";
        fixture.seed("state", key, &[0, 255, 1]).await;
        let client = ScopedClient::new(
            Url::parse(fixture.endpoint()).unwrap(),
            None,
            MonotonicClock::default(),
            CelldPreparationLimits::default(),
        )
        .unwrap();
        let mut url = client.url("state", "value").unwrap();
        url.query_pairs_mut().append_pair("key", key);
        let bytes = client
            .execute(
                Method::GET,
                url.clone(),
                None,
                3,
                client.deadline().unwrap(),
                kind(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes, [0, 255, 1]);
        fixture.seed("state", key, b"").await;
        assert!(
            client
                .execute(
                    Method::GET,
                    url,
                    None,
                    0,
                    client.deadline().unwrap(),
                    kind()
                )
                .await
                .unwrap()
                .unwrap()
                .is_empty()
        );
        assert_eq!(fixture.value("state", key).await.unwrap(), b"");
        let requests = fixture.requests().await;
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.authorization.is_none()
                    && request.encoding.as_deref() == Some("identity")
                    && request.body.is_empty()
                    && request.method == "GET")
        );
        assert_eq!(requests[0].connection, requests[1].connection);
        assert!(requests[0].target.contains("%2B"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn every_response_requires_exactly_one_version_and_identity_encoding() {
        for headers in [
            "",
            "x-celld-store-version: 2\r\n",
            "x-celld-store-version: 1\r\nx-celld-store-version: 1\r\n",
            "x-celld-store-version: 1, 1\r\n",
        ] {
            let fixture = Fixture::new(vec![Reply::custom(
                200,
                "application/octet-stream",
                headers,
                b"v",
            )])
            .await;
            assert_eq!(
                read(&fixture, 100).await,
                Err(PreparationError::UnsupportedVersion)
            );
        }
        for headers in [
            "Content-Encoding: gzip\r\n",
            "Content-Encoding: identity\r\nContent-Encoding: identity\r\n",
        ] {
            let fixture = Fixture::new(vec![Reply::custom(
                200,
                "application/octet-stream",
                &format!("x-celld-store-version: 1\r\n{headers}"),
                b"v",
            )])
            .await;
            assert_eq!(read(&fixture, 100).await, Err(PreparationError::Malformed));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn exact_error_status_categories_and_bounded_strict_json() {
        for (status, category, error) in [
            (400, "invalid_input", PreparationError::InvalidResource),
            (401, "unauthenticated", PreparationError::Unauthenticated),
            (403, "forbidden", PreparationError::Forbidden),
            (413, "limit_exceeded", PreparationError::SizeLimit),
            (
                426,
                "unsupported_version",
                PreparationError::UnsupportedVersion,
            ),
            (503, "unavailable", PreparationError::Unavailable),
            (504, "deadline", PreparationError::Deadline),
        ] {
            let bytes = serde_json::to_vec(&serde_json::json!({"error": category})).unwrap();
            let fixture =
                Fixture::new(vec![Reply::response(status, "application/json", &bytes)]).await;
            assert_eq!(read(&fixture, 100).await, Err(error));
        }
        for bytes in [
            br#"{"error":"forbidden"}"#.as_slice(),
            br#"{"error":"unavailable","extra":1}"#,
            br#"{"error":"unavailable","error":"unavailable"}"#,
            b"not json",
        ] {
            let fixture = Fixture::new(vec![Reply::response(503, "application/json", bytes)]).await;
            assert_eq!(read(&fixture, 100).await, Err(PreparationError::Malformed));
        }
        let fixture = Fixture::new(vec![Reply::response(
            503,
            "application/json",
            &vec![b'x'; 1_025],
        )])
        .await;
        assert_eq!(read(&fixture, 100).await, Err(PreparationError::SizeLimit));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn misses_are_distinct_from_empty_and_bad_statuses() {
        let missing_fixture = Fixture::new(vec![]).await;
        #[cfg(feature = "kv")]
        assert_eq!(read(&missing_fixture, 1).await.unwrap(), None);
        #[cfg(not(feature = "kv"))]
        assert_eq!(
            read(&missing_fixture, 1).await,
            Err(PreparationError::Missing)
        );
        for status in [201, 204, 206, 302, 500] {
            let fixture = Fixture::new(vec![Reply::response(
                status,
                "application/octet-stream",
                b"",
            )])
            .await;
            assert_eq!(read(&fixture, 10).await, Err(PreparationError::Malformed));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn declared_and_chunked_values_are_capped_before_unbounded_collection() {
        let declared = Reply::Bytes(b"HTTP/1.1 200 Fixture\r\nx-celld-store-version: 1\r\nContent-Type: application/octet-stream\r\nContent-Length: 9999999\r\n\r\n".to_vec());
        let chunked = Reply::Bytes(b"HTTP/1.1 200 Fixture\r\nx-celld-store-version: 1\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n".to_vec());
        for reply in [declared, chunked] {
            let fixture = Fixture::new(vec![reply]).await;
            assert_eq!(read(&fixture, 5).await, Err(PreparationError::SizeLimit));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn redirects_never_reach_another_destination() {
        let destination = Fixture::new(vec![]).await;
        let source = Fixture::new(vec![Reply::custom(
            302,
            "application/json",
            &format!(
                "x-celld-store-version: 1\r\nLocation: {}/v1/stores/state/value?key=k\r\n",
                destination.endpoint()
            ),
            b"{}",
        )])
        .await;
        assert_eq!(read(&source, 100).await, Err(PreparationError::Malformed));
        assert_eq!(destination.count().await, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_headers_and_body_share_a_finite_operation_deadline() {
        for reply in [Reply::Stall, Reply::SlowBody] {
            let fixture = Fixture::new(vec![reply]).await;
            let limits = CelldPreparationLimits {
                operation_timeout_ms: 40,
                connect_timeout_ms: 10,
                ..CelldPreparationLimits::default()
            };
            let client = ScopedClient::new(
                Url::parse(fixture.endpoint()).unwrap(),
                None,
                MonotonicClock::default(),
                limits,
            )
            .unwrap();
            let mut url = client.url("state", "value").unwrap();
            url.query_pairs_mut().append_pair("key", "k");
            assert_eq!(
                client
                    .execute(
                        Method::GET,
                        url,
                        None,
                        100,
                        client.deadline().unwrap(),
                        kind()
                    )
                    .await,
                Err(PreparationError::Deadline)
            );
        }
    }

    #[test]
    fn sensitive_header_and_owned_debug_never_expose_tokens_or_resources() {
        let token = CelldAccessToken::new("secret-sentinel".repeat(3)).unwrap();
        let client = ScopedClient::new(
            Url::parse("https://resource-sentinel.invalid").unwrap(),
            Some(&token),
            MonotonicClock::default(),
            CelldPreparationLimits::default(),
        )
        .unwrap();
        assert!(client.authorization.as_ref().unwrap().is_sensitive());
        let debug = format!("{client:?} {token:?}");
        assert!(!debug.contains("secret-sentinel"));
        assert!(!debug.contains("resource-sentinel"));
        for error in [
            PreparationError::Credentials,
            PreparationError::Unauthenticated,
            PreparationError::Forbidden,
            PreparationError::Unavailable,
        ] {
            assert!(!format!("{error} {error:?}").contains("sentinel"));
        }
    }
}
