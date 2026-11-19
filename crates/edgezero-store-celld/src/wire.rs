//! Transport-neutral store-service exchange policy shared by native and portable clients.
//!
//! Every mutation carries a fresh random ID. The store service records it in the
//! same transaction as the write, so a duplicate delivery inside celld or a proxy
//! cannot apply it twice. No client ever resends a mutation.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep request construction, response heads, bodies and validators in lifecycle order"
)]

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use edgezero_core::http::header::{
    ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
};
use edgezero_core::http::{HeaderMap, HeaderValue, Method, StatusCode};
use edgezero_core::time::Deadline;
use serde::Deserialize;
use url::Url;

use crate::PreparationError;
use crate::settings::CelldAccessToken;

pub(crate) const ERROR_CAP: usize = 1_024;
const INSTANCE_HEADER: &str = "x-edgezero-store-instance";
const MUTATION_HEADER: &str = "x-edgezero-mutation-id";
const VERSION_HEADER: &str = "x-edgezero-store-version";
#[cfg(feature = "record-wire")]
const RECORD_HEADER: &str = "x-edgezero-record-version";
#[cfg(feature = "record-wire")]
const EXPECTED_INSTANCE_HEADER: &str = "x-edgezero-expected-instance";

#[derive(Clone, Copy)]
#[cfg_attr(
    not(all(not(target_arch = "wasm32"), feature = "kv")),
    expect(
        dead_code,
        reason = "ordinary and capability exchanges are native-only; portable builds send record exchanges"
    )
)]
pub(crate) enum ResponseKind {
    Exists,
    Mutation,
    Page,
    Value,
    #[cfg(feature = "record-wire")]
    RecordCapabilities,
    #[cfg(feature = "record-wire")]
    RecordExists([u8; 22]),
    #[cfg(feature = "record-wire")]
    RecordMutation([u8; 22]),
    #[cfg(feature = "record-wire")]
    RecordPage([u8; 22]),
    #[cfg(feature = "record-wire")]
    RecordValue([u8; 22]),
}

impl ResponseKind {
    pub(crate) const fn mutation(self) -> bool {
        match self {
            Self::Mutation => true,
            #[cfg(feature = "record-wire")]
            Self::RecordMutation(_) => true,
            _ => false,
        }
    }

    const fn json(self) -> bool {
        match self {
            Self::Page => true,
            #[cfg(feature = "record-wire")]
            Self::RecordCapabilities | Self::RecordPage(_) | Self::RecordValue(_) => true,
            _ => false,
        }
    }

    const fn optional(self) -> bool {
        match self {
            Self::Value => true,
            #[cfg(feature = "record-wire")]
            Self::RecordValue(_) => true,
            _ => false,
        }
    }

    pub(crate) const fn record(self) -> bool {
        match self {
            #[cfg(feature = "record-wire")]
            Self::RecordCapabilities
            | Self::RecordExists(_)
            | Self::RecordMutation(_)
            | Self::RecordPage(_)
            | Self::RecordValue(_) => true,
            _ => false,
        }
    }

    const fn expected_instance(self) -> Option<[u8; 22]> {
        match self {
            #[cfg(feature = "record-wire")]
            Self::RecordExists(instance)
            | Self::RecordMutation(instance)
            | Self::RecordPage(instance)
            | Self::RecordValue(instance) => Some(instance),
            _ => None,
        }
    }
}

/// One bounded store-service exchange. Native uses reqwest; portable uses `HttpClient`.
#[async_trait(?Send)]
pub(crate) trait Exchanger: Send + Sync {
    fn check(&self, deadline: Deadline) -> Result<Duration, PreparationError>;
    fn deadline(&self) -> Result<Deadline, PreparationError>;
    fn url(&self, store: &str, resource: &str) -> Result<Url, PreparationError>;
    async fn execute(
        &self,
        method: Method,
        url: Url,
        body: Option<Bytes>,
        cap: usize,
        deadline: Deadline,
        kind: ResponseKind,
    ) -> Result<Exchange, PreparationError>;
}

/// A validated exchange. The instance identifies the store's durable state.
#[cfg_attr(
    not(all(not(target_arch = "wasm32"), feature = "kv")),
    expect(dead_code, reason = "only native preparation reads the reported instance")
)]
pub(crate) struct Exchange {
    pub body: Option<Vec<u8>>,
    pub instance: Option<String>,
}

impl fmt::Debug for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Exchange").finish_non_exhaustive()
    }
}

/// What a validated response head still needs.
pub(crate) enum Head {
    /// The head alone completes the exchange.
    Complete(Exchange),
    /// Read at most `cap` body bytes, then call [`body`].
    Body {
        cap: usize,
        instance: Option<String>,
        success: bool,
    },
}

/// The bearer header for one credential. The value is marked sensitive.
pub(crate) fn authorization(token: &CelldAccessToken) -> Result<HeaderValue, PreparationError> {
    let mut header = HeaderValue::from_str(&format!("Bearer {}", token.expose()))
        .map_err(|_error| PreparationError::Credentials)?;
    header.set_sensitive(true);
    Ok(header)
}

/// `{endpoint}/v1/stores/{store}/{resource}`, with each segment percent-encoded.
pub(crate) fn store_url(
    endpoint: &Url,
    store: &str,
    resource: &str,
) -> Result<Url, PreparationError> {
    let mut url = endpoint.clone();
    url.path_segments_mut()
        .map_err(|_error| PreparationError::InvalidEndpoint)?
        .clear()
        .extend(["v1", "stores", store, resource]);
    Ok(url)
}

/// Request headers for one exchange, including a fresh mutation ID when required.
pub(crate) fn request_headers(
    kind: ResponseKind,
    authorization: Option<&HeaderValue>,
    has_body: bool,
) -> Result<HeaderMap, PreparationError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    if let Some(value) = authorization {
        headers.insert(AUTHORIZATION, value.clone());
    }
    if kind.mutation() {
        headers.insert(MUTATION_HEADER, mutation_id()?);
    }
    #[cfg(feature = "record-wire")]
    if kind.record() {
        headers.insert(RECORD_HEADER, HeaderValue::from_static("1"));
    }
    #[cfg(feature = "record-wire")]
    if let Some(expected) = kind.expected_instance() {
        headers.insert(
            EXPECTED_INSTANCE_HEADER,
            HeaderValue::from_bytes(&expected).map_err(|_error| PreparationError::Malformed)?,
        );
    }
    if has_body {
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static(if kind.record() {
                "application/json"
            } else {
                "application/octet-stream"
            }),
        );
    }
    Ok(headers)
}

/// Validates a response head. A `None` body means a validated miss, never a protocol
/// failure. Successful responses must name the store instance.
pub(crate) fn head(
    kind: ResponseKind,
    method: &Method,
    status: StatusCode,
    headers: &HeaderMap,
    cap: usize,
) -> Result<Head, PreparationError> {
    let success = status
        == if kind.mutation() {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::OK
        };
    let missing = status == StatusCode::NOT_FOUND;
    let instance = validated_instance(headers, kind, method, status, success)?;

    // A mutation acknowledgement carries no body, so it has no media type.
    if status == StatusCode::NO_CONTENT && success {
        if headers
            .get(CONTENT_LENGTH)
            .is_some_and(|length| length.as_bytes() != b"0")
        {
            return Err(PreparationError::Malformed);
        }
        return Ok(Head::Complete(Exchange {
            body: Some(Vec::new()),
            instance,
        }));
    }
    // The wire HEAD contract has no error body, including non-404 errors.
    if *method == Method::HEAD {
        return if success {
            Ok(Head::Complete(Exchange {
                body: Some(Vec::new()),
                instance,
            }))
        } else if missing {
            Ok(Head::Complete(Exchange {
                body: None,
                instance,
            }))
        } else {
            Err(status_error(status).map_or(PreparationError::Malformed, |(_, error)| error))
        };
    }
    let expected_type = if !success || kind.json() {
        "application/json"
    } else {
        "application/octet-stream"
    };
    validate_content_type(headers, expected_type)?;
    Ok(Head::Body {
        cap: if success { cap } else { ERROR_CAP },
        instance,
        success,
    })
}

/// Completes an exchange from its collected body. Sets `definitive_rejection` only for a
/// validated 4xx error body, which proves the service rejected the request.
pub(crate) fn body(
    kind: ResponseKind,
    status: StatusCode,
    instance: Option<String>,
    success: bool,
    bytes: Vec<u8>,
    definitive_rejection: &mut bool,
) -> Result<Exchange, PreparationError> {
    if success {
        return Ok(Exchange {
            body: Some(bytes),
            instance,
        });
    }
    let error = validated_error(kind, status, &bytes)?;
    if status.is_client_error() {
        *definitive_rejection = true;
    }
    if status == StatusCode::NOT_FOUND && kind.optional() {
        return Ok(Exchange {
            body: None,
            instance,
        });
    }
    Err(error)
}

/// Applies the mutation outcome rule: after dispatch, only a definitive rejection is
/// definitive. Every other record-mutation failure may have committed.
#[cfg(feature = "record-wire")]
pub(crate) fn finish(
    kind: ResponseKind,
    result: Result<Exchange, PreparationError>,
    definitive_rejection: bool,
) -> Result<Exchange, PreparationError> {
    if matches!(kind, ResponseKind::RecordMutation(_)) && !definitive_rejection {
        return result.map_err(|_error| PreparationError::MutationUncertain);
    }
    result
}

/// Without record mutations there is no uncertain outcome to classify.
#[cfg(not(feature = "record-wire"))]
pub(crate) fn finish(
    _kind: ResponseKind,
    result: Result<Exchange, PreparationError>,
    _definitive_rejection: bool,
) -> Result<Exchange, PreparationError> {
    result
}

fn validated_instance(
    headers: &HeaderMap,
    kind: ResponseKind,
    method: &Method,
    status: StatusCode,
    success: bool,
) -> Result<Option<String>, PreparationError> {
    validate_headers(headers)?;
    #[cfg(feature = "record-wire")]
    if kind.record() {
        let mut versions = headers.get_all(RECORD_HEADER).iter();
        if versions.next().is_none_or(|value| value.as_bytes() != b"1") || versions.next().is_some()
        {
            return Err(PreparationError::UnsupportedVersion);
        }
    }
    let observed = instance(headers)?;
    if let Some(expected) = kind.expected_instance()
        && observed
            .as_ref()
            .is_some_and(|value| value.as_bytes() != expected)
    {
        return Err(PreparationError::InstanceChanged);
    }
    let logical_miss =
        status == StatusCode::NOT_FOUND && (*method == Method::HEAD || kind.optional());
    if (success || logical_miss || (kind.record() && status == StatusCode::PRECONDITION_FAILED))
        && observed.is_none()
    {
        return Err(PreparationError::Malformed);
    }
    Ok(observed)
}

fn validated_error(
    kind: ResponseKind,
    status: StatusCode,
    bytes: &[u8],
) -> Result<PreparationError, PreparationError> {
    if status == StatusCode::PRECONDITION_FAILED && !kind.record() {
        return Err(PreparationError::Malformed);
    }
    let parsed: ErrorBody =
        serde_json::from_slice(bytes).map_err(|_error| PreparationError::Malformed)?;
    #[cfg(feature = "record-wire")]
    let record_error = match (kind, status, parsed.error.as_str()) {
        (
            ResponseKind::RecordMutation(_),
            StatusCode::PRECONDITION_FAILED,
            "precondition_failed",
        ) => Some(PreparationError::PreconditionFailed),
        (_, StatusCode::SERVICE_UNAVAILABLE, "instance_changed") if kind.record() => {
            Some(PreparationError::InstanceChanged)
        }
        _ => None,
    };
    #[cfg(not(feature = "record-wire"))]
    let record_error = None;
    let error = if let Some(error) = record_error {
        error
    } else {
        let (expected, error) = status_error(status).ok_or(PreparationError::Malformed)?;
        if parsed.error != expected {
            return Err(PreparationError::Malformed);
        }
        error
    };

    Ok(error)
}

/// A fresh 128-bit mutation ID. It is never reused, including after a failure.
fn mutation_id() -> Result<HeaderValue, PreparationError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|_error| PreparationError::Unavailable)?;
    HeaderValue::from_str(&URL_SAFE_NO_PAD.encode(bytes))
        .map_err(|_error| PreparationError::Unavailable)
}

fn instance(headers: &HeaderMap) -> Result<Option<String>, PreparationError> {
    let mut values = headers.get_all(INSTANCE_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    let bytes = value.as_bytes();
    if values.next().is_some()
        || bytes.len() != 22
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(PreparationError::Malformed);
    }
    Ok(Some(
        String::from_utf8(bytes.to_vec()).map_err(|_error| PreparationError::Malformed)?,
    ))
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorBody {
    error: String,
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
