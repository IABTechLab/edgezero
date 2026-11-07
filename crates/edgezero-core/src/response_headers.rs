use std::fmt;
use std::sync::Arc;

use crate::http::{HeaderMap, HeaderName, HeaderValue};

/// Maximum synthesized field-section bytes: final CRLF plus name/value/`: `/CRLF per value.
pub const MAX_SYNTHESIZED_RESPONSE_HEADER_BYTES: usize = 4096;
/// Maximum synthesized field values, counting repeated names separately.
pub const MAX_SYNTHESIZED_RESPONSE_HEADER_COUNT: usize = 32;

/// Immutable, prevalidated decoration for framework-generated responses, not routed responses.
///
/// Values must be visible ASCII or interior HTAB, without leading/trailing whitespace. This
/// portable subset avoids provider conversion failures or host trimming during fallback delivery.
/// Clones share the bounded field map; debugging never exposes configured values.
#[derive(Clone, Default)]
pub struct SynthesizedResponseHeaders {
    fields: Option<Arc<HeaderMap>>,
}

/// Payload-free construction failure; invalid sets never replace an installed valid snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum SynthesizedResponseHeadersError {
    #[error("synthesized response headers contain a forbidden field")]
    ForbiddenName,
    #[error("synthesized response headers contain a nonportable value")]
    NonPortableValue,
    #[error("synthesized response headers exceed the byte limit")]
    TooManyBytes,
    #[error("synthesized response headers exceed the field limit")]
    TooManyFields,
}

impl fmt::Debug for SynthesizedResponseHeaders {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SynthesizedResponseHeaders")
            .field(
                "field_count",
                &self.fields.as_ref().map_or(0, |fields| fields.len()),
            )
            .finish_non_exhaustive()
    }
}

impl SynthesizedResponseHeaders {
    /// Adds absent names without replacing a response-owned name or losing repeated values.
    #[doc(hidden)]
    #[inline]
    pub fn apply(&self, target: &mut HeaderMap) {
        let Some(fields) = &self.fields else {
            return;
        };
        for name in fields.keys() {
            if !target.contains_key(name) {
                for value in fields.get_all(name) {
                    target.append(name.clone(), value.clone());
                }
            }
        }
    }

    /// Validates a complete replacement before it can be installed on an app or request.
    ///
    /// # Errors
    /// Returns a fixed category for forbidden framing/coding fields, nonportable values, or
    /// either inclusive bound. Field names are already syntax-validated by `HeaderMap`.
    #[inline]
    pub fn new(fields: HeaderMap) -> Result<Self, SynthesizedResponseHeadersError> {
        if fields.is_empty() {
            return Ok(Self::default());
        }
        if fields.len() > MAX_SYNTHESIZED_RESPONSE_HEADER_COUNT {
            return Err(SynthesizedResponseHeadersError::TooManyFields);
        }
        let mut bytes = 2_usize;
        for (name, value) in &fields {
            if matches!(
                name.as_str(),
                "content-length"
                    | "transfer-encoding"
                    | "trailer"
                    | "te"
                    | "connection"
                    | "keep-alive"
                    | "proxy-connection"
                    | "upgrade"
                    | "proxy-authenticate"
                    | "proxy-authorization"
                    | "content-type"
                    | "content-encoding"
            ) {
                return Err(SynthesizedResponseHeadersError::ForbiddenName);
            }
            let value_bytes = value.as_bytes();
            if value_bytes
                .iter()
                .any(|byte| !matches!(*byte, b'\t' | b' '..=b'~'))
                || value_bytes
                    .first()
                    .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
                || value_bytes
                    .last()
                    .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
            {
                return Err(SynthesizedResponseHeadersError::NonPortableValue);
            }
            bytes = bytes
                .checked_add(name.as_str().len())
                .and_then(|sum| sum.checked_add(value_bytes.len()))
                .and_then(|sum| sum.checked_add(4))
                .filter(|sum| *sum <= MAX_SYNTHESIZED_RESPONSE_HEADER_BYTES)
                .ok_or(SynthesizedResponseHeadersError::TooManyBytes)?;
        }
        // Detach caller-owned spare capacity and shared backing allocations before retention.
        let mut compact = HeaderMap::with_capacity(fields.len());
        for (name, value) in &fields {
            let copied_name = HeaderName::from_bytes(name.as_str().as_bytes())
                .map_err(|_invalid_name| SynthesizedResponseHeadersError::ForbiddenName)?;
            let mut copied_value = HeaderValue::from_bytes(value.as_bytes())
                .map_err(|_invalid_value| SynthesizedResponseHeadersError::NonPortableValue)?;
            copied_value.set_sensitive(value.is_sensitive());
            compact.append(copied_name, copied_value);
        }
        drop(fields);
        Ok(Self {
            fields: Some(Arc::new(compact)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{HeaderMap, HeaderName, HeaderValue};

    #[test]
    fn synthesized_header_bounds_count_every_value_and_wire_delimiter() {
        for count in [31_usize, 32, 33] {
            let mut headers = HeaderMap::new();
            for _ in 0..count {
                headers.append("x", HeaderValue::from_static("value"));
            }
            let result = SynthesizedResponseHeaders::new(headers);
            if count > 32 {
                assert_eq!(
                    result.err(),
                    Some(SynthesizedResponseHeadersError::TooManyFields)
                );
            } else {
                result.expect("within count limit");
            }
        }
        for bytes in [4095, 4096, 4097] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x",
                HeaderValue::from_str(&"a".repeat(bytes - 7)).expect("value"),
            );
            let result = SynthesizedResponseHeaders::new(headers);
            if bytes > 4096 {
                assert_eq!(
                    result.err(),
                    Some(SynthesizedResponseHeadersError::TooManyBytes)
                );
            } else {
                result.expect("within byte limit");
            }
        }
        for bytes in [4095_usize, 4096, 4097] {
            let mut headers = HeaderMap::new();
            headers.append("longer-name", HeaderValue::from_static("first"));
            headers.append(
                "longer-name",
                HeaderValue::from_str(&"a".repeat(bytes - 37)).expect("value"),
            );
            let result = SynthesizedResponseHeaders::new(headers);
            if bytes > 4096 {
                assert_eq!(
                    result.err(),
                    Some(SynthesizedResponseHeadersError::TooManyBytes)
                );
            } else {
                result.expect("within aggregate byte limit");
            }
        }
    }

    #[test]
    fn synthesized_headers_compact_retained_storage_and_accept_portable_values() {
        let mut fields = HeaderMap::with_capacity(10000);
        fields.insert("x-empty", HeaderValue::from_static(""));
        fields.insert("x-tab", HeaderValue::from_static("a\tb"));
        let set = SynthesizedResponseHeaders::new(fields).expect("portable values");
        assert!(set.fields.as_ref().expect("nonempty set").capacity() < 100);
        let mut output = HeaderMap::new();
        set.apply(&mut output);
        assert_eq!(output["x-empty"], "");
        assert_eq!(output["x-tab"], "a\tb");
    }

    #[test]
    fn synthesized_headers_reject_framing_and_nonportable_values() {
        for name in [
            "Content-Length",
            "Transfer-Encoding",
            "Trailer",
            "TE",
            "Connection",
            "Keep-Alive",
            "Proxy-Connection",
            "Upgrade",
            "Proxy-Authenticate",
            "Proxy-Authorization",
            "Content-Type",
            "Content-Encoding",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).expect("name"),
                HeaderValue::from_static("x"),
            );
            assert_eq!(
                SynthesizedResponseHeaders::new(headers).err(),
                Some(SynthesizedResponseHeadersError::ForbiddenName)
            );
        }
        for value in [b"\x80".as_slice(), b" leading", b"trailing\t"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x",
                HeaderValue::from_bytes(value).expect("syntactically valid field"),
            );
            assert_eq!(
                SynthesizedResponseHeaders::new(headers).err(),
                Some(SynthesizedResponseHeadersError::NonPortableValue)
            );
        }
        for value in [b"\x7f".as_slice(), b"\r", b"\n", b"\0"] {
            HeaderValue::from_bytes(value).expect_err("invalid field bytes");
        }
    }

    #[test]
    fn synthesized_headers_preserve_existing_names_and_repeated_value_order() {
        let mut configured = HeaderMap::new();
        configured.append("set-cookie", HeaderValue::from_static("first=1"));
        configured.append("set-cookie", HeaderValue::from_static("second=2"));
        configured.insert("allow", HeaderValue::from_static("POST"));
        configured.insert("cache-control", HeaderValue::from_static("public"));
        let set = SynthesizedResponseHeaders::new(configured).expect("validated");
        let mut target = HeaderMap::new();
        target.insert("allow", HeaderValue::from_static("GET"));
        target.insert("cache-control", HeaderValue::from_static("private"));
        set.apply(&mut target);
        assert_eq!(target["allow"], "GET");
        assert_eq!(target["cache-control"], "private");
        assert_eq!(target.get_all("allow").iter().count(), 1);
        assert_eq!(target.get_all("cache-control").iter().count(), 1);
        assert_eq!(
            target.get_all("set-cookie").iter().collect::<Vec<_>>(),
            [
                &HeaderValue::from_static("first=1"),
                &HeaderValue::from_static("second=2")
            ]
        );
        set.apply(&mut target);
        assert_eq!(target.get_all("set-cookie").iter().count(), 2);
        assert_eq!(target.get_all("allow").iter().count(), 1);
        assert_eq!(target.get_all("cache-control").iter().count(), 1);
        assert!(!format!("{set:?}").contains("first=1"));
    }
}
