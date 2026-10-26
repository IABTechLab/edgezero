//! Confidential adapter-owned ingress facts and explicit preservation limits.
//!
//! Metadata describes what a runtime exposes; it cannot recover information
//! erased before conversion. Captured values are not serialized, and Debug
//! implementations omit targets and authorities because they can contain private data.

use std::fmt;
use std::io::Error as IoError;
use std::net::Ipv6Addr;
use std::sync::Arc;

use http::uri::Authority;

use crate::error::EdgeError;
use crate::http::HeaderName;

/// Maximum complete target metadata, measured in UTF-8 bytes.
pub const MAX_TARGET_BYTES: usize = 16 * 1024;

/// Maximum inbound authority metadata, measured in UTF-8 bytes.
pub const MAX_AUTHORITY_BYTES: usize = 1024;

/// A complete bounded target or an explicit acquisition failure.
#[derive(Clone)]
pub enum CapturedTarget {
    /// A complete value created only by the bounded capture constructor.
    Complete(CompleteTarget),
    /// No complete target is available.
    Unavailable(TargetUnavailable),
}

impl CapturedTarget {
    /// Capture a whole runtime target without retaining an oversized prefix.
    #[inline]
    #[must_use]
    pub fn capture(value: &str, source: TargetSource, fidelity: Preservation) -> Self {
        if value.len() > MAX_TARGET_BYTES {
            return Self::Unavailable(TargetUnavailable::TooLarge);
        }
        Self::Complete(CompleteTarget {
            fidelity,
            source,
            value: Arc::from(value),
        })
    }
}

/// A complete bounded target with explicit provenance.
///
/// Fields remain private so callers cannot bypass the capture bound.
///
/// ```compile_fail
/// use edgezero_core::ingress::{CompleteTarget, Preservation, TargetSource};
/// let target = CompleteTarget {
///     value: std::sync::Arc::from("http://example.com/"),
///     source: TargetSource::RuntimeUrl,
///     fidelity: Preservation::Unknown,
/// };
/// ```
#[derive(Clone)]
pub struct CompleteTarget {
    fidelity: Preservation,
    source: TargetSource,
    value: Arc<str>,
}

impl CompleteTarget {
    /// Return the established target preservation guarantee.
    #[inline]
    #[must_use]
    pub const fn fidelity(&self) -> Preservation {
        self.fidelity
    }

    /// Return the runtime acquisition source.
    #[inline]
    #[must_use]
    pub const fn source(&self) -> TargetSource {
        self.source
    }

    /// Borrow the complete captured target.
    #[inline]
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Independent original-field preservation facts.
///
/// These axes compare original HTTP field values with core headers; successful
/// runtime-to-core copying alone does not establish original-wire preservation.
/// `Default` leaves all axes unknown; `RUNTIME_HEADER_MAP` additionally states
/// that the converted map cannot supply global field order.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HeaderFidelity {
    field_multiplicity: Preservation,
    global_field_order: Preservation,
    octets: Preservation,
    same_name_order: Preservation,
}

impl HeaderFidelity {
    /// Conservative baseline for copied runtime headers: original octets,
    /// multiplicity and same-name order are unknown; global order is unavailable.
    pub const RUNTIME_HEADER_MAP: Self = Self::new(
        Preservation::Unknown,
        Preservation::Unknown,
        Preservation::Unknown,
        Preservation::Unavailable,
    );

    /// Return original field-count preservation for this header.
    #[inline]
    #[must_use]
    pub const fn field_multiplicity(&self) -> Preservation {
        self.field_multiplicity
    }

    /// Return original ordering across different field names.
    #[inline]
    #[must_use]
    pub const fn global_field_order(&self) -> Preservation {
        self.global_field_order
    }

    /// Construct four independent fidelity guarantees.
    #[inline]
    #[must_use]
    pub const fn new(
        octets: Preservation,
        field_multiplicity: Preservation,
        same_name_order: Preservation,
        global_field_order: Preservation,
    ) -> Self {
        Self {
            field_multiplicity,
            global_field_order,
            octets,
            same_name_order,
        }
    }

    /// Return original field-value byte preservation.
    #[inline]
    #[must_use]
    pub const fn octets(&self) -> Preservation {
        self.octets
    }

    /// Return original value ordering within a single field name.
    #[inline]
    #[must_use]
    pub const fn same_name_order(&self) -> Preservation {
        self.same_name_order
    }
}

/// Bounded validated origin facts established by the adapter.
///
/// Syntax validation does not make a forwarded header trustworthy. Adapters
/// must establish provenance before calling the constructor.
#[derive(Clone)]
pub struct InboundOrigin {
    authority: Authority,
    scheme: &'static str,
    source: OriginSource,
}

impl InboundOrigin {
    /// Borrow the validated authority in its source-provided spelling.
    ///
    /// Host case and explicit default ports are retained when the source exposes
    /// them, including leading zeros in explicit ports. Port zero is accepted
    /// syntactically; this does not make it a usable destination. Consumers must
    /// normalize authorities before comparing origins.
    #[inline]
    #[must_use]
    pub fn authority(&self) -> &str {
        self.authority.as_str()
    }

    /// Validate a bounded HTTP(S) scheme and authority.
    ///
    /// # Errors
    ///
    /// Returns a bounded validation error for invalid or oversized input.
    #[inline]
    pub fn parse(scheme: &str, authority: &str, source: OriginSource) -> Result<Self, EdgeError> {
        let normalized_scheme = if scheme.eq_ignore_ascii_case("http") {
            "http"
        } else if scheme.eq_ignore_ascii_case("https") {
            "https"
        } else {
            return Err(EdgeError::bad_request("invalid ingress origin"));
        };
        if authority.is_empty()
            || authority.len() > MAX_AUTHORITY_BYTES
            || authority
                .chars()
                .any(|character| matches!(character, '@' | '/' | '?' | '#' | '\\'))
        {
            return Err(EdgeError::bad_request("invalid ingress origin"));
        }
        let parsed: Authority = authority
            .parse()
            .map_err(|_error| EdgeError::bad_request("invalid ingress origin"))?;
        let host = parsed.host();
        if host.chars().any(|character| matches!(character, '[' | ']'))
            && host
                .strip_prefix('[')
                .and_then(|inner| inner.strip_suffix(']'))
                .is_none_or(|inner| inner.parse::<Ipv6Addr>().is_err())
        {
            return Err(EdgeError::bad_request("invalid ingress origin"));
        }
        let suffix = authority
            .strip_prefix(parsed.host())
            .ok_or_else(|| EdgeError::bad_request("invalid ingress origin"))?;
        if parsed.host().is_empty()
            || (!suffix.is_empty()
                && suffix.strip_prefix(':').is_none_or(|port| {
                    port.is_empty()
                        || !port.bytes().all(|byte| byte.is_ascii_digit())
                        || port.parse::<u16>().is_err()
                }))
        {
            return Err(EdgeError::bad_request("invalid ingress origin"));
        }
        Ok(Self {
            authority: parsed,
            scheme: normalized_scheme,
            source,
        })
    }

    /// Compare hosts case-insensitively and ports using each scheme's default.
    ///
    /// This compares authorities, not full origins: the schemes may differ.
    #[inline]
    #[must_use]
    pub fn same_authority(&self, other: &Self) -> bool {
        let port = |origin: &Self| {
            origin
                .authority
                .port_u16()
                .unwrap_or(if origin.scheme == "https" { 443 } else { 80 })
        };
        self.authority
            .host()
            .eq_ignore_ascii_case(other.authority.host())
            && port(self) == port(other)
    }

    /// Return the normalized HTTP(S) scheme.
    #[inline]
    #[must_use]
    pub const fn scheme(&self) -> &str {
        self.scheme
    }

    /// Return the adapter-owned origin acquisition source.
    #[inline]
    #[must_use]
    pub const fn source(&self) -> OriginSource {
        self.source
    }
}

/// Provenance of a validated inbound origin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OriginSource {
    /// Scheme and authority supplied by the runtime request URI.
    ///
    /// Runtimes typically derive authority from the client's Host or :authority.
    /// It identifies the requested origin; it does not establish that an operator
    /// approves or owns that host.
    RuntimeUri,
    /// A transport scheme established by the adapter's listener, combined with
    /// one validated inbound request authority.
    ///
    /// The authority identifies the requested origin; it does not establish
    /// that an operator approves or owns that host.
    TransportBinding,
}

/// Degree of preservation established for a particular fact.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum Preservation {
    /// Pinned source and transport evidence establish equality.
    Preserved,
    /// A known transformation changed the original information.
    Transformed,
    /// The runtime does not expose the required information.
    Unavailable,
    /// Preservation has not been established.
    #[default]
    Unknown,
}

/// Confidential adapter-owned metadata stored in request extensions.
///
/// Absence means unknown capability, not complete preservation. Existing
/// headers remain in the request `HeaderMap` and are never duplicated here.
#[derive(Clone)]
pub struct RequestIngress {
    common_header_fidelity: HeaderFidelity,
    origin: Option<InboundOrigin>,
    overrides: Vec<(HeaderName, HeaderFidelity)>,
    target: CapturedTarget,
}

impl RequestIngress {
    /// Return a header-specific guarantee or the common conservative default.
    #[inline]
    #[must_use]
    pub fn header_fidelity(&self, name: &HeaderName) -> HeaderFidelity {
        self.overrides
            .iter()
            .find_map(|(candidate, fidelity)| (candidate == name).then_some(*fidelity))
            .unwrap_or(self.common_header_fidelity)
    }

    /// Construct metadata without header-specific overrides.
    ///
    /// Sources and preservation guarantees are caller assertions, not validated
    /// provenance or fidelity of captured facts.
    #[inline]
    #[must_use]
    pub fn new(
        target: CapturedTarget,
        origin: Option<InboundOrigin>,
        common_header_fidelity: HeaderFidelity,
    ) -> Self {
        Self {
            common_header_fidelity,
            origin,
            overrides: Vec::new(),
            target,
        }
    }

    /// Borrow the validated origin, when an adapter-established source is available.
    #[inline]
    #[must_use]
    pub const fn origin(&self) -> Option<&InboundOrigin> {
        self.origin.as_ref()
    }

    /// Borrow complete target metadata or its explicit unavailability reason.
    #[inline]
    #[must_use]
    pub const fn target(&self) -> &CapturedTarget {
        &self.target
    }

    /// Set unambiguous per-header fidelity overrides.
    ///
    /// # Errors
    /// Returns an internal error for duplicate override names, a caller mistake.
    #[inline]
    pub fn with_header_overrides(
        mut self,
        mut overrides: Vec<(HeaderName, HeaderFidelity)>,
    ) -> Result<Self, EdgeError> {
        overrides.sort_unstable_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        if overrides
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left.0 == right.0))
        {
            return Err(EdgeError::internal(IoError::other(
                "duplicate ingress header override",
            )));
        }
        self.overrides = overrides;
        Ok(self)
    }
}

impl fmt::Debug for CapturedTarget {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Complete(target) => f.debug_tuple("Complete").field(target).finish(),
            Self::Unavailable(reason) => f.debug_tuple("Unavailable").field(reason).finish(),
        }
    }
}

impl fmt::Debug for CompleteTarget {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompleteTarget")
            .field("source", &self.source)
            .field("fidelity", &self.fidelity)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for InboundOrigin {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboundOrigin")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for RequestIngress {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestIngress")
            .field("common_header_fidelity", &self.common_header_fidelity)
            .finish_non_exhaustive()
    }
}

/// Source of captured target information, independent of fidelity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TargetSource {
    /// A runtime path/query before additional `EdgeZero` parsing.
    RuntimePathAndQuery,
    /// A runtime-exposed URL; earlier normalization may have occurred.
    RuntimeUrl,
    /// A transport-exposed request target with evidence of its provenance.
    TransportRequestTarget,
}

/// Why no complete target was retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TargetUnavailable {
    /// The runtime offers no safe acquisition API.
    NotExposed,
    /// Acquisition failed without retaining error text.
    ReadFailed,
    /// The target exceeded the metadata bound.
    TooLarge,
}

#[cfg(test)]
mod tests {
    use crate::http::StatusCode;
    use crate::http::header::{CONTENT_LENGTH, COOKIE};
    use crate::ingress::{
        CapturedTarget, HeaderFidelity, InboundOrigin, OriginSource, Preservation, RequestIngress,
        TargetSource, TargetUnavailable,
    };

    #[test]
    fn ingress_fidelity_defaults_are_unknown() {
        let fidelity = HeaderFidelity::default();
        assert_eq!(fidelity.octets(), Preservation::Unknown);
        assert_eq!(fidelity.field_multiplicity(), Preservation::Unknown);
        assert_eq!(fidelity.same_name_order(), Preservation::Unknown);
        assert_eq!(fidelity.global_field_order(), Preservation::Unknown);
    }

    #[test]
    fn ingress_header_override_is_field_specific() {
        let common = HeaderFidelity::default();
        let folded = HeaderFidelity::new(
            Preservation::Unknown,
            Preservation::Transformed,
            Preservation::Unknown,
            Preservation::Unavailable,
        );
        let metadata = RequestIngress::new(
            CapturedTarget::Unavailable(TargetUnavailable::NotExposed),
            None,
            common,
        )
        .with_header_overrides(vec![(CONTENT_LENGTH, folded)])
        .expect("should accept distinct header overrides");
        assert_eq!(metadata.header_fidelity(&CONTENT_LENGTH), folded);
        assert_eq!(metadata.header_fidelity(&COOKIE), common);
        assert!(metadata.origin().is_none());
    }

    #[test]
    fn ingress_origin_accepts_bounded_http_authorities() {
        for (scheme, authority) in [
            ("HTTP", "example.com"),
            ("https", "example.com:443"),
            ("http", "[::1]:8787"),
        ] {
            let origin = InboundOrigin::parse(scheme, authority, OriginSource::TransportBinding)
                .expect("should accept a validated HTTP origin");
            assert_eq!(origin.scheme(), scheme.to_ascii_lowercase());
            assert_eq!(origin.authority(), authority);
            assert_eq!(origin.source(), OriginSource::TransportBinding);
        }
    }

    #[test]
    fn ingress_origin_rejects_untrusted_or_invalid_input() {
        for (scheme, authority) in [
            ("ftp", "example.com"),
            ("https", "user@example.com"),
            ("https", "example.com/path"),
            ("https", "example.com?secret=1"),
            ("https", "example.com#fragment"),
            ("https", "example.com:invalid"),
            ("https", "example.com:65536"),
            ("https", "example.com:+443"),
            ("https", "[::1]:+443"),
            ("https", "[]"),
            ("https", "[not-an-ip]"),
            ("https", "example[bad]"),
            ("https", ""),
            ("https", "example.com\\evil"),
        ] {
            assert!(
                InboundOrigin::parse(scheme, authority, OriginSource::RuntimeUri)
                    .err()
                    .is_some(),
                "should reject malformed authority without retaining it",
            );
        }
        assert!(
            InboundOrigin::parse("https", &"a".repeat(1025), OriginSource::RuntimeUri)
                .err()
                .is_some(),
            "should bound origin metadata",
        );
    }

    #[test]
    fn ingress_rejects_duplicate_overrides() {
        let result = RequestIngress::new(
            CapturedTarget::Unavailable(TargetUnavailable::ReadFailed),
            None,
            HeaderFidelity::default(),
        )
        .with_header_overrides(vec![
            (COOKIE, HeaderFidelity::default()),
            (COOKIE, HeaderFidelity::default()),
        ]);
        assert_eq!(
            result
                .expect_err("should reject ambiguous overrides")
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn ingress_debug_never_prints_captured_values() {
        let target = CapturedTarget::capture(
            "/private-target?secret=1",
            TargetSource::RuntimeUrl,
            Preservation::Unknown,
        );
        let CapturedTarget::Complete(complete) = &target else {
            panic!("should capture target")
        };
        let origin =
            InboundOrigin::parse("https", "private-host.example", OriginSource::RuntimeUri)
                .expect("should parse origin");
        let values_output = format!("{target:?} {complete:?} {origin:?}");
        let ingress = RequestIngress::new(target, Some(origin), HeaderFidelity::default());
        let output = format!("{values_output} {ingress:?}");
        assert!(!output.contains("private-target"));
        assert!(!output.contains("secret=1"));
        assert!(!output.contains("private-host"));
    }

    #[test]
    fn ingress_authorities_compare_host_and_effective_port() {
        for (scheme, left, right, same) in [
            ("http", "Example.COM", "example.com:00080", true),
            ("https", "Example.COM", "example.com:443", true),
            ("https", "example.com", "example.com:80", false),
            ("http", "[::1]", "[::1]:80", true),
            ("http", "example.com", "other.example", false),
            ("http", "example.com:0", "example.com:80", false),
        ] {
            let left_origin = InboundOrigin::parse(scheme, left, OriginSource::RuntimeUri)
                .expect("should parse origin");
            let right_origin = InboundOrigin::parse(scheme, right, OriginSource::TransportBinding)
                .expect("should parse origin");
            assert_eq!(left_origin.same_authority(&right_origin), same);
        }
    }

    #[test]
    fn ingress_target_bound_is_atomic() {
        let mut value = "a".repeat(16 * 1024);
        let target =
            CapturedTarget::capture(&value, TargetSource::RuntimeUrl, Preservation::Transformed);
        assert!(
            matches!(&target, CapturedTarget::Complete(complete) if complete.value() == value),
            "should preserve the entire value at the bound",
        );
        value.push('b');
        assert!(
            matches!(
                CapturedTarget::capture(&value, TargetSource::RuntimeUrl, Preservation::Unknown,),
                CapturedTarget::Unavailable(TargetUnavailable::TooLarge),
            ),
            "should retain no truncated prefix",
        );
    }

    #[test]
    fn ingress_target_counts_utf8_bytes() {
        let value = "\u{e9}".repeat(8193);
        assert!(
            matches!(
                CapturedTarget::capture(&value, TargetSource::RuntimeUrl, Preservation::Unknown,),
                CapturedTarget::Unavailable(TargetUnavailable::TooLarge),
            ),
            "should bound bytes rather than Unicode characters",
        );
    }
}
