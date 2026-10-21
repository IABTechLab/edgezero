//! Fixed-size, text-free provider diagnostics, separate from public error classification.
//!
//! Catalogs describe the pinned SDKs, not a claim that an adapter already populates every field.
//! Provider mappers must project only these enums and numeric payloads, never formatted errors.

use std::io::ErrorKind;

/// Reqwest's primary error kind; timeout/connect evidence is retained separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AxumFailureKind {
    Body,
    Builder,
    Decode,
    Redirect,
    Request,
    Status,
    Unknown,
    Upgrade,
}

/// Predicate and I/O evidence available from reqwest without copying its URL or text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AxumFailureCause {
    pub io_kind: Option<ErrorKind>,
    pub is_connect: bool,
    pub is_timeout: bool,
    pub kind: AxumFailureKind,
}

/// Allowlisted JavaScript exception names; arbitrary names and messages are not retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CloudflareFailureCause {
    AbortError,
    TypeError,
    Unknown,
}

/// Fastly 0.13.1 send/backend causes, with only safe numeric or I/O-kind payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FastlyFailureCause {
    BackendBetweenBytesTimeoutTooLarge,
    BackendConnectTimeoutTooLarge,
    BackendDisallowed,
    BackendEncodingError,
    BackendFirstByteTimeoutTooLarge,
    BackendHostError {
        status: i32,
    },
    BackendInvalidHealthcheckValue,
    BackendNameInUse,
    BackendNameTooLong,
    ConnectionLimitReached,
    ConnectionRefused,
    ConnectionTerminated,
    ConnectionTimeout,
    Custom,
    DestinationIpUnroutable,
    DestinationNotFound,
    DestinationUnavailable,
    DnsError {
        rcode: Option<u16>,
        info_code: Option<u16>,
    },
    DnsTimeout,
    FanoutNotEnabled,
    Http2StreamError {
        frame_type: u8,
        error_code: u32,
    },
    HttpCacheApiUnsupported,
    HttpCacheLimitExceeded,
    HttpIncompleteResponse,
    HttpProtocolError,
    HttpRequestCacheKeyInvalid,
    HttpRequestUriInvalid,
    HttpResponseBodyTooLarge,
    HttpResponseHeaderSectionTooLarge,
    HttpResponseStatusInvalid,
    HttpResponseTimeout,
    HttpUpgradeFailed,
    ImageOptimizerUnsupported,
    InternalError {
        status: Option<i32>,
    },
    IoError {
        kind: ErrorKind,
    },
    RequestCollapse,
    TlsAlertReceived {
        alert_id: Option<u8>,
    },
    TlsCertificateError,
    TlsConfigurationError,
    TlsProtocolError,
    Unknown,
}

/// Adapter identity derived from the provider-specific diagnostic variant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProviderFailureAdapter {
    Axum,
    Cloudflare,
    Fastly,
    Spin,
}

/// Provider detail contains no free text, SDK object, URL, header, or body copy.
///
/// The provider identity and cause cannot disagree. Numeric unknown values are preserved;
/// unknown textual values must project to a closed `Unknown` variant.
///
/// ```compile_fail
/// use edgezero_core::error_detail::FastlyFailureCause;
/// let _ = FastlyFailureCause::TlsAlertReceived {
///     alert_id: Some(42),
///     alert_message: "provider-controlled secret".to_owned(),
/// };
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProviderFailureDetail {
    Axum {
        phase: ProviderFailurePhase,
        cause: AxumFailureCause,
    },
    Cloudflare {
        phase: ProviderFailurePhase,
        cause: CloudflareFailureCause,
    },
    Fastly {
        phase: ProviderFailurePhase,
        cause: FastlyFailureCause,
    },
    Spin {
        phase: ProviderFailurePhase,
        cause: SpinFailureCause,
    },
}

impl ProviderFailureDetail {
    #[must_use]
    #[inline]
    pub const fn adapter(self) -> ProviderFailureAdapter {
        match self {
            Self::Axum { .. } => ProviderFailureAdapter::Axum,
            Self::Cloudflare { .. } => ProviderFailureAdapter::Cloudflare,
            Self::Fastly { .. } => ProviderFailureAdapter::Fastly,
            Self::Spin { .. } => ProviderFailureAdapter::Spin,
        }
    }

    #[must_use]
    #[inline]
    pub const fn phase(self) -> ProviderFailurePhase {
        match self {
            Self::Axum { phase, .. }
            | Self::Cloudflare { phase, .. }
            | Self::Fastly { phase, .. }
            | Self::Spin { phase, .. } => phase,
        }
    }
}

/// The provider boundary at which the original failure was observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProviderFailurePhase {
    BackendRegistration,
    RequestBodyUpload,
    ResponseBodyRead,
    ResponseHeaders,
    Send,
}

/// Exact allowlist for Spin's text-only DNS rcode. Missing values remain `None` in the cause.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SpinDnsRcode {
    FormatError,
    NameError,
    NoError,
    NotImplemented,
    Refused,
    ServerFailure,
    Unknown,
}

impl SpinDnsRcode {
    /// Projects only NOERROR, FORMERR, SERVFAIL, NXDOMAIN, NOTIMP and REFUSED.
    /// Other spellings, numeric strings and provider text are deliberately unknown.
    #[must_use]
    #[inline]
    pub fn from_provider_text(text: &str) -> Self {
        match text {
            "NOERROR" => Self::NoError,
            "FORMERR" => Self::FormatError,
            "SERVFAIL" => Self::ServerFailure,
            "NXDOMAIN" => Self::NameError,
            "NOTIMP" => Self::NotImplemented,
            "REFUSED" => Self::Refused,
            _ => Self::Unknown,
        }
    }
}

/// Spin SDK 7 / WASI HTTP 0.3 causes, without coding strings, field names or error messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SpinFailureCause {
    ConfigurationError,
    ConnectionLimitReached,
    ConnectionReadTimeout,
    ConnectionRefused,
    ConnectionTerminated,
    ConnectionTimeout,
    ConnectionWriteTimeout,
    DestinationIpProhibited,
    DestinationIpUnroutable,
    DestinationNotFound,
    DestinationUnavailable,
    DnsError {
        rcode: Option<SpinDnsRcode>,
        info_code: Option<u16>,
    },
    DnsTimeout,
    HttpProtocolError,
    HttpRequestBodySize(Option<u64>),
    HttpRequestDenied,
    HttpRequestHeaderSectionSize(Option<u32>),
    HttpRequestHeaderSize(Option<SpinFieldSize>),
    HttpRequestLengthRequired,
    HttpRequestMethodInvalid,
    HttpRequestTrailerSectionSize(Option<u32>),
    HttpRequestTrailerSize(SpinFieldSize),
    HttpRequestUriInvalid,
    HttpRequestUriTooLong,
    HttpResponseBodySize(Option<u64>),
    HttpResponseContentCoding,
    HttpResponseHeaderSectionSize(Option<u32>),
    HttpResponseHeaderSize(SpinFieldSize),
    HttpResponseIncomplete,
    HttpResponseTimeout,
    HttpResponseTrailerSectionSize(Option<u32>),
    HttpResponseTrailerSize(SpinFieldSize),
    HttpResponseTransferCoding,
    HttpUpgradeFailed,
    InternalError,
    LoopDetected,
    TlsAlertReceived {
        alert_id: Option<u8>,
    },
    TlsCertificateError,
    TlsProtocolError,
    Unknown,
}

/// Sanitized field-size payload. The optional number is retained, never the provider's name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpinFieldSize {
    pub field_size: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn provider_detail_is_copy_and_fixed_size() {
        fn assert_copy<T>()
        where
            T: Copy,
        {
        }
        assert_copy::<ProviderFailureDetail>();
        assert!(size_of::<ProviderFailureDetail>() <= 48);
        for (detail, adapter, phase) in [
            (
                ProviderFailureDetail::Axum {
                    phase: ProviderFailurePhase::Send,
                    cause: AxumFailureCause {
                        io_kind: Some(ErrorKind::ConnectionRefused),
                        is_connect: true,
                        is_timeout: false,
                        kind: AxumFailureKind::Request,
                    },
                },
                ProviderFailureAdapter::Axum,
                ProviderFailurePhase::Send,
            ),
            (
                ProviderFailureDetail::Cloudflare {
                    phase: ProviderFailurePhase::Send,
                    cause: CloudflareFailureCause::TypeError,
                },
                ProviderFailureAdapter::Cloudflare,
                ProviderFailurePhase::Send,
            ),
            (
                ProviderFailureDetail::Fastly {
                    phase: ProviderFailurePhase::ResponseBodyRead,
                    cause: FastlyFailureCause::IoError {
                        kind: ErrorKind::UnexpectedEof,
                    },
                },
                ProviderFailureAdapter::Fastly,
                ProviderFailurePhase::ResponseBodyRead,
            ),
            (
                ProviderFailureDetail::Spin {
                    phase: ProviderFailurePhase::ResponseHeaders,
                    cause: SpinFailureCause::TlsAlertReceived {
                        alert_id: Some(u8::MAX),
                    },
                },
                ProviderFailureAdapter::Spin,
                ProviderFailurePhase::ResponseHeaders,
            ),
        ] {
            let copied = detail;
            assert_eq!(copied, detail);
            assert_eq!(copied.adapter(), adapter);
            assert_eq!(copied.phase(), phase);
        }
    }

    #[test]
    fn spin_dns_text_projection_is_closed_and_does_not_retain_input() {
        for (text, expected) in [
            ("NOERROR", SpinDnsRcode::NoError),
            ("FORMERR", SpinDnsRcode::FormatError),
            ("SERVFAIL", SpinDnsRcode::ServerFailure),
            ("NXDOMAIN", SpinDnsRcode::NameError),
            ("NOTIMP", SpinDnsRcode::NotImplemented),
            ("REFUSED", SpinDnsRcode::Refused),
        ] {
            assert_eq!(SpinDnsRcode::from_provider_text(text), expected);
        }
        for text in [
            "nxdomain",
            "NXDOMAIN ",
            "3",
            "https://example.com/?token=secret",
            "\r\nsecret",
        ] {
            let projected = SpinDnsRcode::from_provider_text(text);
            assert_eq!(projected, SpinDnsRcode::Unknown);
            assert!(!format!("{projected:?}").contains("secret"));
        }
    }
}
