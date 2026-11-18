//! Portable client network metadata captured once by a managed adapter.
//!
//! A [`ClientInfo`] snapshot separates the direct transport peer from the
//! effective client the adapter accepted, and records the viewer's negotiated
//! TLS protocol and cipher where a trusted runtime reports them. Every field
//! is either captured with its source or unavailable with a reason.
//!
//! `Debug` output never contains captured addresses or TLS values.
//!
//! ```
//! use edgezero_core::client_info::{
//!     Capture, ClientAddress, ClientInfo, ClientIpSource, UnavailableReason,
//! };
//!
//! let ip = "203.0.113.7".parse().unwrap();
//! let info = ClientInfo::new(
//!     Capture::Unavailable(UnavailableReason::NotExposed),
//!     Capture::Captured(ClientAddress::new(ip, ClientIpSource::Fastly)),
//!     Capture::Unavailable(UnavailableReason::Missing),
//!     Capture::Unavailable(UnavailableReason::Missing),
//! );
//! assert_eq!(info.effective_client_ip(), Some(ip));
//! assert!(!format!("{info:?}").contains("203.0.113.7"));
//! ```

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::str::Utf8Error;

/// Why a field has no captured value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnavailableReason {
    /// A supported source supplied no value for this request.
    Missing,
    /// The adapter or its configured path cannot supply this field.
    NotExposed,
    /// A candidate value could not be accepted, for example because it was malformed.
    Rejected,
}

/// One captured fact, or the reason it is unavailable.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Capture<T> {
    /// The adapter captured this value.
    Captured(T),
    /// No value is available.
    Unavailable(UnavailableReason),
}

impl<T> Capture<T> {
    /// The captured value, if any.
    #[inline]
    #[must_use]
    pub const fn captured(&self) -> Option<&T> {
        match self {
            Self::Captured(value) => Some(value),
            Self::Unavailable(_) => None,
        }
    }

    /// The reason no value is available, if any.
    #[inline]
    #[must_use]
    pub const fn unavailable_reason(&self) -> Option<UnavailableReason> {
        match self {
            Self::Captured(_) => None,
            Self::Unavailable(reason) => Some(*reason),
        }
    }
}

/// The transport peer's socket address. `Debug` is redacted.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PeerAddress(SocketAddr);

impl PeerAddress {
    /// The peer's IP address.
    #[inline]
    #[must_use]
    pub const fn ip(&self) -> IpAddr {
        self.0.ip()
    }

    /// Wrap a socket address the adapter observed.
    #[inline]
    #[must_use]
    pub const fn new(address: SocketAddr) -> Self {
        Self(address)
    }

    /// The peer's socket address, including its port.
    #[inline]
    #[must_use]
    pub const fn socket_addr(&self) -> SocketAddr {
        self.0
    }
}

impl fmt::Debug for PeerAddress {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PeerAddress([redacted])")
    }
}

/// Where an effective client address came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientIpSource {
    /// A visitor address an operator proxy authenticated with a shared credential.
    AuthenticatedHeader,
    /// Cloudflare's `CF-Connecting-IP` on the operator-declared direct path.
    Cloudflare,
    /// The transport peer, which no configured trusted proxy speaks for.
    DirectPeer,
    /// Fastly's runtime downstream client address.
    Fastly,
    /// The transport peer is a trusted proxy and the client behind it could not be resolved.
    PeerFallback(UnavailableReason),
    /// A `Forwarded` chain from a trusted proxy.
    TrustedForwarded,
    /// An `X-Forwarded-For` chain from a trusted proxy.
    TrustedXForwardedFor,
}

/// An effective client address with its source. `Debug` redacts the address.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ClientAddress {
    ip: IpAddr,
    source: ClientIpSource,
}

impl ClientAddress {
    /// The address.
    #[inline]
    #[must_use]
    pub const fn ip(&self) -> IpAddr {
        self.ip
    }

    /// Record an address with the source the adapter established.
    #[inline]
    #[must_use]
    pub const fn new(ip: IpAddr, source: ClientIpSource) -> Self {
        Self { ip, source }
    }

    /// Where the address came from.
    #[inline]
    #[must_use]
    pub const fn source(&self) -> ClientIpSource {
        self.source
    }
}

impl fmt::Debug for ClientAddress {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientAddress")
            .field("ip", &format_args!("[redacted]"))
            .field("source", &self.source)
            .finish()
    }
}

/// The runtime that reported a viewer TLS value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ViewerTlsSource {
    /// Cloudflare's `request.cf` metadata.
    Cloudflare,
    /// Fastly's client TLS accessors.
    Fastly,
}

/// One runtime TLS field as the adapter read it, before classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeTlsField<'value> {
    /// The runtime reported no value.
    Absent,
    /// The runtime reported something that is not text.
    NotText,
    /// The runtime reported text, possibly empty.
    Text(&'value str),
}

/// A viewer TLS protocol or cipher name, kept exactly as the runtime reported it.
///
/// Providers name equivalent ciphers differently. `Debug` redacts the value.
#[derive(Clone, PartialEq, Eq)]
pub struct ViewerTlsValue {
    source: ViewerTlsSource,
    value: Box<str>,
}

impl ViewerTlsValue {
    /// The value as reported.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Classify one runtime field. Absent and empty text are `Missing`;
    /// anything that is not text is `Rejected`; other text is captured unchanged.
    #[inline]
    #[must_use]
    pub fn capture(field: RuntimeTlsField<'_>, source: ViewerTlsSource) -> Capture<Self> {
        match field {
            RuntimeTlsField::Absent | RuntimeTlsField::Text("") => {
                Capture::Unavailable(UnavailableReason::Missing)
            }
            RuntimeTlsField::Text(value) => Capture::Captured(Self {
                source,
                value: Box::from(value),
            }),
            RuntimeTlsField::NotText => Capture::Unavailable(UnavailableReason::Rejected),
        }
    }

    /// Classify an SDK accessor result shaped `Result<Option<&str>, Utf8Error>`.
    /// Invalid UTF-8 is `Rejected`.
    #[inline]
    #[must_use]
    pub fn capture_utf8(
        result: Result<Option<&str>, Utf8Error>,
        source: ViewerTlsSource,
    ) -> Capture<Self> {
        let field = match result {
            Ok(None) => RuntimeTlsField::Absent,
            Ok(Some(value)) => RuntimeTlsField::Text(value),
            Err(_) => RuntimeTlsField::NotText,
        };
        Self::capture(field, source)
    }

    /// The runtime that reported the value.
    #[inline]
    #[must_use]
    pub const fn source(&self) -> ViewerTlsSource {
        self.source
    }
}

impl fmt::Debug for ViewerTlsValue {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViewerTlsValue")
            .field("value", &format_args!("[redacted]"))
            .field("source", &self.source)
            .finish()
    }
}

/// Client network facts captured once at ingress by a managed adapter.
///
/// Read it with `RequestContext::client_info()`. Request headers and
/// extensions cannot replace it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientInfo {
    direct_peer: Capture<PeerAddress>,
    effective_client: Capture<ClientAddress>,
    tls_cipher: Capture<ViewerTlsValue>,
    tls_version: Capture<ViewerTlsValue>,
}

impl ClientInfo {
    /// The transport peer as the adapter observed it. A peer is not proof of who the visitor is.
    #[inline]
    #[must_use]
    pub const fn direct_peer(&self) -> &Capture<PeerAddress> {
        &self.direct_peer
    }

    /// The effective client address and its source.
    #[inline]
    #[must_use]
    pub const fn effective_client(&self) -> &Capture<ClientAddress> {
        &self.effective_client
    }

    /// The effective client address when its source identifies a client rather
    /// than a trusted proxy. Returns `None` for [`ClientIpSource::PeerFallback`].
    ///
    /// [`ClientIpSource::DirectPeer`] still assumes no unconfigured proxy sits in
    /// front of the peer; inspect [`Self::effective_client`] for the source.
    #[inline]
    #[must_use]
    pub fn effective_client_ip(&self) -> Option<IpAddr> {
        let address = self.effective_client.captured()?;
        match address.source {
            ClientIpSource::Fastly
            | ClientIpSource::Cloudflare
            | ClientIpSource::TrustedForwarded
            | ClientIpSource::TrustedXForwardedFor
            | ClientIpSource::DirectPeer
            | ClientIpSource::AuthenticatedHeader => Some(address.ip),
            ClientIpSource::PeerFallback(_) => None,
        }
    }

    /// Assemble a snapshot from captured facts.
    #[inline]
    #[must_use]
    pub const fn new(
        direct_peer: Capture<PeerAddress>,
        effective_client: Capture<ClientAddress>,
        tls_version: Capture<ViewerTlsValue>,
        tls_cipher: Capture<ViewerTlsValue>,
    ) -> Self {
        Self {
            direct_peer,
            effective_client,
            tls_cipher,
            tls_version,
        }
    }

    /// A snapshot whose fields are all `NotExposed`.
    #[inline]
    #[must_use]
    pub const fn not_exposed() -> Self {
        Self::new(
            Capture::Unavailable(UnavailableReason::NotExposed),
            Capture::Unavailable(UnavailableReason::NotExposed),
            Capture::Unavailable(UnavailableReason::NotExposed),
            Capture::Unavailable(UnavailableReason::NotExposed),
        )
    }

    /// The viewer's negotiated TLS cipher name.
    #[inline]
    #[must_use]
    pub const fn tls_cipher(&self) -> &Capture<ViewerTlsValue> {
        &self.tls_cipher
    }

    /// The viewer's negotiated TLS protocol version.
    #[inline]
    #[must_use]
    pub const fn tls_version(&self) -> &Capture<ViewerTlsValue> {
        &self.tls_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_source(source: ClientIpSource) -> ClientInfo {
        ClientInfo::new(
            Capture::Unavailable(UnavailableReason::NotExposed),
            Capture::Captured(ClientAddress::new("198.51.100.4".parse().unwrap(), source)),
            Capture::Unavailable(UnavailableReason::NotExposed),
            Capture::Unavailable(UnavailableReason::NotExposed),
        )
    }

    #[test]
    fn effective_client_ip_excludes_only_peer_fallback() {
        let ip: IpAddr = "198.51.100.4".parse().unwrap();
        for source in [
            ClientIpSource::Fastly,
            ClientIpSource::Cloudflare,
            ClientIpSource::TrustedForwarded,
            ClientIpSource::TrustedXForwardedFor,
            ClientIpSource::DirectPeer,
            ClientIpSource::AuthenticatedHeader,
        ] {
            assert_eq!(with_source(source).effective_client_ip(), Some(ip));
        }
        for reason in [
            UnavailableReason::NotExposed,
            UnavailableReason::Missing,
            UnavailableReason::Rejected,
        ] {
            let info = with_source(ClientIpSource::PeerFallback(reason));
            assert_eq!(info.effective_client_ip(), None);
            assert_eq!(
                info.effective_client().captured().map(ClientAddress::ip),
                Some(ip)
            );
        }
        assert_eq!(ClientInfo::not_exposed().effective_client_ip(), None);
    }

    #[test]
    fn debug_never_contains_captured_values() {
        let info = ClientInfo::new(
            Capture::Captured(PeerAddress::new("[2001:db8::9]:4431".parse().unwrap())),
            Capture::Captured(ClientAddress::new(
                "203.0.113.77".parse().unwrap(),
                ClientIpSource::TrustedXForwardedFor,
            )),
            ViewerTlsValue::capture(RuntimeTlsField::Text("TLSv1.3"), ViewerTlsSource::Fastly),
            ViewerTlsValue::capture(
                RuntimeTlsField::Text("ECDHE-RSA-AES128-GCM-SHA256"),
                ViewerTlsSource::Fastly,
            ),
        );
        let rendered = format!("{info:?} {info:#?}");
        for secret in [
            "2001:db8::9",
            "4431",
            "203.0.113.77",
            "TLSv1.3",
            "ECDHE-RSA-AES128-GCM-SHA256",
        ] {
            assert!(!rendered.contains(secret), "{secret} leaked: {rendered}");
        }
        assert!(rendered.contains("TrustedXForwardedFor"));
        assert!(rendered.contains("Fastly"));
    }

    #[test]
    fn tls_fields_are_classified_without_a_grammar() {
        let missing = Capture::Unavailable(UnavailableReason::Missing);
        assert_eq!(
            ViewerTlsValue::capture(RuntimeTlsField::Absent, ViewerTlsSource::Cloudflare),
            missing
        );
        assert_eq!(
            ViewerTlsValue::capture(RuntimeTlsField::Text(""), ViewerTlsSource::Cloudflare),
            missing
        );
        assert_eq!(
            ViewerTlsValue::capture(RuntimeTlsField::NotText, ViewerTlsSource::Cloudflare),
            Capture::Unavailable(UnavailableReason::Rejected)
        );
        for value in [
            "AEAD-AES128-GCM-SHA256",
            "ECDHE-RSA-AES128-GCM-SHA256",
            "TLSv1.3",
            "some future name",
        ] {
            let captured =
                ViewerTlsValue::capture(RuntimeTlsField::Text(value), ViewerTlsSource::Cloudflare);
            let tls = captured.captured().unwrap();
            assert_eq!(tls.as_str(), value);
            assert_eq!(tls.source(), ViewerTlsSource::Cloudflare);
        }
    }

    #[test]
    fn fastly_shaped_results_map_to_the_same_classes() {
        let missing = Capture::Unavailable(UnavailableReason::Missing);
        assert_eq!(
            ViewerTlsValue::capture_utf8(Ok(None), ViewerTlsSource::Fastly),
            missing
        );
        assert_eq!(
            ViewerTlsValue::capture_utf8(Ok(Some("")), ViewerTlsSource::Fastly),
            missing
        );
        let invalid = vec![0xff_u8, 0xfe];
        let utf8_error = str::from_utf8(&invalid).unwrap_err();
        assert_eq!(
            ViewerTlsValue::capture_utf8(Err(utf8_error), ViewerTlsSource::Fastly),
            Capture::Unavailable(UnavailableReason::Rejected)
        );
        let captured = ViewerTlsValue::capture_utf8(Ok(Some("TLSv1.2")), ViewerTlsSource::Fastly);
        assert_eq!(
            captured.captured().map(ViewerTlsValue::as_str),
            Some("TLSv1.2")
        );
    }

    #[test]
    fn peer_address_keeps_the_port() {
        let socket: SocketAddr = "[2001:db8::1]:8443".parse().unwrap();
        let peer = PeerAddress::new(socket);
        assert_eq!(peer.socket_addr(), socket);
        assert_eq!(peer.ip(), socket.ip());
    }
}
