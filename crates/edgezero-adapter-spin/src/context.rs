use std::net::IpAddr;
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use std::net::SocketAddr;

use edgezero_core::http::Request;
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use edgezero_core::{
    client_info::{
        Capture, ClientAddress, ClientInfo, ClientIpSource, PeerAddress, UnavailableReason,
    },
    http::HeaderMap,
};

/// Platform-specific request context for Spin.
///
/// Spin exposes client information via special headers
/// (`spin-client-addr`, `spin-full-url`, etc.) rather than
/// a separate runtime context object.
#[derive(Debug, Clone)]
pub struct SpinRequestContext {
    /// The client IP address, parsed from the `spin-client-addr` header.
    /// The header value has the format `ip:port`; only the IP is retained.
    pub client_addr: Option<IpAddr>,
    /// The full URL of the incoming request.
    pub full_url: Option<String>,
}

impl SpinRequestContext {
    /// Retrieve a previously-inserted context from request extensions.
    #[inline]
    pub fn get(request: &Request) -> Option<&SpinRequestContext> {
        request.extensions().get::<SpinRequestContext>()
    }

    /// Store this context in the request's extensions.
    #[inline]
    pub fn insert(request: &mut Request, context: SpinRequestContext) {
        request.extensions_mut().insert(context);
    }
}

/// Spin appends its own `spin-*` fields after the client's headers, so a
/// client-sent field comes first and only the last field can be the runtime's.
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
pub(crate) fn context_from_headers(headers: &HeaderMap) -> SpinRequestContext {
    let last = |name: &str| {
        headers
            .get_all(name)
            .iter()
            .next_back()
            .and_then(|value| value.to_str().ok())
    };
    SpinRequestContext {
        client_addr: last("spin-client-addr").and_then(parse_client_addr),
        full_url: last("spin-full-url").map(str::to_owned),
    }
}

/// `ClientInfo` for the operator-declared direct-ingress mode: the last
/// `spin-client-addr` field, parsed as the socket address Spin writes.
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
pub(crate) fn direct_ingress_client_info(headers: &HeaderMap) -> ClientInfo {
    let peer = match headers.get_all("spin-client-addr").iter().next_back() {
        None => Err(UnavailableReason::Missing),
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|text| text.parse::<SocketAddr>().ok())
            .ok_or(UnavailableReason::Rejected),
    };
    let (direct_peer, effective_client) = match peer {
        Ok(socket) => (
            Capture::Captured(PeerAddress::new(socket)),
            Capture::Captured(ClientAddress::new(
                socket.ip().to_canonical(),
                ClientIpSource::DirectPeer,
            )),
        ),
        Err(reason) => (Capture::Unavailable(reason), Capture::Unavailable(reason)),
    };
    ClientInfo::new(
        direct_peer,
        effective_client,
        Capture::Unavailable(UnavailableReason::NotExposed),
        Capture::Unavailable(UnavailableReason::NotExposed),
    )
}

/// Parse an IP address from a `host:port` string.
///
/// Falls back to parsing the raw value as a bare IP (no port) and also
/// handles IPv6 bracket notation (`[::1]:port`).
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
pub(crate) fn parse_client_addr(raw: &str) -> Option<IpAddr> {
    // Try `ip:port` (IPv4) or `[ip]:port` (IPv6 bracket notation).
    if let Ok(sock) = raw.parse::<SocketAddr>() {
        return Some(sock.ip());
    }
    // Bare IP with no port.
    raw.parse::<IpAddr>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::body::Body;
    use edgezero_core::http::request_builder;
    use edgezero_core::http::{HeaderName, HeaderValue};
    use std::str::FromStr as _;

    #[test]
    fn get_returns_none_when_missing() {
        let request = request_builder()
            .uri("https://example.com")
            .body(Body::empty())
            .expect("request");

        assert!(SpinRequestContext::get(&request).is_none());
    }

    #[test]
    fn inserts_and_retrieves_context() {
        let mut request = request_builder()
            .uri("https://example.com")
            .body(Body::empty())
            .expect("request");

        let context = SpinRequestContext {
            client_addr: Some(IpAddr::from_str("127.0.0.1").unwrap()),
            full_url: Some("https://example.com/path".to_owned()),
        };
        SpinRequestContext::insert(&mut request, context);

        let retrieved = SpinRequestContext::get(&request).expect("context");
        assert_eq!(
            retrieved.client_addr,
            Some(IpAddr::from_str("127.0.0.1").unwrap())
        );
        assert_eq!(
            retrieved.full_url.as_deref(),
            Some("https://example.com/path")
        );
    }

    #[test]
    fn parse_client_addr_invalid() {
        assert!(parse_client_addr("not-an-ip").is_none());
    }

    #[test]
    fn parse_client_addr_ipv4_bare() {
        let ip = parse_client_addr("10.0.0.1").unwrap();
        assert_eq!(ip, IpAddr::from_str("10.0.0.1").unwrap());
    }

    #[test]
    fn parse_client_addr_ipv4_with_port() {
        let ip = parse_client_addr("192.168.1.1:8080").unwrap();
        assert_eq!(ip, IpAddr::from_str("192.168.1.1").unwrap());
    }

    #[test]
    fn parse_client_addr_ipv6_bare() {
        let ip = parse_client_addr("::1").unwrap();
        assert_eq!(ip, IpAddr::from_str("::1").unwrap());
    }

    #[test]
    fn parse_client_addr_ipv6_bracket() {
        let ip = parse_client_addr("[::1]:3000").unwrap();
        assert_eq!(ip, IpAddr::from_str("::1").unwrap());
    }

    fn headers(fields: &[(&str, &[u8])]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in fields {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_bytes(value).unwrap(),
            );
        }
        headers
    }

    #[test]
    fn context_reads_the_runtime_field_after_a_client_prepended_one() {
        let context = context_from_headers(&headers(&[
            ("spin-client-addr", b"192.0.2.1:1"),
            ("spin-full-url", b"https://spoof.example.com/reserved"),
            ("spin-client-addr", b"127.0.0.1:53152"),
            ("spin-full-url", b"http://127.0.0.1:3000/path"),
        ]));
        assert_eq!(
            context.client_addr,
            Some(IpAddr::from_str("127.0.0.1").unwrap())
        );
        assert_eq!(
            context.full_url.as_deref(),
            Some("http://127.0.0.1:3000/path")
        );
        let single = context_from_headers(&headers(&[("spin-client-addr", b"10.0.0.1:80")]));
        assert_eq!(
            single.client_addr,
            Some(IpAddr::from_str("10.0.0.1").unwrap())
        );
    }

    #[test]
    fn direct_ingress_maps_the_last_spin_client_addr() {
        let captured = |fields: &[(&str, &[u8])]| {
            let info = direct_ingress_client_info(&headers(fields));
            assert_eq!(
                info.tls_version(),
                &Capture::Unavailable(UnavailableReason::NotExposed)
            );
            (
                info.direct_peer().captured().map(PeerAddress::socket_addr),
                info.effective_client().captured().copied(),
                info.effective_client().unavailable_reason(),
            )
        };
        let runtime: SocketAddr = "127.0.0.1:53152".parse().unwrap();
        let (peer, client, _) = captured(&[
            ("spin-client-addr", b"192.0.2.1:1"),
            ("spin-client-addr", b"127.0.0.1:53152"),
        ]);
        assert_eq!(peer, Some(runtime));
        assert_eq!(
            client,
            Some(ClientAddress::new(runtime.ip(), ClientIpSource::DirectPeer))
        );
        let ipv6: SocketAddr = "[2001:db8::3]:4000".parse().unwrap();
        let (ipv6_peer, ipv6_client, _) = captured(&[("spin-client-addr", b"[2001:db8::3]:4000")]);
        assert_eq!(ipv6_peer, Some(ipv6));
        assert_eq!(ipv6_client.map(|address| address.ip()), Some(ipv6.ip()));
        let (_, mapped, _) = captured(&[("spin-client-addr", b"[::ffff:192.0.2.9]:4000")]);
        assert_eq!(
            mapped.map(|address| address.ip()),
            Some(IpAddr::from_str("192.0.2.9").unwrap())
        );
        assert_eq!(captured(&[]).2, Some(UnavailableReason::Missing));
        for rejected in [b"127.0.0.1".as_slice(), b"not-an-address", b"\xff\xfe"] {
            assert_eq!(
                captured(&[("spin-client-addr", rejected)]).2,
                Some(UnavailableReason::Rejected),
                "{rejected:?}"
            );
        }
        // A malformed runtime field does not fall back to an earlier, client-sent one.
        assert_eq!(
            captured(&[
                ("spin-client-addr", b"192.0.2.1:1"),
                ("spin-client-addr", b"garbage"),
            ])
            .2,
            Some(UnavailableReason::Rejected)
        );
    }
}
