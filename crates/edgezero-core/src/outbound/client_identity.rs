//! Outbound TLS client identities.
//!
//! An application declares identities by name in its manifest and selects one
//! on an outbound request. Core owns the names, the per-app policy built from the
//! baked manifest, the host rules, the certificate-chain fingerprint and the
//! error type. Adapters own support checks and the runtime binding.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::net::Ipv6Addr;
use std::str::{FromStr as _, from_utf8};

use base16ct::lower::encode_string;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use sha2::{Digest as _, Sha256};

use crate::error::EdgeError;
use crate::http::{StatusCode, Uri};
use crate::manifest::{
    AtomicHost, BakedManifest, HostPat, MAX_CLIENT_IDENTITY_NAME_LEN, Manifest, Port, Scheme,
    canonicalize_outbound_host, is_valid_client_identity_name,
};

const HTTPS_DEFAULT_PORT: u16 = 443;
const PEM_CERTIFICATE_BEGIN: &str = "-----BEGIN CERTIFICATE-----";
const PEM_CERTIFICATE_END: &str = "-----END CERTIFICATE-----";

/// A client certificate chain parsed from its PEM entry.
///
/// Only `CERTIFICATE` blocks are kept, in entry order. `EdgeZero` does not check
/// expiry, extended key usage or the matching key; the origin's handshake does.
#[derive(Clone, Eq, PartialEq)]
pub struct ClientCertificateChain {
    chain_fingerprint: [u8; 32],
    leaf_fingerprint: [u8; 32],
    pem: String,
}

impl ClientCertificateChain {
    /// SHA-256 over every certificate in order, each DER length-prefixed. Any
    /// change to the chain, intermediates included, changes it.
    #[inline]
    #[must_use]
    pub fn chain_fingerprint(&self) -> [u8; 32] {
        self.chain_fingerprint
    }

    /// SHA-256 of the leaf certificate's DER, as `openssl x509 -fingerprint -sha256` shows it.
    #[inline]
    #[must_use]
    pub fn leaf_fingerprint(&self) -> [u8; 32] {
        self.leaf_fingerprint
    }

    /// Parses every `CERTIFICATE` block in `pem`.
    ///
    /// # Errors
    /// Returns [`ClientIdentityError::MaterialInvalid`] for `Certificate` when
    /// there is no block, a block is unterminated, or a block is not base64 DER.
    #[inline]
    pub fn parse(pem: &str) -> Result<Self, ClientIdentityError> {
        let certificates = decode_certificate_blocks(pem)?;
        let Some(leaf) = certificates.first() else {
            return Err(invalid_certificate());
        };
        let leaf_fingerprint: [u8; 32] = Sha256::digest(leaf).into();
        let mut normalized = String::new();
        for der in &certificates {
            normalized.push_str(PEM_CERTIFICATE_BEGIN);
            normalized.push('\n');
            let encoded = STANDARD.encode(der);
            for line in encoded.as_bytes().chunks(64) {
                normalized.push_str(from_utf8(line).unwrap_or_default());
                normalized.push('\n');
            }
            normalized.push_str(PEM_CERTIFICATE_END);
            normalized.push('\n');
        }
        Ok(Self {
            chain_fingerprint: chain_fingerprint(&certificates),
            leaf_fingerprint,
            pem: normalized,
        })
    }

    /// The chain re-encoded as `CERTIFICATE` blocks only.
    #[inline]
    #[must_use]
    pub fn pem(&self) -> &str {
        &self.pem
    }
}

impl fmt::Debug for ClientCertificateChain {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCertificateChain")
            .field("leaf_fingerprint", &encode_string(&self.leaf_fingerprint))
            .finish_non_exhaustive()
    }
}

/// Why a request that selects a client identity was not sent.
///
/// Every reason is raised before network I/O. A handshake the origin rejects is a
/// transport failure, not one of these.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ClientIdentityError {
    /// The request's origin is not in the identity's hosts. `origin` is
    /// `https://host:port`, never a path or query.
    HostNotAllowed {
        origin: String,
        redirect: bool,
    },
    MaterialInvalid {
        which: ClientIdentityMaterial,
    },
    MaterialMissing {
        which: ClientIdentityMaterial,
    },
    RequiresHttps,
    Undeclared,
    /// The adapter cannot present an identity, or the request combines one with
    /// an option that cannot carry it.
    Unsupported {
        adapter: &'static str,
    },
}

impl ClientIdentityError {
    pub(crate) fn wire_kind(&self) -> &'static str {
        match self {
            Self::RequiresHttps | Self::Undeclared | Self::Unsupported { .. } => "internal",
            Self::MaterialInvalid { .. }
            | Self::MaterialMissing { .. }
            | Self::HostNotAllowed {
                redirect: false, ..
            } => "service_unavailable",
            Self::HostNotAllowed { redirect: true, .. } => "bad_gateway",
        }
    }

    /// HTTP status for this reason.
    #[inline]
    #[must_use]
    pub fn wire_status(&self) -> StatusCode {
        match self {
            Self::RequiresHttps | Self::Undeclared | Self::Unsupported { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::MaterialInvalid { .. }
            | Self::MaterialMissing { .. }
            | Self::HostNotAllowed {
                redirect: false, ..
            } => StatusCode::SERVICE_UNAVAILABLE,
            Self::HostNotAllowed { redirect: true, .. } => StatusCode::BAD_GATEWAY,
        }
    }
}

impl fmt::Display for ClientIdentityError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HostNotAllowed {
                origin,
                redirect: false,
            } => write!(f, "{origin} is not in the identity's hosts"),
            Self::HostNotAllowed {
                origin,
                redirect: true,
            } => write!(
                f,
                "redirect to {origin}, which is not in the identity's hosts"
            ),
            Self::MaterialInvalid { which } => write!(f, "{} is invalid", which.as_str()),
            Self::MaterialMissing { which } => write!(f, "{} is missing", which.as_str()),
            Self::RequiresHttps => f.write_str("requests that select an identity must use https"),
            Self::Undeclared => f.write_str("identity is not declared in the manifest"),
            Self::Unsupported { adapter } => {
                write!(
                    f,
                    "{adapter} cannot present a client identity on this request"
                )
            }
        }
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "ClientIdentityError has no source or backtrace beyond Display"
)]
impl Error for ClientIdentityError {}

/// The origins an identity may be presented to, parsed from its runtime hosts entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientIdentityHosts {
    origins: Vec<(String, u16)>,
}

impl ClientIdentityHosts {
    /// Longest accepted hosts entry, in bytes.
    pub const MAX_BYTES: usize = 4_000;

    /// Checks that `uri` targets one of these origins. Only the URI's scheme,
    /// host and port count; a `Host` override does not.
    ///
    /// # Errors
    /// Returns [`ClientIdentityError::RequiresHttps`] for a non-HTTPS URI and
    /// [`ClientIdentityError::HostNotAllowed`] for an unlisted origin.
    #[inline]
    pub fn check(&self, uri: &Uri) -> Result<(), ClientIdentityError> {
        let (host, port) = https_origin(uri)?;
        if self
            .origins
            .iter()
            .any(|(listed_host, listed_port)| *listed_host == host && *listed_port == port)
        {
            Ok(())
        } else {
            Err(ClientIdentityError::HostNotAllowed {
                origin: format!("https://{host}:{port}"),
                redirect: false,
            })
        }
    }

    /// The listed origins as `(host, port)`, hosts in canonical form.
    #[inline]
    pub fn origins(&self) -> impl Iterator<Item = (&str, u16)> {
        self.origins
            .iter()
            .map(|(host, port)| (host.as_str(), *port))
    }

    /// Parses a hosts entry: one `https://host` or `https://host:port` per line,
    /// in the outbound allowlist's host grammar, with no wildcards. Blank lines
    /// are ignored. Every origin must be admitted by the policy's allowlist.
    ///
    /// # Errors
    /// Returns [`ClientIdentityError::MaterialInvalid`] for `Hosts` when the entry
    /// is oversized, empty, malformed, or names an origin outside the allowlist.
    #[inline]
    pub fn parse(text: &str, policy: &ClientIdentityPolicy) -> Result<Self, ClientIdentityError> {
        if text.len() > Self::MAX_BYTES {
            return Err(invalid_hosts());
        }
        let mut origins: Vec<(String, u16)> = Vec::new();
        for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
            let origin = parse_exact_https_origin(line)?;
            if !policy.allows_https_origin(&origin.0, origin.1) {
                return Err(invalid_hosts());
            }
            if !origins.contains(&origin) {
                origins.push(origin);
            }
        }
        if origins.is_empty() {
            return Err(invalid_hosts());
        }
        Ok(Self { origins })
    }
}

/// Which part of an identity's runtime binding failed.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ClientIdentityMaterial {
    Certificate,
    Hosts,
    Key,
    Store,
}

impl ClientIdentityMaterial {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Certificate => "certificate",
            Self::Hosts => "hosts list",
            Self::Key => "private key",
            Self::Store => "store",
        }
    }
}

/// One declared identity, with its logical stores resolved from the manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientIdentityMetadata {
    config_store: Option<String>,
    name: ClientIdentityName,
    secret_store: Option<String>,
}

impl ClientIdentityMetadata {
    /// Logical `[stores.config]` ID, if one resolved.
    #[inline]
    #[must_use]
    pub fn config_store(&self) -> Option<&str> {
        self.config_store.as_deref()
    }

    #[inline]
    #[must_use]
    pub fn name(&self) -> &ClientIdentityName {
        &self.name
    }

    /// Logical `[stores.secrets]` ID, if one resolved.
    #[inline]
    #[must_use]
    pub fn secret_store(&self) -> Option<&str> {
        self.secret_store.as_deref()
    }
}

/// A validated client identity name: 1 to 64 characters of `[a-z0-9-]`, not
/// starting or ending with `-`.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientIdentityName(String);

impl ClientIdentityName {
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// # Errors
    /// Returns a bad-request error when `name` breaks the manifest naming rules.
    #[inline]
    pub fn new(name: &str) -> Result<Self, EdgeError> {
        if is_valid_client_identity_name(name) {
            Ok(Self(name.to_owned()))
        } else {
            Err(EdgeError::bad_request(format!(
                "client identity name must be 1 to {MAX_CLIENT_IDENTITY_NAME_LEN} characters of \
                 [a-z0-9-], not starting or ending with `-`"
            )))
        }
    }
}

impl fmt::Debug for ClientIdentityName {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ClientIdentityName").field(&self.0).finish()
    }
}

impl fmt::Display for ClientIdentityName {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The identities an application declares, plus its outbound allowlist.
///
/// Built once per [`App`](crate::app::App) from the baked manifest and attached
/// to each request's [`HttpClient`](crate::HttpClient) at dispatch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientIdentityPolicy {
    allowlist: Vec<AtomicHost>,
    identities: BTreeMap<ClientIdentityName, ClientIdentityMetadata>,
}

impl ClientIdentityPolicy {
    /// Whether the outbound allowlist admits this HTTPS origin.
    #[inline]
    #[must_use]
    pub fn allows_https_origin(&self, host: &str, port: u16) -> bool {
        self.allowlist
            .iter()
            .any(|entry| entry.permits(Scheme::Https, host, port))
    }

    /// Policy for a baked manifest. Absent and malformed manifests yield an
    /// empty policy, so every identity request fails as undeclared.
    #[inline]
    #[must_use]
    pub fn from_baked(baked: BakedManifest) -> Self {
        match baked {
            BakedManifest::Present(manifest) => Self::from_manifest(manifest),
            BakedManifest::Absent | BakedManifest::Malformed(_) => Self::default(),
        }
    }

    /// Policy for a validated manifest.
    #[inline]
    #[must_use]
    pub fn from_manifest(manifest: &Manifest) -> Self {
        let outbound = &manifest.capabilities.outbound;
        let identities = outbound
            .client_identities
            .iter()
            .filter(|(declared_name, _)| is_valid_client_identity_name(declared_name))
            .map(|(declared_name, declaration)| {
                let name = ClientIdentityName(declared_name.clone());
                let metadata = ClientIdentityMetadata {
                    config_store: declaration
                        .resolved_config_store(&manifest.stores)
                        .map(str::to_owned),
                    name: name.clone(),
                    secret_store: declaration
                        .resolved_secret_store(&manifest.stores)
                        .map(str::to_owned),
                };
                (name, metadata)
            })
            .collect();
        let allowlist = outbound.hosts.as_deref().map_or_else(
            || {
                vec![AtomicHost {
                    host: HostPat::Any,
                    port: Port::Any,
                    scheme: Scheme::Https,
                }]
            },
            |entries| {
                entries
                    .iter()
                    .filter_map(|entry| canonicalize_outbound_host(entry).ok())
                    .flatten()
                    .collect()
            },
        );
        Self {
            allowlist,
            identities,
        }
    }

    #[inline]
    #[must_use]
    pub fn identity(&self, name: &ClientIdentityName) -> Option<&ClientIdentityMetadata> {
        self.identities.get(name)
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.identities.is_empty()
    }
}

#[expect(
    clippy::big_endian_bytes,
    reason = "the chain fingerprint uses a fixed byte order independent of the host architecture"
)]
fn chain_fingerprint(certificates: &[Vec<u8>]) -> [u8; 32] {
    let mut chain = Sha256::new();
    for der in certificates {
        chain.update(u64::try_from(der.len()).unwrap_or(u64::MAX).to_be_bytes());
        chain.update(der);
    }
    chain.finalize().into()
}

fn decode_certificate_blocks(pem: &str) -> Result<Vec<Vec<u8>>, ClientIdentityError> {
    let mut certificates = Vec::new();
    let mut current: Option<String> = None;
    for line in pem.lines().map(str::trim) {
        match (&mut current, line) {
            (None, PEM_CERTIFICATE_BEGIN) => current = Some(String::new()),
            (None, _) => {}
            (Some(body), PEM_CERTIFICATE_END) => {
                let der = STANDARD
                    .decode(body.as_bytes())
                    .map_err(|_error| invalid_certificate())?;
                if der.is_empty() {
                    return Err(invalid_certificate());
                }
                certificates.push(der);
                current = None;
            }
            (Some(_), _) if line.starts_with("-----") => return Err(invalid_certificate()),
            (Some(body), _) => body.push_str(line),
        }
    }
    if current.is_some() {
        return Err(invalid_certificate());
    }
    Ok(certificates)
}

/// Canonical `(host, port)` of an HTTPS URI: lowercase ASCII, IPv6 bracketed.
fn https_origin(uri: &Uri) -> Result<(String, u16), ClientIdentityError> {
    if !is_https(uri) {
        return Err(ClientIdentityError::RequiresHttps);
    }
    let raw_host = uri.host().ok_or(ClientIdentityError::RequiresHttps)?;
    let unbracketed = raw_host
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(raw_host);
    let host = Ipv6Addr::from_str(unbracketed).map_or_else(
        |_error| raw_host.to_ascii_lowercase(),
        |address| format!("[{address}]"),
    );
    Ok((host, uri.port_u16().unwrap_or(HTTPS_DEFAULT_PORT)))
}

fn invalid_certificate() -> ClientIdentityError {
    ClientIdentityError::MaterialInvalid {
        which: ClientIdentityMaterial::Certificate,
    }
}

fn invalid_hosts() -> ClientIdentityError {
    ClientIdentityError::MaterialInvalid {
        which: ClientIdentityMaterial::Hosts,
    }
}

/// Whether `uri` is HTTPS. Shared by core's preflight and the host check.
pub(crate) fn is_https(uri: &Uri) -> bool {
    uri.scheme_str()
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https"))
}

/// One hosts-entry line as an exact HTTPS origin in the allowlist grammar.
fn parse_exact_https_origin(line: &str) -> Result<(String, u16), ClientIdentityError> {
    let explicit_https = line
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"));
    if !explicit_https {
        return Err(invalid_hosts());
    }
    let mut parsed = canonicalize_outbound_host(line).map_err(|_error| invalid_hosts())?;
    let (Some(entry), None) = (parsed.pop(), parsed.pop()) else {
        return Err(invalid_hosts());
    };
    match (entry.host, entry.port, entry.scheme) {
        (HostPat::Exact(host), Port::Exact(port), Scheme::Https) => Ok((host, port)),
        _ => Err(invalid_hosts()),
    }
}

#[cfg(test)]
mod tests;
