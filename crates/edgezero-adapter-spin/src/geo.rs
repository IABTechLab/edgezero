//! Pinned KV geography from an authenticated visitor-IP assertion.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "configuration precedes request-owned resolution"
)]

use edgezero_core::app::StoreMetadata;
use edgezero_core::env_config::EnvConfig;
use edgezero_core::error::EdgeError;
use edgezero_core::geo::GeoProviderId;
use edgezero_core::geo::kv::Receipt;
use edgezero_core::manifest::{BakedManifest, ManifestAdapterClientInfo, ManifestGeoProvider};
use std::time::Duration;

#[cfg(all(feature = "spin", target_arch = "wasm32"))]
use edgezero_core::store_registry::KvRegistry;

/// Immutable deployment selection. Native store handles remain request-local.
#[cfg_attr(
    not(any(test, all(feature = "spin", target_arch = "wasm32"))),
    expect(
        dead_code,
        reason = "resolution fields are consumed only on Spin or in portable tests"
    )
)]
#[derive(Clone, Debug)]
pub struct SpinKvGeo {
    credential_variable: String,
    lookup_timeout: Duration,
    provider: GeoProviderId,
    receipt: Receipt,
    store_id: String,
}

fn invalid_selection() -> EdgeError {
    EdgeError::validation("invalid Spin KV geo selection")
}

impl SpinKvGeo {
    /// Resolve manifest defaults and deployment overrides without performing I/O.
    ///
    /// # Errors
    /// Rejects unsupported settings, undeclared bindings and invalid pins or budgets.
    #[inline]
    pub fn from_config(
        baked: BakedManifest,
        env: &EnvConfig,
        metadata: Option<StoreMetadata>,
    ) -> Result<Option<Self>, EdgeError> {
        let defaults = match baked {
            BakedManifest::Absent => None,
            BakedManifest::Present(manifest) => manifest
                .adapter_entry("spin")
                .and_then(|(_, adapter)| adapter.geo.as_ref()),
            BakedManifest::Malformed(_) | _ => return Err(invalid_selection()),
        };
        let keys = [
            "provider",
            "store_id",
            "pin",
            "reader_ip",
            "credential_variable",
            "lookup_timeout_ms",
        ];
        if env
            .keys_below(&["adapter", "geo"])
            .any(|path| path.len() != 1 || path.first().is_none_or(|key| !keys.contains(key)))
        {
            return Err(invalid_selection());
        }
        if defaults.is_some_and(|geo| {
            geo.alb_peer_cidrs.is_some()
                || geo.origin_credential_header_name.is_some()
                || geo.max_in_flight.is_some()
        }) {
            return Err(invalid_selection());
        }
        let get = |key: &str, default: Option<&str>| {
            env.get(&["adapter", "geo", key])
                .map(str::to_owned)
                .or_else(|| default.map(str::to_owned))
        };
        let provider = get(
            "provider",
            defaults
                .and_then(|geo| geo.provider)
                .map(|provider| match provider {
                    ManifestGeoProvider::Kv => "kv",
                    ManifestGeoProvider::Off => "off",
                    ManifestGeoProvider::CloudFront | _ => "unsupported",
                }),
        );
        let configured_store = get("store_id", defaults.and_then(|geo| geo.store_id.as_deref()));
        let pin = get("pin", defaults.and_then(|geo| geo.pin.as_deref()));
        let reader = get(
            "reader_ip",
            defaults.and_then(|geo| geo.reader_ip.as_deref()),
        );
        let variable = get(
            "credential_variable",
            defaults.and_then(|geo| geo.credential_variable.as_deref()),
        );
        let configured_timeout = get("lookup_timeout_ms", None);
        let default_timeout = defaults.and_then(|geo| geo.lookup_timeout_ms);
        if provider.as_deref().is_none_or(|value| value == "off") {
            if configured_store.is_some()
                || pin.is_some()
                || reader.is_some()
                || variable.is_some()
                || configured_timeout.is_some()
                || default_timeout.is_some()
            {
                return Err(invalid_selection());
            }
            return Ok(None);
        }
        if provider.as_deref() != Some("kv") || reader.as_deref() != Some("authenticated-header") {
            return Err(invalid_selection());
        }
        let store_id = configured_store.ok_or_else(invalid_selection)?;
        if !metadata.is_some_and(|stores| stores.ids.contains(&store_id.as_str())) {
            return Err(invalid_selection());
        }
        let credential_variable = variable.ok_or_else(invalid_selection)?;
        if !valid_variable(&credential_variable) {
            return Err(invalid_selection());
        }
        let timeout = match configured_timeout {
            Some(value) => value.parse::<u32>().map_err(|_error| invalid_selection())?,
            None => default_timeout.unwrap_or(250),
        };
        if !(1..=30000).contains(&timeout) {
            return Err(invalid_selection());
        }
        Ok(Some(Self {
            credential_variable,
            lookup_timeout: Duration::from_millis(u64::from(timeout)),
            provider: GeoProviderId::kv(),
            receipt: Receipt::from_pin(&pin.ok_or_else(invalid_selection)?)
                .map_err(|_error| invalid_selection())?,
            store_id,
        }))
    }
}

/// Resolve `[adapters.spin.client_info] direct_ingress` for this deployment.
///
/// # Errors
/// Rejects `direct_ingress = true` beside an authenticated-header Geo selection. One
/// declares that visitors connect directly, the other that a proxy speaks for them, and
/// `ClientInfo` and `ctx.geo()` would then describe different addresses.
#[inline]
pub fn direct_ingress(baked: BakedManifest, geo: Option<&SpinKvGeo>) -> Result<bool, EdgeError> {
    let declared = match baked {
        BakedManifest::Absent => false,
        BakedManifest::Present(manifest) => manifest
            .adapter_entry("spin")
            .and_then(|(_, adapter)| adapter.client_info)
            .is_some_and(ManifestAdapterClientInfo::direct_ingress),
        BakedManifest::Malformed(_) | _ => return Err(invalid_selection()),
    };
    if declared && geo.is_some() {
        return Err(EdgeError::validation(
            "Spin client_info direct_ingress cannot be combined with authenticated-header geo",
        ));
    }
    Ok(declared)
}

fn valid_variable(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.ends_with('_')
        && !name.contains("__")
        && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use {
    bytes::Bytes,
    edgezero_core::{
        client_info::{Capture, ClientAddress, ClientInfo, ClientIpSource, UnavailableReason},
        geo::{GeoOutcome, GeoStatus},
        key_value_store::KvHandle,
        secret_store::SecretStore,
        time::{Deadline, MonotonicClock},
    },
    std::{future::Future, net::IpAddr},
};

/// The ingress credential as captured, before any secret comparison.
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
pub(crate) enum TokenCapture {
    /// No credential header.
    Absent,
    /// A malformed or repeated credential header.
    Invalid,
    /// Exactly one syntactically valid credential.
    Valid(Bytes),
}

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
pub(crate) struct CapturedGeo {
    ip: Result<Option<IpAddr>, ()>,
    token: TokenCapture,
}

/// The authenticated-header result shared by Geo lookup and `ClientInfo`.
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpinIpAdmission {
    Admitted(IpAddr),
    InvalidIp,
    InvalidToken,
    MissingIp,
    NoToken,
    SecretFailure,
    TokenMismatch,
}

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
impl SpinIpAdmission {
    /// Project the admission into the portable snapshot. Spin exposes no peer or viewer TLS here.
    pub(crate) fn client_info(self) -> ClientInfo {
        let effective_client = match self {
            Self::Admitted(ip) => {
                Capture::Captured(ClientAddress::new(ip, ClientIpSource::AuthenticatedHeader))
            }
            Self::MissingIp | Self::NoToken => Capture::Unavailable(UnavailableReason::Missing),
            Self::InvalidIp | Self::InvalidToken | Self::SecretFailure | Self::TokenMismatch => {
                Capture::Unavailable(UnavailableReason::Rejected)
            }
        };
        ClientInfo::new(
            Capture::Unavailable(UnavailableReason::NotExposed),
            effective_client,
            Capture::Unavailable(UnavailableReason::NotExposed),
            Capture::Unavailable(UnavailableReason::NotExposed),
        )
    }
}

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
pub(crate) mod ingress {
    use super::{CapturedGeo, TokenCapture};
    use bytes::Bytes;
    use edgezero_core::http::HeaderMap;
    use std::net::IpAddr;

    pub(crate) const IP_HEADER: &str = "x-edgezero-geo-ip";
    pub(crate) const TOKEN_HEADER: &str = "x-edgezero-geo-token";

    pub(crate) fn valid_token(token: &[u8]) -> bool {
        (32..=128).contains(&token.len()) && token.iter().all(u8::is_ascii_graphic)
    }

    pub(crate) fn capture_inputs(headers: &mut HeaderMap) -> CapturedGeo {
        let mut tokens = headers.get_all(TOKEN_HEADER).iter();
        let token = match (tokens.next(), tokens.next()) {
            (None, _) => TokenCapture::Absent,
            (Some(value), None) if valid_token(value.as_bytes()) => {
                TokenCapture::Valid(Bytes::copy_from_slice(value.as_bytes()))
            }
            (Some(_), _) => TokenCapture::Invalid,
        };
        let mut ips = headers.get_all(IP_HEADER).iter();
        let ip = match ips.next() {
            None => Ok(None),
            Some(value) if ips.next().is_none() && value.as_bytes().len() <= 45 => {
                value.to_str().map_err(|_error| ()).and_then(|text| {
                    text.parse::<IpAddr>()
                        .map(|ip| Some(ip.to_canonical()))
                        .map_err(|_error| ())
                })
            }
            Some(_) => Err(()),
        };
        headers.remove(TOKEN_HEADER);
        headers.remove(IP_HEADER);
        CapturedGeo { ip, token }
    }
}

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
#[expect(
    clippy::multiple_inherent_impl,
    reason = "request resolution is gated to Spin and portable tests"
)]
impl SpinKvGeo {
    /// Verify the ingress credential and the asserted visitor IP. No KV is read.
    pub(crate) async fn authenticate<Timer, MakeTimer>(
        &self,
        input: CapturedGeo,
        clock: &MonotonicClock,
        admitted_deadline: Deadline,
        secrets: &dyn SecretStore,
        make_timer: MakeTimer,
    ) -> SpinIpAdmission
    where
        Timer: Future<Output = ()>,
        MakeTimer: Fn(Duration) -> Timer,
    {
        use futures_util::future::{Either, select};
        use subtle::ConstantTimeEq as _;
        let token = match input.token {
            TokenCapture::Absent => return SpinIpAdmission::NoToken,
            TokenCapture::Invalid => return SpinIpAdmission::InvalidToken,
            TokenCapture::Valid(token) => token,
        };
        if admitted_deadline.is_expired_at(clock.now()) {
            return SpinIpAdmission::SecretFailure;
        }
        let secret = secrets.get_bytes_bounded(
            "",
            &self.credential_variable,
            clock,
            admitted_deadline,
            128,
            128,
        );
        let remaining = admitted_deadline
            .instant()
            .saturating_duration_since(clock.now());
        let read_value = match select(Box::pin(secret), Box::pin(make_timer(remaining))).await {
            Either::Left((Ok(read), _)) if !admitted_deadline.is_expired_at(clock.now()) => {
                read.value
            }
            Either::Left(_) | Either::Right(_) => return SpinIpAdmission::SecretFailure,
        };
        let Some(expected) = read_value.filter(|value| ingress::valid_token(value)) else {
            return SpinIpAdmission::SecretFailure;
        };
        if !bool::from(token.as_ref().ct_eq(expected.as_ref())) {
            return SpinIpAdmission::TokenMismatch;
        }
        match input.ip {
            Ok(Some(ip)) => SpinIpAdmission::Admitted(ip),
            Ok(None) => SpinIpAdmission::MissingIp,
            Err(()) => SpinIpAdmission::InvalidIp,
        }
    }

    /// Look up an admitted visitor IP in the pinned dataset. Other admission results map to
    /// the same Geo outcomes as before the split.
    pub(crate) async fn lookup<Timer, MakeTimer>(
        &self,
        admission: SpinIpAdmission,
        clock: &MonotonicClock,
        admitted_deadline: Deadline,
        handle: Option<KvHandle>,
        make_timer: MakeTimer,
    ) -> GeoOutcome
    where
        Timer: Future<Output = ()>,
        MakeTimer: Fn(Duration) -> Timer,
    {
        use edgezero_core::geo::kv::{IndexLimits, Resolver};
        use edgezero_core::geo::{GeoError, GeoSource, GeoTrustBasis, GeoUnavailableReason};
        use futures_util::future::{Either, select};
        let failure = || self.outcome(GeoStatus::Error(GeoError::ProviderFailure));
        let ip = match admission {
            SpinIpAdmission::Admitted(ip) => ip,
            SpinIpAdmission::SecretFailure => return failure(),
            SpinIpAdmission::MissingIp => {
                return self.outcome(GeoStatus::Unavailable(
                    GeoUnavailableReason::MissingReaderIp,
                ));
            }
            SpinIpAdmission::InvalidIp
            | SpinIpAdmission::InvalidToken
            | SpinIpAdmission::NoToken
            | SpinIpAdmission::TokenMismatch => {
                return self.outcome(GeoStatus::Unavailable(
                    GeoUnavailableReason::UntrustedIngress,
                ));
            }
        };
        let Some(kv) = handle else { return failure() };
        let deadline = Deadline::at_instant(
            admitted_deadline
                .instant()
                .min(clock.deadline_after(self.lookup_timeout).instant()),
        );
        if deadline.is_expired_at(clock.now()) {
            return failure();
        }
        let lookup = async {
            let resolver = Resolver::open(
                kv,
                &self.receipt,
                IndexLimits {
                    max_object_bytes: 384 * 1024,
                    max_path_objects: 3,
                    cache_bytes: 1024 * 1024,
                    cache_entries: 16,
                },
            )
            .await?;
            resolver.verify_roots().await?;
            resolver.lookup(ip).await
        };
        let lookup_remaining = deadline.instant().saturating_duration_since(clock.now());
        match select(Box::pin(lookup), Box::pin(make_timer(lookup_remaining))).await {
            Either::Left((Ok(found), _)) if !deadline.is_expired_at(clock.now()) => {
                self.outcome(match found {
                    None => GeoStatus::Unavailable(GeoUnavailableReason::NoMatch),
                    Some(raw_location) => match raw_location.geo_location() {
                        Some(location) => GeoStatus::Located {
                            location,
                            source: GeoSource::Kv,
                            trust_basis: GeoTrustBasis::AuthenticatedIngressHeader,
                        },
                        None => GeoStatus::Unavailable(GeoUnavailableReason::UnsupportedGeography),
                    },
                })
            }
            Either::Left(_) | Either::Right(_) => failure(),
        }
    }

    fn outcome(&self, status: GeoStatus) -> GeoOutcome {
        GeoOutcome::Selected {
            provider: self.provider.clone(),
            status,
        }
    }

    /// Authenticate and look up in one step.
    #[cfg(test)]
    pub(crate) async fn resolve<Timer, MakeTimer>(
        &self,
        input: CapturedGeo,
        clock: &MonotonicClock,
        admitted_deadline: Deadline,
        secrets: &dyn SecretStore,
        handle: Option<KvHandle>,
        make_timer: MakeTimer,
    ) -> GeoOutcome
    where
        Timer: Future<Output = ()>,
        MakeTimer: Fn(Duration) -> Timer,
    {
        let admission = self
            .authenticate(input, clock, admitted_deadline, secrets, &make_timer)
            .await;
        self.lookup(admission, clock, admitted_deadline, handle, make_timer)
            .await
    }

    #[cfg(all(feature = "spin", target_arch = "wasm32"))]
    pub(crate) fn handle(&self, configured_registry: Option<&KvRegistry>) -> Option<KvHandle> {
        let registry = configured_registry?;
        registry.named(&self.store_id)
    }
}

#[cfg(test)]
#[path = "geo/kv_tests.rs"]
mod kv_tests;
