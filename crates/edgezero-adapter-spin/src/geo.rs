//! Authenticated ingress geography for Spin deployments.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "group public configuration, platform-gated resolution and colocated tests"
)]

use edgezero_core::error::EdgeError;
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use edgezero_core::geo::GeoProviderId;

/// Enables trusted ingress assertions on managed Spin dispatch.
///
/// Insert this non-secret selection in App state from `Hooks::configure`.
/// The variable must contain a 32–128 byte ASCII credential (bytes 0x21–0x7e). The proxy
/// must replace viewer assertions and protect its transport to Spin. No geography
/// is selected when this configuration is absent.
#[derive(Clone, Debug)]
pub struct SpinGeoIngress {
    credential_variable: String,
    #[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
    provider: GeoProviderId,
}

impl SpinGeoIngress {
    /// Select the Spin variable containing the expected ingress credential.
    ///
    /// # Errors
    /// Rejects names outside `[a-z][a-z0-9_]{0,63}`. The credential itself is
    /// loaded per admitted request and is never retained in application state.
    #[inline]
    pub fn new(credential_variable: &str) -> Result<Self, EdgeError> {
        if credential_variable.is_empty()
            || credential_variable.len() > 64
            || !credential_variable
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_lowercase)
            || !credential_variable
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(EdgeError::validation(
                "invalid Spin geo credential variable",
            ));
        }
        Ok(Self {
            credential_variable: credential_variable.to_owned(),
            #[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
            provider: GeoProviderId::new("spin-ingress").map_err(EdgeError::internal)?,
        })
    }

    /// Name of the variable, never its value.
    #[inline]
    #[must_use]
    pub fn credential_variable(&self) -> &str {
        &self.credential_variable
    }
}

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
pub(crate) mod ingress {
    use super::SpinGeoIngress;
    use bytes::Bytes;
    use edgezero_core::geo::{
        CountryCode, GeoAssertion, GeoError, GeoLocation, GeoOutcome, GeoSource, GeoStatus,
        GeoTrustBasis, GeoUnavailableReason, SubdivisionCode,
    };
    use edgezero_core::http::HeaderMap;
    use edgezero_core::secret_store::SecretError;
    #[cfg(all(feature = "spin", target_arch = "wasm32"))]
    use edgezero_core::secret_store::SecretStore;
    #[cfg(all(feature = "spin", target_arch = "wasm32"))]
    use edgezero_core::time::{Deadline, MonotonicClock};
    use std::future::Future;
    use std::str;
    use subtle::ConstantTimeEq as _;

    pub(crate) const COUNTRY_HEADER: &str = "x-edgezero-geo-country";
    pub(crate) const REGION_HEADER: &str = "x-edgezero-geo-region";
    pub(crate) const TOKEN_HEADER: &str = "x-edgezero-geo-token";

    pub(crate) fn capture_token(headers: &mut HeaderMap) -> Option<Bytes> {
        let mut values = headers.get_all(TOKEN_HEADER).iter();
        let token = values
            .next()
            .filter(|value| valid_token(value.as_bytes()))
            .filter(|_value| values.next().is_none())
            .map(|value| Bytes::copy_from_slice(value.as_bytes()));
        headers.remove(TOKEN_HEADER);
        token
    }

    fn valid_token(token: &[u8]) -> bool {
        (32..=128).contains(&token.len()) && token.iter().all(u8::is_ascii_graphic)
    }

    fn assertion<const N: usize>(headers: &HeaderMap, name: &str) -> GeoAssertion<N> {
        let mut values = headers.get_all(name).iter();
        match (values.next(), values.next()) {
            (None, _) => GeoAssertion::absent(),
            (Some(value), None) => GeoAssertion::from_bytes(value.as_bytes()),
            _ => GeoAssertion::invalid(),
        }
    }

    fn location(headers: &HeaderMap) -> Result<Option<GeoLocation>, GeoError> {
        let country_assertion = assertion::<2>(headers, COUNTRY_HEADER);
        let region_assertion = assertion::<3>(headers, REGION_HEADER);
        let country_bytes = country_assertion.as_bytes()?;
        let region_bytes = region_assertion.as_bytes()?;
        let Some(country_value) = country_bytes else {
            return if region_bytes.is_none() {
                Ok(None)
            } else {
                Err(GeoError::InvalidAssertion)
            };
        };
        let code = |bytes| str::from_utf8(bytes).map_err(|_error| GeoError::InvalidAssertion);
        let country =
            CountryCode::new(code(country_value)?).map_err(|_error| GeoError::InvalidAssertion)?;
        let region = region_bytes
            .map(|bytes| {
                SubdivisionCode::new(code(bytes)?).map_err(|_error| GeoError::InvalidAssertion)
            })
            .transpose()?;
        Ok(Some(GeoLocation::new(country, region)))
    }

    #[expect(
        clippy::multiple_inherent_impl,
        reason = "keep platform-only resolution separate from host-compilable configuration"
    )]
    impl SpinGeoIngress {
        pub(crate) fn outcome(&self, status: GeoStatus) -> GeoOutcome {
            GeoOutcome::Selected {
                provider: self.provider.clone(),
                status,
            }
        }

        pub(crate) fn resolve_value(
            &self,
            supplied: &[u8],
            expected: Option<&[u8]>,
            headers: &HeaderMap,
        ) -> GeoOutcome {
            let Some(expected_value) = expected.filter(|value| valid_token(value)) else {
                return self.outcome(GeoStatus::Error(GeoError::ProviderFailure));
            };
            if !bool::from(supplied.ct_eq(expected_value)) {
                return self.outcome(GeoStatus::Unavailable(
                    GeoUnavailableReason::UntrustedIngress,
                ));
            }
            self.outcome(match location(headers) {
                Ok(Some(location)) => GeoStatus::Located {
                    location,
                    source: GeoSource::SpinIngress,
                    trust_basis: GeoTrustBasis::AuthenticatedIngressHeader,
                },
                Ok(None) => GeoStatus::Unavailable(GeoUnavailableReason::MissingMetadata),
                Err(error) => GeoStatus::Error(error),
            })
        }

        pub(crate) async fn resolve_with<F, R>(
            &self,
            supplied: Option<Bytes>,
            headers: &HeaderMap,
            lookup: F,
        ) -> GeoOutcome
        where
            F: FnOnce() -> R,
            R: Future<Output = Result<Option<Bytes>, SecretError>>,
        {
            let Some(supplied_value) = supplied else {
                return self.outcome(GeoStatus::Unavailable(
                    GeoUnavailableReason::UntrustedIngress,
                ));
            };
            match lookup().await {
                Ok(expected) => self.resolve_value(&supplied_value, expected.as_deref(), headers),
                Err(_error) => self.outcome(GeoStatus::Error(GeoError::ProviderFailure)),
            }
        }

        #[cfg(all(feature = "spin", target_arch = "wasm32"))]
        pub(crate) async fn resolve(
            &self,
            supplied: Option<Bytes>,
            headers: &HeaderMap,
            clock: &MonotonicClock,
            deadline: Deadline,
            secrets: &dyn SecretStore,
        ) -> GeoOutcome {
            self.resolve_with(supplied, headers, || async {
                secrets
                    .get_bytes_bounded("", &self.credential_variable, clock, deadline, 128, 128)
                    .await
                    .map(|read| read.value)
            })
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ingress::*;
    use super::*;
    use edgezero_core::geo::{
        GeoError, GeoOutcome, GeoSource, GeoStatus, GeoTrustBasis, GeoUnavailableReason,
    };
    use edgezero_core::http::{HeaderMap, HeaderValue};

    const TOKEN: &[u8] = b"0123456789abcdef0123456789abcdef";

    #[test]
    fn invalid_token_skips_lookup_and_backend_failure_is_bounded() {
        use edgezero_core::secret_store::SecretError;
        use futures::executor::block_on;
        use std::cell::Cell;
        let selection = SpinGeoIngress::new("geo_token").unwrap();
        let headers = HeaderMap::new();
        let calls = Cell::new(0_u32);
        let lookup = || async {
            calls.set(calls.get() + 1);
            Err(SecretError::DeadlineExceeded)
        };
        let result = block_on(selection.resolve_with(None, &headers, lookup));
        assert!(matches!(
            result,
            GeoOutcome::Selected {
                status: GeoStatus::Unavailable(GeoUnavailableReason::UntrustedIngress),
                ..
            }
        ));
        assert_eq!(calls.get(), 0);
        let failed_lookup = block_on(selection.resolve_with(
            Some(bytes::Bytes::from_static(TOKEN)),
            &headers,
            lookup,
        ));
        assert!(matches!(
            failed_lookup,
            GeoOutcome::Selected {
                status: GeoStatus::Error(GeoError::ProviderFailure),
                ..
            }
        ));
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn selection_rejects_invalid_variable_names() {
        assert_eq!(
            SpinGeoIngress::new("geo_token")
                .unwrap()
                .credential_variable(),
            "geo_token"
        );
        for name in ["", "GEO_TOKEN", "a-b", "1token", "a token", &"a".repeat(65)] {
            assert!(SpinGeoIngress::new(name).is_err(), "{name}");
        }
    }

    #[test]
    fn credential_capture_strips_all_values_and_rejects_ambiguity() {
        let mut headers = HeaderMap::new();
        headers.insert(TOKEN_HEADER, HeaderValue::from_bytes(TOKEN).unwrap());
        assert_eq!(capture_token(&mut headers).as_deref(), Some(TOKEN));
        assert!(!headers.contains_key(TOKEN_HEADER));
        for values in [
            vec![TOKEN, TOKEN],
            vec![b"short".as_slice()],
            vec![b"".as_slice()],
        ] {
            for value in values {
                headers.append(TOKEN_HEADER, HeaderValue::from_bytes(value).unwrap());
            }
            assert!(capture_token(&mut headers).is_none());
            assert!(!headers.contains_key(TOKEN_HEADER));
        }
        headers.insert(
            TOKEN_HEADER,
            HeaderValue::from_str(&"a".repeat(129)).unwrap(),
        );
        assert!(capture_token(&mut headers).is_none());
        assert!(!headers.contains_key(TOKEN_HEADER));
    }

    #[test]
    fn authentication_precedes_geography_and_retains_provenance() {
        let selection = SpinGeoIngress::new("geo_token").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(COUNTRY_HEADER, HeaderValue::from_static("us"));
        headers.insert(REGION_HEADER, HeaderValue::from_static("ca"));
        let outcome = selection.resolve_value(TOKEN, Some(TOKEN), &headers);
        let GeoOutcome::Selected {
            provider,
            status:
                GeoStatus::Located {
                    location,
                    source,
                    trust_basis,
                },
        } = outcome
        else {
            panic!("authenticated country and region must be located");
        };
        assert_eq!(provider.as_str(), "spin-ingress");
        assert_eq!(location.country().as_str(), "US");
        assert_eq!(location.subdivision().unwrap().as_str(), "CA");
        assert_eq!(source, GeoSource::SpinIngress);
        assert_eq!(trust_basis, GeoTrustBasis::AuthenticatedIngressHeader);
        assert!(matches!(
            selection.resolve_value(TOKEN, Some(b"different-different-different-1234"), &headers),
            GeoOutcome::Selected {
                status: GeoStatus::Unavailable(GeoUnavailableReason::UntrustedIngress),
                ..
            }
        ));
        assert!(matches!(
            selection.resolve_value(TOKEN, None, &headers),
            GeoOutcome::Selected {
                status: GeoStatus::Error(GeoError::ProviderFailure),
                ..
            }
        ));
    }

    #[test]
    fn authenticated_assertions_preserve_absence_and_reject_invalid_or_duplicate_fields() {
        let selection = SpinGeoIngress::new("geo_token").unwrap();
        let mut headers = HeaderMap::new();
        assert!(matches!(
            selection.resolve_value(TOKEN, Some(TOKEN), &headers),
            GeoOutcome::Selected {
                status: GeoStatus::Unavailable(GeoUnavailableReason::MissingMetadata),
                ..
            }
        ));
        headers.insert(COUNTRY_HEADER, HeaderValue::from_static("US"));
        let outcome = selection.resolve_value(TOKEN, Some(TOKEN), &headers);
        assert!(
            matches!(outcome, GeoOutcome::Selected { status: GeoStatus::Located { location, .. }, .. } if location.subdivision().is_none())
        );
        for country in ["XX", "USA", " US", "", "T1"] {
            headers.insert(COUNTRY_HEADER, HeaderValue::from_str(country).unwrap());
            assert!(matches!(
                selection.resolve_value(TOKEN, Some(TOKEN), &headers),
                GeoOutcome::Selected {
                    status: GeoStatus::Error(GeoError::InvalidAssertion),
                    ..
                }
            ));
        }
        headers.insert(COUNTRY_HEADER, HeaderValue::from_static("US"));
        headers.append(COUNTRY_HEADER, HeaderValue::from_static("US"));
        assert!(matches!(
            selection.resolve_value(TOKEN, Some(TOKEN), &headers),
            GeoOutcome::Selected {
                status: GeoStatus::Error(GeoError::InvalidAssertion),
                ..
            }
        ));
        headers.remove(COUNTRY_HEADER);
        headers.insert(REGION_HEADER, HeaderValue::from_static("CA"));
        assert!(matches!(
            selection.resolve_value(TOKEN, Some(TOKEN), &headers),
            GeoOutcome::Selected {
                status: GeoStatus::Error(GeoError::InvalidAssertion),
                ..
            }
        ));
    }
}
