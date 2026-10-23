//! Portable geographic assertions and synchronous, local-only providers.
//!
//! Geography is an estimate, not proof of residence, identity or consent.
//! Country-only results and unrecognized subdivision assertions remain partial.
//! Applications own jurisdiction recognition and permission policy.
//!
//! ```
//! use edgezero_core::geo::{CountryCode, GeoLocation, SubdivisionCode};
//!
//! let country_only = GeoLocation::new(CountryCode::new("us")?, None);
//! assert_eq!(country_only.country().as_str(), "US");
//! assert!(country_only.subdivision().is_none());
//!
//! // Syntax-valid does not mean an assigned or recognized US subdivision.
//! let assertion = SubdivisionCode::new("zz")?;
//! let location = GeoLocation::new(CountryCode::new("us")?, Some(assertion));
//! assert_eq!(location.subdivision().map(SubdivisionCode::as_str), Some("ZZ"));
//! # Ok::<(), edgezero_core::geo::GeoValueError>(())
//! ```

#![expect(
    clippy::self_named_module_files,
    reason = "repo convention uses geo.rs with submodules under geo/, not mod.rs"
)]

mod country_codes;

use std::fmt;

/// A bounded validation category. Rejected source values are never retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GeoValueError {
    #[error("invalid country code")]
    InvalidCountry,
    #[error("invalid geographic provider ID")]
    InvalidProviderId,
    #[error("invalid subdivision code")]
    InvalidSubdivision,
    #[error("reserved geographic provider ID")]
    ReservedProviderId,
}

/// A public diagnostic label, never a credential.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeoProviderId(String);

impl GeoProviderId {
    /// Return the validated public label.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Identity reserved for the built-in `CloudFront` provider.
    ///
    /// Adapters must reject this ID for application-supplied selections.
    #[inline]
    #[must_use]
    pub fn cloudfront() -> Self {
        Self("cloudfront".to_owned())
    }

    /// Validate an application provider label matching `[a-z][a-z0-9._-]{0,63}`.
    ///
    /// # Errors
    /// Returns a bounded category for invalid labels or the reserved `cloudfront` ID.
    #[inline]
    pub fn new(value: &str) -> Result<Self, GeoValueError> {
        if value.is_empty()
            || value.len() > 64
            || !value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
            })
        {
            return Err(GeoValueError::InvalidProviderId);
        }
        if value == "cloudfront" {
            return Err(GeoValueError::ReservedProviderId);
        }
        Ok(Self(value.to_owned()))
    }
}

/// An officially assigned ISO 3166-1 alpha-2 code from the pinned snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CountryCode(String);

impl CountryCode {
    /// Return the normalized code.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validate two ASCII letters and normalize them to uppercase.
    ///
    /// # Errors
    /// Rejects unassigned, reserved and unsupported codes without alias substitution.
    #[inline]
    pub fn new(value: &str) -> Result<Self, GeoValueError> {
        if value.len() != 2 || !value.bytes().all(|byte| byte.is_ascii_alphabetic()) {
            return Err(GeoValueError::InvalidCountry);
        }
        let code = value.to_ascii_uppercase();
        if country_codes::ASSIGNED
            .binary_search(&code.as_str())
            .is_err()
        {
            return Err(GeoValueError::InvalidCountry);
        }
        Ok(Self(code))
    }
}

/// A syntax-checked ISO 3166-2 component assertion, without a country prefix.
///
/// This does not verify membership in a country's subdivision catalogue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubdivisionCode(String);

impl SubdivisionCode {
    /// Return the normalized component.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validate one to three ASCII alphanumerics, preserving leading zeroes.
    ///
    /// # Errors
    /// Rejects malformed components. No trimming, decoding or prefix removal occurs.
    #[inline]
    pub fn new(value: &str) -> Result<Self, GeoValueError> {
        if !(1..=3).contains(&value.len())
            || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(GeoValueError::InvalidSubdivision);
        }
        Ok(Self(value.to_ascii_uppercase()))
    }
}

/// A country estimate with an optional syntax-checked subdivision assertion.
///
/// A subdivision-only location cannot be constructed:
/// ```compile_fail
/// use edgezero_core::geo::{GeoLocation, SubdivisionCode};
/// let subdivision = SubdivisionCode::new("CA").unwrap();
/// let location = GeoLocation::new(None, Some(subdivision));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeoLocation {
    country: CountryCode,
    subdivision: Option<SubdivisionCode>,
}

impl GeoLocation {
    /// Return the country estimate.
    #[inline]
    #[must_use]
    pub fn country(&self) -> &CountryCode {
        &self.country
    }

    /// Build a location from validated codes. Country is always required.
    #[inline]
    #[must_use]
    pub fn new(country: CountryCode, subdivision: Option<SubdivisionCode>) -> Self {
        Self {
            country,
            subdivision,
        }
    }

    /// Return the asserted subdivision, if present. Absence remains unknown.
    #[inline]
    #[must_use]
    pub fn subdivision(&self) -> Option<&SubdivisionCode> {
        self.subdivision.as_ref()
    }
}

/// Bounded request-resolution errors, with no raw error chain or source data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GeoError {
    #[error("invalid geographic assertion")]
    InvalidAssertion,
    #[error("geographic provider failure")]
    ProviderFailure,
}

/// An absent, bounded or rejected source assertion.
///
/// Ingress owns duplicate detection and must use [`Self::invalid`] for duplicates,
/// including identical duplicates. A bounded value is not yet a validated code.
#[derive(Clone, PartialEq, Eq)]
pub struct GeoAssertion<const N: usize> {
    state: AssertionState<N>,
}

#[derive(Clone, PartialEq, Eq)]
enum AssertionState<const N: usize> {
    Absent,
    Invalid,
    Value { bytes: [u8; N], len: usize },
}

impl<const N: usize> GeoAssertion<N> {
    /// Record a missing assertion.
    #[inline]
    #[must_use]
    pub const fn absent() -> Self {
        Self {
            state: AssertionState::Absent,
        }
    }

    /// Inspect the bounded assertion, preserving missing versus rejected input.
    ///
    /// # Errors
    /// Returns [`GeoError::InvalidAssertion`] for an ingress-rejected assertion.
    #[inline]
    pub fn as_bytes(&self) -> Result<Option<&[u8]>, GeoError> {
        match &self.state {
            AssertionState::Absent => Ok(None),
            AssertionState::Value { bytes, len } => Ok(Some(&bytes[..*len])),
            AssertionState::Invalid => Err(GeoError::InvalidAssertion),
        }
    }

    /// Copy at most `N` bytes. Oversize values become invalid before copying.
    ///
    /// Empty, malformed and non-UTF-8 bounded values remain values for the
    /// provider to reject; they never become absence or truncated valid input.
    #[inline]
    #[must_use]
    pub fn from_bytes(value: &[u8]) -> Self {
        if value.len() > N {
            return Self::invalid();
        }
        let mut bytes = [0; N];
        bytes[..value.len()].copy_from_slice(value);
        Self {
            state: AssertionState::Value {
                bytes,
                len: value.len(),
            },
        }
    }

    /// Record a rejected assertion without retaining its bytes.
    #[inline]
    #[must_use]
    pub const fn invalid() -> Self {
        Self {
            state: AssertionState::Invalid,
        }
    }
}

impl<const N: usize> fmt::Debug for GeoAssertion<N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.state {
            AssertionState::Absent => "GeoAssertion(Absent)",
            AssertionState::Value { .. } => "GeoAssertion(Value([redacted]))",
            AssertionState::Invalid => "GeoAssertion(Invalid)",
        })
    }
}

/// The complete v1 provider input, admitted by the adapter's protected source.
///
/// No request, reader IP, service handle or arbitrary headers are available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeoInput {
    pub country: GeoAssertion<2>,
    pub subdivision: GeoAssertion<3>,
}

/// Provider-owned resolution, without framework selection or provenance choices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeoResolution {
    Error(GeoError),
    Located(GeoLocation),
    MissingMetadata,
}

/// A synchronous provider performing only bounded local computation.
///
/// Implementations must not perform network, filesystem or other blocking I/O,
/// launch background work or resolve geography again during response finalization.
/// They return owned results without futures. The portable trait intentionally
/// imposes no `Send` or `Sync` bound; native composition adds those bounds.
///
/// Application Rust is trusted code. This narrow input prevents accidental
/// coupling, but does not sandbox a malicious implementation.
pub trait GeoProvider {
    /// Resolve source-admitted assertions, including absent or rejected slots.
    fn resolve(&self, input: &GeoInput) -> GeoResolution;
}

/// Why a selected provider could not supply geography.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoUnavailableReason {
    MissingMetadata,
    UntrustedIngress,
}

/// The source of a located assertion, not a distribution identity attestation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoSource {
    CloudFront,
}

/// The declared infrastructure protection used for source admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoTrustBasis {
    /// Operator-declared credential-gated, network-isolated ALB path.
    /// This does not prove the external listener or network configuration.
    DeclaredProtectedAlb,
}

/// Framework-owned status for a selected provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeoStatus {
    Error(GeoError),
    Located {
        location: GeoLocation,
        source: GeoSource,
        trust_basis: GeoTrustBasis,
    },
    Unavailable(GeoUnavailableReason),
}

/// One owned request outcome. It neither sets HTTP status nor grants permission.
///
/// Every selected status retains the deployment-selected provider identity,
/// including source rejection before a provider call. No fallback is implied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeoOutcome {
    NoSelection,
    Selected {
        provider: GeoProviderId,
        status: GeoStatus,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::str;

    #[test]
    fn geo_country_codes_normalize_and_reject_unsupported() {
        assert_eq!(CountryCode::new("us").unwrap().as_str(), "US");
        assert_eq!(CountryCode::new("gB").unwrap().as_str(), "GB");
        assert_eq!(CountryCode::new("NA").unwrap().as_str(), "NA");
        for value in [
            "", "U", "USA", " US", "US ", "U S", "\u{e9}", "U1", "US,CA", "\0U",
        ] {
            assert_eq!(CountryCode::new(value), Err(GeoValueError::InvalidCountry));
        }
        // Reserved, transitional and user-assigned codes are not aliases.
        for value in [
            "XX", "ZZ", "UK", "XK", "AA", "QM", "QZ", "XA", "XZ", "EU", "SU", "AN", "TP",
        ] {
            assert_eq!(CountryCode::new(value), Err(GeoValueError::InvalidCountry));
        }
        for (value, expected) in [
            ("ca", "CA"),
            ("1", "1"),
            ("01", "01"),
            ("001", "001"),
            ("a1b", "A1B"),
            ("ZZ", "ZZ"),
        ] {
            assert_eq!(SubdivisionCode::new(value).unwrap().as_str(), expected);
        }
        for value in [
            "", "ABCD", "US-CA", " CA", "CA ", "A B", "\u{e9}", "A_", "1,2", "\0",
        ] {
            assert_eq!(
                SubdivisionCode::new(value),
                Err(GeoValueError::InvalidSubdivision)
            );
        }
        let country = CountryCode::new("US").unwrap();
        let unknown = GeoLocation::new(country.clone(), Some(SubdivisionCode::new("ZZ").unwrap()));
        assert_eq!(unknown.country(), &country);
        assert_eq!(unknown.subdivision().unwrap().as_str(), "ZZ");
        assert!(GeoLocation::new(country, None).subdivision().is_none());
    }

    #[test]
    fn country_snapshot_is_ordered_unique_and_complete() {
        use sha2::{Digest as _, Sha256};

        assert_eq!(country_codes::ASSIGNED.len(), 249);
        assert!(
            country_codes::ASSIGNED
                .windows(2)
                .all(|pair| matches!(pair, [first, second] if first < second))
        );
        let mut snapshot = String::new();
        for code in country_codes::ASSIGNED {
            assert_eq!(code.len(), 2);
            assert!(code.bytes().all(|byte| byte.is_ascii_uppercase()));
            assert_eq!(CountryCode::new(code).unwrap().as_str(), *code);
            snapshot.push_str(code);
            snapshot.push('\n');
        }
        assert_eq!(
            format!("{:x}", Sha256::digest(snapshot.as_bytes())),
            country_codes::SNAPSHOT_SHA256
        );
        let mut accepted = 0;
        for first in b'A'..=b'Z' {
            for second in b'A'..=b'Z' {
                let bytes = [first, second];
                let value = str::from_utf8(&bytes).unwrap();
                let valid = country_codes::ASSIGNED.contains(&value);
                assert_eq!(CountryCode::new(value).is_ok(), valid);
                assert_eq!(CountryCode::new(&value.to_ascii_lowercase()).is_ok(), valid);
                accepted += usize::from(valid);
            }
        }
        assert_eq!(accepted, 249);
    }

    #[test]
    fn provider_ids_are_bounded_public_labels_with_a_reserved_builtin() {
        for value in [
            "a",
            "local",
            "test.provider-1_v2",
            &format!("a{}", "9".repeat(63)),
        ] {
            assert_eq!(GeoProviderId::new(value).unwrap().as_str(), value);
        }
        for value in [
            "",
            "A",
            "1a",
            "_a",
            "a b",
            "\u{e9}",
            "a/b",
            "a\n",
            &"a".repeat(65),
        ] {
            assert_eq!(
                GeoProviderId::new(value),
                Err(GeoValueError::InvalidProviderId)
            );
        }
        assert_eq!(
            GeoProviderId::new("cloudfront"),
            Err(GeoValueError::ReservedProviderId)
        );
        assert_eq!(GeoProviderId::cloudfront().as_str(), "cloudfront");
    }

    #[test]
    fn assertions_are_bounded_owned_and_redacted() {
        assert_eq!(GeoAssertion::<2>::absent().as_bytes(), Ok(None));
        assert_eq!(
            GeoAssertion::<2>::invalid().as_bytes(),
            Err(GeoError::InvalidAssertion)
        );
        for bytes in [b"".as_slice(), b"U", b"US", &[0xff, 0xfe]] {
            let assertion = GeoAssertion::<2>::from_bytes(bytes);
            assert_eq!(assertion.as_bytes(), Ok(Some(bytes)));
            assert_eq!(format!("{assertion:?}"), "GeoAssertion(Value([redacted]))");
        }
        assert_eq!(
            GeoAssertion::<2>::from_bytes(b"USA").as_bytes(),
            Err(GeoError::InvalidAssertion)
        );
        assert_eq!(
            GeoAssertion::<3>::from_bytes(b"123").as_bytes(),
            Ok(Some(b"123".as_slice()))
        );
        assert_eq!(
            GeoAssertion::<3>::from_bytes(b"1234").as_bytes(),
            Err(GeoError::InvalidAssertion)
        );
        assert_eq!(
            GeoAssertion::<0>::from_bytes(b"X").as_bytes(),
            Err(GeoError::InvalidAssertion)
        );
        let mut source = *b"us";
        let assertion = GeoAssertion::<2>::from_bytes(&source);
        source.fill(b'X');
        assert_eq!(assertion.as_bytes(), Ok(Some(b"us".as_slice())));
        let input = GeoInput {
            country: assertion,
            subdivision: GeoAssertion::from_bytes(b"ca"),
        };
        let debug = format!("{input:?}");
        assert!(!debug.contains("us"));
        assert!(!debug.contains("ca"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn validated_values_and_outcomes_have_equality_and_safe_diagnostics() {
        let location = GeoLocation::new(
            CountryCode::new("us").unwrap(),
            Some(SubdivisionCode::new("ca").unwrap()),
        );
        assert_eq!(
            location,
            GeoLocation::new(
                CountryCode::new("US").unwrap(),
                Some(SubdivisionCode::new("CA").unwrap())
            )
        );
        let outcome = GeoOutcome::Selected {
            provider: GeoProviderId::cloudfront(),
            status: GeoStatus::Located {
                location,
                source: GeoSource::CloudFront,
                trust_basis: GeoTrustBasis::DeclaredProtectedAlb,
            },
        };
        assert_eq!(outcome.clone(), outcome);
        let debug = format!("{outcome:?}");
        assert!(debug.contains("US"));
        assert!(debug.contains("CA"));
        assert!(debug.contains("cloudfront"));
        assert_ne!(GeoOutcome::NoSelection, outcome);
        for reason in [
            GeoUnavailableReason::MissingMetadata,
            GeoUnavailableReason::UntrustedIngress,
        ] {
            let selected = GeoOutcome::Selected {
                provider: GeoProviderId::new("custom").unwrap(),
                status: GeoStatus::Unavailable(reason),
            };
            assert!(format!("{selected:?}").contains("custom"));
        }
        assert_eq!(
            GeoError::InvalidAssertion.to_string(),
            "invalid geographic assertion"
        );
        assert_eq!(
            GeoError::ProviderFailure.to_string(),
            "geographic provider failure"
        );
        assert_eq!(
            GeoValueError::InvalidCountry.to_string(),
            "invalid country code"
        );
    }

    #[test]
    fn portable_provider_is_synchronous_and_need_not_be_send_or_sync() {
        struct LocalProvider(Rc<Cell<usize>>);
        impl GeoProvider for LocalProvider {
            fn resolve(&self, _input: &GeoInput) -> GeoResolution {
                self.0.set(self.0.get() + 1);
                GeoResolution::MissingMetadata
            }
        }
        let calls = Rc::new(Cell::new(0));
        let provider: Box<dyn GeoProvider> = Box::new(LocalProvider(Rc::clone(&calls)));
        let input = GeoInput {
            country: GeoAssertion::absent(),
            subdivision: GeoAssertion::absent(),
        };
        assert_eq!(provider.resolve(&input), GeoResolution::MissingMetadata);
        assert_eq!(calls.get(), 1);
    }
}
