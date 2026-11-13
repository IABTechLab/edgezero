//! Client JA4 TLS fingerprints captured by adapters.
//!
//! JA4 is a TLS client fingerprinting method by `FoxIO`, licensed under
//! BSD-3-Clause. JA4+ is a trademark of `FoxIO`. Other JA4+ methods are not
//! provided. Fingerprints are only ever taken from trusted runtime APIs, never
//! from request headers.

use std::fmt;

use crate::error::EdgeError;

/// Length in bytes of a JA4 fingerprint.
const JA4_LEN: usize = 36;

/// A JA4 TLS client fingerprint, validated as `a_b_c`.
///
/// `Debug` is redacted and there is no `Display` or `Serialize`; read the value
/// explicitly with [`Ja4Fingerprint::as_str`] or a section accessor.
#[derive(Clone, PartialEq, Eq)]
pub struct Ja4Fingerprint(Box<str>);

impl Ja4Fingerprint {
    /// The full 36-character fingerprint.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// `FoxIO` `JA4_b`: truncated SHA-256 of the sorted cipher suites.
    #[inline]
    #[must_use]
    pub fn cipher_hash(&self) -> &str {
        self.0.get(11..23).unwrap_or_default()
    }

    /// `FoxIO` `JA4_c`: truncated SHA-256 of the sorted extensions and signature algorithms.
    #[inline]
    #[must_use]
    pub fn extension_hash(&self) -> &str {
        self.0.get(24..JA4_LEN).unwrap_or_default()
    }

    /// Validate `value` as a JA4 fingerprint without normalising it.
    ///
    /// # Errors
    /// Returns a bad-request error for any value that is not a JA4
    /// fingerprint. The error never contains the input.
    #[inline]
    pub fn parse(value: &str) -> Result<Self, EdgeError> {
        let valid = value.len() == JA4_LEN
            && value.bytes().enumerate().all(|(position, byte)| {
                let lower_hex = byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
                match position {
                    0 => matches!(byte, b't' | b'q' | b'd'),
                    1 | 2 => byte.is_ascii_lowercase() || byte.is_ascii_digit(),
                    3 => matches!(byte, b'd' | b'i'),
                    4..=7 => byte.is_ascii_digit(),
                    8 | 9 => byte.is_ascii_alphanumeric(),
                    10 | 23 => byte == b'_',
                    _ => lower_hex,
                }
            });
        if valid {
            Ok(Self(Box::from(value)))
        } else {
            Err(EdgeError::bad_request("invalid JA4 fingerprint"))
        }
    }

    /// `FoxIO` `JA4_a`: protocol, TLS version, SNI, cipher and extension counts, ALPN.
    #[inline]
    #[must_use]
    pub fn summary(&self) -> &str {
        self.0.get(0..10).unwrap_or_default()
    }
}

impl fmt::Debug for Ja4Fingerprint {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Ja4Fingerprint([redacted])")
    }
}

/// What an adapter captured for this request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapturedJa4 {
    /// A validated fingerprint with its provenance.
    Captured {
        /// The validated fingerprint.
        fingerprint: Ja4Fingerprint,
        /// Where the fingerprint came from.
        source: Ja4Source,
    },
    /// No fingerprint is available, with the reason.
    Unavailable(Ja4Unavailable),
}

impl CapturedJa4 {
    /// Map a runtime TLS accessor's result. Invalid values are discarded.
    #[inline]
    #[must_use]
    pub fn from_runtime_tls(value: Option<&str>) -> Self {
        match value.map(Ja4Fingerprint::parse) {
            None => Self::Unavailable(Ja4Unavailable::Missing),
            Some(Ok(fingerprint)) => Self::Captured {
                fingerprint,
                source: Ja4Source::RuntimeTls,
            },
            Some(Err(_error)) => Self::Unavailable(Ja4Unavailable::Malformed),
        }
    }
}

/// Where a captured fingerprint came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Ja4Source {
    /// Computed by the runtime that terminated the client's TLS connection.
    RuntimeTls,
}

/// Why no fingerprint was captured.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Ja4Unavailable {
    /// The runtime returned a value that failed validation; it was discarded.
    Malformed,
    /// The runtime exposes JA4 but returned none for this request.
    Missing,
    /// The runtime offers no JA4 API.
    NotExposed,
}

#[cfg(test)]
mod tests {
    use crate::ja4::{CapturedJa4, Ja4Fingerprint, Ja4Source, Ja4Unavailable};

    const EXAMPLE: &str = "t13d1516h2_8daaf6152771_e5627efa2ab1";

    #[test]
    fn ja4_accepts_valid_fingerprints() {
        for value in [
            EXAMPLE,
            "q13d0310h3_55b375c5d22e_cd85d2d88918",
            "d12i0000h2_000000000000_000000000000",
            "t13i1516H2_8daaf6152771_e5627efa2ab1",
            "t1ad151600_8daaf6152771_e5627efa2ab1",
        ] {
            let parsed = Ja4Fingerprint::parse(value).expect("should accept JA4");
            assert_eq!(parsed.as_str(), value);
        }
    }

    #[test]
    fn ja4_section_accessors() {
        let parsed = Ja4Fingerprint::parse(EXAMPLE).expect("should accept JA4");
        assert_eq!(parsed.summary(), "t13d1516h2");
        assert_eq!(parsed.cipher_hash(), "8daaf6152771");
        assert_eq!(parsed.extension_hash(), "e5627efa2ab1");
    }

    #[test]
    fn ja4_rejects_invalid_values() {
        let non_ascii = format!("t13d1516h2_8daaf6152771_e5627efa2a{}", '\u{e9}');
        for value in [
            "",
            "t13d1516h2_8daaf6152771_e5627efa2ab",
            "t13d1516h2_8daaf6152771_e5627efa2ab11",
            " t13d1516h2_8daaf6152771_e5627efa2ab",
            "t13d1516h2_8daaf6152771_e5627efa2ab ",
            "t13d1516h2_8DAAF6152771_e5627efa2ab1",
            "t13d1516h2_8daaf6152771_E5627EFA2AB1",
            "t13d1516h2-8daaf6152771_e5627efa2ab1",
            "t13d1516h2_8daaf6152771-e5627efa2ab1",
            "t13d1516h28daaf6152771_e5627efa2ab12",
            "x13d1516h2_8daaf6152771_e5627efa2ab1",
            "t13x1516h2_8daaf6152771_e5627efa2ab1",
            "t13d15a6h2_8daaf6152771_e5627efa2ab1",
            "t13d1516h2_8daaf615277g_e5627efa2ab1",
            non_ascii.as_str(),
            "e7d705a3286e19ea42f587b344ee6865",
            "t13d1516h2_002f,0035,009c_0005,000a",
        ] {
            assert!(
                Ja4Fingerprint::parse(value).is_err(),
                "should reject {value:?}"
            );
        }
    }

    #[test]
    fn ja4_runtime_mapping() {
        assert_eq!(
            CapturedJa4::from_runtime_tls(None),
            CapturedJa4::Unavailable(Ja4Unavailable::Missing)
        );
        assert_eq!(
            CapturedJa4::from_runtime_tls(Some(EXAMPLE)),
            CapturedJa4::Captured {
                fingerprint: Ja4Fingerprint::parse(EXAMPLE).expect("should accept JA4"),
                source: Ja4Source::RuntimeTls,
            }
        );
        assert_eq!(
            CapturedJa4::from_runtime_tls(Some("not-a-ja4")),
            CapturedJa4::Unavailable(Ja4Unavailable::Malformed)
        );
    }

    #[test]
    fn ja4_never_prints_fingerprint_or_input() {
        let captured = CapturedJa4::from_runtime_tls(Some(EXAMPLE));
        let output = format!("{captured:?}");
        assert!(!output.contains("8daaf6152771"));
        assert!(output.contains("redacted"));
        let error = Ja4Fingerprint::parse("secret-input").expect_err("should reject");
        assert!(!format!("{error:?} {error}").contains("secret-input"));
    }
}
