//! Local normalization of source-admitted `CloudFront` geographic assertions.

use std::str;

use edgezero_core::geo::{
    CountryCode, GeoError, GeoInput, GeoLocation, GeoProvider, GeoResolution, SubdivisionCode,
};

pub(super) struct CloudFrontProvider;

impl GeoProvider for CloudFrontProvider {
    #[inline]
    fn resolve(&self, input: &GeoInput) -> GeoResolution {
        let (Ok(country_slot), Ok(subdivision_slot)) =
            (input.country.as_bytes(), input.subdivision.as_bytes())
        else {
            return GeoResolution::Error(GeoError::InvalidAssertion);
        };
        let Some(country_bytes) = country_slot else {
            return if subdivision_slot.is_none() {
                GeoResolution::MissingMetadata
            } else {
                GeoResolution::Error(GeoError::InvalidAssertion)
            };
        };
        let parsed_country = str::from_utf8(country_bytes)
            .ok()
            .and_then(|value| CountryCode::new(value).ok());
        let Some(country_code) = parsed_country else {
            return GeoResolution::Error(GeoError::InvalidAssertion);
        };
        let subdivision_code = match subdivision_slot {
            None => None,
            Some(bytes) => {
                let parsed_subdivision = str::from_utf8(bytes)
                    .ok()
                    .and_then(|value| SubdivisionCode::new(value).ok());
                let Some(component) = parsed_subdivision else {
                    return GeoResolution::Error(GeoError::InvalidAssertion);
                };
                Some(component)
            }
        };
        GeoResolution::Located(GeoLocation::new(country_code, subdivision_code))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::geo::GeoAssertion;

    fn input(country: Option<&[u8]>, subdivision: Option<&[u8]>) -> GeoInput {
        GeoInput {
            country: country.map_or_else(GeoAssertion::absent, GeoAssertion::from_bytes),
            subdivision: subdivision.map_or_else(GeoAssertion::absent, GeoAssertion::from_bytes),
        }
    }

    fn located(country: &str, subdivision: Option<&str>) -> GeoResolution {
        GeoResolution::Located(GeoLocation::new(
            CountryCode::new(country).unwrap(),
            subdivision.map(|value| SubdivisionCode::new(value).unwrap()),
        ))
    }

    #[test]
    fn geo_cloudfront_parsing_matches_spec() {
        let invalid = GeoResolution::Error(GeoError::InvalidAssertion);
        let cases = [
            (input(None, None), GeoResolution::MissingMetadata),
            (input(Some(b"us"), Some(b"ca")), located("US", Some("CA"))),
            (input(Some(b"US"), None), located("US", None)),
            (input(Some(b"US"), Some(b"ZZ")), located("US", Some("ZZ"))),
            (input(Some(b"GB"), Some(b"eng")), located("GB", Some("ENG"))),
            (input(Some(b"JP"), Some(b"01")), located("JP", Some("01"))),
            (input(Some(b"BR"), Some(b"1")), located("BR", Some("1"))),
            (input(Some(b"AU"), Some(b"001")), located("AU", Some("001"))),
            (input(Some(b"US"), Some(b"a1B")), located("US", Some("A1B"))),
            (input(None, Some(b"CA")), invalid.clone()),
            (input(Some(b""), None), invalid.clone()),
            (input(Some(b"U"), None), invalid.clone()),
            (input(Some(b"USA"), None), invalid.clone()),
            (input(Some(b"USAA"), None), invalid.clone()),
            (input(Some(b"XX"), None), invalid.clone()),
            (input(Some(b"ZZ"), None), invalid.clone()),
            (input(Some(b"UK"), None), invalid.clone()),
            (input(Some(b"XK"), None), invalid.clone()),
            (input(Some(b"U1"), None), invalid.clone()),
            (input(Some(b" US"), None), invalid.clone()),
            (input(Some(b"US "), None), invalid.clone()),
            (input(Some(b" U"), None), invalid.clone()),
            (input(Some(b"U\t"), None), invalid.clone()),
            (input(Some(b"U\0"), None), invalid.clone()),
            (input(Some(b"\xc3\xa9"), None), invalid.clone()),
            (input(Some(b"\xff\xfe"), None), invalid.clone()),
            (input(Some(b"US,CA"), None), invalid.clone()),
            (input(Some(b"%55S"), None), invalid.clone()),
            (input(Some(b"US"), Some(b"")), invalid.clone()),
            (input(Some(b"US"), Some(b"ABCD")), invalid.clone()),
            (input(Some(b"US"), Some(b"US-CA")), invalid.clone()),
            (input(Some(b"US"), Some(b" CA")), invalid.clone()),
            (input(Some(b"US"), Some(b"CA ")), invalid.clone()),
            (input(Some(b"US"), Some(b"A B")), invalid.clone()),
            (input(Some(b"US"), Some(b"A\t")), invalid.clone()),
            (input(Some(b"US"), Some(b"C\0")), invalid.clone()),
            (input(Some(b"US"), Some(b"\xc3\xa9")), invalid.clone()),
            (input(Some(b"US"), Some(b"\xff")), invalid.clone()),
            (input(Some(b"US"), Some(b"A,B")), invalid.clone()),
            (input(Some(b"US"), Some(b"A-B")), invalid.clone()),
            (input(Some(b"US"), Some(b"A_B")), invalid.clone()),
            (input(Some(b"US"), Some(b"%41")), invalid.clone()),
            (
                GeoInput {
                    country: GeoAssertion::invalid(),
                    subdivision: GeoAssertion::absent(),
                },
                invalid.clone(),
            ),
            (
                GeoInput {
                    country: GeoAssertion::from_bytes(b"US"),
                    subdivision: GeoAssertion::invalid(),
                },
                invalid.clone(),
            ),
            (
                GeoInput {
                    country: GeoAssertion::absent(),
                    subdivision: GeoAssertion::invalid(),
                },
                invalid,
            ),
        ];
        let provider = CloudFrontProvider;
        for (index, (input, expected)) in cases.iter().enumerate() {
            assert_eq!(&provider.resolve(input), expected, "case {index}");
        }
    }
}
