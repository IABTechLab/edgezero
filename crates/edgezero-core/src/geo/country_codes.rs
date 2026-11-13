//! Assigned ISO 3166-1 alpha-2 country codes, captured 2026-10-22.
//!
//! Authority: ISO 3166 Maintenance Agency, <https://www.iso.org/obp/ui/#search/code/>
//! and <https://www.iso.org/iso-3166-country-codes.html>.
//! Imported from datasets/country-list revision
//! ca393dfa16db21ce07be77902c1c129d63c86d9f, data.csv:
//! <https://github.com/datasets/country-list/blob/ca393dfa16db21ce07be77902c1c129d63c86d9f/data.csv>
//! Source CSV SHA-256: 3187a6850a6d2d8f03c099ac6eb2477d38fe7d2a85a3a5c1394eaf12e6e03d76.
//!
//! Only the 249 code elements are used, sorted by ASCII code. No country names,
//! software, or original arrangement are reproduced. The upstream list derives
//! from pycountry and Debian iso-codes; its maintainers dedicate their own work
//! under ODC-PDDL-1.0. See that revision's README.md and ATTRIBUTION.txt.
//! ISO permits free-of-charge use of the codes:
//! <https://committee.iso.org/iso-3166-1-new-access.html>.
//! This table does not reproduce the ISO standard and is not endorsed by ISO.
//!
//! Updates require an explicit source revision, checksum and exclusion review.
//! The snapshot checksum covers sorted codes, each followed by one LF byte.

#[cfg(test)]
pub(super) const SNAPSHOT_SHA256: &str =
    "801ef127f0b3e6b4e971c239c9b8475caedb65c17573d84ca1b57eed72523a0e";

pub(super) const ASSIGNED: &[&str] = &[
    "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS", "AT", "AU", "AW", "AX", "AZ",
    "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS",
    "BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN",
    "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE",
    "EG", "EH", "ER", "ES", "ET", "FI", "FJ", "FK", "FM", "FO", "FR", "GA", "GB", "GD", "GE", "GF",
    "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY", "HK", "HM",
    "HN", "HR", "HT", "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT", "JE", "JM",
    "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ", "LA", "LB", "LC",
    "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK",
    "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA",
    "NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA", "PE", "PF", "PG",
    "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY", "QA", "RE", "RO", "RS", "RU", "RW",
    "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS",
    "ST", "SV", "SX", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO",
    "TR", "TT", "TV", "TW", "TZ", "UA", "UG", "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI",
    "VN", "VU", "WF", "WS", "YE", "YT", "ZA", "ZM", "ZW",
];
