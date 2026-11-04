//! Immutable local MMDB snapshots. Preparation is synchronous and Unix-only.
//!
//! File bytes and network visits are caller-bounded. Before verification the
//! upstream tree allowance `3 * node_count + 1` must fit that visit budget.
//! Upstream verification charges at most eight times each section's bytes and
//! memoizes shared values. Each additional structural decode and borrowed path
//! decode has the reader's 65,536-value, payload and depth limits. Structural
//! decoding deliberately expands shared values through the normal decoder.
//! Thus total decode work is bounded per unique record, not globally by bytes.
//! The dedup set is bounded by network visits; upstream tables are O(file bytes).
//! These bounds are not wall-clock or fixed-RSS promises.
//!
//! Metadata receives upstream typed decoding and verification. Version 0.32.0
//! exposes neither a raw metadata decoder nor a metadata `LookupResult`. Therefore
//! unknown metadata fields retain upstream verifier gaps for non-string map keys
//! and pointer chains. Reachable data records receive the stricter pass below.

#![expect(
    clippy::field_scoped_visibility_modifiers,
    reason = "snapshot metadata is shared only with the native composition parent"
)]
#![expect(
    clippy::map_err_ignore,
    clippy::wildcard_enum_match_arm,
    reason = "vendor and I/O details deliberately collapse to source-free error categories"
)]

use std::collections::HashSet;
#[cfg(any(unix, test))]
use std::io::{ErrorKind, Read};

use base16ct::lower::encode_string;
use edgezero_core::geo::{
    CountryCode, GeoError, GeoInput, GeoLocation, GeoProvider, GeoResolution, SubdivisionCode,
};
use maxminddb::{LookupResult, MaxMindDbError, Reader, WithinOptions, path};
use sha2::{Digest as _, Sha256};

use super::{MaxMindGeoConfig, NativeGeoConfigError};

mod structure;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaxMindSnapshot {
    pub(super) database_type: String,
    pub(super) build_epoch: u64,
    pub(super) sha256: String,
}

#[derive(Clone, Copy)]
enum Dataset {
    City,
    Country,
}

pub(super) struct MaxMindProvider {
    reader: Reader<Vec<u8>>,
    dataset: Dataset,
    pub(super) metadata: MaxMindSnapshot,
}

impl MaxMindProvider {
    pub(super) fn prepare(config: &MaxMindGeoConfig) -> Result<Self, NativeGeoConfigError> {
        let byte_cap = config.limits.max_database_bytes;
        let visit_cap = config.limits.max_validation_networks;
        if byte_cap == 0 || visit_cap == 0 || byte_cap.checked_add(1).is_none() {
            return Err(NativeGeoConfigError::DatabaseLimit);
        }
        let bytes = acquire(config)?;
        let checksum = encode_string(&Sha256::digest(&bytes));
        let reader = Reader::from_source(bytes).map_err(database_error)?;
        let reader_metadata = reader.metadata();
        let dataset = match reader_metadata.database_type.as_str() {
            "GeoLite2-City" => Dataset::City,
            "GeoLite2-Country" => Dataset::Country,
            _ => return Err(NativeGeoConfigError::UnsupportedDatabase),
        };
        if !matches!(reader_metadata.ip_version, 4 | 6) {
            return Err(NativeGeoConfigError::UnsupportedDatabase);
        }
        let node_count = usize::try_from(reader_metadata.node_count)
            .map_err(|_| NativeGeoConfigError::DatabaseLimit)?;
        let tree_allowance = node_count
            .checked_mul(3)
            .and_then(|n| n.checked_add(1))
            .ok_or(NativeGeoConfigError::DatabaseLimit)?;
        if tree_allowance > visit_cap {
            return Err(NativeGeoConfigError::DatabaseLimit);
        }
        reader.verify().map_err(database_error)?;
        let options = WithinOptions::default()
            .include_aliased_networks()
            .include_networks_without_data();
        let networks = reader.networks(options).map_err(database_error)?;
        let mut offsets = HashSet::new();
        for (index, next_network) in networks.enumerate() {
            // Count no-data leaves and aliases before deduplication.
            if index >= visit_cap {
                return Err(NativeGeoConfigError::DatabaseLimit);
            }
            let result = next_network.map_err(database_error)?;
            if let Some(offset) = result.offset()
                && offsets.insert(offset)
            {
                result
                    .decode::<structure::StructuralValue>()
                    .map_err(database_error)?;
                projection(&result, dataset).map_err(|error| match error {
                    MaxMindDbError::ResourceLimit { .. } => NativeGeoConfigError::DatabaseLimit,
                    _ => NativeGeoConfigError::InvalidDatabaseSchema,
                })?;
            }
        }
        let metadata = MaxMindSnapshot {
            database_type: reader_metadata.database_type.clone(),
            build_epoch: reader_metadata.build_epoch,
            sha256: checksum,
        };
        Ok(Self {
            reader,
            dataset,
            metadata,
        })
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "map_err transfers ownership and intentionally drops vendor diagnostics"
)]
fn database_error(error: MaxMindDbError) -> NativeGeoConfigError {
    match error {
        MaxMindDbError::ResourceLimit { .. } => NativeGeoConfigError::DatabaseLimit,
        _ => NativeGeoConfigError::InvalidDatabase,
    }
}

// The identical borrowed paths validate consumed types during preparation and
// resolve them during lookup. Duplicate keys consequently use the first value.
fn projection<'database>(
    result: &LookupResult<'database, Vec<u8>>,
    dataset: Dataset,
) -> Result<(Option<&'database str>, Option<&'database str>), MaxMindDbError> {
    let country = result.decode_path::<&str>(&path!["country", "iso_code"])?;
    let subdivision = match dataset {
        Dataset::City => result.decode_path::<&str>(&path!["subdivisions", 0_usize, "iso_code"])?,
        Dataset::Country => None,
    };
    Ok((country, subdivision))
}

impl GeoProvider for MaxMindProvider {
    fn resolve(&self, input: &GeoInput) -> GeoResolution {
        let GeoInput::ReaderIp { address, .. } = input else {
            return GeoResolution::Error(GeoError::ProviderFailure);
        };
        // Checked ingress already normalizes mapped addresses. Do not invent
        // source admission or address rewriting inside the database provider.
        if address.is_ipv6() && self.reader.metadata().ip_version == 4 {
            return GeoResolution::NoMatch;
        }
        let Ok(result) = self.reader.lookup(*address) else {
            return GeoResolution::Error(GeoError::ProviderFailure);
        };
        let Ok((country_slot, subdivision_slot)) = projection(&result, self.dataset) else {
            return GeoResolution::Error(GeoError::ProviderFailure);
        };
        let Some(country_value) = country_slot else {
            return if subdivision_slot.is_none() {
                GeoResolution::NoMatch
            } else {
                GeoResolution::Error(GeoError::InvalidAssertion)
            };
        };
        let Ok(country) = CountryCode::new(country_value) else {
            return GeoResolution::Error(GeoError::InvalidAssertion);
        };
        let Ok(subdivision) = subdivision_slot.map(SubdivisionCode::new).transpose() else {
            return GeoResolution::Error(GeoError::InvalidAssertion);
        };
        GeoResolution::Located(GeoLocation::new(country, subdivision))
    }
}

#[cfg(unix)]
#[expect(
    clippy::filetype_is_file,
    reason = "only regular files are accepted; all special file kinds must fail"
)]
fn acquire(config: &MaxMindGeoConfig) -> Result<Vec<u8>, NativeGeoConfigError> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt as _;

    // Opening first avoids pathname races; NONBLOCK prevents waiting for a FIFO
    // writer. Descriptor metadata rejects pipes/devices before any read.
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&config.path)
        .map_err(|_| NativeGeoConfigError::UnavailableDatabase)?;
    let metadata = file
        .metadata()
        .map_err(|_| NativeGeoConfigError::UnavailableDatabase)?;
    if !metadata.file_type().is_file() {
        return Err(NativeGeoConfigError::UnavailableDatabase);
    }
    let cap = config.limits.max_database_bytes;
    let file_cap = u64::try_from(cap).map_err(|_| NativeGeoConfigError::DatabaseLimit)?;
    if metadata.len() > file_cap {
        return Err(NativeGeoConfigError::DatabaseLimit);
    }
    read_bounded(file, cap)
}

// No special-file-safe acquisition has been proven for other platforms. Fail
// before opening anything rather than potentially blocking on a named pipe.
#[cfg(not(unix))]
fn acquire(_config: &MaxMindGeoConfig) -> Result<Vec<u8>, NativeGeoConfigError> {
    Err(NativeGeoConfigError::UnavailableDatabase)
}

#[cfg(any(unix, test))]
#[expect(
    clippy::indexing_slicing,
    reason = "slice ends are bounded by chunk length and Read's returned count"
)]
fn read_bounded(mut source: impl Read, cap: usize) -> Result<Vec<u8>, NativeGeoConfigError> {
    let limit = cap
        .checked_add(1)
        .ok_or(NativeGeoConfigError::DatabaseLimit)?;
    let mut bytes = Vec::new();
    // Incremental fixed-size reads avoid reserving the full operator limit for a
    // tiny database. Vec capacity can exceed length; it is O(cap), not cap exact.
    let mut chunk = [0; 8192];
    while bytes.len() < limit {
        let remaining = chunk.len().min(limit.saturating_sub(bytes.len()));
        let count = match source.read(&mut chunk[..remaining]) {
            Ok(count) => count,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return Err(NativeGeoConfigError::UnavailableDatabase),
        };
        if count == 0 {
            break;
        }
        bytes
            .try_reserve(count)
            .map_err(|_| NativeGeoConfigError::DatabaseLimit)?;
        bytes.extend_from_slice(&chunk[..count]);
    }
    if bytes.len() > cap {
        return Err(NativeGeoConfigError::DatabaseLimit);
    }
    Ok(bytes)
}
