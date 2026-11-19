//! Versioned, exact-key KV geography index.
// Keep the codec, writer, cache and reader together in wire-format order.
#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "wire-format and lifecycle grouping"
)]
// Header and record lengths are checked before byte addressing. The tree format uses
// network byte order and binary search/byte-budget division operates on bounded counts.
#![expect(
    clippy::arithmetic_side_effects,
    clippy::big_endian_bytes,
    clippy::indexing_slicing,
    clippy::integer_division,
    clippy::integer_division_remainder_used,
    clippy::missing_asserts_for_indexing,
    reason = "fixed-width, validated wire-format data and bounded byte budgets"
)]

use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    sync::Mutex,
};

use super::{CountryCode, GeoLocation, SubdivisionCode};
use crate::key_value_store::KvHandle;
use anyhow::{Context as _, Result, bail, ensure};
use base16ct::lower::{decode as decode_hex, encode_string};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::str::from_utf8;

const HEADER: usize = 12;
const MAGIC: &[u8; 4] = b"EZG1";

#[must_use]
#[inline]
pub fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
#[must_use]
#[inline]
pub fn hex(bytes: &[u8]) -> String {
    encode_string(bytes)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Location([u8; 6]);

impl Location {
    /// Normalize a source location without forcing it into core's country allowlist.
    ///
    /// # Errors
    /// Rejects invalid source country or subdivision syntax.
    #[inline]
    pub fn new(raw_country: &str, raw_subdivision: &str) -> Result<Self> {
        // Source-format DTO, not GeoLocation. Preserve XK and report the domain mismatch.
        ensure!(
            raw_country.len() == 2 && raw_country.bytes().all(|byte| byte.is_ascii_alphabetic()),
            "invalid source country syntax"
        );
        let country = raw_country.to_ascii_uppercase();
        let subdivision = if raw_subdivision.is_empty() {
            None
        } else {
            Some(SubdivisionCode::new(raw_subdivision)?)
        };
        let mut bytes = [0; 6];
        bytes[..2].copy_from_slice(country.as_bytes());
        if let Some(code) = subdivision {
            let encoded = code.as_str().as_bytes();
            bytes[2] = u8::try_from(encoded.len())?;
            bytes[3..3 + encoded.len()].copy_from_slice(encoded);
        }
        Ok(Self(bytes))
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let raw: [u8; 6] = bytes.try_into()?;
        let len = usize::from(raw[2]);
        ensure!(
            len <= 3
                && raw[..2].iter().all(u8::is_ascii_uppercase)
                && raw[3..3 + len]
                    .iter()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
                && raw[3 + len..].iter().all(|byte| *byte == 0),
            "invalid location encoding"
        );
        Ok(Self(raw))
    }

    /// Map a validated source location to core geography without inventing a country.
    #[must_use]
    #[inline]
    pub fn geo_location(self) -> Option<GeoLocation> {
        let country = CountryCode::new(from_utf8(&self.0[..2]).ok()?).ok()?;
        let subdivision = if self.0[2] > 0 {
            let text = from_utf8(&self.0[3..3 + usize::from(self.0[2])]).ok()?;
            Some(SubdivisionCode::new(text).ok()?)
        } else {
            None
        };
        Some(GeoLocation::new(country, subdivision))
    }

    #[must_use]
    #[inline]
    pub fn core_country_supported(self) -> bool {
        self.geo_location().is_some()
    }

    #[must_use]
    #[inline]
    pub fn json(self) -> serde_json::Value {
        let country = from_utf8(&self.0[..2]).unwrap_or_default();
        let subdivision = if self.0[2] == 0 {
            None
        } else {
            from_utf8(&self.0[3..3 + usize::from(self.0[2])]).ok()
        };
        serde_json::json!({"country": country, "subdivision": subdivision, "core_country_supported": self.core_country_supported()})
    }
}

#[cfg(feature = "geo-kv-build")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Range {
    pub start: u128,
    pub end: u128,
    pub location: Location,
}

/// Return the wire width in bytes for an IP address family.
///
/// # Errors
/// Rejects unknown families.
fn width(family: u8) -> Result<usize> {
    match family {
        4 => Ok(4),
        6 => Ok(16),
        _ => bail!("invalid address family"),
    }
}

#[must_use]
#[inline]
pub fn address(input: IpAddr) -> (u8, u128) {
    match input.to_canonical() {
        IpAddr::V4(ip) => (4, u32::from(ip).into()),
        IpAddr::V6(ip) => (6, u128::from(ip)),
    }
}

fn max_address(family: u8) -> u128 {
    if family == 4 {
        u32::MAX.into()
    } else {
        u128::MAX
    }
}
fn read_address(bytes: &[u8]) -> u128 {
    let mut raw = [0; 16];
    raw[16 - bytes.len()..].copy_from_slice(bytes);
    u128::from_be_bytes(raw)
}
#[cfg(feature = "geo-kv-build")]
fn write_address(out: &mut Vec<u8>, value: u128, width: usize) {
    out.extend_from_slice(&value.to_be_bytes()[16 - width..]);
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeRef {
    pub start: u128,
    pub end: u128,
    pub hash: [u8; 32],
    pub level: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub edition: String,
    pub files: Vec<(String, String)>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema: u32,
    pub generation: String,
    pub object_bytes: usize,
    pub source: Source,
    pub ipv4: Option<NodeRef>,
    pub ipv6: Option<NodeRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub manifest_key: String,
    pub manifest_hash: [u8; 32],
}

#[cfg(feature = "geo-kv-build")]
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryEntry {
    pub key: String,
    pub sha256: String,
    pub bytes: usize,
}

#[must_use]
#[inline]
pub fn node_key(generation: &str, family: u8, hash: &[u8; 32]) -> String {
    format!("geo/v1/{generation}/v{family}/{}", hex(hash))
}

#[cfg(feature = "geo-kv-build")]
fn header(family: u8, level: u16, count: usize) -> Result<Vec<u8>> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&[family, u8::from(level > 0)]);
    bytes.extend_from_slice(&level.to_be_bytes());
    bytes.extend_from_slice(&u32::try_from(count)?.to_be_bytes());
    Ok(bytes)
}

#[cfg(feature = "geo-kv-build")]
async fn publish_node(
    kv: &KvHandle,
    generation: &str,
    family: u8,
    reference: &NodeRef,
    bytes: Vec<u8>,
    inventory: &mut Vec<InventoryEntry>,
) -> Result<()> {
    let key = node_key(generation, family, &reference.hash);
    inventory.push(InventoryEntry {
        key: key.clone(),
        sha256: hex(&reference.hash),
        bytes: bytes.len(),
    });
    kv.put_bytes(&key, bytes.into()).await?;
    Ok(())
}

#[cfg(feature = "geo-kv-build")]
async fn build_family(
    kv: &KvHandle,
    generation: &str,
    family: u8,
    ranges: &[Range],
    object_bytes: usize,
    inventory: &mut Vec<InventoryEntry>,
) -> Result<Option<NodeRef>> {
    let address_bytes = width(family)?;
    let leaf_capacity = (object_bytes - HEADER) / (2 * address_bytes + 6);
    let directory_capacity = (object_bytes - HEADER) / (2 * address_bytes + 32);
    ensure!(
        leaf_capacity > 0 && directory_capacity >= 2,
        "object budget cannot fit a branching directory"
    );
    let mut previous_end = None;
    for range in ranges {
        ensure!(
            range.start <= range.end && range.end <= max_address(family),
            "invalid range"
        );
        ensure!(
            previous_end.is_none_or(|end| range.start > end),
            "overlapping or unordered ranges"
        );
        Location::decode(&range.location.0)?;
        previous_end = Some(range.end);
    }
    let mut children = Vec::new();
    for chunk in ranges.chunks(leaf_capacity) {
        let mut bytes = header(family, 0, chunk.len())?;
        for range in chunk {
            write_address(&mut bytes, range.start, address_bytes);
            write_address(&mut bytes, range.end, address_bytes);
            bytes.extend_from_slice(&range.location.0);
        }
        let reference = NodeRef {
            start: chunk[0].start,
            end: chunk[chunk.len() - 1].end,
            hash: digest(&bytes),
            level: 0,
        };
        publish_node(kv, generation, family, &reference, bytes, inventory).await?;
        children.push(reference);
    }
    while children.len() > 1 {
        let mut parents = Vec::new();
        for chunk in children.chunks(directory_capacity) {
            let level = chunk[0]
                .level
                .checked_add(1)
                .context("tree depth overflow")?;
            let mut bytes = header(family, level, chunk.len())?;
            for child in chunk {
                write_address(&mut bytes, child.start, address_bytes);
                write_address(&mut bytes, child.end, address_bytes);
                bytes.extend_from_slice(&child.hash);
            }
            let reference = NodeRef {
                start: chunk[0].start,
                end: chunk[chunk.len() - 1].end,
                hash: digest(&bytes),
                level,
            };
            publish_node(kv, generation, family, &reference, bytes, inventory).await?;
            parents.push(reference);
        }
        children = parents;
    }
    Ok(children.pop())
}

/// Build a generation in an artifact store. Use the standalone publisher for remote writes.
///
/// # Errors
/// Rejects invalid ranges, object budgets, encoding failures and storage errors.
#[cfg(feature = "geo-kv-build")]
#[inline]
pub async fn build(
    kv: &KvHandle,
    source: Source,
    v4: &[Range],
    v6: &[Range],
    object_bytes: usize,
) -> Result<(Receipt, Manifest, Vec<InventoryEntry>)> {
    // The minimum follows the encoding: header plus two IPv6 directory records.
    ensure!(
        (MIN_OBJECT_BYTES..=KvHandle::MAX_VALUE_SIZE).contains(&object_bytes),
        "invalid object byte budget"
    );
    let generation = hex(&digest(&serde_json::to_vec(&(
        1_u32,
        object_bytes,
        &source,
    ))?));
    let mut inventory = Vec::new();
    let ipv4 = build_family(kv, &generation, 4, v4, object_bytes, &mut inventory).await?;
    let ipv6 = build_family(kv, &generation, 6, v6, object_bytes, &mut inventory).await?;
    let manifest = Manifest {
        schema: 1,
        generation: generation.clone(),
        object_bytes,
        source,
        ipv4,
        ipv6,
    };
    let bytes = serde_json::to_vec(&manifest)?;
    ensure!(
        bytes.len() <= object_bytes,
        "manifest exceeds target value budget"
    );
    let receipt = Receipt {
        manifest_key: format!("geo/v1/{generation}/manifest"),
        manifest_hash: digest(&bytes),
    };
    inventory.push(InventoryEntry {
        key: receipt.manifest_key.clone(),
        sha256: hex(&receipt.manifest_hash),
        bytes: bytes.len(),
    });
    kv.put_bytes(&receipt.manifest_key, bytes.into()).await?;
    Ok((receipt, manifest, inventory))
}

#[derive(Clone)]
struct Object {
    bytes: Bytes,
    width: usize,
    record_bytes: usize,
    count: usize,
}

impl Object {
    fn record(&self, index: usize) -> &[u8] {
        &self.bytes[HEADER + index * self.record_bytes..HEADER + (index + 1) * self.record_bytes]
    }

    fn bounds(&self, index: usize) -> (u128, u128) {
        let record = self.record(index);
        (
            read_address(&record[..self.width]),
            read_address(&record[self.width..2 * self.width]),
        )
    }

    fn decode(bytes: Bytes, family: u8, reference: &NodeRef, max_bytes: usize) -> Result<Self> {
        ensure!(
            bytes.len() <= max_bytes && bytes.len() >= HEADER,
            "invalid object length"
        );
        ensure!(digest(&bytes) == reference.hash, "object digest mismatch");
        ensure!(
            &bytes[..4] == MAGIC && bytes[4] == family,
            "invalid object header"
        );
        let level = u16::from_be_bytes(bytes[6..8].try_into()?);
        ensure!(
            level == reference.level && bytes[5] == u8::from(level > 0),
            "invalid object level"
        );
        let address_bytes = width(family)?;
        let count = usize::try_from(u32::from_be_bytes(bytes[8..12].try_into()?))?;
        let record_bytes = 2 * address_bytes + if level == 0 { 6 } else { 32 };
        ensure!(
            count > 0
                && count
                    .checked_mul(record_bytes)
                    .and_then(|n| n.checked_add(HEADER))
                    == Some(bytes.len()),
            "invalid object records"
        );
        let object = Self {
            bytes,
            width: address_bytes,
            record_bytes,
            count,
        };
        let mut previous_end = None;
        for index in 0..count {
            let (start, end) = object.bounds(index);
            ensure!(
                start <= end && previous_end.is_none_or(|prior| start > prior),
                "invalid record ordering"
            );
            if level == 0 {
                Location::decode(&object.record(index)[2 * address_bytes..])?;
            }
            previous_end = Some(end);
        }
        ensure!(
            object.bounds(0).0 == reference.start && object.bounds(count - 1).1 == reference.end,
            "child envelope mismatch"
        );
        Ok(object)
    }

    fn child(&self, index: usize, level: u16) -> NodeRef {
        let (start, end) = self.bounds(index);
        let mut hash = [0; 32];
        hash.copy_from_slice(&self.record(index)[2 * self.width..]);
        NodeRef {
            start,
            end,
            hash,
            level: level - 1,
        }
    }

    fn find(&self, ip: u128) -> Option<usize> {
        let mut lo = 0;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.bounds(mid).0 <= ip {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        (lo > 0 && ip <= self.bounds(lo - 1).1).then(|| lo - 1)
    }
}

const MIN_OBJECT_BYTES: usize = HEADER + 2 * (32 + 32);

/// Limits for one prepared generation. Time and concurrent request limits belong to the adapter.
#[derive(Clone, Copy, Debug)]
pub struct IndexLimits {
    pub max_object_bytes: usize,
    pub max_path_objects: usize,
    pub cache_bytes: usize,
    pub cache_entries: usize,
}

#[derive(Clone, Copy, Default, Debug, Serialize)]
pub struct CacheStats {
    pub hits: u64,
    pub peak_payload_bytes: usize,
    pub max_object_bytes: usize,
}

#[derive(Default)]
struct Cache {
    bytes: usize,
    values: HashMap<[u8; 32], Object>,
    order: VecDeque<[u8; 32]>,
    stats: CacheStats,
}

impl Cache {
    fn get(&mut self, hash: &[u8; 32]) -> Option<Object> {
        let object = self.values.get(hash)?.clone();
        self.order.retain(|cached_hash| cached_hash != hash);
        self.order.push_back(*hash);
        self.stats.hits = self.stats.hits.saturating_add(1);
        Some(object)
    }

    fn insert(&mut self, hash: [u8; 32], object: Object, limits: IndexLimits) {
        let size = object.bytes.len();
        self.stats.max_object_bytes = self.stats.max_object_bytes.max(size);
        if size > limits.cache_bytes || limits.cache_entries == 0 || self.values.contains_key(&hash)
        {
            return;
        }
        while self.bytes > limits.cache_bytes - size || self.values.len() >= limits.cache_entries {
            let Some(evicted) = self.order.pop_front() else {
                return;
            };
            let Some(removed) = self.values.remove(&evicted) else {
                return;
            };
            self.bytes -= removed.bytes.len();
        }
        self.bytes += size;
        self.values.insert(hash, object);
        self.order.push_back(hash);
        self.stats.peak_payload_bytes = self.stats.peak_payload_bytes.max(self.bytes);
    }
}

impl Receipt {
    /// Parse a single deployment pin, `<generation>@<manifest sha256>`.
    ///
    /// # Errors
    /// Rejects noncanonical hex, incomplete pins, and malformed manifest identities.
    #[inline]
    pub fn from_pin(pin: &str) -> Result<Self> {
        let (generation, digest) = pin.split_once('@').context("missing Geo pin separator")?;
        ensure!(valid_generation(generation), "invalid Geo generation");
        let mut manifest_hash = [0; 32];
        ensure!(
            decode_hex(digest, &mut manifest_hash)?.len() == 32,
            "invalid Geo manifest digest length"
        );
        Ok(Self {
            manifest_key: manifest_key(generation),
            manifest_hash,
        })
    }
}

fn valid_generation(generation: &str) -> bool {
    generation.len() == 64
        && generation
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn manifest_key(generation: &str) -> String {
    format!("geo/v1/{generation}/manifest")
}

/// A pin-validated resolver. Its cache belongs exclusively to this binding and generation.
pub struct Resolver {
    kv: KvHandle,
    manifest: Manifest,
    limits: IndexLimits,
    cache: Mutex<Cache>,
}

impl Resolver {
    /// Read and validate the pinned manifest, without preloading objects.
    ///
    /// # Errors
    /// Returns an error for missing or incompatible index data or invalid limits.
    #[inline]
    pub async fn open(kv: KvHandle, receipt: &Receipt, limits: IndexLimits) -> Result<Self> {
        ensure!(
            (MIN_OBJECT_BYTES..=KvHandle::MAX_VALUE_SIZE).contains(&limits.max_object_bytes)
                && limits.max_path_objects > 0,
            "invalid Geo index limits"
        );
        ensure!(
            receipt
                .manifest_key
                .strip_prefix("geo/v1/")
                .and_then(|tail| tail.strip_suffix("/manifest"))
                .is_some_and(valid_generation),
            "invalid manifest key"
        );
        let bytes = kv
            .get_bytes(&receipt.manifest_key)
            .await?
            .context("missing manifest")?;
        ensure!(
            bytes.len() <= limits.max_object_bytes,
            "manifest exceeds value budget"
        );
        ensure!(
            digest(&bytes) == receipt.manifest_hash,
            "manifest digest mismatch"
        );
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        ensure!(
            manifest.schema == 1 && valid_generation(&manifest.generation),
            "invalid manifest identity"
        );
        ensure!(
            receipt.manifest_key == manifest_key(&manifest.generation),
            "manifest key mismatch"
        );
        ensure!(
            (MIN_OBJECT_BYTES..=limits.max_object_bytes).contains(&manifest.object_bytes)
                && bytes.len() <= manifest.object_bytes,
            "invalid manifest object budget"
        );
        for (family, root) in [(4, &manifest.ipv4), (6, &manifest.ipv6)] {
            if let Some(reference) = root {
                ensure!(
                    reference.start <= reference.end
                        && reference.end <= max_address(family)
                        && usize::from(reference.level) < limits.max_path_objects,
                    "invalid family root"
                );
            }
        }
        Ok(Self {
            kv,
            manifest,
            limits,
            cache: Mutex::new(Cache::default()),
        })
    }

    #[must_use]
    #[inline]
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    #[must_use]
    #[inline]
    pub fn cache_stats(&self) -> CacheStats {
        self.cache
            .lock()
            .map_or_else(|_error| CacheStats::default(), |cache| cache.stats)
    }

    async fn read_object(&self, family: u8, reference: &NodeRef) -> Result<Object> {
        let key = node_key(&self.manifest.generation, family, &reference.hash);
        let bytes = self
            .kv
            .get_bytes(&key)
            .await?
            .context("missing required index object")?;
        Object::decode(bytes, family, reference, self.manifest.object_bytes)
    }

    /// Verify both nonempty roots through the serving binding before readiness.
    ///
    /// # Errors
    /// Fails when any expected root cannot be read or validated.
    #[inline]
    pub async fn verify_roots(&self) -> Result<()> {
        for (family, root) in [(4, &self.manifest.ipv4), (6, &self.manifest.ipv6)] {
            if let Some(reference) = root {
                self.read_object(family, reference).await?;
            }
        }
        Ok(())
    }

    /// Validate the complete tree with memory proportional to depth, without populating the cache.
    ///
    /// # Errors
    /// Fails when any expected object cannot be read or validated.
    #[cfg(feature = "geo-kv-build")]
    #[inline]
    pub async fn verify_tree(&self) -> Result<usize> {
        let mut objects = 1_usize; // the manifest was verified by open
        for (family, root) in [(4, &self.manifest.ipv4), (6, &self.manifest.ipv6)] {
            let Some(root_ref) = root else { continue };
            let mut next = Some(root_ref.clone());
            let mut directories = Vec::new();
            loop {
                if let Some(reference) = next.take() {
                    let object = self.read_object(family, &reference).await?;
                    objects = objects
                        .checked_add(1)
                        .context("Geo object count overflow")?;
                    if reference.level != 0 {
                        directories.push((object, reference.level, 0));
                    }
                }
                while let Some((directory, level, slot)) = directories.last_mut() {
                    if *slot < directory.count {
                        next = Some(directory.child(*slot, *level));
                        *slot += 1;
                        break;
                    }
                    directories.pop();
                }
                if next.is_none() {
                    break;
                }
            }
        }
        Ok(objects)
    }

    /// Resolve one normalized IP. A matched but unsupported country is still a source location.
    ///
    /// # Errors
    /// Required missing or invalid objects are errors, not geographic gaps.
    #[inline]
    pub async fn lookup(&self, input: IpAddr) -> Result<Option<Location>> {
        let (family, ip) = address(input);
        let root = if family == 4 {
            &self.manifest.ipv4
        } else {
            &self.manifest.ipv6
        };
        let Some(mut reference) = root.clone() else {
            return Ok(None);
        };
        loop {
            if ip < reference.start || ip > reference.end {
                return Ok(None);
            }
            let cached = self
                .cache
                .lock()
                .map_err(|_error| anyhow::anyhow!("Geo cache unavailable"))?
                .get(&reference.hash);
            let object = if let Some(object) = cached {
                // A digest identifies bytes, not the envelope stated by each parent.
                ensure!(
                    object.bytes[4] == family
                        && object.bounds(0).0 == reference.start
                        && object.bounds(object.count - 1).1 == reference.end
                        && u16::from_be_bytes(object.bytes[6..8].try_into()?) == reference.level,
                    "cached child descriptor mismatch"
                );
                object
            } else {
                let object = self.read_object(family, &reference).await?;
                self.cache
                    .lock()
                    .map_err(|_error| anyhow::anyhow!("Geo cache unavailable"))?
                    .insert(reference.hash, object.clone(), self.limits);
                object
            };
            let Some(index) = object.find(ip) else {
                return Ok(None);
            };
            if reference.level == 0 {
                return Ok(Some(Location::decode(
                    &object.record(index)[2 * object.width..],
                )?));
            }
            reference = object.child(index, reference.level);
        }
    }
}

#[cfg(test)]
#[cfg(feature = "geo-kv-build")]
mod tests {
    use super::*;
    use crate::key_value_store::{KvError, KvPage, KvStore};
    use async_trait::async_trait;
    use futures::executor::block_on;
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    #[derive(Default)]
    struct MemoryStore(Mutex<HashMap<String, Bytes>>, AtomicUsize);
    #[expect(
        clippy::missing_trait_methods,
        reason = "fixture has no record operations"
    )]
    #[async_trait(?Send)]
    impl KvStore for MemoryStore {
        async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
            self.1.fetch_add(1, Ordering::Relaxed);
            Ok(self.0.lock().unwrap().get(key).cloned())
        }
        async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
            self.0.lock().unwrap().insert(key.into(), value);
            Ok(())
        }
        async fn delete(&self, _: &str) -> Result<(), KvError> {
            panic!("lookup must not delete")
        }
        async fn list_keys_page(
            &self,
            _: &str,
            _: Option<&str>,
            _: usize,
        ) -> Result<KvPage, KvError> {
            panic!("lookup must not list")
        }
        async fn put_bytes_with_ttl(&self, _: &str, _: Bytes, _: Duration) -> Result<(), KvError> {
            panic!("no TTL")
        }
    }
    fn source() -> Source {
        Source {
            edition: "synthetic".into(),
            files: vec![],
        }
    }
    fn limits(cache_bytes: usize) -> IndexLimits {
        IndexLimits {
            max_object_bytes: 1024,
            max_path_objects: 8,
            cache_bytes,
            cache_entries: 1024,
        }
    }
    fn ranges() -> Vec<Range> {
        (0..10_000)
            .map(|i| Range {
                start: i * 4,
                end: i * 4 + 1,
                location: Location::new("US", if i % 2 == 0 { "CA" } else { "" }).unwrap(),
            })
            .collect()
    }

    #[test]
    fn round_trip_multilevel_cache_and_determinism() {
        block_on(async {
            let raw = Arc::new(MemoryStore::default());
            let kv = KvHandle::new(Arc::<MemoryStore>::clone(&raw));
            let ranges = ranges();
            let (receipt, manifest, _) =
                build(&kv, source(), &ranges, &ranges, 1024).await.unwrap();
            assert!(manifest.ipv6.as_ref().unwrap().level >= 3);
            let before = raw.0.lock().unwrap().clone();
            let (again, _, _) = build(&kv, source(), &ranges, &ranges, 1024).await.unwrap();
            assert_eq!(receipt, again);
            assert_eq!(before, *raw.0.lock().unwrap());
            for budget in [0, 1024, 1024 * 1024] {
                let resolver = Resolver::open(kv.clone(), &receipt, limits(budget))
                    .await
                    .unwrap();
                for family in [4_u8, 6] {
                    for point in 0..410_u128 {
                        let ip = if family == 4 {
                            IpAddr::from(u32::try_from(point).unwrap().to_be_bytes())
                        } else {
                            IpAddr::from(point.to_be_bytes())
                        };
                        let expected = ranges
                            .iter()
                            .find(|range| range.start <= point && point <= range.end)
                            .map(|range| range.location);
                        assert_eq!(resolver.lookup(ip).await.unwrap(), expected);
                    }
                }
                assert!(resolver.cache_stats().peak_payload_bytes <= budget);
            }
        });
    }

    #[test]
    fn repeated_lookup_uses_cache_without_backend_reads() {
        block_on(async {
            let raw = Arc::new(MemoryStore::default());
            let kv = KvHandle::new(Arc::<MemoryStore>::clone(&raw));
            let ranges = ranges();
            let (receipt, manifest, _) =
                build(&kv, source(), &ranges, &ranges, 1024).await.unwrap();
            for budget in [0, 1024 * 1024] {
                for (text, root) in [
                    ("0.0.0.0", manifest.ipv4.as_ref().unwrap()),
                    ("::", manifest.ipv6.as_ref().unwrap()),
                ] {
                    let before_open = raw.1.load(Ordering::Relaxed);
                    let resolver = Resolver::open(kv.clone(), &receipt, limits(budget))
                        .await
                        .unwrap();
                    assert_eq!(raw.1.load(Ordering::Relaxed), before_open + 1);
                    let path_objects = usize::from(root.level) + 1;
                    assert!(path_objects > 1);
                    let ip = text.parse().unwrap();
                    assert_eq!(resolver.lookup(ip).await.unwrap(), Some(ranges[0].location));
                    let after_cold = raw.1.load(Ordering::Relaxed);
                    assert_eq!(after_cold, before_open + 1 + path_objects);
                    assert_eq!(resolver.cache_stats().hits, 0);
                    assert_eq!(resolver.lookup(ip).await.unwrap(), Some(ranges[0].location));
                    let stats = resolver.cache_stats();
                    if budget == 0 {
                        assert_eq!(raw.1.load(Ordering::Relaxed), after_cold + path_objects);
                        assert_eq!(stats.hits, 0);
                        assert_eq!(stats.peak_payload_bytes, 0);
                    } else {
                        assert_eq!(raw.1.load(Ordering::Relaxed), after_cold);
                        assert_eq!(stats.hits, u64::try_from(path_objects).unwrap());
                        assert!(stats.peak_payload_bytes > 0);
                        assert!(stats.peak_payload_bytes <= budget);
                    }
                    assert!(stats.max_object_bytes > 0);
                    assert!(stats.max_object_bytes <= 1024);
                }
            }
        });
    }

    #[test]
    fn cache_evicts_least_recently_used_objects_at_both_limits() {
        block_on(async {
            let raw = Arc::new(MemoryStore::default());
            let kv = KvHandle::new(Arc::<MemoryStore>::clone(&raw));
            let leaf_capacity = (1024 - HEADER) / (2 * 4 + 6);
            let ranges: Vec<_> = ranges().into_iter().take(3 * leaf_capacity).collect();
            let (receipt, manifest, _) = build(&kv, source(), &ranges, &[], 1024).await.unwrap();
            let reference = manifest.ipv4.as_ref().unwrap();
            assert_eq!(reference.level, 1);
            let root_bytes =
                raw.0.lock().unwrap()[&node_key(&manifest.generation, 4, &reference.hash)].len();
            let leaf_bytes = HEADER + leaf_capacity * (2 * 4 + 6);
            let payload_bytes = root_bytes + 2 * leaf_bytes;
            for cache_limits in [
                IndexLimits {
                    cache_entries: 3,
                    ..limits(1024 * 1024)
                },
                limits(payload_bytes),
            ] {
                let resolver = Resolver::open(kv.clone(), &receipt, cache_limits)
                    .await
                    .unwrap();
                let mut hits = 0;
                for (slot, reads, new_hits) in [
                    (0, 2, 0),                 // Load the root and leaf A.
                    (leaf_capacity, 1, 1),     // Load B, retaining A.
                    (0, 0, 2),                 // Refresh A so B becomes least recently used.
                    (2 * leaf_capacity, 1, 1), // Load C, evicting B.
                    (0, 0, 2),                 // A must still be cached.
                    (leaf_capacity, 1, 1),     // B must be fetched again.
                ] {
                    let before = raw.1.load(Ordering::Relaxed);
                    let ip = IpAddr::from(u32::try_from(ranges[slot].start).unwrap().to_be_bytes());
                    assert_eq!(
                        resolver.lookup(ip).await.unwrap(),
                        Some(ranges[slot].location)
                    );
                    assert_eq!(raw.1.load(Ordering::Relaxed), before + reads);
                    hits += new_hits;
                    assert_eq!(resolver.cache_stats().hits, hits);
                }
                let stats = resolver.cache_stats();
                assert_eq!(stats.peak_payload_bytes, payload_bytes);
                assert_eq!(stats.max_object_bytes, leaf_bytes);
            }
        });
    }

    #[test]
    fn full_family_and_empty_family() {
        block_on(async {
            for family in [4, 6] {
                let kv = KvHandle::new(Arc::new(MemoryStore::default()));
                let ranges = [Range {
                    start: 0,
                    end: max_address(family),
                    location: Location::new("GB", "ENG").unwrap(),
                }];
                let (receipt, _, _) = build(
                    &kv,
                    source(),
                    if family == 4 { &ranges } else { &[] },
                    if family == 6 { &ranges } else { &[] },
                    1024,
                )
                .await
                .unwrap();
                let resolver = Resolver::open(kv, &receipt, limits(0)).await.unwrap();
                for point in [0, max_address(family)] {
                    let ip = if family == 4 {
                        IpAddr::from(u32::try_from(point).unwrap().to_be_bytes())
                    } else {
                        IpAddr::from(point.to_be_bytes())
                    };
                    assert_eq!(resolver.lookup(ip).await.unwrap(), Some(ranges[0].location));
                }
                let empty = if family == 4 { "::1" } else { "1.0.0.1" };
                assert_eq!(resolver.lookup(empty.parse().unwrap()).await.unwrap(), None);
            }
        });
    }

    #[test]
    fn source_country_roundtrip_does_not_claim_domain_acceptance() {
        block_on(async {
            let kv = KvHandle::new(Arc::new(MemoryStore::default()));
            let ranges = [Range {
                start: 0,
                end: 1,
                location: Location::new("XK", "").unwrap(),
            }];
            let (receipt, _, _) = build(&kv, source(), &ranges, &[], 1024).await.unwrap();
            let resolver = Resolver::open(kv, &receipt, limits(0)).await.unwrap();
            let location = resolver
                .lookup("0.0.0.1".parse().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(location, ranges[0].location);
            assert!(!location.core_country_supported());
        });
    }

    #[test]
    fn missing_and_corrupt_objects_are_errors_not_misses() {
        block_on(async {
            let raw = Arc::new(MemoryStore::default());
            let kv = KvHandle::new(Arc::<MemoryStore>::clone(&raw));
            let (receipt, manifest, _) = build(&kv, source(), &ranges(), &[], 1024).await.unwrap();
            let key = node_key(
                &manifest.generation,
                4,
                &manifest.ipv4.as_ref().unwrap().hash,
            );
            let old = raw.0.lock().unwrap().remove(&key).unwrap();
            let resolver = Resolver::open(kv.clone(), &receipt, limits(0))
                .await
                .unwrap();
            assert!(
                resolver
                    .lookup("0.0.0.0".parse().unwrap())
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("missing required")
            );
            let mut corrupt = old.to_vec();
            corrupt[0] ^= 1;
            raw.0.lock().unwrap().insert(key, corrupt.into());
            assert!(
                resolver
                    .lookup("0.0.0.0".parse().unwrap())
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("digest mismatch")
            );
            let mut bad_receipt = receipt.clone();
            bad_receipt.manifest_hash[0] ^= 1;
            assert!(Resolver::open(kv, &bad_receipt, limits(0)).await.is_err());
        });
    }

    #[test]
    fn malformed_records_with_matching_digest_are_rejected() {
        let reference = |data: &[u8]| NodeRef {
            start: 0,
            end: 4,
            hash: digest(data),
            level: 0,
        };
        let mut bytes = header(4, 0, 2).unwrap();
        for (start, end) in [(0, 2), (2, 4)] {
            write_address(&mut bytes, start, 4);
            write_address(&mut bytes, end, 4);
            bytes.extend_from_slice(&Location::new("US", "CA").unwrap().0);
        }
        assert!(Object::decode(bytes.clone().into(), 4, &reference(&bytes), 140).is_err());
        let mut invalid = header(4, 0, 1).unwrap();
        write_address(&mut invalid, 0, 4);
        write_address(&mut invalid, 4, 4);
        invalid.extend_from_slice(&[b'U', b'S', 4, b'C', b'A', 0]);
        assert!(Object::decode(invalid.clone().into(), 4, &reference(&invalid), 140).is_err());
        invalid.pop();
        assert!(Object::decode(invalid.clone().into(), 4, &reference(&invalid), 140).is_err());
    }

    #[test]
    fn rejects_overlaps_with_a_compatible_object_budget() {
        block_on(async {
            for family in [4_u8, 6] {
                let kv = KvHandle::new(Arc::new(MemoryStore::default()));
                let valid: Vec<_> = ranges().into_iter().take(2).collect();
                let mut overlapping = valid.clone();
                overlapping[1].start = valid[0].end;
                for input in [&valid, &overlapping] {
                    let result = build(
                        &kv,
                        source(),
                        if family == 4 { input } else { &[] },
                        if family == 6 { input } else { &[] },
                        1024,
                    )
                    .await;
                    if input == &valid {
                        result.unwrap();
                    } else {
                        assert_eq!(
                            result.unwrap_err().to_string(),
                            "overlapping or unordered ranges"
                        );
                    }
                }
            }
        });
    }

    #[test]
    fn rejects_too_small_object_budget() {
        block_on(async {
            let kv = KvHandle::new(Arc::new(MemoryStore::default()));
            assert_eq!(
                build(&kv, source(), &[], &[], 139)
                    .await
                    .unwrap_err()
                    .to_string(),
                "invalid object byte budget"
            );
        });
    }
}
