//! Experimental p1 encoding. No filesystem or runtime-specific API in lookup.
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::IpAddr,
};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use edgezero_core::{
    geo::{CountryCode, SubdivisionCode},
    key_value_store::KvHandle,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const HEADER: usize = 12;
const MAGIC: &[u8; 4] = b"EZP1";

pub fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Location(pub [u8; 6]);

impl Location {
    pub fn new(country: &str, subdivision: &str) -> Result<Self> {
        // Source-format DTO, not GeoLocation. Preserve XK and report the domain mismatch.
        ensure!(
            country.len() == 2 && country.bytes().all(|b| b.is_ascii_alphabetic()),
            "invalid source country syntax"
        );
        let country = country.to_ascii_uppercase();
        let subdivision = if subdivision.is_empty() {
            None
        } else {
            Some(SubdivisionCode::new(subdivision)?)
        };
        let mut bytes = [0; 6];
        bytes[..2].copy_from_slice(country.as_bytes());
        if let Some(subdivision) = subdivision {
            let s = subdivision.as_str().as_bytes();
            bytes[2] = s.len() as u8;
            bytes[3..3 + s.len()].copy_from_slice(s);
        }
        Ok(Self(bytes))
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let raw: [u8; 6] = bytes.try_into()?;
        let len = raw[2] as usize;
        ensure!(
            len <= 3 && raw[3 + len..].iter().all(|b| *b == 0),
            "invalid location encoding"
        );
        let canonical = Self::new(
            std::str::from_utf8(&raw[..2])?,
            std::str::from_utf8(&raw[3..3 + len])?,
        )?;
        ensure!(canonical.0 == raw, "noncanonical location");
        Ok(canonical)
    }

    pub fn core_country_supported(self) -> bool {
        CountryCode::new(std::str::from_utf8(&self.0[..2]).expect("validated source code")).is_ok()
    }

    pub fn json(self) -> serde_json::Value {
        let country = std::str::from_utf8(&self.0[..2]).expect("validated country");
        let subdivision = if self.0[2] == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(&self.0[3..3 + self.0[2] as usize])
                    .expect("validated subdivision"),
            )
        };
        serde_json::json!({"country": country, "subdivision": subdivision, "core_country_supported": self.core_country_supported()})
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Range {
    pub start: u128,
    pub end: u128,
    pub location: Location,
}

pub fn width(family: u8) -> Result<usize> {
    match family {
        4 => Ok(4),
        6 => Ok(16),
        _ => bail!("invalid address family"),
    }
}

pub fn address(ip: IpAddr) -> (u8, u128) {
    match ip {
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
fn write_address(out: &mut Vec<u8>, value: u128, width: usize) {
    out.extend_from_slice(&value.to_be_bytes()[16 - width..]);
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRef {
    pub start: u128,
    pub end: u128,
    pub hash: [u8; 32],
    pub level: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Source {
    pub edition: String,
    pub files: Vec<(String, String)>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub schema: u32,
    pub generation: String,
    pub object_bytes: usize,
    pub source: Source,
    pub ipv4: Option<NodeRef>,
    pub ipv6: Option<NodeRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub manifest_key: String,
    pub manifest_hash: [u8; 32],
}

#[derive(Debug, Serialize)]
pub struct InventoryEntry {
    pub key: String,
    pub sha256: String,
    pub bytes: usize,
}

pub fn node_key(generation: &str, family: u8, hash: &[u8; 32]) -> String {
    format!("geo/p1/{generation}/v{family}/{}", hex(hash))
}

fn header(family: u8, level: u16, count: usize) -> Result<Vec<u8>> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&[family, u8::from(level > 0)]);
    bytes.extend_from_slice(&level.to_be_bytes());
    bytes.extend_from_slice(&u32::try_from(count)?.to_be_bytes());
    Ok(bytes)
}

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

async fn build_family(
    kv: &KvHandle,
    generation: &str,
    family: u8,
    ranges: &[Range],
    object_bytes: usize,
    inventory: &mut Vec<InventoryEntry>,
) -> Result<Option<NodeRef>> {
    let w = width(family)?;
    let leaf_capacity = (object_bytes - HEADER) / (2 * w + 6);
    let directory_capacity = (object_bytes - HEADER) / (2 * w + 32);
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
            write_address(&mut bytes, range.start, w);
            write_address(&mut bytes, range.end, w);
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
                write_address(&mut bytes, child.start, w);
                write_address(&mut bytes, child.end, w);
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

pub async fn build(
    kv: &KvHandle,
    source: Source,
    v4: &[Range],
    v6: &[Range],
    object_bytes: usize,
) -> Result<(Receipt, Manifest, Vec<InventoryEntry>)> {
    // The minimum follows the encoding: header plus two IPv6 directory records.
    ensure!(
        (HEADER + 2 * (32 + 32)..=KvHandle::MAX_VALUE_SIZE).contains(&object_bytes),
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
        manifest_key: format!("geo/p1/{generation}/manifest"),
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
    fn record(&self, i: usize) -> &[u8] {
        &self.bytes[HEADER + i * self.record_bytes..HEADER + (i + 1) * self.record_bytes]
    }

    fn bounds(&self, i: usize) -> (u128, u128) {
        let r = self.record(i);
        (
            read_address(&r[..self.width]),
            read_address(&r[self.width..2 * self.width]),
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
        let w = width(family)?;
        let count = u32::from_be_bytes(bytes[8..12].try_into()?) as usize;
        let record_bytes = 2 * w + if level == 0 { 6 } else { 32 };
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
            width: w,
            record_bytes,
            count,
        };
        let mut previous_end = None;
        let mut locations = HashSet::new();
        for i in 0..count {
            let (start, end) = object.bounds(i);
            ensure!(
                start <= end && previous_end.is_none_or(|e| start > e),
                "invalid record ordering"
            );
            if level == 0 {
                let raw: [u8; 6] = object.record(i)[2 * w..].try_into()?;
                if locations.insert(raw) {
                    Location::decode(&raw)?;
                }
            }
            previous_end = Some(end);
        }
        ensure!(
            object.bounds(0).0 == reference.start && object.bounds(count - 1).1 == reference.end,
            "child envelope mismatch"
        );
        Ok(object)
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

#[derive(Default, Debug, Serialize)]
pub struct CacheStats {
    pub hits: u64,
    pub peak_payload_bytes: usize,
    pub max_object_bytes: usize,
}

struct Cache {
    budget: usize,
    bytes: usize,
    values: HashMap<[u8; 32], Object>,
    order: VecDeque<[u8; 32]>,
    stats: CacheStats,
}

impl Cache {
    fn new(budget: usize) -> Self {
        Self {
            budget,
            bytes: 0,
            values: HashMap::new(),
            order: VecDeque::new(),
            stats: CacheStats::default(),
        }
    }

    fn get(&mut self, hash: &[u8; 32]) -> Option<Object> {
        let object = self.values.get(hash)?.clone();
        self.order.retain(|h| h != hash);
        self.order.push_back(*hash);
        self.stats.hits += 1;
        Some(object)
    }

    fn insert(&mut self, hash: [u8; 32], object: Object) {
        let size = object.bytes.len();
        self.stats.max_object_bytes = self.stats.max_object_bytes.max(size);
        if size > self.budget {
            return;
        }
        while self.bytes + size > self.budget {
            let hash = self.order.pop_front().expect("cache accounting");
            self.bytes -= self.values.remove(&hash).expect("cache entry").bytes.len();
        }
        self.bytes += size;
        self.values.insert(hash, object);
        self.order.push_back(hash);
        self.stats.peak_payload_bytes = self.stats.peak_payload_bytes.max(self.bytes);
    }
}

pub struct Resolver {
    kv: KvHandle,
    pub manifest: Manifest,
    cache: Cache,
}

impl Resolver {
    pub async fn open(kv: KvHandle, receipt: &Receipt, cache_bytes: usize) -> Result<Self> {
        let bytes = kv
            .get_bytes(&receipt.manifest_key)
            .await?
            .context("missing manifest")?;
        ensure!(
            digest(&bytes) == receipt.manifest_hash,
            "manifest digest mismatch"
        );
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        ensure!(
            manifest.schema == 1
                && manifest.generation.len() == 64
                && manifest.generation.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid manifest identity"
        );
        ensure!(
            receipt.manifest_key == format!("geo/p1/{}/manifest", manifest.generation),
            "manifest key mismatch"
        );
        ensure!(
            (HEADER + 128..=KvHandle::MAX_VALUE_SIZE).contains(&manifest.object_bytes),
            "invalid manifest object budget"
        );
        for (family, root) in [(4, &manifest.ipv4), (6, &manifest.ipv6)] {
            if let Some(root) = root {
                ensure!(
                    root.start <= root.end && root.end <= max_address(family),
                    "invalid family root"
                );
            }
        }
        Ok(Self {
            kv,
            manifest,
            cache: Cache::new(cache_bytes),
        })
    }

    pub fn cache_stats(&self) -> &CacheStats {
        &self.cache.stats
    }

    pub async fn lookup(&mut self, ip: IpAddr) -> Result<Option<Location>> {
        let (family, ip) = address(ip);
        let Some(mut reference) = (if family == 4 {
            &self.manifest.ipv4
        } else {
            &self.manifest.ipv6
        })
        .clone() else {
            return Ok(None);
        };
        loop {
            if ip < reference.start || ip > reference.end {
                return Ok(None);
            }
            let object = if let Some(object) = self.cache.get(&reference.hash) {
                object
            } else {
                let key = node_key(&self.manifest.generation, family, &reference.hash);
                let bytes = self
                    .kv
                    .get_bytes(&key)
                    .await?
                    .context("missing required index object")?;
                let object = Object::decode(bytes, family, &reference, self.manifest.object_bytes)?;
                self.cache.insert(reference.hash, object.clone());
                object
            };
            // Cached content still has to agree with this parent's envelope and level.
            ensure!(
                object.bounds(0).0 == reference.start
                    && object.bounds(object.count - 1).1 == reference.end
                    && u16::from_be_bytes(object.bytes[6..8].try_into()?) == reference.level,
                "cached child descriptor mismatch"
            );
            let Some(i) = object.find(ip) else {
                return Ok(None);
            };
            if reference.level == 0 {
                return Ok(Some(Location(
                    object.record(i)[2 * object.width..].try_into()?,
                )));
            }
            let (start, end) = object.bounds(i);
            reference = NodeRef {
                start,
                end,
                hash: object.record(i)[2 * object.width..].try_into()?,
                level: reference.level - 1,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use edgezero_core::key_value_store::{KvError, KvPage, KvStore};
    use futures::executor::block_on;
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[derive(Default)]
    struct MemoryStore(Mutex<HashMap<String, Bytes>>);
    #[async_trait(?Send)]
    impl KvStore for MemoryStore {
        async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }
        async fn put_bytes(&self, key: &str, bytes: Bytes) -> Result<(), KvError> {
            self.0.lock().unwrap().insert(key.into(), bytes);
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
            let kv = KvHandle::new(raw.clone());
            let ranges = ranges();
            let (receipt, manifest, _) =
                build(&kv, source(), &ranges, &ranges, 1024).await.unwrap();
            assert!(manifest.ipv6.as_ref().unwrap().level >= 3);
            let before = raw.0.lock().unwrap().clone();
            let (again, _, _) = build(&kv, source(), &ranges, &ranges, 1024).await.unwrap();
            assert_eq!(receipt, again);
            assert_eq!(before, *raw.0.lock().unwrap());
            for budget in [0, 1024, 1024 * 1024] {
                let mut resolver = Resolver::open(kv.clone(), &receipt, budget).await.unwrap();
                for family in [4, 6] {
                    for n in 0..410_u128 {
                        let ip = if family == 4 {
                            IpAddr::from((n as u32).to_be_bytes())
                        } else {
                            IpAddr::from(n.to_be_bytes())
                        };
                        let expected = ranges
                            .iter()
                            .find(|r| r.start <= n && n <= r.end)
                            .map(|r| r.location);
                        assert_eq!(resolver.lookup(ip).await.unwrap(), expected);
                    }
                }
                assert!(resolver.cache_stats().peak_payload_bytes <= budget);
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
                let mut resolver = Resolver::open(kv, &receipt, 0).await.unwrap();
                for n in [0, max_address(family)] {
                    let ip = if family == 4 {
                        IpAddr::from((n as u32).to_be_bytes())
                    } else {
                        IpAddr::from(n.to_be_bytes())
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
            let mut resolver = Resolver::open(kv, &receipt, 0).await.unwrap();
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
            let kv = KvHandle::new(raw.clone());
            let (receipt, manifest, _) = build(&kv, source(), &ranges(), &[], 1024).await.unwrap();
            let key = node_key(
                &manifest.generation,
                4,
                &manifest.ipv4.as_ref().unwrap().hash,
            );
            let old = raw.0.lock().unwrap().remove(&key).unwrap();
            let mut resolver = Resolver::open(kv.clone(), &receipt, 0).await.unwrap();
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
            assert!(Resolver::open(kv, &bad_receipt, 0).await.is_err());
        });
    }

    #[test]
    fn malformed_records_with_matching_digest_are_rejected() {
        let reference = |b: &[u8]| NodeRef {
            start: 0,
            end: 4,
            hash: digest(b),
            level: 0,
        };
        let mut bytes = header(4, 0, 2).unwrap();
        for (start, end) in [(0, 2), (2, 4)] {
            write_address(&mut bytes, start, 4);
            write_address(&mut bytes, end, 4);
            bytes.extend_from_slice(&Location::new("US", "CA").unwrap().0);
        }
        assert!(Object::decode(bytes.clone().into(), 4, &reference(&bytes), 140).is_err());
        let mut bytes = header(4, 0, 1).unwrap();
        write_address(&mut bytes, 0, 4);
        write_address(&mut bytes, 4, 4);
        bytes.extend_from_slice(&[b'U', b'S', 4, b'C', b'A', 0]);
        assert!(Object::decode(bytes.clone().into(), 4, &reference(&bytes), 140).is_err());
        bytes.pop();
        assert!(Object::decode(bytes.clone().into(), 4, &reference(&bytes), 140).is_err());
    }

    #[test]
    fn rejects_overlaps_and_too_small_objects() {
        block_on(async {
            let kv = KvHandle::new(Arc::new(MemoryStore::default()));
            let mut ranges = ranges();
            ranges[1].start = 1;
            assert!(build(&kv, source(), &ranges, &[], 140).await.is_err());
            assert!(build(&kv, source(), &[], &[], 139).await.is_err());
        });
    }
}
