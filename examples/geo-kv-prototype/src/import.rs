//! CSV import and independent, unmerged CIDR oracle for the format experiment.
use std::{collections::HashMap, fs::File, io::Read, net::IpAddr, path::Path};

use anyhow::{Context, Result, ensure};
use ipnet::IpNet;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::index::{Location, Range, Source, address, hex};

#[derive(Debug, Deserialize)]
struct LocationRow {
    geoname_id: u32,
    country_iso_code: String,
    #[serde(default)]
    subdivision_1_iso_code: String,
}

#[derive(Debug, Deserialize)]
struct BlockRow {
    network: IpNet,
    geoname_id: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct SourceRange {
    pub network: IpNet,
    pub location: Option<Location>,
}

impl SourceRange {
    pub fn start(&self) -> u128 {
        address(self.network.network()).1
    }
    pub fn end(&self) -> u128 {
        address(self.network.broadcast()).1
    }
}

#[derive(Debug, Serialize)]
pub struct FamilyStats {
    pub source_networks: usize,
    pub without_country: usize,
    pub country_only: usize,
    pub merged_intervals: usize,
    pub unsupported_core_country_networks: std::collections::BTreeMap<String, usize>,
}

pub struct Imported {
    pub source: Source,
    pub locations: usize,
    pub raw4: Vec<SourceRange>,
    pub raw6: Vec<SourceRange>,
}

fn locations(reader: impl Read) -> Result<HashMap<u32, Option<Location>>> {
    let mut out = HashMap::new();
    for row in csv::Reader::from_reader(reader).deserialize::<LocationRow>() {
        let row = row?;
        let location = if row.country_iso_code.is_empty() {
            ensure!(
                row.subdivision_1_iso_code.is_empty(),
                "subdivision without country for geoname {}",
                row.geoname_id
            );
            None
        } else {
            Some(
                Location::new(&row.country_iso_code, &row.subdivision_1_iso_code).with_context(
                    || format!("invalid location codes for geoname {}", row.geoname_id),
                )?,
            )
        };
        ensure!(
            out.insert(row.geoname_id, location).is_none(),
            "duplicate geoname identity {}",
            row.geoname_id
        );
    }
    Ok(out)
}

fn blocks(
    reader: impl Read,
    locations: &HashMap<u32, Option<Location>>,
    family: u8,
) -> Result<Vec<SourceRange>> {
    let mut out = Vec::new();
    for row in csv::Reader::from_reader(reader).deserialize::<BlockRow>() {
        let row = row?;
        ensure!(
            address(row.network.addr()).0 == family,
            "wrong CSV address family"
        );
        ensure!(
            row.network.addr() == row.network.network(),
            "CIDR has host bits"
        );
        let location = row
            .geoname_id
            .map(|id| {
                locations
                    .get(&id)
                    .copied()
                    .with_context(|| format!("missing referenced geoname {id}"))
            })
            .transpose()?
            .flatten();
        out.push(SourceRange {
            network: row.network,
            location,
        });
    }
    out.sort_unstable_by_key(SourceRange::start);
    for pair in out.windows(2) {
        ensure!(
            pair[0].end() < pair[1].start(),
            "overlapping source networks"
        );
    }
    Ok(out)
}

struct HashedReader {
    file: File,
    hash: Sha256,
}

impl HashedReader {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            hash: Sha256::new(),
        })
    }
    fn finish(self) -> String {
        hex(&self.hash.finalize())
    }
}

impl Read for HashedReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let n = self.file.read(buffer)?;
        self.hash.update(&buffer[..n]);
        Ok(n)
    }
}

pub fn load(directory: &Path) -> Result<Imported> {
    let edition = if directory.join("GeoLite2-City-Locations-en.csv").is_file() {
        "GeoLite2-City"
    } else {
        "GeoLite2-Country"
    };
    let files: Vec<String> = ["Locations-en.csv", "Blocks-IPv4.csv", "Blocks-IPv6.csv"]
        .iter()
        .map(|suffix| format!("{edition}-{suffix}"))
        .collect();
    // Hash the same byte stream CSV consumes, rather than re-reading mutable files.
    let mut reader = HashedReader::open(&directory.join(&files[0]))?;
    let locations = locations(&mut reader)?;
    let mut hashes = vec![reader.finish()];
    let mut reader = HashedReader::open(&directory.join(&files[1]))?;
    let raw4 = blocks(&mut reader, &locations, 4)?;
    hashes.push(reader.finish());
    let mut reader = HashedReader::open(&directory.join(&files[2]))?;
    let raw6 = blocks(&mut reader, &locations, 6)?;
    hashes.push(reader.finish());
    let files = files.into_iter().zip(hashes).collect();
    Ok(Imported {
        source: Source {
            edition: edition.into(),
            files,
        },
        locations: locations.len(),
        raw4,
        raw6,
    })
}

pub fn project(raw: &[SourceRange]) -> (Vec<Range>, FamilyStats) {
    let mut out: Vec<Range> = Vec::new();
    let mut stats = FamilyStats {
        source_networks: raw.len(),
        without_country: 0,
        country_only: 0,
        merged_intervals: 0,
        unsupported_core_country_networks: std::collections::BTreeMap::new(),
    };
    for row in raw {
        let Some(location) = row.location else {
            stats.without_country += 1;
            continue;
        };
        if !location.core_country_supported() {
            let code = std::str::from_utf8(&location.0[..2]).expect("validated source country");
            *stats
                .unsupported_core_country_networks
                .entry(code.into())
                .or_default() += 1;
        }
        if location.0[2] == 0 {
            stats.country_only += 1;
        }
        let start = row.start();
        let end = row.end();
        if let Some(previous) = out.last_mut()
            && previous.location == location
            && previous.end.checked_add(1) == Some(start)
        {
            previous.end = end;
            continue;
        }
        out.push(Range {
            start,
            end,
            location,
        });
    }
    stats.merged_intervals = out.len();
    (out, stats)
}

// Uses the original CIDRs and ipnet.contains, not the encoded/merged interval search.
pub fn oracle(raw: &[SourceRange], ip: IpAddr) -> Option<Location> {
    let value = address(ip).1;
    let i = raw.partition_point(|r| r.start() <= value);
    if i == 0 {
        return None;
    }
    raw[i - 1]
        .network
        .contains(&ip)
        .then_some(raw[i - 1].location)
        .flatten()
}

pub fn samples(raw: &[SourceRange], family: u8, count: usize) -> Vec<IpAddr> {
    let max = if family == 4 {
        u32::MAX.into()
    } else {
        u128::MAX
    };
    let mut values = vec![0, max];
    let boundary_rows = (count / 8).min(raw.len());
    for i in 0..boundary_rows {
        let row = &raw[i * raw.len() / boundary_rows];
        values.extend([row.start(), row.end()]);
        if let Some(before) = row.start().checked_sub(1) {
            values.push(before);
        }
        if let Some(after) = row.end().checked_add(1).filter(|n| *n <= max) {
            values.push(after);
        }
    }
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    let mut random = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    while values.len() < count {
        let n = (u128::from(random()) << 64) | u128::from(random());
        values.push(n & max);
    }
    values
        .into_iter()
        .map(|n| {
            if family == 4 {
                IpAddr::from((n as u32).to_be_bytes())
            } else {
                IpAddr::from(n.to_be_bytes())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    const LOCATIONS: &str = "geoname_id,country_iso_code,subdivision_1_iso_code,country_name\n1,US,CA,\"United, States\"\n2,GB,,United Kingdom\n3,,,\n";

    #[test]
    fn csv_join_country_only_and_no_registered_fallback() {
        let loc = locations(LOCATIONS.as_bytes()).unwrap();
        let raw = blocks("network,geoname_id,registered_country_geoname_id\n1.0.0.0/24,,1\n1.0.1.0/24,1,2\n1.0.2.0/24,2,1\n1.0.3.0/24,3,1\n".as_bytes(), &loc, 4).unwrap();
        assert_eq!(raw[0].location, None);
        assert_eq!(raw[1].location, Some(Location::new("US", "CA").unwrap()));
        assert_eq!(raw[2].location, Some(Location::new("GB", "").unwrap()));
        assert_eq!(raw[3].location, None);
        assert_eq!(oracle(&raw, "1.0.1.255".parse().unwrap()), raw[1].location);
    }

    #[test]
    fn source_codes_are_preserved_without_changing_core_policy() {
        let loc = locations("geoname_id,country_iso_code\n1,XK\n".as_bytes()).unwrap();
        let raw = blocks("network,geoname_id\n0.0.0.0/0,1\n".as_bytes(), &loc, 4).unwrap();
        let (ranges, stats) = project(&raw);
        assert!(!ranges[0].location.core_country_supported());
        assert_eq!(stats.unsupported_core_country_networks.get("XK"), Some(&1));
    }

    #[test]
    fn invalid_csv_data_is_rejected() {
        assert!(
            locations("geoname_id,country_iso_code,subdivision_1_iso_code\n1,,CA\n".as_bytes())
                .is_err()
        );
        assert!(locations("geoname_id,country_iso_code\n1,US\n1,GB\n".as_bytes()).is_err());
        let loc = locations(LOCATIONS.as_bytes()).unwrap();
        for (csv, family) in [
            ("network,geoname_id\n1.0.0.0/24,999\n", 4),
            ("network,geoname_id\n::/0,1\n", 4),
            ("network,geoname_id\n1.0.0.1/24,1\n", 4),
            ("network,geoname_id\n1.0.0.0/24,1\n1.0.0.0/25,2\n", 4),
        ] {
            assert!(blocks(csv.as_bytes(), &loc, family).is_err());
        }
    }

    #[test]
    fn projection_merges_only_adjacent_equal_locations_and_preserves_gaps() {
        let loc = locations(LOCATIONS.as_bytes()).unwrap();
        let raw = blocks("network,geoname_id\n0.0.0.0/32,1\n0.0.0.1/32,1\n0.0.0.2/32,\n0.0.0.3/32,1\n255.255.255.255/32,2\n".as_bytes(), &loc, 4).unwrap();
        let (ranges, stats) = project(&raw);
        assert_eq!(stats.without_country, 1);
        assert_eq!(ranges.len(), 3);
        assert_eq!((ranges[0].start, ranges[0].end), (0, 1));
        assert_eq!(
            (ranges[2].start, ranges[2].end),
            (u32::MAX.into(), u32::MAX.into())
        );
        let raw = blocks("network,geoname_id\n::/0,2\n".as_bytes(), &loc, 6).unwrap();
        assert_eq!(project(&raw).0[0].end, u128::MAX);
    }
}
