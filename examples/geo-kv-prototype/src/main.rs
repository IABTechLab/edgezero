mod import;
mod index;
mod store;

use std::{fs, net::IpAddr, path::Path, sync::Arc, time::Instant};

use anyhow::{Context, Result, bail, ensure};
use edgezero_core::key_value_store::KvHandle;
use futures::executor::block_on;
use serde_json::json;

use index::{Receipt, Resolver, digest, hex};
use store::ArtifactStore;

fn peak_rss_bytes() -> Option<u64> {
    // Linux process high-water mark, including importer/oracle/backend/cache.
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
                .map(|n| n * 1024)
        })
}

fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

fn read_receipt(out: &Path) -> Result<Receipt> {
    Ok(serde_json::from_slice(&fs::read(
        out.join("receipt.json"),
    )?)?)
}

async fn build(source: &Path, out: &Path, object_bytes: usize) -> Result<()> {
    let started = Instant::now();
    let imported = import::load(source)?;
    let import_seconds = started.elapsed().as_secs_f64();
    let (v4, stats4) = import::project(&imported.raw4);
    let (v6, stats6) = import::project(&imported.raw6);
    let projection_seconds = started.elapsed().as_secs_f64() - import_seconds;
    fs::create_dir_all(out)?;
    let store = Arc::new(ArtifactStore::new(out));
    let kv = KvHandle::new(store.clone());
    let build_started = Instant::now();
    let (receipt, manifest, inventory) =
        index::build(&kv, imported.source, &v4, &v6, object_bytes).await?;
    let build_seconds = build_started.elapsed().as_secs_f64();
    if out.join("receipt.json").exists() {
        ensure!(
            read_receipt(out)? == receipt,
            "output already selects another generation; use a different output directory"
        );
    }
    let verify_started = Instant::now();
    for item in &inventory {
        let bytes = kv
            .get_bytes(&item.key)
            .await?
            .context("published object missing")?;
        ensure!(
            bytes.len() == item.bytes && hex(&digest(&bytes)) == item.sha256,
            "published object read-back mismatch"
        );
    }
    let report = json!({
        "prototype": "p1, not a production format",
        "transport": "local artifact files through KvHandle; no remote KV measurements",
        "source": manifest.source,
        "manifest_key": receipt.manifest_key,
        "manifest_sha256": hex(&receipt.manifest_hash),
        "locations": imported.locations,
        "ipv4": stats4,
        "ipv6": stats6,
        "object_budget_bytes": object_bytes,
        "objects_including_manifest": inventory.len(),
        "encoded_bytes_including_manifest": inventory.iter().map(|i| i.bytes).sum::<usize>(),
        "largest_object_bytes": inventory.iter().map(|i| i.bytes).max(),
        "ipv4_lookup_objects": manifest.ipv4.as_ref().map(|r| usize::from(r.level) + 1),
        "ipv6_lookup_objects": manifest.ipv6.as_ref().map(|r| usize::from(r.level) + 1),
        "import_seconds_including_source_hashing": import_seconds,
        "projection_seconds": projection_seconds,
        "build_and_write_seconds": build_seconds,
        "readback_seconds": verify_started.elapsed().as_secs_f64(),
        "total_seconds": started.elapsed().as_secs_f64(),
        "process_peak_rss_bytes": peak_rss_bytes(),
        "store": store.stats(),
    });
    write_json(&out.join("receipt.json"), &receipt)?;
    write_json(&out.join("inventory.json"), &inventory)?;
    write_json(&out.join("build-report.json"), &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn measure(
    name: &str,
    resolver: &mut Resolver,
    store: &ArtifactStore,
    queries: &[(IpAddr, Option<index::Location>)],
) -> Result<serde_json::Value> {
    store.reset_stats();
    let mut times = Vec::with_capacity(queries.len());
    let mut read_histogram = std::collections::BTreeMap::new();
    let mut located = 0;
    let hits_before = resolver.cache_stats().hits;
    for (ip, expected) in queries {
        let before = store.stats().reads;
        let started = Instant::now();
        let actual = resolver.lookup(*ip).await?;
        times.push(started.elapsed().as_secs_f64() * 1_000_000.0);
        ensure!(actual == *expected, "CSV oracle mismatch");
        located += usize::from(actual.is_some());
        *read_histogram
            .entry(store.stats().reads - before)
            .or_insert(0_usize) += 1;
    }
    times.sort_by(f64::total_cmp);
    let stats = store.stats();
    Ok(json!({
        "pass": name, "queries": queries.len(), "located": located,
        "no_match": queries.len() - located, "oracle_mismatches": 0,
        "mean_microseconds": times.iter().sum::<f64>() / times.len() as f64,
        "p50_microseconds": times[times.len() / 2],
        "p95_microseconds": times[(times.len() - 1) * 95 / 100],
        "max_microseconds": times[times.len() - 1],
        "reads_per_query_histogram": read_histogram,
        "reads_per_query_mean": stats.reads as f64 / queries.len() as f64,
        "bytes_per_query_mean": stats.read_bytes as f64 / queries.len() as f64,
        "cache_hits": resolver.cache_stats().hits - hits_before,
        "peak_cached_payload_bytes": resolver.cache_stats().peak_payload_bytes,
        "max_decoded_object_bytes": resolver.cache_stats().max_object_bytes,
        "store": stats,
    }))
}

async fn verify(source: &Path, out: &Path, count: usize, cache_bytes: usize) -> Result<()> {
    ensure!(
        count >= 2,
        "at least two samples per family are needed for endpoints"
    );
    let imported = import::load(source)?;
    let mut queries = Vec::new();
    for (family, raw) in [(4, &imported.raw4), (6, &imported.raw6)] {
        for ip in import::samples(raw, family, count) {
            queries.push((ip, import::oracle(raw, ip)));
        }
    }
    let receipt = read_receipt(out)?;
    let store = Arc::new(ArtifactStore::new(out));
    let kv = KvHandle::new(store.clone());
    let mut disabled = Resolver::open(kv.clone(), &receipt, 0).await?;
    ensure!(
        disabled.manifest.source == imported.source,
        "oracle source differs from indexed source"
    );
    let mut reports = vec![measure("cache_disabled", &mut disabled, &store, &queries).await?];
    let mut enabled = Resolver::open(kv, &receipt, cache_bytes).await?;
    reports.push(measure("cache_enabled_first_pass", &mut enabled, &store, &queries).await?);
    reports.push(measure("cache_enabled_repeat", &mut enabled, &store, &queries).await?);
    let report = json!({
        "transport": "local file artifact KV; OS page cache uncontrolled; not network latency",
        "manifest_sha256": hex(&receipt.manifest_hash),
        "sample_method": "per family: zero/max, stratified source CIDR endpoints and neighbors, deterministic uniform random remainder",
        "samples_per_family": count,
        "manifest_reads_per_resolver_initialization": 1,
        "cache_payload_budget_bytes": cache_bytes,
        "passes": reports,
        "process_peak_rss_bytes_including_csv_oracle": peak_rss_bytes(),
    });
    write_json(&out.join("verification-report.json"), &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn lookup(out: &Path, ips: &[String]) -> Result<()> {
    ensure!(!ips.is_empty(), "provide at least one IP address");
    let receipt = read_receipt(out)?;
    let store = Arc::new(ArtifactStore::new(out));
    let mut resolver =
        Resolver::open(KvHandle::new(store.clone()), &receipt, 4 * 1024 * 1024).await?;
    let mut results = Vec::new();
    for text in ips {
        let ip: IpAddr = text.parse()?;
        results.push(resolver.lookup(ip).await?.map(index::Location::json));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "results_in_input_order": results,
            "store": store.stats(), "cache": resolver.cache_stats(),
            "process_peak_rss_bytes_without_csv_import": peak_rss_bytes(),
        }))?
    );
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    block_on(async {
        match args.first().map(String::as_str) {
            Some("build") if (3..=4).contains(&args.len()) => {
                build(
                    Path::new(&args[1]),
                    Path::new(&args[2]),
                    args.get(3)
                        .map(|s| s.parse())
                        .transpose()?
                        .unwrap_or(256 * 1024),
                )
                .await
            }
            Some("verify") if (3..=5).contains(&args.len()) => {
                verify(
                    Path::new(&args[1]),
                    Path::new(&args[2]),
                    args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(2048),
                    args.get(4)
                        .map(|s| s.parse())
                        .transpose()?
                        .unwrap_or(4 * 1024 * 1024),
                )
                .await
            }
            Some("lookup") if args.len() >= 3 => lookup(Path::new(&args[1]), &args[2..]).await,
            _ => bail!(
                "usage: geo-kv-prototype build <csv-directory> <artifact-directory> [object-bytes]\n       geo-kv-prototype verify <csv-directory> <artifact-directory> [samples-per-family] [cache-bytes]\n       geo-kv-prototype lookup <artifact-directory> <ip> [ip ...]"
            ),
        }
    })
}
