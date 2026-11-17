use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn command(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_geo-kv-prototype"))
        .args(args)
        .output()
        .unwrap()
}
fn success(args: &[&str]) -> Value {
    let output = command(args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn text(path: &Path) -> &str {
    path.to_str().unwrap()
}

fn source(dir: &Path, city: bool) {
    let edition = if city { "City" } else { "Country" };
    let locations = if city {
        "geoname_id,country_iso_code,subdivision_1_iso_code,ignored\n1,US,CA,extra\n2,GB,,extra\n3,XK,,extra\n4,,,extra\n"
    } else {
        "geoname_id,country_iso_code,ignored\n1,US,extra\n2,GB,extra\n3,XK,extra\n4,,extra\n"
    };
    fs::write(
        dir.join(format!("GeoLite2-{edition}-Locations-en.csv")),
        locations,
    )
    .unwrap();
    fs::write(dir.join(format!("GeoLite2-{edition}-Blocks-IPv4.csv")), "network,geoname_id,registered_country_geoname_id\n0.0.0.0/32,1,2\n0.0.0.2/31,2,1\n1.0.0.0/24,,1\n2.0.0.0/24,4,1\n3.0.0.0/24,3,1\n255.255.255.255/32,2,1\n").unwrap();
    fs::write(dir.join(format!("GeoLite2-{edition}-Blocks-IPv6.csv")), "network,geoname_id\n::/128,1\n::2/127,2\n::ffff:0:0/96,3\n2001:db8::/32,3\nffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128,2\n").unwrap();
}

#[test]
fn city_and_country_cli_build_oracle_and_deterministic_inventory() {
    for city in [true, false] {
        let src = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let again = tempfile::tempdir().unwrap();
        source(src.path(), city);
        success(&["build", text(src.path()), text(out.path()), "1024"]);
        success(&["build", text(src.path()), text(again.path()), "1024"]);
        let default_out = tempfile::tempdir().unwrap();
        let defaults = success(&["build", text(src.path()), text(default_out.path())]);
        assert_eq!(defaults["object_budget_bytes"], 16 * 1024_u64);
        // A completed build can also be repeated in-place.
        success(&["build", text(src.path()), text(out.path()), "1024"]);
        let rejected = command(&["build", text(src.path()), text(out.path()), "2048"]);
        assert!(!rejected.status.success());
        assert!(
            String::from_utf8_lossy(&rejected.stderr)
                .contains("output already selects another generation")
        );
        assert_eq!(fs::read_dir(out.path().join("geo/v1")).unwrap().count(), 1);
        let build_report: Value =
            serde_json::from_slice(&fs::read(out.path().join("build-report.json")).unwrap())
                .unwrap();
        for file in build_report["source"]["files"].as_array().unwrap() {
            use sha2::{Digest as _, Sha256};
            let filename = file[0].as_str().unwrap();
            let csv = fs::read(src.path().join(filename)).unwrap();
            let actual: String = Sha256::digest(csv)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(file[1], actual, "{filename}");
        }
        for name in ["receipt.json", "inventory.json"] {
            assert_eq!(
                fs::read(out.path().join(name)).unwrap(),
                fs::read(again.path().join(name)).unwrap()
            );
        }
        let inventory: Vec<Value> =
            serde_json::from_slice(&fs::read(out.path().join("inventory.json")).unwrap()).unwrap();
        for item in inventory {
            let key = item["key"].as_str().unwrap();
            assert!(key.starts_with("geo/v1/"));
            assert_eq!(
                fs::read(out.path().join(key)).unwrap(),
                fs::read(again.path().join(key)).unwrap()
            );
        }
        let report = success(&["verify", text(src.path()), text(out.path()), "256", "1024"]);
        assert_eq!(report["passes"][0]["oracle_matches"], 512);
        let report = success(&[
            "lookup",
            text(out.path()),
            "0.0.0.0",
            "0.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::ffff:0.0.0.0",
            "2001:db8::1",
        ]);
        let results = &report["results_in_input_order"];
        assert_eq!(results[0]["country"], "US");
        assert_eq!(
            results[0]["subdivision"],
            if city { json!("CA") } else { Value::Null }
        );
        assert!(results[1].is_null());
        assert_eq!(results[2]["country"], "GB");
        assert_eq!(results[3], results[0]);
        assert!(results[4].is_null());
        assert_eq!(results[5], results[0]);
        assert_eq!(results[6]["country"], "XK");
        assert_eq!(results[6]["core_country_supported"], false);
    }
}

#[test]
fn errors_do_not_echo_reader_addresses_or_raw_input() {
    let source = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    self::source(source.path(), true);
    success(&["build", text(source.path()), text(out.path()), "1024"]);
    let marker = "private-reader-input-not-an-ip";
    let output = command(&["lookup", text(out.path()), marker]);
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(marker));
    assert!(output.stdout.is_empty());
    let secret = "private-location-record-and-credential";
    fs::write(
        source.path().join("GeoLite2-City-Locations-en.csv"),
        format!("geoname_id,country_iso_code\n1,{secret}\n"),
    )
    .unwrap();
    let output = command(&["build", text(source.path()), text(out.path()), "1024"]);
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    assert!(output.stdout.is_empty());
}
