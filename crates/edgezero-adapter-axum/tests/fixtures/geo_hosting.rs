// Real-listener reader-IP scenarios. Included by production_hosting's tests so
// process ownership and root-free shutdown retain the existing fixture helpers.

#[cfg(feature = "geo-maxmind")]
use std::path::{Path, PathBuf};

fn geo_reader_address(ipv6: bool) -> SocketAddr {
    TcpListener::bind(if ipv6 { "[::1]:0" } else { "127.0.0.1:0" })
        .expect("reader listener reservation")
        .local_addr()
        .expect("reader listener address")
}

fn geo_forwarding(family: &str, reader: &str) -> String {
    match family {
        "forwarded" if reader.contains(':') => format!("Forwarded: for=\"[{reader}]\"\r\n"),
        "forwarded" => format!("Forwarded: for={reader}\r\n"),
        "x-forwarded" => format!("X-Forwarded-For: {reader}\r\n"),
        _ => panic!("unknown fixture forwarding family"),
    }
}

fn assert_reader_location(value: &Value, provider: &str, source: &str, reader_source: &str) {
    assert_eq!(value["geo"]["status"], "located");
    assert_eq!(value["geo"]["provider"], provider);
    assert_eq!(value["geo"]["country"], "US");
    assert_eq!(value["geo"]["subdivision"], "CA");
    assert_eq!(value["geo"]["source"], source);
    assert_eq!(
        value["geo"]["trust_basis"],
        format!("CheckedReaderIp({reader_source})")
    );
}

fn assert_geo_startup_rejected(server: &mut Server, cwd: &TempDir, category: &str) {
    assert!(!server.wait().success(), "{}", server.log());
    let log = server.log();
    assert!(log.contains(category), "{log}");
    for forbidden in [
        "INITIALIZED",
        "HOOK_ENTERED",
        "ready",
        "event=geo_database",
        "PRIVATE_RECORD_SENTINEL",
    ] {
        assert!(!log.contains(forbidden), "{forbidden}: {log}");
    }
    TcpStream::connect_timeout(&server.addr, Duration::from_millis(50))
        .expect_err("rejected preparation must not bind");
    assert_eq!(fs::read_dir(cwd.path()).expect("unrelated cwd").count(), 0);
}

#[test]
#[cfg(not(feature = "geo-maxmind"))]
fn geo_hosting_rejects_unavailable_maxmind() {
    let cwd = tempfile::tempdir().expect("unrelated cwd");
    let database = tempfile::tempdir().expect("separate database root");
    let path = database.path().join("PRIVATE_DATABASE_PATH.mmdb");
    let addr = address();
    let mut command = geo_command("geo-maxmind", &cwd, addr);
    command.env("FIXTURE_GEO_DB", &path);
    let mut server = Server::spawn(command, addr);
    assert_geo_startup_rejected(&mut server, &cwd, "maxmind implementation unavailable");
    assert!(!server.log().contains(&path.display().to_string()));
    assert!(!server.log().contains("PRIVATE_DATABASE_PATH"));
    assert_eq!(
        fs::read_dir(database.path())
            .expect("database root")
            .count(),
        0
    );
}

#[test]
fn geo_hosting_supplied_reader_without_builtin() {
    // This test deliberately has no built-in feature gate. The axum-only lane
    // exercises the same programmatic composition as combined-feature builds.
    for ipv6 in [false, true] {
        let cwd = tempfile::tempdir().expect("unrelated cwd");
        let addr = geo_reader_address(ipv6);
        let mut server = Server::spawn(geo_command("geo-supplied-reader", &cwd, addr), addr);
        server.ready();
        let first = geo_response(&server, "CloudFront-Viewer-Country: GB\r\n");
        assert_reader_location(&first, "fixture-reader", "CheckedReaderIp", "DirectPeer");
        let second = geo_response(&server, "");
        assert_reader_location(&second, "fixture-reader", "CheckedReaderIp", "DirectPeer");
        assert_eq!(
            second["resolutions"].as_u64(),
            first["resolutions"].as_u64().map(|n| n + 1)
        );
        finish_geo(&mut server, &cwd);
        let log = server.log();
        let calls = log
            .lines()
            .filter(|line| line.starts_with("GEO_CALL "))
            .collect::<Vec<_>>();
        let last = calls
            .last()
            .expect("admitted requests invoke supplied provider");
        let total: usize = last
            .split_whitespace()
            .nth(1)
            .expect("count")
            .parse()
            .expect("numeric count");
        assert_eq!(calls.len(), total, "each lookup increments exactly once");
        for call in calls {
            assert!(call.contains("[redacted]"), "{call}");
            assert!(!call.contains("127.0.0.1"), "{call}");
            assert!(!call.contains("::1"), "{call}");
        }
    }
}

#[expect(clippy::cognitive_complexity, reason = "one process-owned matrix checks both forwarding families, both socket families and terminal call counts")]
#[test]
fn geo_hosting_supplied_reader_checked_forwarding_and_call_counts() {
    for family in ["forwarded", "x-forwarded"] {
        for ipv6 in [false, true] {
            let cwd = tempfile::tempdir().expect("unrelated cwd");
            let addr = geo_reader_address(ipv6);
            let mut command = geo_command("geo-supplied-reader", &cwd, addr);
            command
                .env("FIXTURE_GEO_EXPECTATION", "forwarded-required")
                .env("FIXTURE_GEO_PROXY_FAMILY", family);
            let mut server = Server::spawn(command, addr);
            server.ready();
            let absent = geo_response(&server, "");
            assert_eq!(absent["geo"]["status"], "missing_reader_ip");
            assert_eq!(absent["geo"]["provider"], "fixture-reader");
            assert_eq!(absent["resolutions"], 0_u64);
            let source = if family == "forwarded" {
                "TrustedForwarded"
            } else {
                "TrustedXForwarded"
            };
            for (index, reader) in ["192.0.2.1", "2001:db8::1", "::ffff:192.0.2.1"]
                .into_iter()
                .enumerate()
            {
                // Missing forwarded host/scheme is intentionally a partial
                // result. A checked client is sufficient for reader admission.
                let value = geo_response(&server, &geo_forwarding(family, reader));
                assert_reader_location(&value, "fixture-reader", "CheckedReaderIp", source);
                assert_eq!(value["resolutions"], index + 1);
            }
            let no_match = geo_response(&server, &geo_forwarding(family, "198.51.100.9"));
            assert_eq!(no_match["geo"]["status"], "no_match");
            assert_eq!(no_match["geo"]["provider"], "fixture-reader");
            assert_eq!(no_match["resolutions"], 4_u64);
            let malformed = if family == "forwarded" {
                "Forwarded: for=invalid\r\n"
            } else {
                "X-Forwarded-For: invalid\r\n"
            };
            let wrong_family = if family == "forwarded" {
                "x-forwarded"
            } else {
                "forwarded"
            };
            for headers in [
                malformed.to_owned(),
                geo_forwarding(family, "127.0.0.1"),
                geo_forwarding(wrong_family, "192.0.2.1"),
                "CloudFront-Viewer-Country: US\r\nCloudFront-Viewer-Country-Region: CA\r\n"
                    .to_owned(),
            ] {
                let value = geo_response(&server, &headers);
                assert_eq!(value["geo"]["status"], "missing_reader_ip");
                assert_eq!(
                    value["resolutions"], 4_u64,
                    "inadmissible readers never call provider"
                );
            }
            let rejected = raw_request(
                addr,
                &format!(
                    "GET /native-refuse HTTP/1.1\r\nHost: fixture\r\n{}Connection: close\r\n\r\n",
                    geo_forwarding(family, "192.0.2.1")
                ),
            );
            assert!(rejected.starts_with("HTTP/1.1 503"), "{rejected}");
            let mut aborted_socket = server.socket().expect("abort socket");
            write!(
                aborted_socket,
                "GET /native-abort HTTP/1.1\r\nHost: fixture\r\n{}Connection: close\r\n\r\n",
                geo_forwarding(family, "192.0.2.1")
            )
            .expect("abort request");
            let mut aborted = Vec::new();
            if let Err(error) = aborted_socket.read_to_end(&mut aborted) {
                assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
            }
            assert!(aborted.is_empty(), "admission abort must not dispatch");
            let after = geo_response(&server, &geo_forwarding(family, "192.0.2.1"));
            assert_eq!(
                after["resolutions"], 5_u64,
                "refused and aborted requests invoke no provider"
            );
            server.wait_line("GEO_COMPLETED 5");
            finish_geo(&mut server, &cwd);
            let log = server.log();
            let calls = log
                .lines()
                .filter(|line| line.starts_with("GEO_CALL "))
                .collect::<Vec<_>>();
            assert_eq!(calls.len(), 5, "no finalization lookup");
            for call in calls {
                assert!(call.contains("[redacted]"), "{call}");
                for reader in ["192.0.2.1", "2001:db8::1", "198.51.100.9"] {
                    assert!(
                        !call.contains(reader),
                        "reader input Debug must redact addresses: {call}"
                    );
                }
            }
        }
    }
}

#[test]
fn geo_hosting_reader_expectations_and_untrusted_forwarding() {
    for family in ["forwarded", "x-forwarded"] {
        for expectation in ["peer-allowed", "forwarded-required"] {
            for untrusted in [false, true] {
                let cwd = tempfile::tempdir().expect("unrelated cwd");
                let addr = address();
                let mut command = geo_command("geo-supplied-reader", &cwd, addr);
                command
                    .env("FIXTURE_GEO_EXPECTATION", expectation)
                    .env("FIXTURE_GEO_PROXY_FAMILY", family);
                if untrusted {
                    command.env("FIXTURE_GEO_PROXY_NETWORKS", "203.0.113.0/24");
                }
                let mut server = Server::spawn(command, addr);
                server.ready();
                let direct = geo_response(&server, "");
                if expectation == "peer-allowed" {
                    assert_reader_location(
                        &direct,
                        "fixture-reader",
                        "CheckedReaderIp",
                        "DirectPeer",
                    );
                } else {
                    assert_eq!(direct["geo"]["status"], "missing_reader_ip");
                    assert_eq!(direct["resolutions"], 0_u64);
                }
                let forwarded = geo_response(&server, &geo_forwarding(family, "192.0.2.1"));
                if untrusted {
                    if expectation == "peer-allowed" {
                        assert_reader_location(
                            &forwarded,
                            "fixture-reader",
                            "CheckedReaderIp",
                            "DirectPeer",
                        );
                    } else {
                        assert_eq!(forwarded["geo"]["status"], "missing_reader_ip");
                        assert_eq!(forwarded["resolutions"], 0_u64);
                    }
                } else {
                    let source = if family == "forwarded" {
                        "TrustedForwarded"
                    } else {
                        "TrustedXForwarded"
                    };
                    assert_reader_location(&forwarded, "fixture-reader", "CheckedReaderIp", source);
                }
                finish_geo(&mut server, &cwd);
            }
        }
    }
}

#[test]
fn geo_hosting_supplied_reader_drain_does_not_resolve_pipeline() {
    let cwd = tempfile::tempdir().expect("unrelated cwd");
    let addr = address();
    let mut command = geo_command("geo-supplied-reader", &cwd, addr);
    command
        .env("FIXTURE_GEO_EXPECTATION", "forwarded-required")
        .env("FIXTURE_GEO_PROXY_FAMILY", "forwarded");
    let mut server = Server::spawn(command, addr);
    server.ready();
    let mut client = server.socket().expect("finite pipeline socket");
    client.write_all(b"GET /finite HTTP/1.1\r\nHost: fixture\r\nForwarded: for=192.0.2.1\r\n\r\nGET /geo HTTP/1.1\r\nHost: fixture\r\nForwarded: for=192.0.2.1\r\nConnection: close\r\n\r\n").expect("pipeline");
    server.wait_line("DISPATCH");
    assert!(
        server
            .request("/stop")
            .expect("caller stop")
            .ends_with("stopping")
    );
    server.wait_line("DRAINING");
    let mut response = String::new();
    client
        .read_to_string(&mut response)
        .expect("drained finite response");
    assert!(response.contains("finite-complete"), "{response}");
    assert!(
        !response.contains("immutable"),
        "pipelined Geo must not dispatch"
    );
    assert!(server.wait().success(), "{}", server.log());
    assert_eq!(
        server
            .log()
            .lines()
            .filter(|line| line.starts_with("GEO_CALL "))
            .count(),
        1
    );
    assert_eq!(fs::read_dir(cwd.path()).expect("unrelated cwd").count(), 0);
}

#[cfg(feature = "geo-maxmind")]
fn geo_database_fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/geoip")
        .join(name)
}

#[cfg(feature = "geo-maxmind")]
fn geo_maxmind_command(cwd: &TempDir, addr: SocketAddr, path: &Path) -> Command {
    let mut command = geo_command("geo-maxmind", cwd, addr);
    command.env("FIXTURE_GEO_DB", path);
    command
}

#[cfg(feature = "geo-maxmind")]
fn assert_geo_snapshot(
    server: &Server,
    fixture: &str,
    database_type: &str,
    epoch: u64,
    private_path: &Path,
) {
    let sums = fs::read_to_string(geo_database_fixture("SHA256SUMS")).expect("fixture checksums");
    let hash = sums
        .lines()
        .find_map(|line| {
            let (hash, name) = line.split_once("  ")?;
            (name == fixture).then_some(hash)
        })
        .expect("snapshot checksum");
    let log = server.log();
    let snapshots = captured_json(&log, "FRAMEWORK_LOG ")
        .into_iter()
        .filter(|record| {
            record["message"]
                .as_str()
                .is_some_and(|message| message.contains("event=geo_database"))
        })
        .collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 1, "one loaded snapshot diagnostic: {log}");
    let message = snapshots[0]["message"].as_str().expect("snapshot message");
    assert!(
        message.contains(&format!("database_type={database_type}")),
        "{message}"
    );
    assert!(
        message.contains(&format!("build_epoch={epoch}")),
        "{message}"
    );
    assert!(message.contains(&format!("sha256={hash}")), "{message}");
    assert!(!message.contains("127.0.0.1"));
    assert!(!message.contains("192.0.2"));
    assert!(
        !log.contains(&private_path.display().to_string()),
        "database paths must stay redacted: {log}"
    );
    assert!(!log.contains("PRIVATE_DATABASE_PATH"), "{log}");
}

#[test]
#[cfg(feature = "geo-maxmind")]
fn geo_hosting_maxmind_snapshot_and_restart() {
    let cwd = tempfile::tempdir().expect("unrelated cwd");
    let database = tempfile::tempdir().expect("separate database root");
    let path = database.path().join("PRIVATE_DATABASE_PATH.mmdb");
    fs::copy(geo_database_fixture("city.mmdb"), &path).expect("initial City snapshot");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).expect("read-only database");
    fs::set_permissions(database.path(), fs::Permissions::from_mode(0o555))
        .expect("read-only database directory");
    let addr = address();
    let mut server = Server::spawn(geo_maxmind_command(&cwd, addr, &path), addr);
    server.ready();
    let first = geo_response(&server, "CloudFront-Viewer-Country: GB\r\n");
    assert_reader_location(&first, "maxmind", "LocalDatabase", "DirectPeer");
    assert_geo_snapshot(&server, "city.mmdb", "GeoLite2-City", 1_793_750_400, &path);
    assert_eq!(
        fs::read_dir(database.path()).expect("no sidecars").count(),
        1
    );
    assert_eq!(
        fs::metadata(&path)
            .expect("source permissions")
            .permissions()
            .mode()
            & 0o777,
        0o444
    );
    assert_eq!(
        fs::read(&path).expect("unchanged source"),
        fs::read(geo_database_fixture("city.mmdb")).expect("City bytes")
    );
    fs::set_permissions(database.path(), fs::Permissions::from_mode(0o755))
        .expect("publish replacement");
    let staged = database.path().join("next.mmdb");
    fs::copy(geo_database_fixture("city-replacement.mmdb"), &staged)
        .expect("stage complete replacement");
    fs::rename(&staged, &path).expect("publish replacement atomically");
    let changed = geo_response(&server, "");
    assert_eq!(
        changed["geo"], first["geo"],
        "active reader retains original bytes"
    );
    fs::remove_file(&path).expect("delete loaded source");
    let deleted = geo_response(&server, "");
    assert_eq!(
        deleted["geo"], first["geo"],
        "active reader needs no filesystem access"
    );
    assert_eq!(
        fs::read_dir(database.path())
            .expect("no sidecars after deletion")
            .count(),
        0
    );
    finish_geo(&mut server, &cwd);
    assert_geo_snapshot(&server, "city.mmdb", "GeoLite2-City", 1_793_750_400, &path);
    fs::copy(geo_database_fixture("city-replacement.mmdb"), &path)
        .expect("publish restart snapshot");
    let restart_addr = address();
    let mut restarted = Server::spawn(geo_maxmind_command(&cwd, restart_addr, &path), restart_addr);
    restarted.ready();
    let replacement = geo_response(&restarted, "");
    assert_eq!(replacement["geo"]["country"], "US");
    assert_eq!(replacement["geo"]["subdivision"], "NY");
    assert_eq!(replacement["geo"]["provider"], "maxmind");
    assert_geo_snapshot(
        &restarted,
        "city-replacement.mmdb",
        "GeoLite2-City",
        1_793_836_800,
        &path,
    );
    finish_geo(&mut restarted, &cwd);
    fs::write(&path, b"PRIVATE_RECORD_SENTINEL invalid replacement").expect("invalid replacement");
    let invalid_addr = address();
    let mut invalid = Server::spawn(geo_maxmind_command(&cwd, invalid_addr, &path), invalid_addr);
    assert_geo_startup_rejected(&mut invalid, &cwd, "invalid database");
    assert!(!invalid.log().contains("PRIVATE_DATABASE_PATH"));
    assert_eq!(
        fs::read_dir(database.path())
            .expect("no replacement sidecars")
            .count(),
        1
    );
}

#[test]
#[cfg(feature = "geo-maxmind")]
fn geo_hosting_maxmind_checked_inputs_and_record_outcomes() {
    for family in ["forwarded", "x-forwarded"] {
        for (fixture, reader, country, subdivision) in [
            ("city.mmdb", "192.0.2.1", "US", Some("CA")),
            ("country.mmdb", "192.0.2.1", "US", None),
            ("ipv6.mmdb", "2001:db8::1", "GB", Some("ENG")),
        ] {
            let cwd = tempfile::tempdir().expect("unrelated cwd");
            let database = tempfile::tempdir().expect("separate database root");
            let path = database.path().join("PRIVATE_DATABASE_PATH.mmdb");
            fs::copy(geo_database_fixture(fixture), &path).expect("reader database");
            let addr = address();
            let mut command = geo_maxmind_command(&cwd, addr, &path);
            command
                .env("FIXTURE_GEO_EXPECTATION", "forwarded-required")
                .env("FIXTURE_GEO_PROXY_FAMILY", family);
            let mut server = Server::spawn(command, addr);
            server.ready();
            let absent = geo_response(&server, "");
            assert_eq!(absent["geo"]["status"], "missing_reader_ip");
            assert_eq!(absent["geo"]["provider"], "maxmind");
            let located = geo_response(&server, &geo_forwarding(family, reader));
            assert_eq!(located["geo"]["status"], "located");
            assert_eq!(located["geo"]["country"], country);
            assert_eq!(located["geo"]["subdivision"].as_str(), subdivision);
            assert_eq!(located["geo"]["source"], "LocalDatabase");
            let source = if family == "forwarded" {
                "TrustedForwarded"
            } else {
                "TrustedXForwarded"
            };
            assert_eq!(
                located["geo"]["trust_basis"],
                format!("CheckedReaderIp({source})")
            );
            let no_match = geo_response(&server, &geo_forwarding(family, "198.51.100.9"));
            assert_eq!(no_match["geo"]["status"], "no_match");
            assert_eq!(no_match["geo"]["provider"], "maxmind");
            if fixture == "city.mmdb" {
                for (case_reader, expected, case_subdivision) in [
                    ("192.0.2.2", "located", None),
                    ("192.0.2.3", "located", Some("01")),
                    ("192.0.2.4", "located", Some("ZZ")),
                    ("192.0.2.5", "located", None),
                    ("192.0.2.7", "no_match", None),
                    ("192.0.2.8", "invalid_assertion", None),
                    ("192.0.2.10", "invalid_assertion", None),
                    ("2001:db8::1", "no_match", None),
                    ("::ffff:192.0.2.1", "located", Some("CA")),
                ] {
                    let value = geo_response(&server, &geo_forwarding(family, case_reader));
                    assert_eq!(value["geo"]["status"], expected, "reader={case_reader}");
                    assert_eq!(value["geo"]["subdivision"].as_str(), case_subdivision);
                }
            }
            finish_geo(&mut server, &cwd);
        }
    }
}

#[test]
#[cfg(feature = "geo-maxmind")]
fn geo_hosting_maxmind_reader_expectations_on_real_peers() {
    for ipv6 in [false, true] {
        for family in ["forwarded", "x-forwarded"] {
            for expectation in ["peer-allowed", "forwarded-required"] {
                let cwd = tempfile::tempdir().expect("unrelated cwd");
                let database = tempfile::tempdir().expect("separate database root");
                let path = database.path().join("PRIVATE_DATABASE_PATH.mmdb");
                fs::copy(geo_database_fixture("city.mmdb"), &path).expect("City database");
                let addr = geo_reader_address(ipv6);
                let mut command = geo_maxmind_command(&cwd, addr, &path);
                command
                    .env("FIXTURE_GEO_EXPECTATION", expectation)
                    .env("FIXTURE_GEO_PROXY_FAMILY", family);
                let mut server = Server::spawn(command, addr);
                server.ready();
                let direct = geo_response(&server, "");
                if expectation == "forwarded-required" {
                    assert_eq!(direct["geo"]["status"], "missing_reader_ip");
                } else if ipv6 {
                    assert_eq!(
                        direct["geo"]["status"], "no_match",
                        "IPv6 peer against IPv4-only database"
                    );
                } else {
                    assert_reader_location(&direct, "maxmind", "LocalDatabase", "DirectPeer");
                }
                let forwarded = geo_response(&server, &geo_forwarding(family, "192.0.2.1"));
                let source = if family == "forwarded" {
                    "TrustedForwarded"
                } else {
                    "TrustedXForwarded"
                };
                assert_reader_location(&forwarded, "maxmind", "LocalDatabase", source);
                let malformed = geo_response(
                    &server,
                    if family == "forwarded" {
                        "Forwarded: for=invalid\r\n"
                    } else {
                        "X-Forwarded-For: invalid\r\n"
                    },
                );
                assert_eq!(
                    malformed["geo"], direct["geo"],
                    "malformed forwarding cannot invent a checked client"
                );
                finish_geo(&mut server, &cwd);
            }
        }
    }
}

#[test]
#[cfg(feature = "geo-maxmind")]
fn geo_hosting_maxmind_preparation_failures_never_initialize_or_bind() {
    for (fixture, category, byte_limit, visit_limit) in [
        ("missing", "database unavailable", None, None),
        ("directory", "database unavailable", None, None),
        ("empty", "invalid database", None, None),
        ("invalid", "invalid database", None, None),
        ("truncated", "invalid database", None, None),
        ("unsupported.mmdb", "unsupported database", None, None),
        (
            "bad-country-code.mmdb",
            "incompatible database schema",
            None,
            None,
        ),
        (
            "bad-subdivision-code.mmdb",
            "incompatible database schema",
            None,
            None,
        ),
        ("bad-offpath-pointer.mmdb", "invalid database", None, None),
        ("bad-offpath-utf8.mmdb", "invalid database", None, None),
        ("bad-metadata.mmdb", "invalid database", None, None),
        (
            "expansion-limit.mmdb",
            "preparation limit exceeded",
            None,
            None,
        ),
        ("city.mmdb", "preparation limit exceeded", Some(1_usize), None),
        ("city.mmdb", "preparation limit exceeded", None, Some(1_usize)),
        (
            "city.mmdb",
            "invalid database preparation limits",
            Some(0_usize),
            None,
        ),
        (
            "city.mmdb",
            "invalid database preparation limits",
            None,
            Some(0_usize),
        ),
    ] {
        let cwd = tempfile::tempdir().expect("unrelated cwd");
        let database = tempfile::tempdir().expect("separate database root");
        let path = database.path().join("PRIVATE_DATABASE_PATH.mmdb");
        match fixture {
            "missing" => {}
            "directory" => fs::create_dir_all(&path).expect("nonregular database"),
            "empty" => fs::write(&path, []).expect("empty database"),
            "invalid" => fs::write(&path, b"PRIVATE_RECORD_SENTINEL").expect("invalid database"),
            "truncated" => {
                let mut bytes = fs::read(geo_database_fixture("city.mmdb")).expect("City bytes");
                bytes.truncate(bytes.len().div_ceil(2));
                fs::write(&path, bytes).expect("truncated database");
            }
            name => {
                fs::copy(geo_database_fixture(name), &path).expect("malformed or limited database");
            }
        }
        let addr = address();
        let mut command = geo_maxmind_command(&cwd, addr, &path);
        if let Some(limit) = byte_limit {
            command.env("FIXTURE_GEO_MAX_BYTES", limit.to_string());
        }
        if let Some(limit) = visit_limit {
            command.env("FIXTURE_GEO_MAX_NETWORKS", limit.to_string());
        }
        let mut server = Server::spawn(command, addr);
        assert_geo_startup_rejected(&mut server, &cwd, category);
        let log = server.log();
        assert!(!log.contains(&path.display().to_string()), "{log}");
        assert!(!log.contains("PRIVATE_DATABASE_PATH"), "{log}");
        assert!(!log.contains("192.0.2.1"), "{log}");
        assert!(!log.contains("203.0.113.1"), "{log}");
        assert_eq!(
            fs::read_dir(database.path())
                .expect("no preparation sidecars")
                .count(),
            usize::from(fixture != "missing")
        );
    }
}
