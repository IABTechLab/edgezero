//! Fastly client-identity (mTLS) contract against host-side origins that require
//! client certificates, under the pinned Viceroy.
//!
//! Certificates are generated per run. The origins record every TCP accept before
//! TLS starts, and separately record the client certificate of each completed
//! handshake, so "never sent" is checked against accepts, not inferred.

use super::*;
use std::io::{ErrorKind, Read as _, Write as _};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use sha2::{Digest as _, Sha256};

const BIN: &str = "fixture-fastly-client-identity";
const IDENTITY: &str = "fixture-mtls";
const CONFIG_STORE: &str = "fixture_mtls_config";
const SECRET_STORE: &str = "fixture_mtls_secrets";
/// Cases each launch scenario runs, in order; the suite event must report the total.
const EXPECTED_CASES: usize = 9;

/// What one origin observed.
#[derive(Clone, Debug, Default)]
struct OriginRecords {
    accepts: usize,
    failures: Vec<String>,
    handshakes: Vec<String>,
}

struct Origin {
    port: u16,
    records: Arc<Mutex<OriginRecords>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

struct ClientCertificate {
    fingerprint: String,
    key_pem: String,
    pem: String,
}

impl Origin {
    /// Serves HTTPS one connection at a time. `redirect_to` answers `/redirect`
    /// with a 302 to that port on localhost.
    fn start(config: Arc<ServerConfig>, redirect_to: Option<u16>) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let records = Arc::new(Mutex::new(OriginRecords::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let records = Arc::clone(&records);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            records.lock().unwrap().accepts += 1;
                            serve(stream, &config, redirect_to, &records);
                        }
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            })
        };
        Ok(Self {
            port,
            records,
            stop,
            thread: Some(thread),
        })
    }

    fn snapshot(&self) -> OriginRecords {
        self.records.lock().unwrap().clone()
    }

    /// Waits briefly for the origin thread to finish recording a connection.
    fn settled(&self, accepts: usize) -> OriginRecords {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let records = self.snapshot();
            let finished = records.handshakes.len() + records.failures.len();
            if (records.accepts >= accepts && finished >= records.accepts)
                || Instant::now() >= deadline
            {
                return records;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Origin {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn serve(
    stream: std::net::TcpStream,
    config: &Arc<ServerConfig>,
    redirect_to: Option<u16>,
    records: &Mutex<OriginRecords>,
) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let Ok(connection) = ServerConnection::new(Arc::clone(config)) else {
        records.lock().unwrap().failures.push("server setup".into());
        return;
    };
    let mut tls = StreamOwned::new(connection, stream);
    while tls.conn.is_handshaking() {
        if let Err(error) = tls.conn.complete_io(&mut tls.sock) {
            records.lock().unwrap().failures.push(error.to_string());
            return;
        }
    }
    let presented = tls
        .conn
        .peer_certificates()
        .and_then(|chain| chain.first())
        .map(|leaf| hex(&Sha256::digest(leaf.as_ref())));
    let Some(fingerprint) = presented else {
        records
            .lock()
            .unwrap()
            .failures
            .push("handshake completed without a client certificate".into());
        return;
    };
    records.lock().unwrap().handshakes.push(fingerprint.clone());
    let mut head = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") && head.len() < 16 * 1024 {
        match tls.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => head.extend_from_slice(&buffer[..read]),
        }
    }
    let path = String::from_utf8_lossy(&head)
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
        .to_owned();
    let response = match redirect_to {
        Some(port) if path == "/redirect" => format!(
            "HTTP/1.1 302 Found\r\nlocation: https://localhost:{port}/end\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        ),
        _ => format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{fingerprint}",
            fingerprint.len()
        ),
    };
    let _ = tls.write_all(response.as_bytes());
    let _ = tls.flush();
    tls.conn.send_close_notify();
    let _ = tls.conn.complete_io(&mut tls.sock);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn certificate_authority(name: &str) -> Result<CertifiedIssuer<'static, KeyPair>> {
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    Ok(CertifiedIssuer::self_signed(params, KeyPair::generate()?)?)
}

fn client_certificate(
    issuer: &CertifiedIssuer<'static, KeyPair>,
    name: &str,
) -> Result<ClientCertificate> {
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.distinguished_name.push(DnType::CommonName, name);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let key = KeyPair::generate()?;
    let certificate = params.signed_by(&key, issuer)?;
    Ok(ClientCertificate {
        fingerprint: hex(&Sha256::digest(certificate.der().as_ref())),
        key_pem: key.serialize_pem(),
        pem: certificate.pem(),
    })
}

fn server_config(authority: &CertifiedIssuer<'static, KeyPair>) -> Result<Arc<ServerConfig>> {
    let mut params = CertificateParams::new(vec!["localhost".to_owned()])?;
    params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate()?;
    let certificate = params.signed_by(&key, authority)?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(authority.der().to_vec()))?;
    let verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
            .build()?;
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            vec![CertificateDer::from(certificate.der().to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )?;
    Ok(Arc::new(config))
}

/// Writes a Viceroy config whose stores hold one identity binding.
fn write_config(path: &Path, client: &ClientCertificate, hosts: Option<&str>) -> Result<()> {
    let mut contents = toml::Table::new();
    contents.insert(
        format!("{IDENTITY}.certificate"),
        toml::Value::String(client.pem.clone()),
    );
    if let Some(origins) = hosts {
        contents.insert(
            format!("{IDENTITY}.hosts"),
            toml::Value::String(origins.to_owned()),
        );
    }
    let mut config_store = toml::Table::new();
    config_store.insert("format".into(), "inline-toml".into());
    config_store.insert("contents".into(), toml::Value::Table(contents));
    let mut secret = toml::Table::new();
    secret.insert("key".into(), format!("{IDENTITY}.private-key").into());
    secret.insert("data".into(), client.key_pem.clone().into());
    let mut config_stores = toml::Table::new();
    config_stores.insert(CONFIG_STORE.into(), toml::Value::Table(config_store));
    let mut secret_stores = toml::Table::new();
    secret_stores.insert(
        SECRET_STORE.into(),
        toml::Value::Array(vec![toml::Value::Table(secret)]),
    );
    let mut local_server = toml::Table::new();
    local_server.insert("config_stores".into(), toml::Value::Table(config_stores));
    local_server.insert("secret_stores".into(), toml::Value::Table(secret_stores));
    let mut root = toml::Table::new();
    root.insert("manifest_version".into(), 3.into());
    root.insert("name".into(), "client-identity-fixture".into());
    root.insert("language".into(), "rust".into());
    root.insert("local_server".into(), toml::Value::Table(local_server));
    fs::write(path, toml::to_string(&root)?)?;
    Ok(())
}

/// One fixture call: `case` against `target`.
fn call(port: u16, case: &str, target: &str) -> Result<Value> {
    let reply = net::request(
        port,
        &format!("/identity/{case}"),
        &token(),
        Some(target),
        None,
    )?;
    guest(&reply)
}

fn reason(report: &Value) -> &str {
    report["reason"].as_str().unwrap_or("")
}

/// Checks one report against origin A before and after the call, and origin B.
type Check<'a> =
    Box<dyn Fn(&Value, &OriginRecords, &OriginRecords, &OriginRecords) -> Result<()> + 'a>;

struct Expectation<'a> {
    case: &'a str,
    target: String,
    check: Check<'a>,
}

/// What every launch shares: the runtime, run directory, trusted CA and origins.
struct Launcher<'a> {
    certificate_authority: &'a Path,
    exe: &'a Path,
    origin_a: &'a Origin,
    origin_b: &'a Origin,
    run: &'a Path,
}

/// Runs one Viceroy launch and its cases. `before` and `after` are origin A's
/// records around each call; origin B must never see a connection.
fn run_scenario(
    launcher: &Launcher<'_>,
    scenario: &str,
    (client, hosts): (&ClientCertificate, Option<&str>),
    expectations: Vec<Expectation<'_>>,
    records: &mut Records,
) -> Result<usize> {
    let Launcher {
        certificate_authority,
        exe,
        origin_a,
        origin_b,
        run,
    } = *launcher;
    let config = run.join(format!("{scenario}.fastly.toml"));
    write_config(&config, client, hosts)?;
    let log = run.join(format!("{scenario}.runtime.log"));
    let mut executed = 0;
    let outcome = (|| -> Result<()> {
        let port = net::port()?;
        let mut command = fastly_command(exe, &config, run, "client-identity", port)?;
        command.env("SSL_CERT_FILE", certificate_authority);
        let mut process = Process::start(&mut command, &log, port)?;
        for expectation in &expectations {
            let before = origin_a.settled(origin_a.snapshot().accepts);
            let report = call(port, expectation.case, &expectation.target)?;
            let after = origin_a.settled(before.accepts);
            let second = origin_b.snapshot();
            (expectation.check)(&report, &before, &after, &second).map_err(|error| {
                format!("{scenario}/{}: {error}; report {report}", expectation.case)
            })?;
            require(second.accepts == 0, "origin B must never be connected")?;
            records.push(json!({
                "event":"fastly-client-identity-case", "status":"pass",
                "scenario":scenario, "case":expectation.case, "report":report,
                "origin_accepts":after.accepts - before.accepts,
                "qualification":false,
            }))?;
            executed += 1;
        }
        process.finish()?;
        Ok(())
    })();
    settle(
        records,
        &log,
        json!({"adapter":"fastly","suite":"client-identity","scenario":scenario,"qualification":false}),
        json!({"phase":"client-identity"}),
        outcome,
    )?;
    Ok(executed)
}

fn unchanged(before: &OriginRecords, after: &OriginRecords) -> Result<()> {
    require(
        after.accepts == before.accepts,
        "origin A recorded a TCP accept for a request that must fail before I/O",
    )
}

pub(super) fn validate_events(events: &[Value]) -> Result<()> {
    require(
        !events.iter().any(|event| event["status"] == "fail"),
        "failed client identity evidence",
    )?;
    let suites = events
        .iter()
        .filter(|event| event["event"] == "fastly-client-identity")
        .collect::<Vec<_>>();
    require(
        suites.len() == 1 && suites[0]["status"] == "pass" && suites[0]["cases"] == EXPECTED_CASES,
        "missing or incomplete client identity suite",
    )?;
    let cases = events
        .iter()
        .filter(|event| {
            event["event"] == "fastly-client-identity-case" && event["status"] == "pass"
        })
        .count();
    require(
        cases == EXPECTED_CASES,
        "client identity case evidence is incomplete",
    )
}

pub(super) fn run(exe: &Path, parent: &Path, records: &mut Records) -> Result<()> {
    let run = parent.join("client-identity");
    fs::create_dir_all(&run)?;

    let output = Command::new(exe).arg("--version").output()?;
    require(output.status.success(), "Viceroy version command failed")?;
    let version = String::from_utf8(output.stdout)?.trim().to_owned();
    let pins = fs::read_to_string(repository_root().join(".tool-versions"))?;
    let pin = pins
        .lines()
        .find_map(|line| line.strip_prefix("viceroy "))
        .map(str::trim)
        .ok_or("missing Viceroy pin")?;
    require(
        version.split_whitespace().nth(1) == Some(pin),
        "client identity contract requires the pinned Viceroy runtime",
    )?;

    let mut build = cargo_build()?;
    if let Some(target) = std::env::var_os("CARGO_TARGET_DIR") {
        build.env("CARGO_TARGET_DIR", target);
    }
    let built = build
        .args([
            "build",
            "--locked",
            "--offline",
            "--release",
            "-p",
            "fixture-fastly",
            "--features",
            "client-identity",
            "--bin",
            BIN,
            "--target",
            "wasm32-wasip1",
            "--message-format=json-render-diagnostics",
        ])
        .stderr(Stdio::from(fs::File::create(run.join("build.log"))?))
        .output()?;
    fs::write(run.join("build.jsonl"), &built.stdout)?;
    require(
        built.status.success(),
        "Fastly client identity fixture build failed",
    )?;
    let wasm = stage(&built_executable(&built.stdout, BIN)?, &run)?;
    records.push(json!({
        "event":"build", "adapter":"fastly", "suite":"client-identity",
        "variant":"fastly-client-identity", "target":"wasm32-wasip1",
        "artifact":artifact(&wasm)?, "runtime":artifact(exe)?, "runtime_version":version,
    }))?;

    let authority = certificate_authority("edgezero client identity test CA")?;
    let unrelated = certificate_authority("unrelated test CA")?;
    let client = client_certificate(&authority, "edgezero fixture client")?;
    let replacement = client_certificate(&authority, "edgezero replacement client")?;
    let wrong = client_certificate(&unrelated, "edgezero wrong-CA client")?;
    let ca_path = run.join("ca.pem");
    fs::write(&ca_path, authority.pem())?;
    let config = server_config(&authority)?;
    let origin_b = Origin::start(Arc::clone(&config), None)?;
    let origin_a = Origin::start(config, Some(origin_b.port))?;
    let a = origin_a.port;
    let hosts = format!("https://localhost:{a}");
    let launcher = Launcher {
        certificate_authority: &ca_path,
        exe,
        origin_a: &origin_a,
        origin_b: &origin_b,
        run: &run,
    };

    let primary_fingerprint = client.fingerprint.clone();
    let mut executed = run_scenario(
        &launcher,
        "primary",
        (&client, Some(&hosts)),
        vec![
            Expectation {
                case: "with",
                target: format!("https://localhost:{a}/ok"),
                check: Box::new(move |report, before, after, _| {
                    require(
                        report["outcome"] == "response" && report["status"] == 200,
                        "identity request must succeed",
                    )?;
                    require(after.accepts == before.accepts + 1, "one connection")?;
                    require(
                        after.handshakes.last() == Some(&primary_fingerprint),
                        "origin must see the configured client certificate",
                    )
                }),
            },
            Expectation {
                case: "without",
                target: format!("https://localhost:{a}/ok"),
                check: Box::new(|report, before, after, _| {
                    require(
                        report["outcome"] == "error" && report["reason"].is_null(),
                        "request without an identity must fail as a transport error",
                    )?;
                    require(
                        after.accepts == before.accepts + 1,
                        "the handshake is attempted",
                    )?;
                    require(
                        after.handshakes.len() == before.handshakes.len(),
                        "no client certificate is accepted",
                    )?;
                    require(
                        after.failures.len() > before.failures.len(),
                        "origin records a failed handshake",
                    )
                }),
            },
            Expectation {
                case: "undeclared",
                target: format!("https://localhost:{a}/ok"),
                check: Box::new(|report, before, after, _| {
                    require(
                        report["status"] == 500 && reason(report) == "Undeclared",
                        "undeclared identity",
                    )?;
                    unchanged(before, after)
                }),
            },
            Expectation {
                case: "optimizer",
                target: format!("https://localhost:{a}/ok"),
                check: Box::new(|report, before, after, _| {
                    require(
                        reason(report).starts_with("Unsupported"),
                        "identity with Image Optimizer is unsupported",
                    )?;
                    unchanged(before, after)
                }),
            },
            Expectation {
                case: "with",
                target: format!("http://localhost:{a}/ok"),
                check: Box::new(|report, before, after, _| {
                    require(
                        report["status"] == 500 && reason(report) == "RequiresHttps",
                        "identity requires https",
                    )?;
                    unchanged(before, after)
                }),
            },
            Expectation {
                case: "follow",
                target: format!("https://localhost:{a}/redirect"),
                check: Box::new(|report, before, after, second| {
                    require(
                        report["status"] == 502 && report["hop"] == 1,
                        "redirect hop fails as a redirect failure",
                    )?;
                    require(
                        reason(report).contains("HostNotAllowed")
                            && reason(report).contains("redirect: true"),
                        "unlisted redirect target",
                    )?;
                    require(
                        after.accepts == before.accepts + 1,
                        "only the first hop is sent",
                    )?;
                    require(
                        second.accepts == 0,
                        "the unlisted origin is never connected",
                    )
                }),
            },
        ],
        records,
    )?;

    executed += run_scenario(
        &launcher,
        "wrong-ca",
        (&wrong, Some(&hosts)),
        vec![Expectation {
            case: "with",
            target: format!("https://localhost:{a}/ok"),
            check: Box::new(|report, before, after, _| {
                require(
                    report["outcome"] == "error" && report["reason"].is_null(),
                    "wrong-CA certificate must fail as a transport error",
                )?;
                require(
                    after.handshakes.len() == before.handshakes.len(),
                    "no client certificate is accepted",
                )?;
                require(
                    after.failures.len() > before.failures.len(),
                    "origin rejects the certificate",
                )
            }),
        }],
        records,
    )?;

    let replacement_fingerprint = replacement.fingerprint.clone();
    executed += run_scenario(
        &launcher,
        "replaced",
        (&replacement, Some(&hosts)),
        vec![Expectation {
            case: "with",
            target: format!("https://localhost:{a}/ok"),
            check: Box::new(move |report, _, after, _| {
                require(report["status"] == 200, "replaced certificate must succeed")?;
                require(
                    after.handshakes.last() == Some(&replacement_fingerprint),
                    "origin must see the replaced certificate",
                )
            }),
        }],
        records,
    )?;

    executed += run_scenario(
        &launcher,
        "missing-hosts",
        (&client, None),
        vec![Expectation {
            case: "with",
            target: format!("https://localhost:{a}/ok"),
            check: Box::new(|report, before, after, _| {
                require(
                    report["status"] == 503
                        && reason(report).contains("MaterialMissing")
                        && reason(report).contains("Hosts"),
                    "missing hosts entry",
                )?;
                unchanged(before, after)
            }),
        }],
        records,
    )?;

    require(
        executed == EXPECTED_CASES,
        "unexpected client identity case count",
    )?;
    records.push(json!({
        "event":"fastly-client-identity", "adapter":"fastly", "status":"pass",
        "cases":executed, "qualification":false,
    }))?;
    Ok(())
}
