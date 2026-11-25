//! Explicit generated native consumer proofs for the `EdgeZero` store service.
//! Ordinary tests require no service. The ignored real-service proof requires an
//! explicit official `CELLD_BIN`, the service directory in
//! `EDGEZERO_CELLD_STORE_SERVICE`, Node.js and OpenSSL; missing tools are
//! failures, not skips.

#[cfg(test)]
#[cfg(unix)]
mod tests {
    #![expect(
        clippy::arbitrary_source_item_ordering,
        reason = "keep fixture ownership, generated source and proof steps together"
    )]

    use std::collections::BTreeMap;
    use std::env;
    use std::fmt::Write as _;
    use std::fs;
    use std::io::{ErrorKind, Read as _, Write as _};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use serde_json::json;
    use sha2::{Digest as _, Sha256};
    use tempfile::TempDir;
    use url::Url;

    const APP_TOKEN: &str = "fixture-app-token-not-a-production-credential-1234567890";
    const INSTANCE: &str = "AAAAAAAAAAAAAAAAAAAAAA";
    const WAIT: Duration = Duration::from_secs(30);

    fn token_sha256(token: &str) -> String {
        let mut encoded = String::new();
        for byte in Sha256::digest(token) {
            write!(&mut encoded, "{byte:02x}").expect("format into String");
        }
        encoded
    }

    fn deadline(duration: Duration) -> Instant {
        Instant::now()
            .checked_add(duration)
            .expect("representable proof deadline")
    }

    type Rows = BTreeMap<(String, String), Vec<u8>>;

    struct Fixture {
        endpoint: String,
        requests: Arc<AtomicUsize>,
        mutation_ids: Arc<Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
        worker: Option<JoinHandle<()>>,
    }

    impl Fixture {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("scoped fixture");
            listener.set_nonblocking(true).expect("nonblocking fixture");
            let endpoint = format!("http://{}", listener.local_addr().expect("fixture address"));
            let rows = Arc::new(Mutex::new(Rows::new()));
            let requests = Arc::new(AtomicUsize::new(0));
            let mutation_ids = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let (worker_rows, worker_requests, worker_ids, worker_stop) = (
                Arc::clone(&rows),
                Arc::clone(&requests),
                Arc::clone(&mutation_ids),
                Arc::clone(&stop),
            );
            let worker = thread::spawn(move || {
                let end = deadline(Duration::from_mins(10));
                while !worker_stop.load(Ordering::Relaxed) && Instant::now() < end {
                    match listener.accept() {
                        Ok((mut stream, peer)) => {
                            assert!(peer.ip().is_loopback());
                            stream
                                .set_read_timeout(Some(Duration::from_secs(3)))
                                .expect("fixture timeout");
                            stream
                                .set_write_timeout(Some(Duration::from_secs(3)))
                                .expect("fixture timeout");
                            worker_requests.fetch_add(1, Ordering::Relaxed);
                            let (method, target, body, mutation_id) = read_request(&mut stream);
                            if let Some(id) = mutation_id {
                                worker_ids.lock().expect("mutation IDs").push(id);
                            }
                            let (status, payload, kind) =
                                fixture_response(&method, &target, &body, &worker_rows);
                            write!(stream,"HTTP/1.1 {status}\r\nx-edgezero-store-version: 1\r\nx-edgezero-store-instance: {INSTANCE}\r\ncontent-type: {kind}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",payload.len()).expect("fixture headers");
                            stream.write_all(&payload).expect("fixture body");
                        }
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("scoped fixture accept: {error}"),
                    }
                }
            });
            Self {
                endpoint,
                requests,
                mutation_ids,
                stop,
                worker: Some(worker),
            }
        }
    }
    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(worker) = self.worker.take() {
                worker.join().expect("reap scoped fixture");
            }
        }
    }

    fn read_request(stream: &mut TcpStream) -> (String, String, Vec<u8>, Option<String>) {
        let mut bytes = Vec::new();
        loop {
            let mut byte = [0];
            stream.read_exact(&mut byte).expect("fixture request head");
            bytes.push(byte[0]);
            assert!(bytes.len() <= 16 * 1024, "bounded fixture head");
            if bytes.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let head = String::from_utf8(bytes).expect("ASCII fixture head");
        let mut line = head
            .lines()
            .next()
            .expect("request line")
            .split_ascii_whitespace();
        let method = line.next().expect("method").to_owned();
        let target = line.next().expect("target").to_owned();
        let header = |name: &str| {
            head.lines()
                .filter_map(|header_line| header_line.split_once(':'))
                .find_map(|(key, value)| {
                    key.eq_ignore_ascii_case(name)
                        .then(|| value.trim().to_owned())
                })
        };
        let length = header("content-length").map_or(0, |value| value.parse().expect("length"));
        assert!(length <= 1024 * 1024, "bounded fixture value");
        let mut body = vec![0; length];
        stream.read_exact(&mut body).expect("fixture request body");
        (method, target, body, header("x-edgezero-mutation-id"))
    }

    fn fixture_response(
        method: &str,
        target: &str,
        body: &[u8],
        contents: &Mutex<Rows>,
    ) -> (&'static str, Vec<u8>, &'static str) {
        let url = Url::parse(&format!("http://fixture{target}")).expect("fixture URL");
        let parts = url.path().split('/').collect::<Vec<_>>();
        let parameters = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
        let store = parts.get(3).expect("store").to_string();
        let value_key = parameters.get("key").cloned().unwrap_or_default();
        let mut rows = contents.lock().expect("fixture rows");
        if parts.last() == Some(&"keys") {
            let prefix = parameters.get("prefix").map_or("", String::as_str);
            let after = parameters
                .get("cursor")
                .and_then(|continuation| continuation.strip_prefix("after:"))
                .unwrap_or("");
            let limit = parameters
                .get("limit")
                .map_or(100, |value| value.parse::<usize>().expect("page limit"));
            let available_keys = rows
                .keys()
                .filter(|(alias, key)| {
                    alias == &store && key.starts_with(prefix) && key.as_str() > after
                })
                .map(|(_, key)| key.clone())
                .collect::<Vec<_>>();
            let more = available_keys.len() > limit;
            let keys = available_keys.into_iter().take(limit).collect::<Vec<_>>();
            let cursor =
                more.then(|| format!("after:{}", keys.last().expect("nonempty continuing page")));
            return (
                "200 OK",
                serde_json::to_vec(&json!({"keys":keys,"cursor":cursor})).expect("page"),
                "application/json",
            );
        }
        let pair = (store, value_key);
        match (method, parts.last().copied()) {
            ("GET" | "HEAD", Some("value")) => match rows.get(&pair) {
                Some(value) => (
                    "200 OK",
                    if method == "HEAD" {
                        Vec::new()
                    } else {
                        value.clone()
                    },
                    "application/octet-stream",
                ),
                None => (
                    "404 Not Found",
                    if method == "HEAD" {
                        Vec::new()
                    } else {
                        serde_json::to_vec(&json!({"error":"not_found"})).expect("miss")
                    },
                    "application/json",
                ),
            },
            ("POST", Some("put")) => {
                rows.insert(pair, body.to_vec());
                ("204 No Content", Vec::new(), "application/octet-stream")
            }
            ("POST", Some("delete")) => {
                rows.remove(&pair);
                ("204 No Content", Vec::new(), "application/octet-stream")
            }
            _ => panic!("unexpected store fixture request"),
        }
    }

    struct App {
        child: Child,
        address: SocketAddr,
        log: PathBuf,
    }
    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for App {
        fn drop(&mut self) {
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }
    impl App {
        fn start(
            binary: &Path,
            project: &Path,
            bindings: &Path,
            mode: &str,
            tls: Option<(&str, &Path)>,
        ) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("app address");
            let address = listener.local_addr().expect("app address");
            drop(listener);
            let log = project.join(format!("app-{}-{mode}.log", address.port()));
            let mut command = Command::new(binary);
            command
                .current_dir(project)
                .env_clear()
                .env("HOME", project.join("empty-home"))
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .env("EDGEZERO__STORE_BINDINGS_FILE", bindings)
                .env("EDGEZERO__ADAPTER__HOST", "127.0.0.1")
                .env("EDGEZERO__ADAPTER__PORT", address.port().to_string())
                .env("CELLD_PROOF_MODE", mode)
                .stdout(Stdio::null())
                .stderr(Stdio::from(fs::File::create(&log).expect("app log")));
            if let Some((token, ca)) = tls {
                command
                    .env("CELLD_APP_TOKEN", token)
                    .env("SSL_CERT_FILE", ca);
            }
            Self {
                child: command.spawn().expect("generated app"),
                address,
                log,
            }
        }
        fn request(&mut self) -> String {
            let end = deadline(WAIT);
            loop {
                if let Some(status) = self.child.try_wait().expect("app state") {
                    panic!(
                        "generated app exited {status}: {}",
                        fs::read_to_string(&self.log).unwrap_or_default()
                    );
                }
                if let Ok(mut stream) =
                    TcpStream::connect_timeout(&self.address, Duration::from_millis(50))
                {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .expect("app response timeout");
                    write!(
                        stream,
                        "GET /celld-proof HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
                    )
                    .expect("app request");
                    let mut response = String::new();
                    stream.read_to_string(&mut response).expect("app response");
                    return response;
                }
                assert!(Instant::now() < end, "app startup deadline");
                thread::sleep(Duration::from_millis(10));
            }
        }
        fn rejected(&mut self) {
            let end = deadline(WAIT);
            loop {
                if let Some(status) = self.child.try_wait().expect("rejected app state") {
                    assert!(!status.success(), "selected unavailable provider must fail");
                    break;
                }
                assert!(
                    Instant::now() < end,
                    "invalid source unexpectedly stayed alive"
                );
                thread::sleep(Duration::from_millis(10));
            }
            TcpStream::connect_timeout(&self.address, Duration::from_millis(50))
                .expect_err("rejected startup has no listener");
        }
    }

    fn generate(root: &Path) -> PathBuf {
        let output = Command::new(env!("CARGO_BIN_EXE_edgezero"))
            .args(["new", "celld-proof", "--dir"])
            .arg(root)
            .output()
            .expect("real CLI scaffold");
        assert!(
            output.status.success(),
            "scaffold: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let project = root.join("celld-proof");
        let manifest = project.join("edgezero.toml");
        let mut text = fs::read_to_string(&manifest).expect("generated manifest");
        text.push_str(
            r#"
[[triggers.http]]
id = "celld-proof"
path = "/celld-proof"
methods = ["GET"]
handler = "celld_proof_core::handlers::celld_probe"
adapters = ["axum"]
[stores.kv]
ids = ["state"]
"#,
        );
        fs::write(manifest, text).expect("probe declarations");
        fs::write(
            project.join("crates/celld-proof-core/src/config.rs"),
            "
pub struct StartupState {
    pub mutated: std::sync::atomic::AtomicBool,
}
",
        )
        .expect("startup state");
        let handlers = project.join("crates/celld-proof-core/src/handlers.rs");
        let mut source = fs::read_to_string(&handlers).expect("generated handlers");
        source.push_str(r#"
#[action]
pub async fn celld_probe(
    edgezero_core::extractor::State(state): edgezero_core::extractor::State<std::sync::Arc<crate::config::StartupState>>,
    kv: edgezero_core::extractor::Kv,
) -> Result<Text<String>, EdgeError> {
    let store = kv.default().ok_or_else(|| EdgeError::service_unavailable("KV missing"))?;
    if !state.mutated.swap(true, std::sync::atomic::Ordering::SeqCst) {
        match std::env::var("CELLD_PROOF_MODE").unwrap_or_default().as_str() {
            "put" => {
                store.put_bytes("probe/binary", vec![0,255,1,128].into()).await?;
                store.put_bytes("probe/empty", Vec::new().into()).await?;
                store.put_bytes_with_ttl("probe/ttl", vec![7].into(),std::time::Duration::from_secs(60)).await?;
            }
            "delete" => { store.delete("probe/binary").await?; }
            _ => {}
        }
    }
    let binary = store.get_bytes("probe/binary").await?.map(|v|v.to_vec());
    let empty = store.get_bytes("probe/empty").await?.is_some_and(|v|v.is_empty());
    let exists = store.exists("probe/binary").await?;
    let ttl = store.get_bytes("probe/ttl").await?.is_some();
    let page = store.list_keys_page("probe/",None,1).await?;
    let next = store.list_keys_page("probe/",page.cursor.as_deref(),1).await?;
    Ok(Text::new(format!("binary={binary:?};empty={empty};exists={exists};ttl={ttl};first={};next={}",page.keys.join(","),next.keys.join(","))))
}
"#);
        fs::write(handlers, source).expect("portable proof handler");
        fs::write(
            project.join("crates/celld-proof-adapter-axum/src/main.rs"),
            "
use std::sync::{Arc,atomic::AtomicBool};
use edgezero_adapter_axum::dev_server::run_production_app_with_initializer;
use celld_proof_core::config::StartupState;
fn main() -> anyhow::Result<()> {
    run_production_app_with_initializer::<celld_proof_core::App,_>(|app,_stores| {
        Box::pin(async move {
            app.insert_state(Arc::new(StartupState{mutated:AtomicBool::new(false)}));
            Ok(())
        })
    })?;
    Ok(())
}
",
        )
        .expect("production initializer");
        project
    }

    fn bindings(project: &Path, endpoint: &str, authenticated: bool) -> PathBuf {
        let path = project.join("celld-bindings.toml");
        let access = if authenticated {
            "mode = \"authenticated\"\ntoken_env = \"CELLD_APP_TOKEN\""
        } else {
            "mode = \"trusted-network\""
        };
        fs::write(
            &path,
            format!(
                r#"version = 1
[kv.state]
provider = "celld-kv"
endpoint = "{endpoint}"
store = "state"
[kv.state.access]
{access}
"#
            ),
        )
        .expect("native celld bindings");
        path
    }

    fn build(project: &Path, features: Option<&str>) -> PathBuf {
        let target = env::var_os("EDGEZERO_GENERATED_TEST_TARGET_DIR")
            .map_or_else(|| project.join("target"), PathBuf::from);
        let mut command = Command::new(env!("CARGO_BIN_EXE_edgezero"));
        command
            .current_dir(project)
            .env("CARGO_TARGET_DIR", &target)
            .args(["build", "--adapter", "axum", "--", "--offline"]);
        if let Some(selected_features) = features {
            command.args(["--features", selected_features]);
        }
        let output = command.output().expect("real CLI build");
        assert!(
            output.status.success(),
            "generated build: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        target.join("debug/celld-proof-adapter-axum")
    }

    #[test]
    #[ignore = "builds actual generated native consumers; coordinator owns Cargo integration"]
    fn generated_celld_default_off_kv_restart() {
        let temp = TempDir::new().expect("project");
        let project = generate(temp.path());
        let fixture = Fixture::start();
        let binding = bindings(&project, &fixture.endpoint, false);
        let default_off = build(&project, None);
        let mut app = App::start(&default_off, &project, &binding, "read", None);
        app.rejected();
        assert_eq!(
            fixture.requests.load(Ordering::Relaxed),
            0,
            "source validation precedes any store I/O"
        );
        let binary = build(&project, Some("celld-kv"));
        let mut first = App::start(&binary, &project, &binding, "put", None);
        let response = first.request();
        assert!(
            response.contains("200 OK")
                && response.contains("binary=Some([0, 255, 1, 128])")
                && response.contains("empty=true")
                && response.contains("exists=true")
                && response.contains("first=probe/binary")
                && response.contains("next=probe/empty"),
            "generated live KV: {response}"
        );
        drop(first);
        let ids = fixture.mutation_ids.lock().expect("mutation IDs").clone();
        assert_eq!(ids.len(), 3, "one ID per mutation, never resent");
        let mut unique = ids.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), ids.len());
        let mut second = App::start(&binary, &project, &binding, "read", None);
        let restarted_response = second.request();
        assert!(
            restarted_response.contains("binary=Some([0, 255, 1, 128])"),
            "restart keeps remote values: {restarted_response}"
        );
        drop(second);
        let all_binary = build(&project, Some("celld-kv,aws-stores,aws-dynamodb"));
        let mut coexistence = App::start(&all_binary, &project, &binding, "read", None);
        let coexistence_response = coexistence.request();
        assert!(
            coexistence_response.contains("empty=true"),
            "compiled AWS providers do not require AWS inputs for celld-only bindings: {coexistence_response}"
        );
        drop(coexistence);
        let mut deleting = App::start(&binary, &project, &binding, "delete", None);
        assert!(deleting.request().contains("binary=None"));
        drop(deleting);
        assert!(
            !project.join(".edgezero").exists(),
            "remote-bound IDs create no local roots"
        );
    }

    /// The official celld binary running the `EdgeZero` store service, behind a
    /// local TLS terminator because celld does not terminate TLS itself.
    struct StoreService {
        celld: Child,
        terminator: Child,
        endpoint: String,
        cert: PathBuf,
    }
    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for StoreService {
        fn drop(&mut self) {
            for child in [&mut self.terminator, &mut self.celld] {
                // celld dev runs one process group; stop it like a supervisor would.
                let _signalled = Command::new("kill")
                    .args(["-TERM", &format!("-{}", child.id())])
                    .status();
                let end = deadline(Duration::from_secs(15));
                while child.try_wait().ok().flatten().is_none() && Instant::now() < end {
                    thread::sleep(Duration::from_millis(20));
                }
                let _killed = child.kill();
                let _reaped = child.wait();
            }
        }
    }
    impl StoreService {
        fn prepare(root: &Path) -> PathBuf {
            let source = env::var_os("EDGEZERO_CELLD_STORE_SERVICE")
                .map(PathBuf::from)
                .expect(
                    "EDGEZERO_CELLD_STORE_SERVICE is required; this proof never silently skips",
                );
            assert!(source.is_absolute(), "explicit service directory required");
            let service = root.join("store-service");
            fs::create_dir_all(service.join("src")).expect("service copy");
            fs::copy(source.join("src/index.js"), service.join("src/index.js"))
                .expect("exact service module");
            let policy = json!({
                "version": 1_u8,
                "stores": [{"alias": "state", "namespace": "proof-state"}],
                "access": {"mode": "authenticated", "tokens": [{
                    "id": "app",
                    "sha256": token_sha256(APP_TOKEN),
                    "grants": [{"store": "state", "operations": ["read", "write", "delete", "list"]}]
                }]}
            });
            let wrangler = json!({
                "name": "edgezero-store",
                "main": "src/index.js",
                "no_bundle": true,
                "compatibility_date": "2026-10-01",
                "durable_objects": {"bindings": [{"name": "EDGEZERO_STORE", "class_name": "EdgeZeroStore"}]},
                "migrations": [{"tag": "v1", "new_sqlite_classes": ["EdgeZeroStore"]}],
                "vars": {"EDGEZERO_STORE_POLICY": policy.to_string()}
            });
            fs::write(
                service.join("wrangler.json"),
                serde_json::to_vec_pretty(&wrangler).expect("service configuration"),
            )
            .expect("write service configuration");
            let cert = root.join("terminator-cert.pem");
            let key = root.join("terminator-key.pem");
            let certificate_status = Command::new("openssl")
                .args([
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=celld-proof",
                    "-addext",
                    "subjectAltName=IP:127.0.0.1",
                    "-addext",
                    "basicConstraints=critical,CA:FALSE",
                    "-addext",
                    "extendedKeyUsage=serverAuth",
                    "-keyout",
                ])
                .arg(&key)
                .arg("-out")
                .arg(&cert)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("explicit OpenSSL prerequisite");
            assert!(certificate_status.success());
            service
        }

        fn wait_for_line(log: &Path, child: &mut Child, marker: &str) -> String {
            let end = deadline(Duration::from_mins(2));
            loop {
                let text = fs::read_to_string(log).unwrap_or_default();
                if let Some(rest) = text
                    .lines()
                    .find_map(|line| line.split_once(marker).map(|(_, rest)| rest.trim()))
                {
                    return rest
                        .split_whitespace()
                        .next()
                        .expect("listener address")
                        .to_owned();
                }
                if let Some(status) = child.try_wait().expect("child state") {
                    panic!("{marker} process exited {status}: {text}");
                }
                assert!(Instant::now() < end, "{marker} deadline");
                thread::sleep(Duration::from_millis(20));
            }
        }

        fn start(root: &Path, service: &Path) -> Self {
            use std::os::unix::process::CommandExt as _;
            let binary = env::var_os("CELLD_BIN")
                .map(PathBuf::from)
                .expect("CELLD_BIN is required; this proof never silently skips");
            assert!(
                binary.is_absolute() && binary.is_file(),
                "explicit official celld executable required"
            );
            let reserved = TcpListener::bind("127.0.0.1:0").expect("celld port");
            let port = reserved.local_addr().expect("celld port").port();
            drop(reserved);
            let log = root.join(format!("celld-{port}.log"));
            let output = fs::File::create(&log).expect("celld log");
            let mut celld = Command::new(binary)
                .current_dir(service)
                .env_clear()
                .env("PATH", env::var_os("PATH").unwrap_or_default())
                .env("HOME", root.join("celld-home"))
                .env("NO_COLOR", "1")
                .args([
                    "dev",
                    ".",
                    "--no-watch",
                    "--logs",
                    "--port",
                    &port.to_string(),
                ])
                .process_group(0)
                .stdout(Stdio::from(output.try_clone().expect("log clone")))
                .stderr(Stdio::from(output))
                .spawn()
                .expect("official celld process");
            let address = Self::wait_for_line(&log, &mut celld, "celld listening on ");
            assert_eq!(address, format!("127.0.0.1:{port}"));
            let script = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../scripts/celld_store_tls_terminator.mjs");
            let terminator_log = root.join(format!("terminator-{port}.log"));
            let terminator_output = fs::File::create(&terminator_log).expect("terminator log");
            let cert = root.join("terminator-cert.pem");
            let mut terminator = Command::new("node")
                .arg(script)
                .arg("--cert")
                .arg(&cert)
                .arg("--key")
                .arg(root.join("terminator-key.pem"))
                .args(["--target", &address])
                .env_clear()
                .env("PATH", env::var_os("PATH").unwrap_or_default())
                .process_group(0)
                .stdout(Stdio::from(
                    terminator_output.try_clone().expect("log clone"),
                ))
                .stderr(Stdio::from(terminator_output))
                .spawn()
                .expect("explicit Node.js prerequisite");
            let tls = Self::wait_for_line(&terminator_log, &mut terminator, "listening on ");
            Self {
                celld,
                terminator,
                endpoint: format!("https://{tls}"),
                cert,
            }
        }
    }

    #[test]
    #[ignore = "requires explicit official CELLD_BIN, EDGEZERO_CELLD_STORE_SERVICE, Node.js and OpenSSL; runs actual generated consumer over verified TLS"]
    fn generated_native_consumer_uses_stock_celld_store_service() {
        let temp = TempDir::new().expect("real-service fixture");
        let service_dir = StoreService::prepare(temp.path());
        let service = StoreService::start(temp.path(), &service_dir);
        let project = generate(temp.path());
        let binding = bindings(&project, &service.endpoint, true);
        let binary = build(&project, Some("celld-kv"));
        let mut first = App::start(
            &binary,
            &project,
            &binding,
            "put",
            Some((APP_TOKEN, &service.cert)),
        );
        let response = first.request();
        assert!(
            response.contains("200 OK")
                && response.contains("binary=Some([0, 255, 1, 128])")
                && response.contains("empty=true")
                && response.contains("exists=true")
                && response.contains("ttl=true")
                && response.contains("first=probe/binary")
                && response.contains("next=probe/empty"),
            "real service path: {response}"
        );
        thread::sleep(Duration::from_secs(61));
        let expired = first.request();
        assert!(
            expired.contains("ttl=false") && expired.contains("binary=Some([0, 255, 1, 128])"),
            "live elapsed service expiry: {expired}"
        );
        drop(first);
        drop(service);
        let restarted_service = StoreService::start(temp.path(), &service_dir);
        let restarted_binding = bindings(&project, &restarted_service.endpoint, true);
        let mut second = App::start(
            &binary,
            &project,
            &restarted_binding,
            "read",
            Some((APP_TOKEN, &restarted_service.cert)),
        );
        let restarted_response = second.request();
        assert!(
            restarted_response.contains("binary=Some([0, 255, 1, 128])")
                && restarted_response.contains("ttl=false"),
            "real service restart keeps acknowledged writes: {restarted_response}"
        );
        drop(second);
        let wrong = "wrong-fixture-token-longer-than-32-bytes";
        let mut rejected = App::start(
            &binary,
            &project,
            &restarted_binding,
            "read",
            Some((wrong, &restarted_service.cert)),
        );
        let denied = rejected.request();
        assert!(
            denied.starts_with("HTTP/1.1 5"),
            "an unknown token gets no data: {denied}"
        );
        drop(rejected);
        let diagnostic = fs::read_dir(&project)
            .expect("project logs")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("app-"))
            .map(|entry| fs::read_to_string(entry.path()).unwrap_or_default())
            .collect::<String>();
        assert!(
            !diagnostic.contains(APP_TOKEN)
                && !diagnostic.contains(wrong)
                && !diagnostic.contains(&restarted_service.endpoint)
        );
        assert!(!project.join(".edgezero").exists());
    }
}
