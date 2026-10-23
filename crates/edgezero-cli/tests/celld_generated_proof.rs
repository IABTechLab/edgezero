//! Explicit generated native consumer proofs for the scoped API embedded in celld.
//! Ordinary tests require no service. The ignored real-service proof requires an
//! explicit `CELLD_BIN`, curl and OpenSSL; missing tools are failures, not skips.

#[cfg(test)]
#[cfg(unix)]
mod tests {
    #![expect(
        clippy::arbitrary_source_item_ordering,
        reason = "keep fixture ownership, generated source and proof steps together"
    )]

    use std::collections::BTreeMap;
    use std::env;
    use std::fs;
    use std::io::{ErrorKind, Read as _, Write as _};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use base16ct::lower::encode_string;
    use edgezero_core::blob_envelope::BlobEnvelope;
    use serde_json::json;
    use sha2::{Digest as _, Sha256};
    use tempfile::TempDir;
    use url::Url;

    const APP_TOKEN: &str = "fixture-app-token-not-a-production-credential-1234567890";
    const OPERATOR_TOKEN: &str = "fixture-operator-token-not-a-production-credential-123456";
    const WAIT: Duration = Duration::from_secs(30);

    fn deadline(duration: Duration) -> Instant {
        Instant::now()
            .checked_add(duration)
            .expect("representable proof deadline")
    }

    fn envelope(greeting: &str) -> Vec<u8> {
        serde_json::to_vec(&BlobEnvelope::new(
            json!({"greeting":greeting}),
            "2026-10-15T12:00:00Z".to_owned(),
        ))
        .expect("whole configuration envelope")
    }

    type Rows = BTreeMap<(String, String), Vec<u8>>;

    struct Fixture {
        endpoint: String,
        rows: Arc<Mutex<Rows>>,
        requests: Arc<AtomicUsize>,
        config_reads: Arc<AtomicUsize>,
        stop: Arc<AtomicBool>,
        worker: Option<JoinHandle<()>>,
    }

    impl Fixture {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("scoped fixture");
            listener.set_nonblocking(true).expect("nonblocking fixture");
            let endpoint = format!("http://{}", listener.local_addr().expect("fixture address"));
            let rows = Arc::new(Mutex::new(BTreeMap::from([(
                ("app-config".to_owned(), "settings".to_owned()),
                envelope("first-config"),
            )])));
            let requests = Arc::new(AtomicUsize::new(0));
            let config_reads = Arc::new(AtomicUsize::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let (worker_rows, worker_requests, worker_config, worker_stop) = (
                Arc::clone(&rows),
                Arc::clone(&requests),
                Arc::clone(&config_reads),
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
                            let (method, target, body) = read_request(&mut stream);
                            let (status, payload, kind) = fixture_response(
                                &method,
                                &target,
                                &body,
                                &worker_rows,
                                &worker_config,
                            );
                            write!(stream,"HTTP/1.1 {status}\r\nx-celld-store-version: 1\r\ncontent-type: {kind}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",payload.len()).expect("fixture headers");
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
                rows,
                requests,
                config_reads,
                stop,
                worker: Some(worker),
            }
        }
    }
    #[expect(
        clippy::missing_trait_methods,
        reason = "Drop::pin_drop is unstable and conflicts with drop"
    )]
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(worker) = self.worker.take() {
                worker.join().expect("reap scoped fixture");
            }
        }
    }

    fn read_request(stream: &mut TcpStream) -> (String, String, Vec<u8>) {
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
        let length = head
            .lines()
            .filter_map(|header| header.split_once(':'))
            .find_map(|(key, value)| {
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().expect("length"))
            })
            .unwrap_or(0);
        assert!(length <= 1024 * 1024, "bounded fixture value");
        let mut body = vec![0; length];
        stream.read_exact(&mut body).expect("fixture request body");
        (method, target, body)
    }

    fn fixture_response(
        method: &str,
        target: &str,
        body: &[u8],
        contents: &Mutex<Rows>,
        reads: &AtomicUsize,
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
        let pair = (store.clone(), value_key);
        match method {
            "GET" | "HEAD" => {
                if store == "app-config" {
                    reads.fetch_add(1, Ordering::Relaxed);
                }
                match rows.get(&pair) {
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
                }
            }
            "PUT" => {
                rows.insert(pair, body.to_vec());
                ("204 No Content", Vec::new(), "application/octet-stream")
            }
            "DELETE" => {
                rows.remove(&pair);
                ("204 No Content", Vec::new(), "application/octet-stream")
            }
            _ => panic!("unexpected scoped fixture method"),
        }
    }

    struct App {
        child: Child,
        address: SocketAddr,
        log: PathBuf,
    }
    #[expect(
        clippy::missing_trait_methods,
        reason = "Drop::pin_drop is unstable and conflicts with drop"
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
[stores.config]
ids = ["app_config"]
[stores.kv]
ids = ["state"]
"#,
        );
        fs::write(manifest, text).expect("probe declarations");
        fs::write(
            project.join("crates/celld-proof-core/src/config.rs"),
            "
use serde::Deserialize;
use validator::Validate;
#[derive(Debug, Deserialize, Validate, edgezero_core::AppConfig)]
#[serde(deny_unknown_fields)]
pub struct CelldProofConfig {
    #[validate(length(min = 3_u64))]
    pub greeting: String,
}
pub struct StartupState {
    pub greeting: String,
    pub mutated: std::sync::atomic::AtomicBool,
}
",
        )
        .expect("typed config");
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
    Ok(Text::new(format!("greeting={};binary={binary:?};empty={empty};exists={exists};ttl={ttl};first={};next={}",state.greeting,page.keys.join(","),next.keys.join(","))))
}
"#);
        fs::write(handlers, source).expect("portable proof handler");
        fs::write(project.join("crates/celld-proof-adapter-axum/src/main.rs"),r#"
use std::sync::{Arc,atomic::AtomicBool};
use edgezero_adapter_axum::dev_server::run_production_app_with_initializer;
use edgezero_core::{EdgeError,extractor::AppConfig};
use celld_proof_core::config::{CelldProofConfig,StartupState};
fn main() -> anyhow::Result<()> {
    run_production_app_with_initializer::<celld_proof_core::App,_>(|app,stores| {
        Box::pin(async move {
            let binding=stores.config().and_then(|r|r.named_ref("app_config"))
                .ok_or_else(|| EdgeError::service_unavailable("config binding missing"))?;
            let config=AppConfig::<CelldProofConfig>::from_binding(binding,None,stores.secrets(),
                app.config_extraction_limits(),app.monotonic_clock()).await?;
            app.insert_state(Arc::new(StartupState{greeting:config.greeting.clone(),mutated:AtomicBool::new(false)}));
            Ok(())
        })
    })?;
    Ok(())
}
"#).expect("production initializer");
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
[config.app_config]
provider = "celld-config"
endpoint = "{endpoint}"
default_key = "settings"
[config.app_config.access]
{access}
[config.app_config.documents.settings]
store = "app-config"
key = "settings"
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
    fn generated_celld_default_off_kv_and_config_restart() {
        let temp = TempDir::new().expect("project");
        let project = generate(temp.path());
        let fixture = Fixture::start();
        let binding = bindings(&project, &fixture.endpoint, false);
        for features in [None, Some("celld-config"), Some("celld-kv")] {
            let binary = build(&project, features);
            let mut app = App::start(&binary, &project, &binding, "read", None);
            app.rejected();
            assert_eq!(
                fixture.requests.load(Ordering::Relaxed),
                0,
                "all source validation must precede config I/O"
            );
        }
        let binary = build(&project, Some("celld-config,celld-kv"));
        let mut first = App::start(&binary, &project, &binding, "put", None);
        let response = first.request();
        assert!(
            response.contains("200 OK")
                && response.contains("greeting=first-config")
                && response.contains("binary=Some([0, 255, 1, 128])")
                && response.contains("empty=true")
                && response.contains("exists=true")
                && response.contains("first=probe/binary")
                && response.contains("next=probe/empty"),
            "generated live KV/config: {response}"
        );
        assert_eq!(fixture.config_reads.load(Ordering::Relaxed), 1);
        fixture.rows.lock().expect("fixture update").insert(
            ("app-config".to_owned(), "settings".to_owned()),
            envelope("second-config"),
        );
        assert!(first.request().contains("greeting=first-config"));
        assert_eq!(
            fixture.config_reads.load(Ordering::Relaxed),
            1,
            "requests use immutable startup snapshot"
        );
        drop(first);
        let mut second = App::start(&binary, &project, &binding, "read", None);
        let restarted_response = second.request();
        assert!(
            restarted_response.contains("greeting=second-config")
                && restarted_response.contains("binary=Some([0, 255, 1, 128])"),
            "restart adoption and persistence: {restarted_response}"
        );
        drop(second);
        let all_binary = build(
            &project,
            Some("celld-config,celld-kv,aws-stores,aws-dynamodb"),
        );
        let mut coexistence = App::start(&all_binary, &project, &binding, "read", None);
        let coexistence_response = coexistence.request();
        assert!(
            coexistence_response.contains("greeting=second-config")
                && coexistence_response.contains("empty=true"),
            "compiled AWS providers do not require AWS inputs for celld-only bindings: {coexistence_response}"
        );
        drop(coexistence);
        let mut deleting = App::start(&binary, &project, &binding, "delete", None);
        assert!(deleting.request().contains("binary=None"));
        drop(deleting);
        for value in [vec![255], Vec::new(), envelope("x")] {
            fixture
                .rows
                .lock()
                .expect("malformed config")
                .insert(("app-config".to_owned(), "settings".to_owned()), value);
            let mut failed = App::start(&binary, &project, &binding, "read", None);
            failed.rejected();
        }
        fixture
            .rows
            .lock()
            .expect("missing config")
            .remove(&("app-config".to_owned(), "settings".to_owned()));
        let mut missing = App::start(&binary, &project, &binding, "read", None);
        missing.rejected();
        assert!(
            !project.join(".edgezero").exists(),
            "remote-bound IDs create no local roots"
        );
    }

    struct Celld {
        child: Child,
        endpoint: String,
        cert: PathBuf,
    }
    #[expect(
        clippy::missing_trait_methods,
        reason = "Drop::pin_drop is unstable and conflicts with drop"
    )]
    impl Drop for Celld {
        fn drop(&mut self) {
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }
    impl Celld {
        fn start(root: &Path) -> Self {
            let binary = env::var_os("CELLD_BIN")
                .map(PathBuf::from)
                .expect("CELLD_BIN is required; this proof never silently skips");
            assert!(
                binary.is_absolute() && binary.is_file(),
                "explicit celld executable required"
            );
            let cert = root.join("celld-cert.pem");
            let key = root.join("celld-key.pem");
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
            let policy = root.join("celld-policy.json");
            fs::write(&policy,serde_json::to_vec(&json!({
                "version":1_u8,"listen":"127.0.0.1:0","tls":{"cert":cert,"key":key},
                "stores":[{"alias":"app-config","namespace":"proof-config"},{"alias":"state","namespace":"proof-state"}],
                "access":{"mode":"authenticated","tokens":[
                    {"id":"app","sha256":encode_string(&Sha256::digest(APP_TOKEN)),"grants":[{"store":"app-config","operations":["read"]},{"store":"state","operations":["read","write","delete","list"]}]},
                    {"id":"operator","sha256":encode_string(&Sha256::digest(OPERATOR_TOKEN)),"grants":[{"store":"app-config","operations":["read","write"]}]}
                ]}
            })).expect("operator policy")).expect("write operator policy");
            let log = root.join("celld.log");
            let output = fs::File::create(&log).expect("celld log");
            let child = Command::new(binary)
                .current_dir(root)
                .env_clear()
                .env("CELLD_TEST_DATA_DIR", root.join("celld-data"))
                .args([
                    "--listen",
                    "127.0.0.1:0",
                    "--internal-listen",
                    "127.0.0.1:0",
                    "--store-api-config",
                ])
                .arg(&policy)
                .stdout(Stdio::from(output.try_clone().expect("log clone")))
                .stderr(Stdio::from(output))
                .spawn()
                .expect("real celld process");
            let mut service = Self {
                child,
                endpoint: String::new(),
                cert,
            };
            let end = deadline(WAIT);
            loop {
                let text = fs::read_to_string(&log).expect("celld log");
                if let Some(address) = text.lines().find_map(|line| {
                    line.split_once("celld scoped stores listening on ")
                        .map(|(_, listen_address)| listen_address.trim())
                }) {
                    service.endpoint = format!("https://{address}");
                    break;
                }
                if let Some(status) = service.child.try_wait().expect("celld state") {
                    panic!("celld startup {status}: {text}");
                }
                assert!(Instant::now() < end, "real celld startup deadline");
                thread::sleep(Duration::from_millis(10));
            }
            service
        }
        fn publish(&self, root: &Path, greeting: &str) {
            let document = root.join("published-config.json");
            fs::write(&document, envelope(greeting)).expect("operator-owned document");
            let mut child = Command::new("curl")
                .args([
                    "--disable",
                    "--silent",
                    "--show-error",
                    "--noproxy",
                    "*",
                    "--max-time",
                    "10",
                    "--cacert",
                ])
                .arg(&self.cert)
                .args(["--request", "PUT", "--data-binary"])
                .arg(format!("@{}", document.display()))
                .args([
                    "--url",
                    &format!("{}/v1/stores/app-config/value?key=settings", self.endpoint),
                    "--config",
                    "-",
                    "--output",
                ])
                .arg(root.join("publish-response"))
                .args(["--write-out", "%{http_code}"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("explicit curl operator prerequisite");
            child
                .stdin
                .take()
                .expect("operator pipe")
                .write_all(
                    format!("header = \"Authorization: Bearer {OPERATOR_TOKEN}\"\n").as_bytes(),
                )
                .expect("operator credential on private stdin");
            let output = child.wait_with_output().expect("operator publish");
            assert!(
                output.status.success(),
                "operator publish: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(String::from_utf8_lossy(&output.stdout), "204");
        }
    }

    #[test]
    #[ignore = "requires explicit modified CELLD_BIN, curl and OpenSSL; runs actual generated consumer over verified TLS"]
    fn generated_native_consumer_uses_embedded_celld_over_tls() {
        let temp = TempDir::new().expect("real-service fixture");
        let service = Celld::start(temp.path());
        service.publish(temp.path(), "first-config");
        let project = generate(temp.path());
        let binding = bindings(&project, &service.endpoint, true);
        let binary = build(&project, Some("celld-config,celld-kv"));
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
                && response.contains("greeting=first-config")
                && response.contains("binary=Some([0, 255, 1, 128])")
                && response.contains("ttl=true"),
            "real service path: {response}"
        );
        service.publish(temp.path(), "second-config");
        assert!(first.request().contains("greeting=first-config"));
        thread::sleep(Duration::from_secs(61));
        let expired = first.request();
        assert!(
            expired.contains("ttl=false") && expired.contains("greeting=first-config"),
            "live elapsed service expiry with immutable configuration: {expired}"
        );
        drop(first);
        drop(service);
        let restarted_service = Celld::start(temp.path());
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
            restarted_response.contains("greeting=second-config")
                && restarted_response.contains("binary=Some([0, 255, 1, 128])"),
            "real service restart adoption: {restarted_response}"
        );
        drop(second);
        let mut rejected = App::start(
            &binary,
            &project,
            &restarted_binding,
            "read",
            Some((
                "wrong-fixture-token-longer-than-32-bytes",
                &restarted_service.cert,
            )),
        );
        rejected.rejected();
        let diagnostic = fs::read_to_string(&rejected.log).expect("closed error");
        assert!(
            !diagnostic.contains(APP_TOKEN)
                && !diagnostic.contains(OPERATOR_TOKEN)
                && !diagnostic.contains(&restarted_service.endpoint)
        );
        assert!(!project.join(".edgezero").exists());
    }
}
