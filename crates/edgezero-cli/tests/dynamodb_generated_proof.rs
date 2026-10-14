//! Draft ignored proof for the generated Axum `DynamoDB` KV consumer.
//!
//! Coordinator-owned integration target only: do not add this file to the repository or run
//! Cargo here. The generated child uses the real generated `main`/`run_generated_app`; only its
//! generated handler/manifest are extended in the temporary project. `DynamoDB` SDK traffic goes
//! to a literal-loopback JSON-protocol fixture with dummy credentials and a cleared child env.

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    mod generated_dynamodb_proof {
        #![expect(
            clippy::arbitrary_source_item_ordering,
            reason = "group test fixture types and their protocol helpers by responsibility"
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

        use serde_json::{Value, json};
        use tempfile::TempDir;

        const WAIT: Duration = Duration::from_secs(25);
        const TABLE: &str = "edgezero-kv";
        const NAMESPACE: &str = "fixture/state";

        fn deadline_after(duration: Duration) -> Instant {
            Instant::now()
                .checked_add(duration)
                .expect("proof deadline within supported range")
        }

        /// Tiny `DynamoDB` JSON-protocol fixture. Persists rows across generated process restarts and
        /// implements only the DescribeTable/GetItem/PutItem/DeleteItem/Query calls this proof sends.
        struct DynamoFixture {
            endpoint: String,
            requests: Arc<AtomicUsize>,
            stop: Arc<AtomicBool>,
            worker: Option<JoinHandle<()>>,
        }

        impl DynamoFixture {
            fn start() -> Self {
                let listener = TcpListener::bind("127.0.0.1:0").expect("bind literal loopback");
                listener.set_nonblocking(true).expect("nonblocking fixture");
                let endpoint = format!("http://{}", listener.local_addr().expect("fixture addr"));
                let rows = Arc::new(Mutex::new(BTreeMap::new()));
                let shared_rows = Arc::clone(&rows);
                let stop = Arc::new(AtomicBool::new(false));
                let worker_stop = Arc::clone(&stop);
                let requests = Arc::new(AtomicUsize::new(0));
                let worker_requests = Arc::clone(&requests);
                let worker = thread::spawn(move || {
                    let deadline = deadline_after(Duration::from_mins(3));
                    while !worker_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                        match listener.accept() {
                            Ok((mut stream, peer)) => {
                                assert!(peer.ip().is_loopback(), "fixture accepts loopback only");
                                stream
                                    .set_read_timeout(Some(Duration::from_secs(4)))
                                    .expect("read timeout");
                                stream
                                    .set_write_timeout(Some(Duration::from_secs(4)))
                                    .expect("write timeout");
                                worker_requests.fetch_add(1, Ordering::Relaxed);
                                let (target, body) = read_aws_request(&mut stream);
                                let response = dispatch(&target, &body, &shared_rows);
                                write_aws_response(&mut stream, &response);
                            }
                            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(5));
                            }
                            Err(error) => panic!("DynamoDB fixture accept: {error}"),
                        }
                    }
                });
                Self {
                    endpoint,
                    requests,
                    stop,
                    worker: Some(worker),
                }
            }

            fn count(&self) -> usize {
                self.requests.load(Ordering::Relaxed)
            }
        }

        impl Drop for DynamoFixture {
            fn drop(&mut self) {
                self.stop.store(true, Ordering::Relaxed);
                if let Some(worker) = self.worker.take() {
                    let _joined = worker.join();
                }
            }
        }

        fn read_aws_request(stream: &mut TcpStream) -> (String, Value) {
            let mut data = Vec::new();
            let mut chunk = [0_u8; 4096];
            let header_end = loop {
                let count = stream.read(&mut chunk).expect("read request");
                assert_ne!(count, 0, "request ended before headers");
                let read_bytes = chunk
                    .get(..count)
                    .expect("Read does not report more bytes than the supplied buffer");
                data.extend_from_slice(read_bytes);
                if let Some(index) = data.windows(4).position(|window| window == b"\r\n\r\n") {
                    break index
                        .checked_add(4)
                        .expect("bounded header delimiter offset");
                }
                assert!(data.len() < 64 * 1024, "bounded request headers");
            };
            let headers = String::from_utf8(
                data.get(..header_end)
                    .expect("header boundary within request")
                    .to_vec(),
            )
            .expect("HTTP headers");
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().expect("length"))
                })
                .expect("content-length");
            while data.len().saturating_sub(header_end) < content_length {
                let count = stream.read(&mut chunk).expect("read body");
                assert_ne!(count, 0, "request ended before body");
                let read_bytes = chunk
                    .get(..count)
                    .expect("Read does not report more bytes than the supplied buffer");
                data.extend_from_slice(read_bytes);
            }
            let target = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("x-amz-target")
                        .then(|| value.trim().to_owned())
                })
                .expect("AWS target header");
            let body_end = header_end
                .checked_add(content_length)
                .expect("request body boundary does not overflow");
            let body_bytes = data
                .get(header_end..body_end)
                .expect("complete request body");
            let body = serde_json::from_slice(body_bytes).expect("AWS JSON body");
            (target, body)
        }

        fn dispatch(
            target: &str,
            body: &Value,
            rows: &Arc<Mutex<BTreeMap<String, Value>>>,
        ) -> Value {
            let operation = target.rsplit('.').next().unwrap_or(target);
            match operation {
                "DescribeTable" => json!({"Table": {
                    "TableName": TABLE, "TableStatus": "ACTIVE",
                    "KeySchema": [
                        {"AttributeName":"pk", "KeyType":"HASH"},
                        {"AttributeName":"sk", "KeyType":"RANGE"}
                    ],
                    "AttributeDefinitions": [
                        {"AttributeName":"pk", "AttributeType":"S"},
                        {"AttributeName":"sk", "AttributeType":"S"}
                    ],
                    "GlobalSecondaryIndexes": [], "LocalSecondaryIndexes": []
                }}),
                "PutItem" => {
                    assert_eq!(
                        body.get("TableName").and_then(Value::as_str),
                        Some(TABLE),
                        "PutItem targets the fixture table"
                    );
                    let item = body
                        .get("Item")
                        .cloned()
                        .expect("PutItem request includes Item");
                    let key = required_string(string_attribute(&item, "sk"), "PutItem sort key")
                        .to_owned();
                    rows.lock().expect("fixture rows").insert(key, item);
                    json!({})
                }
                "GetItem" => {
                    assert_eq!(
                        body.get("TableName").and_then(Value::as_str),
                        Some(TABLE),
                        "GetItem targets the fixture table"
                    );
                    let key = required_string(
                        body.get("Key").and_then(|key| string_attribute(key, "sk")),
                        "GetItem sort key",
                    );
                    let item = rows.lock().expect("fixture rows").get(key).cloned();
                    item.map_or_else(|| json!({}), |found_item| json!({"Item": found_item}))
                }
                "DeleteItem" => {
                    assert_eq!(
                        body.get("TableName").and_then(Value::as_str),
                        Some(TABLE),
                        "DeleteItem targets the fixture table"
                    );
                    let key = required_string(
                        body.get("Key").and_then(|key| string_attribute(key, "sk")),
                        "DeleteItem sort key",
                    );
                    rows.lock().expect("fixture rows").remove(key);
                    json!({})
                }
                "Query" => {
                    assert_eq!(
                        body.get("TableName").and_then(Value::as_str),
                        Some(TABLE),
                        "Query targets the fixture table"
                    );
                    let values = body
                        .get("ExpressionAttributeValues")
                        .expect("Query includes expression values");
                    let pk =
                        required_string(string_attribute(values, ":pk"), "Query partition key");
                    let prefix = string_attribute(values, ":prefix").unwrap_or("");
                    let limit = body
                        .get("Limit")
                        .and_then(Value::as_u64)
                        .and_then(|value| usize::try_from(value).ok())
                        .unwrap_or(1000);
                    let all = rows.lock().expect("fixture rows");
                    let items = all
                        .values()
                        .filter(|item| {
                            string_attribute(item, "pk") == Some(pk)
                                && string_attribute(item, "sk")
                                    .is_some_and(|key| key.starts_with(prefix))
                        })
                        .take(limit)
                        .cloned()
                        .collect::<Vec<_>>();
                    json!({"Items": items, "Count": items.len(), "ScannedCount": items.len()})
                }
                unknown => panic!("unexpected DynamoDB operation {unknown}: {body}"),
            }
        }

        fn string_attribute<'attribute>(
            item: &'attribute Value,
            name: &str,
        ) -> Option<&'attribute str> {
            item.get(name)?.get("S")?.as_str()
        }

        fn required_string<'attribute>(
            value: Option<&'attribute str>,
            field: &str,
        ) -> &'attribute str {
            value.expect(field)
        }

        fn write_aws_response(stream: &mut TcpStream, body: &Value) {
            let bytes = serde_json::to_vec(body).expect("serialize response");
            write!(stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/x-amz-json-1.0\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            bytes.len()).expect("response headers");
            stream.write_all(&bytes).expect("response body");
        }

        struct GeneratedApp {
            address: SocketAddr,
            child: Child,
        }

        impl GeneratedApp {
            fn start(
                executable: &Path,
                project: &Path,
                binding: &Path,
                endpoint: &str,
                mode: &str,
            ) -> Self {
                let listener = TcpListener::bind("127.0.0.1:0").expect("reserve app port");
                let address = listener.local_addr().expect("app address");
                drop(listener);
                let mut command = Command::new(executable);
                command
                    .current_dir(project)
                    .env_clear()
                    .env("HOME", project.join("empty-home"))
                    .env("AWS_ACCESS_KEY_ID", "dummy-access")
                    .env("AWS_SECRET_ACCESS_KEY", "dummy-secret")
                    .env("AWS_EC2_METADATA_DISABLED", "true")
                    .env("AWS_CONFIG_FILE", project.join("empty-aws-config"))
                    .env(
                        "AWS_SHARED_CREDENTIALS_FILE",
                        project.join("empty-aws-creds"),
                    )
                    .env("AWS_ENDPOINT_URL", endpoint)
                    .env("EDGEZERO__STORE_BINDINGS_FILE", binding)
                    .env("EDGEZERO__DATA_DIR", project.join("must-not-create-data"))
                    .env("EDGEZERO__ADAPTER__HOST", "127.0.0.1")
                    .env("EDGEZERO__ADAPTER__PORT", address.port().to_string())
                    .env("KV_PROOF_MODE", mode)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());
                Self {
                    child: command.spawn().expect("spawn generated app"),
                    address,
                }
            }

            fn request(&mut self, path: &str, body: Option<&str>) -> String {
                let deadline = deadline_after(WAIT);
                loop {
                    if let Some(status) = self.child.try_wait().expect("child status") {
                        panic!("generated server exited before request {path}: {status}");
                    }
                    if let Ok(mut stream) =
                        TcpStream::connect_timeout(&self.address, Duration::from_millis(50))
                    {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .expect("response timeout");
                        let (method, content_type, payload) = body
                            .map_or(("GET", "", ""), |value| {
                                ("POST", "Content-Type: application/json\r\n", value)
                            });
                        write!(stream, "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{content_type}Content-Length: {}\r\nConnection: close\r\n\r\n{payload}", payload.len()).expect("send handler request");
                        let mut response = String::new();
                        stream
                            .read_to_string(&mut response)
                            .expect("read handler response");
                        if !response.starts_with("HTTP/1.1 503")
                            && !response.starts_with("HTTP/1.1 500")
                        {
                            return response;
                        }
                        return response;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "generated listener did not start"
                    );
                    thread::sleep(Duration::from_millis(10));
                }
            }

            fn stop(mut self) {
                if self.child.try_wait().expect("child status").is_none() {
                    let _killed = self.child.kill();
                }
                self.child.wait().expect("reap generated child");
            }
        }

        impl Drop for GeneratedApp {
            fn drop(&mut self) {
                if self.child.try_wait().ok().flatten().is_none() {
                    let _killed = self.child.kill();
                    let _reaped = self.child.wait();
                }
            }
        }

        fn install_consumer(project: &Path) {
            let manifest =
                fs::read_to_string(project.join("edgezero.toml")).expect("generated manifest");
            let entry = manifest
                .lines()
                .find_map(|line| line.strip_prefix("entry = "))
                .expect("generated core entry")
                .trim_matches('"');
            let core_dir = project.join(entry);
            let core_name = core_dir
                .file_name()
                .expect("core directory")
                .to_string_lossy()
                .replace('-', "_");
            let handlers = core_dir.join("src/handlers.rs");
            let mut handler_source = fs::read_to_string(&handlers).expect("generated handlers");
            handler_source.push_str(r#"
#[action]
pub async fn kv_probe(kv: edgezero_core::extractor::Kv) -> Result<Text<String>, EdgeError> {
    let store = kv.default().ok_or_else(|| EdgeError::service_unavailable("default KV unavailable"))?;
    let mode = std::env::var("KV_PROOF_MODE").unwrap_or_default();
    match mode.as_str() {
        "put" => {
            store.put("fixture/item", &"persisted-value").await?;
            store.put_with_ttl("fixture/ttl", &"ttl-value", std::time::Duration::from_secs(60)).await?;
        }
        "ttl" => { store.put_with_ttl("fixture/ttl", &"ttl-value", std::time::Duration::from_secs(60)).await?; }
        "delete" => { store.delete("fixture/item").await?; }
        _ => {}
    }
    let value = store.get::<String>("fixture/item").await?.unwrap_or_else(|| "missing".to_owned());
    let ttl = store.get::<String>("fixture/ttl").await?.unwrap_or_else(|| "missing".to_owned());
    let page = store.list_keys_page("fixture/", None, 100).await?;
    Ok(Text::new(format!("value={value};ttl={ttl};keys={}", page.keys.join(","))))
}
"#);
            fs::write(handlers, handler_source).expect("write temporary generated handler");

            let manifest_path = project.join("edgezero.toml");
            let mut manifest_source =
                fs::read_to_string(&manifest_path).expect("generated manifest");
            manifest_source.push_str(
                r#"
[[triggers.http]]
id = "kv-proof"
path = "/kv-proof"
methods = ["GET", "POST"]
handler = "{core_name}::handlers::kv_probe"
adapters = ["axum"]

[stores.kv]
ids = ["state"]
"#,
            );
            let rendered_manifest = manifest_source.replace("{core_name}", &core_name);
            fs::write(manifest_path, rendered_manifest)
                .expect("write temporary generated manifest");
        }

        fn write_bindings(project: &Path) -> PathBuf {
            let path = project.join("bindings.toml");
            let bindings = format!(
                r#"version = 1

[kv.state]
provider = "aws-dynamodb"
region = "us-east-1"
table = "{TABLE}"
namespace = "{NAMESPACE}"
"#
            );
            fs::write(&path, bindings).expect("write native TOML bindings fixture");
            path
        }

        fn target_dir(project: &Path) -> PathBuf {
            env::var_os("EDGEZERO_GENERATED_TEST_TARGET_DIR")
                .map_or_else(|| project.join("target"), PathBuf::from)
        }

        fn build_generated(project: &Path, target: &Path, enabled: bool) -> PathBuf {
            let mut command = Command::new(env!("CARGO_BIN_EXE_edgezero"));
            command.args(["build", "--adapter", "axum", "--", "--offline"]);
            if enabled {
                command.args(["--features", "aws-dynamodb"]);
            }
            let output = command
                .current_dir(project)
                .env("CARGO_TARGET_DIR", target)
                .output()
                .expect("real edgezero build");
            assert!(
                output.status.success(),
                "generated {}build should succeed: {}",
                if enabled { "enabled " } else { "default-off " },
                String::from_utf8_lossy(&output.stderr)
            );
            target.join("debug/dynamodb-proof-adapter-axum")
        }

        fn prove_default_off(
            project: &Path,
            bindings: &Path,
            fixture: &DynamoFixture,
            binary: &Path,
        ) {
            // Reject the explicit binding before local data, SDK calls, or listener startup.
            let mut rejected =
                GeneratedApp::start(binary, project, bindings, &fixture.endpoint, "read");
            let exit_deadline = deadline_after(WAIT);
            loop {
                if rejected
                    .child
                    .try_wait()
                    .expect("default-off process state")
                    .is_some()
                {
                    break;
                }
                assert!(
                    Instant::now() < exit_deadline,
                    "default-off selection unexpectedly listened"
                );
                thread::sleep(Duration::from_millis(10));
            }
            assert!(
                !project.join("must-not-create-data").exists(),
                "default-off selection must not create local DATA_DIR"
            );
            assert!(
                TcpStream::connect_timeout(&rejected.address, Duration::from_millis(50)).is_err(),
                "default-off selection must not bind listener"
            );
            assert_eq!(
                fixture.count(),
                0,
                "provider rejection must happen before SDK calls"
            );
            rejected.stop();
        }

        fn prove_enabled_consumer(
            project: &Path,
            bindings: &Path,
            fixture: &DynamoFixture,
            binary: &Path,
        ) {
            let mut first =
                GeneratedApp::start(binary, project, bindings, &fixture.endpoint, "put");
            let first_response = first.request("/kv-proof", None);
            assert!(
                first_response.contains("200 OK") && first_response.contains("persisted-value"),
                "KV put/get: {first_response}"
            );
            assert!(
                first_response.contains("fixture/item"),
                "KV query/list: {first_response}"
            );
            assert!(
                first_response.contains("ttl-value"),
                "TTL write/read: {first_response}"
            );
            first.stop();

            // A fresh generated process reads fixture-retained data, not process-local state.
            let mut second =
                GeneratedApp::start(binary, project, bindings, &fixture.endpoint, "read");
            let second_response = second.request("/kv-proof", None);
            assert!(
                second_response.contains("persisted-value"),
                "data survives generated process restart: {second_response}"
            );
            second.stop();

            let mut third =
                GeneratedApp::start(binary, project, bindings, &fixture.endpoint, "delete");
            let third_response = third.request("/kv-proof", None);
            assert!(
                third_response.contains("value=missing")
                    && third_response.contains("ttl=ttl-value"),
                "KV delete preserves other key: {third_response}"
            );
            third.stop();
        }

        #[test]
        #[ignore = "builds and launches a generated native app; coordinator owns Cargo integration"]
        fn generated_cli_default_off_and_enabled_dynamodb_crud_list_ttl_restart() {
            let temp = TempDir::new().expect("temp project");
            let output = Command::new(env!("CARGO_BIN_EXE_edgezero"))
                .args(["new", "dynamodb-proof", "--dir"])
                .arg(temp.path())
                .output()
                .expect("real edgezero new");
            assert!(
                output.status.success(),
                "edgezero new: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let project = temp.path().join("dynamodb-proof");
            install_consumer(&project);
            let bindings = write_bindings(&project);
            let fixture = DynamoFixture::start();
            let target = target_dir(&project);

            let default_binary = build_generated(&project, &target, false);
            prove_default_off(&project, &bindings, &fixture, &default_binary);

            // The generated Axum alias opts into the actual provider feature.
            let enabled_binary = build_generated(&project, &target, true);
            prove_enabled_consumer(&project, &bindings, &fixture, &enabled_binary);
        }
    }
}
