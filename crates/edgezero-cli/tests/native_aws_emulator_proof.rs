//! Independent local AWS proof. The runner supplies pinned `DynamoDB` Local, Moto and Agent
//! services. This test uses one real scaffold and one generated build with all three providers.
//! Run with `node scripts/run_native_aws_integration.mjs`.
//! No AWS SDK dependency is added to the CLI or to the temporary application.

#[cfg(test)]
#[cfg(unix)]
mod tests {
    #![expect(
        clippy::arbitrary_source_item_ordering,
        reason = "Group local-service proof helpers and source fixtures by responsibility."
    )]

    use std::collections::BTreeSet;
    use std::env;
    use std::fmt::Write as _;
    use std::fs;
    use std::io::{self, Read as _, Write as _};
    use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, ExitStatus, Stdio};
    use std::str::from_utf8;
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use edgezero_core::blob_envelope::BlobEnvelope;
    use serde_json::{Value, json};
    use tempfile::{NamedTempFile, TempDir};
    use toml::map::Map;

    const WAIT: Duration = Duration::from_secs(45);
    const BUILD_WAIT: Duration = Duration::from_mins(15);
    const RAW_A: &str = "  opaque A\nnot JSON: caf\u{e9}\t\n";
    const RAW_B: &str = "\tupdated B\nnot JSON: e\u{301}\n";

    struct Services {
        dynamodb: String,
        secrets: String,
        agent: String,
        directory: PathBuf,
        node: String,
        token: String,
    }

    impl Services {
        fn required() -> Self {
            let required =
                |name| env::var(name).unwrap_or_else(|_| panic!("required harness env {name}"));
            let endpoint = |name| {
                let value = required(name);
                let address = value
                    .strip_prefix("http://")
                    .and_then(|authority| authority.parse::<SocketAddr>().ok())
                    .expect("endpoint must be a literal http://127.0.0.1:<port>");
                assert_eq!(address.ip(), Ipv4Addr::LOCALHOST);
                assert_ne!(address.port(), 0);
                assert_eq!(value, format!("http://{address}"));
                value
            };
            let directory = PathBuf::from(required("EDGEZERO_AWS_TEST_AGENT_DIRECTORY"));
            assert!(directory.is_absolute() && directory.is_dir());
            let token = required("EDGEZERO_AWS_TEST_AGENT_TOKEN");
            assert_eq!(token, "local-agent-token", "only dummy Agent credentials");
            let node = required("EDGEZERO_AWS_TEST_NODE");
            assert!(
                Path::new(&node).is_absolute(),
                "runner supplies its Node executable"
            );
            Self {
                dynamodb: endpoint("EDGEZERO_AWS_TEST_DYNAMODB_ENDPOINT"),
                secrets: endpoint("EDGEZERO_AWS_TEST_SECRETS_ENDPOINT"),
                agent: endpoint("EDGEZERO_AWS_TEST_AGENT_ENDPOINT"),
                directory,
                node,
                token,
            }
        }

        fn admin(&self, action: &str, input: &Value) -> Value {
            let mut command = Command::new(&self.node);
            command
                .arg(
                    Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("../../scripts/native_aws_test_admin.mjs"),
                )
                .arg(action)
                .arg(input.to_string())
                .env("EDGEZERO_AWS_TEST_DYNAMODB_ENDPOINT", &self.dynamodb)
                .env("EDGEZERO_AWS_TEST_SECRETS_ENDPOINT", &self.secrets);
            let output = checked(command, WAIT, &format!("admin {action}"));
            serde_json::from_slice(&output).expect("admin stdout is JSON")
        }

        fn publish(&self, message: &str, raw: &str) -> String {
            let typed = serde_json::to_string(&BlobEnvelope::new(
                json!({"message": message, "nested": {"token": "current"}}),
                "2026-10-07T12:00:00Z".to_owned(),
            ))
            .expect("serialize real typed envelope");
            for (name, bytes) in [
                ("probe:test:raw", raw),
                ("probe:test:typed", typed.as_str()),
            ] {
                let pending = self.directory.join(format!(".{name}.pending"));
                fs::write(&pending, bytes).expect("writable Agent mounted directory");
                fs::rename(pending, self.directory.join(name))
                    .expect("atomic Agent file replacement");
            }
            typed
        }

        fn wait_agent(&self, profile: &str, expected: &str) {
            let address = self
                .agent
                .strip_prefix("http://")
                .expect("validated endpoint")
                .parse()
                .expect("literal Agent address");
            let deadline = deadline(WAIT);
            loop {
                if let Ok((status, body)) = http(
                    address,
                    "GET",
                    &format!("/applications/probe/environments/test/configurations/{profile}"),
                    &format!("Authorization: Bearer {}\r\n", self.token),
                    b"",
                ) && status == 200
                    && body == expected.as_bytes()
                {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "Agent did not independently serve updated {profile}"
                );
                thread::sleep(Duration::from_millis(100));
            }
        }
    }

    /// Output goes to files, so a full pipe cannot deadlock startup or a failed build. All children
    /// inherit the runner's process group; the Node runner also cleans descendants on every exit.
    struct Process {
        child: Child,
        stdout: NamedTempFile,
        stderr: NamedTempFile,
    }

    impl Process {
        fn spawn(mut command: Command) -> Self {
            let stdout = NamedTempFile::new().expect("stdout file");
            let stderr = NamedTempFile::new().expect("stderr file");
            command
                .stdout(Stdio::from(stdout.reopen().expect("stdout handle")))
                .stderr(Stdio::from(stderr.reopen().expect("stderr handle")));
            Self {
                child: command.spawn().expect("spawn bounded subprocess"),
                stdout,
                stderr,
            }
        }

        fn logs(&self) -> String {
            format!(
                "{}\n{}",
                String::from_utf8_lossy(&fs::read(self.stdout.path()).expect("stdout")),
                String::from_utf8_lossy(&fs::read(self.stderr.path()).expect("stderr"))
            )
        }

        fn wait(&mut self, timeout: Duration) -> ExitStatus {
            let until = deadline(timeout);
            loop {
                if let Some(status) = self.child.try_wait().expect("subprocess status") {
                    return status;
                }
                assert!(
                    Instant::now() < until,
                    "subprocess timed out: {}",
                    self.logs()
                );
                thread::sleep(Duration::from_millis(20));
            }
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "Drop::pin_drop is unstable and conflicts with drop"
    )]
    impl Drop for Process {
        fn drop(&mut self) {
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }

    #[expect(
        clippy::print_stdout,
        reason = "The mandatory integration runner needs phase progress on slow generated builds and service failures."
    )]
    fn phase(message: &str) {
        println!("{message}");
    }

    fn checked(command: Command, timeout: Duration, label: &str) -> Vec<u8> {
        let mut process = Process::spawn(command);
        assert!(
            process.wait(timeout).success(),
            "{label} failed: {}",
            process.logs()
        );
        fs::read(process.stdout.path()).expect("command stdout")
    }

    fn deadline(timeout: Duration) -> Instant {
        Instant::now()
            .checked_add(timeout)
            .expect("finite deadline")
    }

    fn reserve_address() -> SocketAddr {
        TcpListener::bind("127.0.0.1:0")
            .expect("reserve loopback port")
            .local_addr()
            .expect("loopback address")
    }

    fn http(
        address: SocketAddr,
        method: &str,
        path: &str,
        headers: &str,
        body: &[u8],
    ) -> io::Result<(u16, Vec<u8>)> {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(200))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )?;
        stream.write_all(body)?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response)?;
        let boundary = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .ok_or_else(|| io::Error::other("missing HTTP headers end"))?;
        let (head_bytes, header_tail) = response
            .split_at_checked(boundary)
            .ok_or_else(|| io::Error::other("HTTP header boundary"))?;
        let head = from_utf8(head_bytes).map_err(io::Error::other)?;
        let status = head
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| io::Error::other("missing HTTP status"))?
            .parse::<u16>()
            .map_err(io::Error::other)?;
        let mut payload = header_tail
            .strip_prefix(b"\r\n\r\n")
            .ok_or_else(|| io::Error::other("HTTP response boundary"))?
            .to_vec();
        if head
            .lines()
            .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
        {
            let mut decoded = Vec::new();
            let mut remaining = payload.as_slice();
            loop {
                let end = remaining
                    .windows(2)
                    .position(|bytes| bytes == b"\r\n")
                    .ok_or_else(|| io::Error::other("missing chunk header"))?;
                let (chunk_header, chunk_tail) = remaining
                    .split_at_checked(end)
                    .ok_or_else(|| io::Error::other("chunk header boundary"))?;
                let size = usize::from_str_radix(
                    from_utf8(chunk_header)
                        .map_err(io::Error::other)?
                        .split(';')
                        .next()
                        .ok_or_else(|| io::Error::other("missing chunk size"))?,
                    16,
                )
                .map_err(io::Error::other)?;
                if size == 0 {
                    break;
                }
                let chunk_payload = chunk_tail
                    .strip_prefix(b"\r\n")
                    .ok_or_else(|| io::Error::other("chunk header delimiter"))?;
                let (data, body_tail) = chunk_payload
                    .split_at_checked(size)
                    .ok_or_else(|| io::Error::other("truncated HTTP chunk"))?;
                decoded.extend_from_slice(data);
                remaining = body_tail
                    .strip_prefix(b"\r\n")
                    .ok_or_else(|| io::Error::other("chunk body delimiter"))?;
            }
            payload = decoded;
        }
        Ok((status, payload))
    }

    struct App {
        process: Process,
        address: SocketAddr,
        entered: PathBuf,
        release: PathBuf,
        _startup: TempDir,
    }

    impl App {
        fn spawn(
            project: &Path,
            binary: &Path,
            bindings: &Path,
            services: &Services,
            token: &str,
        ) -> Self {
            let address = reserve_address();
            let startup = tempfile::tempdir_in(project).expect("unique startup gate directory");
            let entered = startup.path().join("entered");
            let release = startup.path().join("release");
            let mut command = Command::new(binary);
            command.current_dir(project).env_clear()
            .env("HOME", project.join("empty-home"))
            .env("AWS_CONFIG_FILE", project.join("empty-aws-config"))
            .env("AWS_SHARED_CREDENTIALS_FILE", project.join("empty-aws-credentials"))
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_ACCESS_KEY_ID", "DUMMYLOCALACCESS123")
            .env("AWS_SECRET_ACCESS_KEY", "DUMMYLOCALSECRET456")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL", format!("http://{}", reserve_address()))
            .env("AWS_ENDPOINT_URL_DYNAMODB", &services.dynamodb)
            .env("AWS_ENDPOINT_URL_SECRETS_MANAGER", &services.secrets)
            .env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
            .env("APPCONFIG_AGENT_TOKEN", token)
            .env("EDGEZERO__STORE_BINDINGS_FILE", bindings)
            .env("EDGEZERO__DATA_DIR", project.join("must-not-create-data"))
            .env("EDGEZERO__ADAPTER__HOST", "127.0.0.1")
            .env("EDGEZERO__ADAPTER__PORT", address.port().to_string())
            .env("EDGEZERO__LOGGING__LEVEL", "off")
            .env("RUST_LOG", "off,aws_config=off,aws_smithy_runtime=off,aws_sdk_dynamodb=off,aws_sdk_secretsmanager=off,reqwest=off,hyper=off")
            .env("PROOF_ENTERED", &entered).env("PROOF_RELEASE", &release);
            Self {
                process: Process::spawn(command),
                address,
                entered,
                release,
                _startup: startup,
            }
        }

        fn ready(&mut self, expected_secret: &str) {
            let until = deadline(WAIT);
            while !self.entered.is_file() {
                assert!(
                    self.process
                        .child
                        .try_wait()
                        .expect("startup status")
                        .is_none(),
                    "startup failed before initializer: {}",
                    self.process.logs()
                );
                self.assert_closed();
                assert!(
                    Instant::now() < until,
                    "initializer timeout: {}",
                    self.process.logs()
                );
                thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(
                fs::read_to_string(&self.entered).expect("resolved initializer marker"),
                expected_secret,
                "nested secret must resolve before readiness marker"
            );
            self.assert_closed();
            fs::write(&self.release, b"release").expect("release initializer");
            while TcpStream::connect_timeout(&self.address, Duration::from_millis(30)).is_err() {
                assert!(
                    self.process
                        .child
                        .try_wait()
                        .expect("startup status")
                        .is_none(),
                    "{}",
                    self.process.logs()
                );
                assert!(
                    Instant::now() < until,
                    "listener timeout: {}",
                    self.process.logs()
                );
                thread::sleep(Duration::from_millis(10));
            }
        }

        fn assert_closed(&self) {
            assert!(
                TcpStream::connect_timeout(&self.address, Duration::from_millis(30)).is_err(),
                "application listened before successful initialization"
            );
        }

        fn rejects(mut self) {
            let until = deadline(WAIT);
            loop {
                self.assert_closed();
                assert!(
                    !self.entered.exists(),
                    "invalid provider reached typed readiness marker"
                );
                if let Some(status) = self
                    .process
                    .child
                    .try_wait()
                    .expect("negative startup status")
                {
                    assert!(!status.success(), "invalid startup exited successfully");
                    self.assert_closed();
                    return;
                }
                assert!(
                    Instant::now() < until,
                    "invalid startup did not fail closed: {}",
                    self.process.logs()
                );
                thread::sleep(Duration::from_millis(10));
            }
        }

        fn call(&mut self, request: &Value) -> Value {
            assert!(
                self.process
                    .child
                    .try_wait()
                    .expect("running app status")
                    .is_none(),
                "{}",
                self.process.logs()
            );
            let bytes = serde_json::to_vec(request).expect("probe request JSON");
            let (status, response) = http(
                self.address,
                "POST",
                "/proof",
                "Content-Type: application/json\r\n",
                &bytes,
            )
            .expect("bounded application HTTP request");
            assert_eq!(
                status,
                200,
                "probe {request}: {}",
                String::from_utf8_lossy(&response)
            );
            serde_json::from_slice(&response).expect("probe JSON response")
        }
    }

    fn install_consumer(project: &Path) {
        let core = project.join("crates/native-aws-probe-core");
        let mut manifest: toml::Value =
            toml::from_str(&fs::read_to_string(project.join("edgezero.toml")).expect("manifest"))
                .expect("scaffold TOML");
        let root = manifest.as_table_mut().expect("manifest root table");
        root.insert(
            "stores".to_owned(),
            toml::Value::try_from(json!({
                "config": {"ids": ["app_config"], "default": "app_config"},
                "secrets": {"ids": ["vault"], "default": "vault"},
                "kv": {"ids": ["primary", "secondary"], "default": "primary"}
            }))
            .expect("store declaration TOML"),
        );
        root.get_mut("triggers")
            .and_then(toml::Value::as_table_mut)
            .expect("scaffold triggers")
            .insert(
                "http".to_owned(),
                toml::Value::try_from(json!([{
                    "id": "proof", "path": "/proof", "methods": ["POST"],
                    "handler": "native_aws_probe_core::handlers::proof", "adapters": ["axum"]
                }]))
                .expect("probe trigger TOML"),
            );
        root.get_mut("adapters")
            .and_then(toml::Value::as_table_mut)
            .and_then(|adapters| adapters.get_mut("axum"))
            .and_then(toml::Value::as_table_mut)
            .expect("scaffold Axum adapter")
            .entry("build")
            .or_insert_with(|| toml::Value::Table(Map::new()))
            .as_table_mut()
            .expect("Axum build settings")
            .insert(
                "features".to_owned(),
                toml::Value::try_from(vec![
                    "aws-appconfig-agent",
                    "aws-secrets-manager",
                    "aws-dynamodb",
                ])
                .expect("all three native provider features"),
            );
        fs::write(
            project.join("edgezero.toml"),
            toml::to_string(&manifest).expect("manifest TOML"),
        )
        .expect("write temporary store declarations");
        let core_manifest = core.join("Cargo.toml");
        let mut dependencies: toml::Value =
            toml::from_str(&fs::read_to_string(&core_manifest).expect("core Cargo manifest"))
                .expect("core TOML");
        dependencies["dependencies"]
            .as_table_mut()
            .expect("core dependencies")
            .insert("serde_json".to_owned(), toml::Value::String("1".to_owned()));
        fs::write(
            core_manifest,
            toml::to_string(&dependencies).expect("core TOML"),
        )
        .expect("temporary core JSON dependency");
        fs::write(
            core.join("src/lib.rs"),
            "pub mod config;\nmod handlers;\nedgezero_core::app!(\"../../edgezero.toml\");\n",
        )
        .expect("minimal temporary generated core");
        fs::write(core.join("src/config.rs"), CONFIG_SOURCE).expect("typed nested config");
        fs::write(core.join("src/handlers.rs"), HANDLER_SOURCE).expect("public store API consumer");
        fs::write(
            project.join("native-aws-probe.toml"),
            "message = \"local validation only\"\n[nested]\ntoken = \"current\"\n",
        )
        .expect("local build validation config");
        fs::write(
            project.join("crates/native-aws-probe-adapter-axum/src/main.rs"),
            MAIN_SOURCE,
        )
        .expect("public production initializer");
        fs::create_dir_all(project.join("empty-home")).expect("isolated HOME");
        fs::write(project.join("empty-aws-config"), "").expect("empty config");
        fs::write(project.join("empty-aws-credentials"), "").expect("empty credentials");
    }

    const CONFIG_SOURCE: &str = "
use serde::{Deserialize, Serialize};
use validator::Validate;
#[derive(Debug, Deserialize, Serialize, Validate, edgezero_core::AppConfig)]
#[serde(deny_unknown_fields)]
pub struct NativeAwsProbeConfig {
    pub message: String,
    #[app_config(nested)]
    #[validate(nested)]
    pub nested: Nested,
}
#[derive(Debug, Deserialize, Serialize, Validate, edgezero_core::AppConfig)]
#[serde(deny_unknown_fields)]
pub struct Nested {
    #[secret]
    #[validate(length(min = 8_u64))]
    pub token: String,
}
pub struct StartupState {
    pub message: String,
    pub token: String,
}
";

    const MAIN_SOURCE: &str = r#"
use std::sync::Arc;
use std::time::{Duration, Instant};
use edgezero_adapter_axum::dev_server::run_production_app_with_initializer;
use edgezero_core::extractor::AppConfig;
use edgezero_core::EdgeError;
use native_aws_probe_core::config::{NativeAwsProbeConfig, StartupState};
fn main() -> anyhow::Result<()> {
    run_production_app_with_initializer::<native_aws_probe_core::App, _>(|app, stores| {
        Box::pin(async move {
            let binding = stores.config().and_then(|registry| registry.named_ref("app_config"))
                .ok_or_else(|| EdgeError::service_unavailable("missing typed config binding"))?;
            let config = AppConfig::<NativeAwsProbeConfig>::from_binding(
                binding, None, stores.secrets(), app.config_extraction_limits(), app.monotonic_clock(),
            ).await?;
            let entered = std::env::var("PROOF_ENTERED").map_err(EdgeError::internal)?;
            std::fs::write(entered, &config.nested.token).map_err(EdgeError::internal)?;
            let release = std::path::PathBuf::from(std::env::var("PROOF_RELEASE").map_err(EdgeError::internal)?);
            let deadline = Instant::now() + Duration::from_secs(45);
            while !release.is_file() {
                if Instant::now() >= deadline {
                    return Err(EdgeError::service_unavailable("initializer gate timed out"));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            app.insert_state(Arc::new(StartupState { message: config.message, token: config.nested.token }));
            Ok(())
        })
    })?;
    Ok(())
}
"#;

    const HANDLER_SOURCE: &str = r#"
use std::sync::Arc;
use bytes::Bytes;
use edgezero_core::{action, EdgeError};
use edgezero_core::extractor::{Config, Json as RequestJson, Kv, Secrets, State};
use edgezero_core::body::Body;
use edgezero_core::http::{Response, StatusCode, response_builder};
use serde_json::{Value, json};
use crate::config::StartupState;
#[action]
pub async fn proof(
    State(state): State<Arc<StartupState>>, config: Config, secrets: Secrets,
    kv: Kv, RequestJson(input): RequestJson<Value>,
) -> Result<Response, EdgeError> {
    let op = input["op"].as_str().unwrap_or("");
    if op == "snapshot" {
        let config = config.named("app_config").ok_or_else(|| EdgeError::service_unavailable("missing config"))?;
        let raw = config.get("raw").await?.ok_or_else(|| EdgeError::service_unavailable("missing raw"))?;
        let secrets = secrets.named("vault").ok_or_else(|| EdgeError::service_unavailable("missing secrets"))?;
        let mut values = serde_json::Map::new();
        for name in ["current", "pinned", "candidate", "version", "json", "binary"] {
            values.insert(name.to_owned(), json!(secrets.require_bytes(name).await?.to_vec()));
        }
        return json_response(json!({"message": state.message, "token": state.token,
            "raw": raw.as_bytes(), "secrets": values}));
    }
    let namespace = input["namespace"].as_str().unwrap_or("primary");
    let store = kv.named(namespace).ok_or_else(|| EdgeError::service_unavailable("missing KV"))?;
    let key = input["key"].as_str().unwrap_or("");
    let result = match op {
        "put" | "ttl" => {
            let value: Vec<u8> = serde_json::from_value(input["value"].clone()).map_err(EdgeError::internal)?;
            if op == "ttl" {
                store.put_bytes_with_ttl(key, Bytes::from(value), std::time::Duration::from_secs(120)).await?;
            } else { store.put_bytes(key, Bytes::from(value)).await?; }
            json!(true)
        }
        "large_put" => { store.put_bytes(key, Bytes::from(vec![input["byte"].as_u64().unwrap_or(0) as u8; 256 * 1024])).await?; json!(true) }
        "get" => json!(store.get_bytes(key).await?.map(|value| value.to_vec())),
        "exists" => json!(store.exists(key).await?),
        "delete" => { store.delete(key).await?; json!(true) }
        "list" => {
            let page = store.list_keys_page(input["prefix"].as_str().unwrap_or(""), input["cursor"].as_str(),
                input["limit"].as_u64().unwrap_or(100) as usize).await?;
            json!({"keys": page.keys, "cursor": page.cursor})
        }
        _ => return Err(EdgeError::bad_request("unknown proof operation")),
    };
    json_response(result)
}
fn json_response(value: Value) -> Result<Response, EdgeError> {
    response_builder().status(StatusCode::OK).header("content-type", "application/json")
        .body(Body::json(&value).map_err(EdgeError::internal)?)
        .map_err(EdgeError::internal)
}
"#;

    fn write_bindings(
        project: &Path,
        services: &Services,
        seed: &Value,
        table: &str,
        profile: &str,
        name: &str,
    ) -> PathBuf {
        let mut source = format!(
            r#"version = 1
[config.app_config]
provider = "aws-appconfig-agent"
default_key = "settings"
endpoint = "{}"
access_token_env = "APPCONFIG_AGENT_TOKEN"
[config.app_config.documents.settings]
application = "probe"
environment = "test"
profile = "{profile}"
[config.app_config.documents.raw]
application = "probe"
environment = "test"
profile = "raw"
[secrets.vault]
provider = "aws-secrets-manager"
region = "us-east-1"
"#,
            services.agent
        );
        for (key, secret, selector) in [
            ("current", "text", String::new()),
            ("pinned", "text", "version_stage = \"PINNED\"\n".to_owned()),
            (
                "candidate",
                "text",
                "version_stage = \"CANDIDATE\"\n".to_owned(),
            ),
            (
                "version",
                "text",
                format!(
                    "version_id = {}\n",
                    toml_string(
                        seed["secrets"]["text"]["version_a"]
                            .as_str()
                            .expect("text version A")
                    )
                ),
            ),
            ("json", "json", String::new()),
            ("binary", "binary", String::new()),
        ] {
            write!(
                source,
                "[secrets.vault.entries.{key}]\nsecret_id = {}\n{selector}",
                toml_string(
                    seed["secrets"][secret]["arn"]
                        .as_str()
                        .expect("seed secret ARN")
                )
            )
            .expect("write secret binding to String");
        }
        for namespace in ["primary", "secondary"] {
            write!(source, "[kv.{namespace}]\nprovider = \"aws-dynamodb\"\nregion = \"us-east-1\"\ntable = {}\nnamespace = \"{namespace}\"\n", toml_string(table)).expect("write KV binding to String");
        }
        let path = project.join(format!("{name}.toml"));
        fs::write(&path, source).expect("emulator bindings");
        path
    }

    fn toml_string(value: &str) -> String {
        toml::Value::String(value.to_owned()).to_string()
    }

    fn snapshot(app: &mut App, message: &str, raw: &str, current: &str) {
        assert_eq!(
            app.call(&json!({"op": "snapshot"})),
            json!({
                "message": message, "token": current, "raw": raw.as_bytes(), "secrets": {
                    "current": current.as_bytes(), "pinned": current.as_bytes(),
                    "candidate": b"secret-B".as_slice(), "version": b"secret-A".as_slice(), "json": b" { \"answer\": 42 }\n".as_slice(),
                    "binary": [0_u8, 255_u8, 10_u8, 34_u8]
                }
            }),
            "all config and secret bytes must remain exact startup snapshots"
        );
    }

    fn put(app: &mut App, namespace: &str, key: &str, value: &[u8]) {
        assert_eq!(
            app.call(&json!({"op":"put", "namespace":namespace, "key":key, "value":value})),
            true
        );
    }

    fn get(app: &mut App, namespace: &str, key: &str) -> Value {
        app.call(&json!({"op":"get", "namespace":namespace, "key":key}))
    }

    fn list(app: &mut App, namespace: &str, prefix: &str, cursor: &Value, limit: u32) -> Value {
        app.call(&json!({"op":"list", "namespace":namespace, "prefix":prefix, "cursor":cursor, "limit":limit}))
    }

    fn traverse(app: &mut App, namespace: &str, prefix: &str, limit: u32) -> Vec<String> {
        let mut cursor = Value::Null;
        let mut keys = Vec::new();
        let mut cursors = BTreeSet::new();
        for _ in 0_u32..100_u32 {
            let page = list(app, namespace, prefix, &cursor, limit);
            for value in page["keys"].as_array().expect("list keys") {
                let key = value.as_str().expect("string key").to_owned();
                assert!(key.starts_with(prefix));
                assert!(
                    keys.last().is_none_or(|previous| previous < &key),
                    "sorted pages without duplicates"
                );
                keys.push(key);
            }
            cursor = page["cursor"].clone();
            if cursor.is_null() {
                return keys;
            }
            assert!(
                cursors.insert(cursor.as_str().expect("cursor string").to_owned()),
                "cursor must advance"
            );
        }
        panic!("pagination did not terminate");
    }

    fn prove_kv_crud_and_namespaces(app: &mut App) {
        phase("AWS emulator proof: raw KV CRUD, namespaces, Unicode and cursors");
        let raw = [0, 255, 10, 34, 128];
        assert_eq!(get(app, "primary", "crud/missing"), Value::Null);
        assert_eq!(
            app.call(&json!({"op":"exists", "key":"crud/missing"})),
            false
        );
        put(app, "primary", "crud/raw", &raw);
        assert_eq!(get(app, "primary", "crud/raw"), json!(raw));
        assert_eq!(app.call(&json!({"op":"exists", "key":"crud/raw"})), true);
        put(app, "primary", "crud/raw", b"overwritten");
        assert_eq!(
            get(app, "primary", "crud/raw"),
            json!(b"overwritten".as_slice())
        );
        put(app, "primary", "crud/empty", b"");
        assert_eq!(get(app, "primary", "crud/empty"), json!([]));
        assert_eq!(app.call(&json!({"op":"exists", "key":"crud/empty"})), true);
        for key in ["crud/raw", "crud/empty", "crud/missing"] {
            assert_eq!(app.call(&json!({"op":"delete", "key":key})), true);
            assert_eq!(get(app, "primary", key), Value::Null);
            assert_eq!(app.call(&json!({"op":"exists", "key":key})), false);
        }
        put(app, "primary", "isolation/same", b"primary");
        put(app, "secondary", "isolation/same", b"secondary");
        put(app, "primary", "isolation/only-primary", b"p");
        put(app, "secondary", "isolation/only-secondary", b"s");
        assert_eq!(
            get(app, "primary", "isolation/same"),
            json!(b"primary".as_slice())
        );
        assert_eq!(
            get(app, "secondary", "isolation/same"),
            json!(b"secondary".as_slice())
        );
        assert_eq!(get(app, "primary", "isolation/only-secondary"), Value::Null);
        assert_eq!(get(app, "secondary", "isolation/only-primary"), Value::Null);
        assert_eq!(
            traverse(app, "primary", "isolation/", 1),
            ["isolation/only-primary", "isolation/same"]
        );
        assert_eq!(
            traverse(app, "secondary", "isolation/", 1),
            ["isolation/only-secondary", "isolation/same"]
        );
        put(app, "primary", "unicode/\u{e9}", b"NFC");
        put(app, "primary", "unicode/e\u{301}", b"NFD");
        assert_eq!(
            get(app, "primary", "unicode/\u{e9}"),
            json!(b"NFC".as_slice())
        );
        assert_eq!(
            get(app, "primary", "unicode/e\u{301}"),
            json!(b"NFD".as_slice())
        );
        assert_eq!(
            traverse(app, "primary", "unicode/", 1),
            ["unicode/e\u{301}", "unicode/\u{e9}"]
        );
        let expected = [
            "prefix/01",
            "prefix/02",
            "prefix/03",
            "prefix/04",
            "prefix/05",
        ];
        for key in expected.into_iter().rev() {
            put(app, "primary", key, b"v");
        }
        put(app, "primary", "prefix-outside", b"excluded");
        assert_eq!(traverse(app, "primary", "prefix/", 2), expected);
    }

    fn prove_kv_large_pages(app: &mut App) {
        phase("AWS emulator proof: DynamoDB 1 MiB page boundary with keys-only projection");
        let large_keys = (0_u8..6_u8)
            .map(|index| format!("large/{index}"))
            .collect::<Vec<_>>();
        for (index, key) in large_keys.iter().enumerate() {
            assert_eq!(
                app.call(&json!({"op":"large_put", "key":key, "byte":index})),
                true
            );
        }
        let first = list(app, "primary", "large/", &Value::Null, 100);
        let count = first["keys"].as_array().expect("large keys").len();
        assert!(
            count > 0 && count < large_keys.len(),
            "service byte cap must produce a short first page: {first}"
        );
        assert!(
            first["cursor"].is_string(),
            "short service page retains continuation"
        );
        assert_eq!(traverse(app, "primary", "large/", 100), large_keys);
        for (index, key) in large_keys.iter().enumerate() {
            let byte = u8::try_from(index).expect("six fixture values");
            assert_eq!(
                get(app, "primary", key),
                json!(vec![byte; 256 * 1024]),
                "large value exact bytes"
            );
        }
    }

    fn prove_kv_expiry(app: &mut App, services: &Services) {
        phase(
            "AWS emulator proof: expiry filtering does not delete; renewal writes both TTL fields",
        );
        let expired = list(app, "primary", "expired/", &Value::Null, 1);
        assert_eq!(expired["keys"], json!([]));
        assert!(
            expired["cursor"].is_string(),
            "empty filtered page must keep service cursor"
        );
        let alive = list(app, "primary", "expired/", &expired["cursor"], 1);
        assert_eq!(alive["keys"], json!(["expired/1"]));
        assert_eq!(traverse(app, "primary", "expired/", 1), ["expired/1"]);
        assert_eq!(get(app, "primary", "expired/0"), Value::Null);
        assert_eq!(app.call(&json!({"op":"exists", "key":"expired/0"})), false);
        let stored = services.admin("item", &json!({"namespace":"primary", "key":"expired/0"}));
        assert_eq!(
            stored["present"], true,
            "expired read must not physically delete"
        );
        assert_eq!(stored["expires_at_ms"], 1_u64);
        assert_eq!(stored["ttl_epoch_s"], 1_u64);
        assert_eq!(get(app, "primary", "expired/1"), json!(b"live".as_slice()));
        assert_eq!(get(app, "primary", "renew/item"), Value::Null);
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("epoch")
            .as_millis();
        assert_eq!(
            app.call(&json!({"op":"ttl", "key":"renew/item", "value":b"renewed".as_slice()})),
            true
        );
        assert_eq!(
            get(app, "primary", "renew/item"),
            json!(b"renewed".as_slice())
        );
        let renewed = services.admin("item", &json!({"namespace":"primary", "key":"renew/item"}));
        assert_eq!(renewed["present"], true);
        assert_eq!(renewed["value"], json!(b"renewed".as_slice()));
        let expiration_ms = u128::from(renewed["expires_at_ms"].as_u64().expect("millisecond TTL"));
        let ttl = u128::from(
            renewed["ttl_epoch_s"]
                .as_u64()
                .expect("service TTL seconds"),
        );
        assert!(
            expiration_ms >= before.checked_add(60_000).expect("bounded epoch threshold"),
            "renewed TTL is at least 60 seconds"
        );
        assert_eq!(
            ttl,
            expiration_ms.div_ceil(1000),
            "TTL fields describe the same expiry"
        );
        put(app, "primary", "renew/item", b"permanent");
        let permanent = services.admin("item", &json!({"namespace":"primary", "key":"renew/item"}));
        assert_eq!(
            permanent,
            json!({"present":true, "value":b"permanent".as_slice(), "expires_at_ms":null, "ttl_epoch_s":null}),
            "normal overwrite must clear both expiration fields"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the single-build, startup and restart lifecycle together as one end-to-end proof."
    )]
    #[ignore = "requires pinned local services; run node scripts/run_native_aws_integration.mjs"]
    fn generated_native_aws_emulator_contracts() {
        let services = Services::required();
        phase("AWS emulator proof: seed independent services and scaffold real project");
        let seed = services.admin("seed", &json!({}));
        let typed_a = services.publish("message-A", RAW_A);
        services.wait_agent("raw", RAW_A);
        services.wait_agent("typed", &typed_a);
        let temp = TempDir::new().expect("temporary generated project");
        let mut new = Command::new(env!("CARGO_BIN_EXE_edgezero"));
        new.args(["new", "native-aws-probe", "--dir"])
            .arg(temp.path());
        checked(new, WAIT, "real edgezero new");
        let project = temp.path().join("native-aws-probe");
        install_consumer(&project);
        let target = env::var_os("EDGEZERO_GENERATED_TEST_TARGET_DIR").map_or_else(
            || project.join("target"),
            |value| {
                let path = PathBuf::from(value);
                if path.is_absolute() {
                    path
                } else {
                    project.join(path)
                }
            },
        );
        phase("AWS emulator proof: one generated all-provider build");
        let mut build = Command::new(env!("CARGO_BIN_EXE_edgezero"));
        build
            .args(["build", "--adapter", "axum", "--", "--offline"])
            .current_dir(&project)
            .env("CARGO_TARGET_DIR", &target);
        checked(build, BUILD_WAIT, "single combined generated build");
        let binary = target.join("debug/native-aws-probe-adapter-axum");
        assert!(binary.is_file(), "generated executable");
        let table = seed["table"].as_str().expect("seed table");
        let bindings = write_bindings(&project, &services, &seed, table, "typed", "valid-bindings");
        phase("AWS emulator proof: enabled invalid resources fail before listener");
        for field in ["missing_table", "wrong_table", "indexed_table"] {
            let bad = write_bindings(
                &project,
                &services,
                &seed,
                seed[field].as_str().expect("negative table"),
                "typed",
                field,
            );
            App::spawn(&project, &binary, &bad, &services, &services.token).rejects();
        }
        App::spawn(
            &project,
            &binary,
            &bindings,
            &services,
            "wrong-local-agent-token",
        )
        .rejects();
        let missing = write_bindings(
            &project,
            &services,
            &seed,
            table,
            "missing-profile",
            "missing-profile",
        );
        App::spawn(&project, &binary, &missing, &services, &services.token).rejects();

        phase("AWS emulator proof: combined typed initializer, exact config and secret snapshots");
        let mut first = App::spawn(&project, &binary, &bindings, &services, &services.token);
        first.ready("secret-A");
        snapshot(&mut first, "message-A", RAW_A, "secret-A");
        prove_kv_crud_and_namespaces(&mut first);
        prove_kv_large_pages(&mut first);
        prove_kv_expiry(&mut first, &services);
        phase(
            "AWS emulator proof: rotate services, independently observe Agent B, retain running A",
        );
        services.admin("rotate", &json!({}));
        let typed_b = services.publish("message-B", RAW_B);
        services.wait_agent("raw", RAW_B);
        services.wait_agent("typed", &typed_b);
        snapshot(&mut first, "message-A", RAW_A, "secret-A");
        drop(first);

        phase("AWS emulator proof: restart sees B, pinned version stays A, KV persists");
        let mut second = App::spawn(&project, &binary, &bindings, &services, &services.token);
        second.ready("secret-B");
        snapshot(&mut second, "message-B", RAW_B, "secret-B");
        assert_eq!(
            get(&mut second, "primary", "renew/item"),
            json!(b"permanent".as_slice())
        );
        assert_eq!(
            get(&mut second, "primary", "isolation/same"),
            json!(b"primary".as_slice())
        );
        assert_eq!(
            get(&mut second, "secondary", "isolation/same"),
            json!(b"secondary".as_slice())
        );
        assert_eq!(
            second.call(&json!({"op":"delete", "key":"renew/item"})),
            true
        );
        assert_eq!(get(&mut second, "primary", "renew/item"), Value::Null);
        assert_eq!(
            get(&mut second, "primary", "isolation/same"),
            json!(b"primary".as_slice())
        );
        assert_eq!(
            get(&mut second, "primary", "expired/1"),
            json!(b"live".as_slice())
        );
        phase("AWS emulator proof: deletion preserves loaded B but fresh startup fails closed");
        services.admin("delete_secret", &json!({}));
        snapshot(&mut second, "message-B", RAW_B, "secret-B");
        drop(second);
        App::spawn(&project, &binary, &bindings, &services, &services.token).rejects();
        assert!(
            !project.join("must-not-create-data").exists(),
            "explicit AWS stores must not create local DATA_DIR"
        );
        phase("AWS emulator proof: all independent local-service contracts passed");
    }
}
