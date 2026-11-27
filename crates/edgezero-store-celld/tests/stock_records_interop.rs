//! Explicit public-handle proof on the owned service and official stock celld.
#![cfg(all(not(target_arch = "wasm32"), feature = "records"))]

#[cfg(test)]
mod tests {
    #![expect(
        clippy::arbitrary_source_item_ordering,
        reason = "keep local service and lost-reply fixture lifecycles before public contract assertions"
    )]

    use std::env;
    use std::fmt::Write as _;
    use std::fs::{self, File};
    use std::net::TcpListener as StdTcpListener;
    #[cfg(unix)]
    use std::os::unix::process::CommandExt as _;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use bytes::Bytes;
    use edgezero_core::key_value_store::{KvError, KvHandle};
    use edgezero_core::record_store::{
        RecordKeyLayout, RecordLayout, RecordWrite, RecordWriteMode, RecordWriteOutcome,
        contract::assert_record_contract,
    };
    use edgezero_core::time::MonotonicClock;
    use edgezero_store_celld::CelldPreparation;
    use edgezero_store_celld::settings::{CelldAccess, CelldKvSettings, CelldPreparationLimits};
    use reqwest::header::{
        CONNECTION, CONTENT_LENGTH, HeaderMap, HeaderName, HeaderValue, TRANSFER_ENCODING,
    };
    use reqwest::{Client, Method};
    use serde_json::json;
    use sha2::Digest as _;
    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use url::Url;

    struct LocalService {
        child: Child,
        _directory: TempDir,
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable and conflicts with the required drop method"
    )]
    impl Drop for LocalService {
        fn drop(&mut self) {
            #[cfg(unix)]
            let _group = Command::new("kill")
                .args(["-KILL", "--", &format!("-{}", self.child.id())])
                .status();
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }

    fn verify_official_binary(binary: &Path) {
        assert!(
            binary.is_absolute() && binary.is_file(),
            "explicit absolute celld executable required"
        );
        let version = Command::new(binary).arg("--version").output().unwrap();
        assert!(version.status.success());
        assert_eq!(
            String::from_utf8(version.stdout).unwrap().trim(),
            "celld 0.6.3"
        );
        let mut digest = String::with_capacity(64);
        for byte in sha2::Sha256::digest(fs::read(binary).unwrap()) {
            write!(&mut digest, "{byte:02x}").unwrap();
        }
        assert_eq!(
            digest, "eb0a8158421b69d68dd5d28f2298a7d411dde498295833deeb1cc5ce53b7eb75",
            "select the verified official release, not a modified binary"
        );
    }

    async fn service_fixture() -> (String, Option<LocalService>) {
        if let Some(endpoint) = env::var_os("EDGEZERO_CELLD_RECORDS_ENDPOINT") {
            return (
                endpoint
                    .into_string()
                    .expect("fixture endpoint must be UTF-8"),
                None,
            );
        }
        let binary = env::var_os("CELLD_BIN").map(PathBuf::from)
            .expect("explicit official CELLD_BIN or EDGEZERO_CELLD_RECORDS_ENDPOINT required; no passing skip");
        verify_official_binary(&binary);
        let source = env::var_os("EDGEZERO_CELLD_STORE_SERVICE")
            .map(PathBuf::from)
            .expect("explicit EDGEZERO_CELLD_STORE_SERVICE required");
        assert!(source.is_absolute());
        let directory = TempDir::new().expect("disposable stock celld fixture");
        fs::create_dir_all(directory.path().join("src")).unwrap();
        fs::copy(
            source.join("src/index.js"),
            directory.path().join("src/index.js"),
        )
        .unwrap();
        let policy = json!({
            "version":1_u8,
            "stores":[{"alias":"identity","namespace":"stock-record-interop"}],
            "access":{"mode":"trusted-network","grants":[{"store":"identity","operations":["read","write","delete","list"]}]}
        });
        let configuration = json!({
            "name":"edgezero-store","main":"src/index.js","no_bundle":true,
            "compatibility_date":"2026-10-01",
            "durable_objects":{"bindings":[{"name":"EDGEZERO_STORE","class_name":"EdgeZeroStore"}]},
            "migrations":[{"tag":"v1","new_sqlite_classes":["EdgeZeroStore"]}],
            "vars":{"EDGEZERO_STORE_POLICY":policy.to_string()}
        });
        fs::write(
            directory.path().join("wrangler.json"),
            serde_json::to_vec_pretty(&configuration).unwrap(),
        )
        .unwrap();
        let reserved = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let port = reserved.local_addr().unwrap().port();
        drop(reserved);
        let log = directory.path().join("celld.log");
        let output = File::create(&log).unwrap();
        let mut command = Command::new(binary);
        command
            .current_dir(directory.path())
            .env_clear()
            .env("PATH", env::var_os("PATH").unwrap_or_default())
            .env("HOME", directory.path().join("home"))
            .env("TMPDIR", env::temp_dir())
            .env("NO_COLOR", "1")
            .args([
                "dev",
                ".",
                "--no-watch",
                "--logs",
                "--port",
                &port.to_string(),
            ])
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output));
        #[cfg(unix)]
        command.process_group(0);
        let mut service = LocalService {
            child: command.spawn().expect("start official celld"),
            _directory: directory,
        };
        let end = Instant::now().checked_add(Duration::from_secs(30)).unwrap();
        loop {
            let text = fs::read_to_string(&log).unwrap();
            if let Some(address) = text.lines().find_map(|line| {
                let (_, address) = line.split_once("celld listening on ")?;
                address.split_whitespace().next()
            }) {
                assert_eq!(address, format!("127.0.0.1:{port}"));
                return (format!("http://{address}"), Some(service));
            }
            if let Some(status) = service.child.try_wait().unwrap() {
                panic!("stock celld startup {status}: {text}");
            }
            assert!(Instant::now() < end, "stock celld startup deadline: {text}");
            sleep(Duration::from_millis(10)).await;
        }
    }

    struct Layout;
    impl RecordKeyLayout for Layout {
        fn id(&self) -> &'static str {
            "stock-celld-record-interop"
        }
        fn version(&self) -> u32 {
            1
        }
        fn validate(&self) -> Result<(), KvError> {
            Ok(())
        }
        fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError> {
            key.find('/')
                .and_then(|index| key.get(..=index))
                .ok_or_else(|| KvError::Validation("missing interop group".to_owned()))
        }
        fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
            if prefix.ends_with('/') && prefix.matches('/').count() == 1 && prefix.len() > 1 {
                Ok(())
            } else {
                Err(KvError::Validation("invalid interop group".to_owned()))
            }
        }
    }

    async fn prepare(endpoint: &str) -> KvHandle {
        let mut preparation =
            CelldPreparation::new(MonotonicClock::default(), CelldPreparationLimits::default())
                .unwrap();
        preparation
            .prepare_record_store(
                &CelldKvSettings {
                    endpoint: endpoint.to_owned(),
                    access: CelldAccess::TrustedNetwork {
                        allow_remote_http: false,
                    },
                    store: "identity".to_owned(),
                },
                &RecordLayout::new(Layout).unwrap(),
                None,
            )
            .await
            .unwrap()
    }

    fn write(body: &[u8], mode: RecordWriteMode) -> RecordWrite<'_> {
        RecordWrite {
            body,
            metadata: None,
            ttl: None,
            mode,
        }
    }

    struct LoseMutationReply {
        endpoint: String,
        mutations: Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }
    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable and conflicts with the required drop method"
    )]
    impl Drop for LoseMutationReply {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn recreate_after_delete(client: &Client, destination: &str, mut headers: HeaderMap) {
        let mut url = Url::parse(destination).unwrap();
        url.set_path("/v1/stores/identity/record-put");
        headers.remove("x-edgezero-mutation-id");
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let recreated = client
            .post(url)
            .headers(headers)
            .header("x-edgezero-mutation-id", URL_SAFE_NO_PAD.encode(nonce))
            .json(&json!({"body":"cmVjcmVhdGVk","metadata":null,"ttl_ms":null,"mode":"overwrite"}))
            .send()
            .await
            .unwrap();
        assert_eq!(recreated.status(), 204);
    }

    impl LoseMutationReply {
        async fn start(upstream: &str) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let destination = upstream.to_owned();
            let mutations = Arc::new(AtomicUsize::new(0));
            let seen = Arc::clone(&mutations);
            let task = tokio::spawn(async move {
                let client = Client::builder().no_proxy().build().unwrap();
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut socket = BufReader::new(stream);
                    let mut line = String::new();
                    if socket.read_line(&mut line).await.unwrap() == 0 {
                        continue;
                    }
                    let mut parts = line.split_whitespace();
                    let method = Method::from_bytes(parts.next().unwrap().as_bytes()).unwrap();
                    let target = parts.next().unwrap().to_owned();
                    let mut length = 0;
                    let mut headers = HeaderMap::new();
                    loop {
                        line.clear();
                        socket.read_line(&mut line).await.unwrap();
                        if line == "\r\n" {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':') {
                            if name.eq_ignore_ascii_case("content-length") {
                                length = value.trim().parse().unwrap();
                                continue;
                            }
                            if !name.eq_ignore_ascii_case("host")
                                && !name.eq_ignore_ascii_case("connection")
                            {
                                headers.insert(
                                    HeaderName::from_bytes(name.as_bytes()).unwrap(),
                                    HeaderValue::from_str(value.trim()).unwrap(),
                                );
                            }
                        }
                    }
                    let mut body = vec![0; length];
                    socket.read_exact(&mut body).await.unwrap();
                    let reply = client
                        .request(method.clone(), format!("{destination}{target}"))
                        .headers(headers.clone())
                        .body(body)
                        .send()
                        .await
                        .unwrap();
                    if method == Method::POST {
                        assert_eq!(
                            reply.status(),
                            204,
                            "upstream committed before losing acknowledgement"
                        );
                        seen.fetch_add(1, Ordering::SeqCst);
                        if target
                            .split('?')
                            .next()
                            .unwrap()
                            .ends_with("/record-delete")
                        {
                            recreate_after_delete(
                                &client,
                                &format!("{destination}{target}"),
                                headers,
                            )
                            .await;
                        }
                        continue;
                    }
                    let status = reply.status().as_u16();
                    let mut response_headers = String::new();
                    for (name, value) in reply.headers() {
                        if name != CONTENT_LENGTH && name != TRANSFER_ENCODING && name != CONNECTION
                        {
                            write!(
                                &mut response_headers,
                                "{name}: {}\r\n",
                                value.to_str().unwrap()
                            )
                            .unwrap();
                        }
                    }
                    let response_body = reply.bytes().await.unwrap();
                    let head = format!(
                        "HTTP/1.1 {status} Fixture\r\n{response_headers}content-length: {}\r\nconnection: close\r\n\r\n",
                        response_body.len()
                    );
                    socket.get_mut().write_all(head.as_bytes()).await.unwrap();
                    socket.get_mut().write_all(&response_body).await.unwrap();
                }
            });
            Self {
                endpoint,
                mutations,
                task,
            }
        }
    }

    async fn maximum_length_key_pages(kv: &KvHandle) {
        let records = kv.require_records().unwrap();
        for prefix in [
            format!("{}ab/", "\u{e9}\"\\".repeat(127)),
            format!("{}/", "\"".repeat(510)),
        ] {
            assert_eq!(
                prefix.len(),
                511,
                "prefix length is measured in UTF-8 bytes"
            );
            let expected = [format!("{prefix}a"), format!("{prefix}b")];
            for key in &expected {
                assert_eq!(key.len(), 512);
                kv.put_bytes(key, Bytes::from_static(b"long-key"))
                    .await
                    .unwrap();
            }
            let partition = records.partition(&prefix).unwrap();
            let first = records.list_partition(&partition, None, 1).await.unwrap();
            assert_eq!(first.keys, [expected[0].clone()]);
            assert!(
                first.cursor.is_some(),
                "full first page must preserve native continuation"
            );
            let last = records
                .list_partition(&partition, first.cursor.as_deref(), 1)
                .await
                .unwrap();
            assert_eq!(last.keys, [expected[1].clone()]);
            assert!(
                last.cursor.is_none(),
                "lookahead must establish the exact-multiple end"
            );
            let ordinary_first = kv.list_keys_page(&prefix, None, 1).await.unwrap();
            assert_eq!(ordinary_first.keys, [expected[0].clone()]);
            assert!(ordinary_first.cursor.is_some());
            let ordinary_last = kv
                .list_keys_page(&prefix, ordinary_first.cursor.as_deref(), 1)
                .await
                .unwrap();
            assert_eq!(ordinary_last.keys, [expected[1].clone()]);
            assert!(ordinary_last.cursor.is_none());
            for key in &expected {
                kv.delete(key).await.unwrap();
            }
        }
    }

    async fn independent_races(kv: &KvHandle, endpoint: &str) {
        let independent = prepare(endpoint).await;
        let records = kv.require_records().unwrap();
        let other = independent.require_records().unwrap();
        let (add_left, add_right) = tokio::join!(
            records.insert("race/key", write(b"a", RecordWriteMode::Add)),
            other.insert("race/key", write(b"b", RecordWriteMode::Add))
        );
        let outcomes = [add_left.unwrap(), add_right.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|value| **value == RecordWriteOutcome::Written)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|value| **value == RecordWriteOutcome::PreconditionFailed)
                .count(),
            1
        );
        let generation = records
            .lookup("race/key")
            .await
            .unwrap()
            .unwrap()
            .generation;
        let mode = RecordWriteMode::IfGenerationMatch(generation);
        let (cas_left, cas_right) = tokio::join!(
            records.insert("race/key", write(b"a", mode)),
            other.insert("race/key", write(b"b", mode))
        );
        let cas_outcomes = [cas_left.unwrap(), cas_right.unwrap()];
        assert_eq!(
            cas_outcomes
                .iter()
                .filter(|value| **value == RecordWriteOutcome::Written)
                .count(),
            1
        );
        assert_eq!(
            cas_outcomes
                .iter()
                .filter(|value| **value == RecordWriteOutcome::PreconditionFailed)
                .count(),
            1
        );
        records.delete("race/key").await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires explicit official CELLD_BIN and EDGEZERO_CELLD_STORE_SERVICE, or a disposable EDGEZERO_CELLD_RECORDS_ENDPOINT with alias identity"]
    async fn stock_celld_public_contract_races_and_lost_acknowledgement() {
        let (endpoint, _service) = service_fixture().await;
        let url = Url::parse(&endpoint).unwrap();
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert!(url.port().is_some());
        let kv = prepare(&endpoint).await;
        assert_record_contract(&kv, "contract/").await;
        independent_races(&kv, &endpoint).await;
        kv.put_bytes("left/key", Bytes::from_static(b"left"))
            .await
            .unwrap();
        kv.put_bytes("right/key", Bytes::from_static(b"right"))
            .await
            .unwrap();
        let mut cursor = None;
        let mut keys = Vec::new();
        loop {
            let page = kv.list_keys_page("", cursor.as_deref(), 1).await.unwrap();
            keys.extend(page.keys);
            cursor = page.cursor;
            if cursor.is_none() {
                break;
            }
        }
        keys.sort();
        assert_eq!(keys, ["left/key", "right/key"]);
        for key in keys {
            kv.delete(&key).await.unwrap();
        }
        let proxy = LoseMutationReply::start(&endpoint).await;
        let through_proxy = prepare(&proxy.endpoint).await;
        assert!(matches!(
            through_proxy
                .put_bytes("uncertain/key", Bytes::from_static(b"committed"))
                .await,
            Err(KvError::MutationUncertain)
        ));
        assert_eq!(
            kv.get_bytes("uncertain/key")
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"committed"
        );
        assert_eq!(proxy.mutations.load(Ordering::SeqCst), 1);
        assert!(matches!(
            through_proxy.delete("uncertain/key").await,
            Err(KvError::MutationUncertain)
        ));
        assert_eq!(
            kv.get_bytes("uncertain/key")
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"recreated"
        );
        assert_eq!(
            proxy.mutations.load(Ordering::SeqCst),
            2,
            "one original record-put and one original record-delete, no replay"
        );
        kv.delete("uncertain/key").await.unwrap();
        maximum_length_key_pages(&kv).await;
    }
}
