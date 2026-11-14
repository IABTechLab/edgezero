//! Explicit v2 public-handle proof against a disposable rebuilt service.
#![cfg(all(not(target_arch = "wasm32"), feature = "records-v2"))]

#[cfg(test)]
mod tests {
    #![expect(
        clippy::arbitrary_source_item_ordering,
        reason = "keep local service and lost-reply fixture lifecycles before public contract assertions"
    )]

    use std::env;
    use std::fs::{self, File};
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use edgezero_core::key_value_store::{KvError, KvHandle};
    use edgezero_core::record_store::{
        RecordKeyLayout, RecordLayout, RecordWrite, RecordWriteMode, RecordWriteOutcome,
        contract::assert_record_contract,
    };
    use edgezero_core::time::MonotonicClock;
    use edgezero_store_celld::CelldPreparation;
    use edgezero_store_celld::settings::{CelldAccess, CelldKvSettings, CelldPreparationLimits};
    use serde_json::json;
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
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }

    async fn service_fixture() -> (String, Option<LocalService>) {
        if let Some(endpoint) = env::var_os("CELLD_RECORDS_V2_ENDPOINT") {
            return (
                endpoint
                    .into_string()
                    .expect("fixture endpoint must be UTF-8"),
                None,
            );
        }
        let binary = env::var_os("CELLD_BIN").map(PathBuf::from).expect(
        "explicit CELLD_RECORDS_V2_ENDPOINT or rebuilt CELLD_BIN required; never a passing skip",
    );
        assert!(
            binary.is_absolute() && binary.is_file(),
            "CELLD_BIN must select an absolute executable"
        );
        let directory = TempDir::new().expect("disposable celld fixture");
        let policy = directory.path().join("policy.json");
        fs::write(&policy, serde_json::to_vec(&json!({
        "version":1_u8,"listen":"127.0.0.1:0",
        "stores":[{"alias":"identity","namespace":"edgezero-records-v2-interop"}],
        "access":{"mode":"trusted-network","grants":[{"store":"identity","operations":["read","write","delete","list"]}]}
    })).unwrap()).unwrap();
        let log = directory.path().join("celld.log");
        let output = File::create(&log).unwrap();
        let child = Command::new(binary)
            .current_dir(directory.path())
            .env_clear()
            .env("TMPDIR", env::temp_dir())
            .env("CELLD_TEST_DATA_DIR", directory.path().join("data"))
            .args([
                "--listen",
                "127.0.0.1:0",
                "--internal-listen",
                "127.0.0.1:0",
                "--store-api-config",
            ])
            .arg(policy)
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .expect("start explicit rebuilt celld");
        let mut service = LocalService {
            child,
            _directory: directory,
        };
        let end = Instant::now().checked_add(Duration::from_secs(30)).unwrap();
        loop {
            let text = fs::read_to_string(&log).unwrap();
            if let Some(address) = text.lines().find_map(|line| {
                line.split_once("celld scoped stores listening on ")
                    .map(|(_, address)| address.trim())
            }) {
                return (format!("http://{address}"), Some(service));
            }
            if let Some(status) = service.child.try_wait().unwrap() {
                panic!("celld startup {status}: {text}");
            }
            assert!(
                Instant::now() < end,
                "celld fixture startup deadline: {text}"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    struct Layout;
    impl RecordKeyLayout for Layout {
        fn id(&self) -> &'static str {
            "celld-v2-interop"
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

    impl LoseMutationReply {
        async fn start(upstream: &str) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let destination = upstream.to_owned();
            let mutations = Arc::new(AtomicUsize::new(0));
            let seen = Arc::clone(&mutations);
            let task = tokio::spawn(async move {
                let client = reqwest::Client::builder().no_proxy().build().unwrap();
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut socket = BufReader::new(stream);
                    let mut line = String::new();
                    if socket.read_line(&mut line).await.unwrap() == 0 {
                        continue;
                    }
                    let mut parts = line.split_whitespace();
                    let method =
                        reqwest::Method::from_bytes(parts.next().unwrap().as_bytes()).unwrap();
                    let target = parts.next().unwrap().to_owned();
                    let mut length = 0;
                    loop {
                        line.clear();
                        socket.read_line(&mut line).await.unwrap();
                        if line == "\r\n" {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':')
                            && name.eq_ignore_ascii_case("content-length")
                        {
                            length = value.trim().parse().unwrap();
                        }
                    }
                    let mut body = vec![0; length];
                    socket.read_exact(&mut body).await.unwrap();
                    let reply = client
                        .request(method.clone(), format!("{destination}{target}"))
                        .header("content-type", "application/json")
                        .body(body)
                        .send()
                        .await
                        .unwrap();
                    if method == reqwest::Method::PUT || method == reqwest::Method::DELETE {
                        assert_eq!(
                            reply.status(),
                            204,
                            "upstream must acknowledge commit before dropping response"
                        );
                        seen.fetch_add(1, Ordering::SeqCst);
                        if method == reqwest::Method::DELETE {
                            // A replayed delete would erase this independently recreated value.
                            let recreated = client.put(format!("{destination}{target}")).json(&json!({"body":"cmVjcmVhdGVk","metadata":null,"ttl_ms":null,"mode":"overwrite"})).send().await.unwrap();
                            assert_eq!(recreated.status(), 204);
                        }
                        continue;
                    }
                    let status = reply.status().as_u16();
                    let content_type = reply.headers()["content-type"].to_str().unwrap().to_owned();
                    let store_version = reply.headers()["x-celld-store-version"]
                        .to_str()
                        .unwrap()
                        .to_owned();
                    let record_version = reply.headers()["x-celld-record-version"]
                        .to_str()
                        .unwrap()
                        .to_owned();
                    let response_body = reply.bytes().await.unwrap();
                    let head = format!(
                        "HTTP/1.1 {status} Fixture\r\ncontent-type: {content_type}\r\nx-celld-store-version: {store_version}\r\nx-celld-record-version: {record_version}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
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
    #[ignore = "requires explicit rebuilt CELLD_BIN or fresh CELLD_RECORDS_V2_ENDPOINT, alias identity"]
    async fn real_celld_records_v2_public_contract_races_and_lost_acknowledgement() {
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
            "one original PUT and one original DELETE, no replay"
        );
        kv.delete("uncertain/key").await.unwrap();
        maximum_length_key_pages(&kv).await;
    }
}
