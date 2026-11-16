//! Private real TCP fixture speaking the store service wire protocol. No transport mocking.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep request/reply models before fixture ownership and TCP serving"
)]
#![expect(
    clippy::field_scoped_visibility_modifiers,
    reason = "fixture records are inspected by sibling colocated test modules"
)]

use std::collections::{BTreeMap, VecDeque};
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{
    AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader,
};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, timeout};
use tokio_rustls::TlsAcceptor;
use url::Url;

/// Headers every service response carries, for hand-written replies.
pub(crate) const SERVICE_HEADERS: &str =
    "x-edgezero-store-version: 1\r\nx-edgezero-store-instance: AAAAAAAAAAAAAAAAAAAAAA\r\n";

pub(crate) struct Request {
    pub(crate) body: Vec<u8>,
    pub(crate) mutation_id: Option<String>,
    #[cfg(feature = "records")]
    pub(crate) expected_instance: Option<String>,
    #[cfg(feature = "records")]
    pub(crate) record_version: Option<String>,
    pub(crate) connection: usize,
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) authorization: Option<String>,
    pub(crate) encoding: Option<String>,
}

pub(crate) enum Reply {
    Bytes(Vec<u8>),
    Drop,
    Stall,
    SlowBody,
}

impl Reply {
    pub(crate) fn response(status: u16, content_type: &str, body: &[u8]) -> Self {
        Self::custom(status, content_type, SERVICE_HEADERS, body)
    }

    pub(crate) fn custom(status: u16, content_type: &str, headers: &str, body: &[u8]) -> Self {
        let mut reply = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{headers}\r\n", body.len()).into_bytes();
        reply.extend_from_slice(body);
        Self::Bytes(reply)
    }
}

#[derive(Default)]
struct State {
    requests: Vec<Request>,
    replies: VecDeque<Reply>,
    values: BTreeMap<(String, String), Vec<u8>>,
}

pub(crate) struct Fixture {
    endpoint: String,
    state: Arc<Mutex<State>>,
    task: JoinHandle<()>,
}

impl Fixture {
    pub(crate) async fn new(replies: Vec<Reply>) -> Self {
        Self::start(replies, None).await
    }

    pub(crate) async fn tls(certificate: &'static [u8], replies: Vec<Reply>) -> Self {
        use tokio_rustls::rustls::ServerConfig;
        use tokio_rustls::rustls::crypto::aws_lc_rs::default_provider;
        use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            include_bytes!("fixtures/tls-key.der").to_vec(),
        ));
        let config = ServerConfig::builder_with_provider(default_provider().into())
            .with_safe_default_protocol_versions()
            .expect("fixture provider supports default TLS protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![CertificateDer::from(certificate.to_vec())], key)
            .expect("fixture certificate matches the bundled private key");
        Self::start(replies, Some(TlsAcceptor::from(Arc::new(config)))).await
    }

    #[expect(
        clippy::integer_division_remainder_used,
        reason = "Tokio select generates bounded branch-index arithmetic"
    )]
    async fn start(replies: Vec<Reply>, acceptor: Option<TlsAcceptor>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture loopback listener");
        let scheme = if acceptor.is_some() { "https" } else { "http" };
        let endpoint = format!(
            "{scheme}://{}",
            listener.local_addr().expect("read bound fixture address")
        );
        let state = Arc::new(Mutex::new(State {
            replies: replies.into(),
            ..State::default()
        }));
        let server_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            let mut index = 0_usize;
            loop {
                tokio::select! {
                    biased;
                    accepted = listener.accept() => {
                        let Ok((stream, _address)) = accepted else { break; };
                        let connection_state = Arc::clone(&server_state);
                        let connection_acceptor = acceptor.clone();
                        index = index.checked_add(1).expect("fixture connection count must fit usize");
                        connections.spawn(async move {
                            if let Some(tls_acceptor) = connection_acceptor {
                                if let Ok(Ok(tls_stream)) = timeout(
                                    Duration::from_secs(3), tls_acceptor.accept(stream),
                                ).await {
                                    serve(tls_stream, connection_state, index).await;
                                }
                            } else {
                                serve(stream, connection_state, index).await;
                            }
                        });
                    }
                    Some(_result) = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            endpoint,
            state,
            task,
        }
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) async fn seed(&self, store: &str, key: &str, bytes: &[u8]) {
        self.state
            .lock()
            .await
            .values
            .insert((store.to_owned(), key.to_owned()), bytes.to_vec());
    }

    pub(crate) async fn value(&self, store: &str, key: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .await
            .values
            .get(&(store.to_owned(), key.to_owned()))
            .cloned()
    }

    pub(crate) async fn count(&self) -> usize {
        self.state.lock().await.requests.len()
    }

    pub(crate) async fn requests(&self) -> Vec<Request> {
        mem::take(&mut self.state.lock().await.requests)
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request<S>(stream: &mut BufReader<S>, connection: usize) -> Option<Request>
where
    S: AsyncRead + Unpin,
{
    let mut first = String::new();
    if stream.read_line(&mut first).await.unwrap_or(0) == 0 {
        return None;
    }
    let mut parts = first.split_whitespace();
    let method = parts
        .next()
        .expect("fixture request line includes a method")
        .to_owned();
    let target = parts
        .next()
        .expect("fixture request line includes a target")
        .to_owned();
    let mut length = 0;
    let mut authorization = None;
    let mut encoding = None;
    let mut mutation_id = None;
    #[cfg(feature = "records")]
    let mut expected_instance = None;
    #[cfg(feature = "records")]
    let mut record_version = None;
    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
            return None;
        }
        if line == "\r\n" {
            break;
        }
        if let Some((name, raw_value)) = line.split_once(':') {
            let value = raw_value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                length = value
                    .parse()
                    .expect("fixture Content-Length is a valid length");
            }
            if name.eq_ignore_ascii_case("authorization") {
                authorization = Some(value.to_owned());
            }
            if name.eq_ignore_ascii_case("accept-encoding") {
                encoding = Some(value.to_owned());
            }
            if name.eq_ignore_ascii_case("x-edgezero-mutation-id") {
                mutation_id = Some(value.to_owned());
            }
            #[cfg(feature = "records")]
            if name.eq_ignore_ascii_case("x-edgezero-expected-instance") {
                expected_instance = Some(value.to_owned());
            }
            #[cfg(feature = "records")]
            if name.eq_ignore_ascii_case("x-edgezero-record-version") {
                record_version = Some(value.to_owned());
            }
        }
    }
    let mut body = vec![0; length];
    if stream.read_exact(&mut body).await.is_err() {
        return None;
    }
    Some(Request {
        body,
        mutation_id,
        #[cfg(feature = "records")]
        expected_instance,
        #[cfg(feature = "records")]
        record_version,
        connection,
        method,
        target,
        authorization,
        encoding,
    })
}

async fn serve<S>(socket: S, state: Arc<Mutex<State>>, connection: usize)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut stream = BufReader::new(socket);
    while let Some(request) = read_request(&mut stream, connection).await {
        let reply = {
            let mut locked = state.lock().await;
            let dynamic = apply(&request, &mut locked.values);
            locked.requests.push(request);
            locked.replies.pop_front().unwrap_or(dynamic)
        };
        match reply {
            Reply::Bytes(bytes) => {
                if stream.get_mut().write_all(&bytes).await.is_err() {
                    return;
                }
            }
            Reply::Drop => return,
            Reply::Stall => {
                sleep(Duration::from_secs(5)).await;
                return;
            }
            Reply::SlowBody => {
                let head = format!(
                    "HTTP/1.1 200 Fixture\r\n{SERVICE_HEADERS}Content-Type: application/octet-stream\r\nContent-Length: 2\r\n\r\nx"
                );
                if stream.get_mut().write_all(head.as_bytes()).await.is_err() {
                    return;
                }
                sleep(Duration::from_secs(5)).await;
                return;
            }
        }
    }
}

fn apply(request: &Request, values: &mut BTreeMap<(String, String), Vec<u8>>) -> Reply {
    let url = Url::parse(&format!("http://fixture{}", request.target))
        .expect("fixture request target forms a valid URL");
    let segments: Vec<_> = url
        .path_segments()
        .expect("fixture URL has path segments")
        .collect();
    let store = (*segments
        .get(2)
        .expect("fixture store path includes an alias"))
    .to_owned();
    let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
    let key = params.get("key").cloned().unwrap_or_default();
    let target = (store.clone(), key);
    match (request.method.as_str(), url.path().rsplit('/').next()) {
        ("POST", Some("put")) => {
            values.insert(target, request.body.clone());
            Reply::response(204, "application/octet-stream", b"")
        }
        ("POST", Some("delete")) => {
            values.remove(&target);
            Reply::response(204, "application/octet-stream", b"")
        }
        ("HEAD", _) => Reply::response(
            if values.contains_key(&target) {
                200
            } else {
                404
            },
            "application/octet-stream",
            b"",
        ),
        _ if url.path().ends_with("/keys") => {
            let prefix = params.get("prefix").map_or("", String::as_str);
            let after = params.get("cursor").map_or("", String::as_str);
            let limit: usize = params
                .get("limit")
                .expect("fixture page request has a limit")
                .parse()
                .expect("fixture page limit is numeric");
            let mut keys: Vec<_> = values
                .keys()
                .filter(|(alias, stored_key)| {
                    *alias == store && stored_key.starts_with(prefix) && stored_key.as_str() > after
                })
                .map(|(_, stored_key)| stored_key.clone())
                .collect();
            let more = keys.len() > limit;
            keys.truncate(limit);
            let cursor = if more { keys.last().cloned() } else { None };
            let bytes = serde_json::to_vec(&serde_json::json!({"keys": keys, "cursor": cursor}))
                .expect("serialize fixture page JSON");
            Reply::response(200, "application/json", &bytes)
        }
        _ => values.get(&target).map_or_else(
            || Reply::response(404, "application/json", br#"{"error":"not_found"}"#),
            |bytes| Reply::response(200, "application/octet-stream", bytes),
        ),
    }
}
