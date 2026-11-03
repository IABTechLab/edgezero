#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "the private native-driver companion is grouped by execution flow"
)]

use super::super::*;
use crate as core_api;
use crate::http::HeaderName;
use serde_json::{Value, json};

#[path = "capture.rs"]
mod capture;
#[path = "oracle.rs"]
mod oracle;
#[path = "requests.rs"]
mod requests;
#[path = "vectors.rs"]
mod vectors;

struct ScriptClient {
    parts: Arc<Mutex<Vec<Value>>>,
    reads: Arc<AtomicUsize>,
    time: TestTime,
}

fn scripted_error(phase: crate::ProviderFailurePhase) -> EdgeError {
    EdgeError::bad_gateway_with_detail(
        "scripted native failure",
        BadGatewayReason::Transport,
        crate::ProviderFailureDetail::Axum {
            phase,
            cause: crate::AxumFailureCause {
                io_kind: Some(ErrorKind::UnexpectedEof),
                is_connect: false,
                is_timeout: false,
                kind: crate::AxumFailureKind::Body,
            },
        },
    )
}

impl ScriptClient {
    fn head(&self, request: OutboundDispatchRequest) -> Result<OutboundHead, EdgeError> {
        let deadline = request.budget().deadline.instant();
        let (parts, context) = request.into_parts();
        let components = parts.uri.path().split('/').collect::<Vec<_>>();
        let case = vectors::find(components.get(2).copied().unwrap_or_default())
            .ok_or_else(|| EdgeError::bad_request("unknown core parity case"))?;
        let token = components.get(3).copied().unwrap_or_default();
        let mut recorded = requests::settings(&parts);
        recorded["normalized_private"] = json!(parts.headers.contains_key("x-private"));
        self.parts
            .lock()
            .map_err(|error| EdgeError::internal(IoError::other(error.to_string())))?
            .push(recorded);
        let reply = case.reply(token);
        if reply.head_delay_ms != 0 {
            self.time.advance_to(
                self.time
                    .now()
                    .checked_add(Duration::from_millis(reply.head_delay_ms))
                    .expect("bounded head delay")
                    .min(deadline),
            );
        }
        if reply.close_head {
            return Err(scripted_error(crate::ProviderFailurePhase::Send));
        }
        let mut headers = HeaderMap::new();
        for (name, value) in reply.fields {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).map_err(EdgeError::internal)?,
                HeaderValue::from_str(&value).map_err(EdgeError::internal)?,
            );
        }
        if let Some(length) = reply.declared {
            headers.insert(
                "content-length",
                HeaderValue::from_str(&length.to_string()).map_err(EdgeError::internal)?,
            );
        } else {
            headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        }
        let reads = Arc::clone(&self.reads);
        let time = self.time.clone();
        let mut phase = 0_u8;
        let source = stream::poll_fn(move |_context| {
            reads.fetch_add(1, Ordering::SeqCst);
            if phase == 0 {
                phase = 1;
                if reply.body_delay_ms != 0 {
                    time.advance_to(
                        time.now()
                            .checked_add(Duration::from_millis(reply.body_delay_ms))
                            .expect("bounded body delay")
                            .min(deadline),
                    );
                }
                Poll::Ready(Some(Ok(Bytes::copy_from_slice(&reply.body))))
            } else if phase == 1 && reply.close_body {
                phase = 2;
                Poll::Ready(Some(Err(scripted_error(
                    crate::ProviderFailurePhase::ResponseBodyRead,
                ))))
            } else {
                Poll::Ready(None)
            }
        });
        OutboundHead::new(
            StatusCode::from_u16(reply.status).map_err(EdgeError::internal)?,
            OutboundHeaderInput::new(
                "core",
                headers,
                OutboundHeaderFidelity::AdapterVisibleFields,
            ),
            Box::pin(source),
            context,
            no_owner(),
        )
    }
}

impl OutboundHttpClient for ScriptClient {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        let head = self.head(request);
        Ok(OutboundPending::new(
            Box::pin(async move { head?.finish().await }),
            no_owner(),
        ))
    }
    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        Box::pin(ready(self.head(request)))
    }
    fn progress(&self) -> Rc<dyn super::super::super::OutboundProgressRuntime> {
        Rc::new(super::super::super::IndependentOutboundProgress)
    }
    fn start_batch_until(&self, _batch: OutboundPreparedBatch) -> OutboundBatch {
        panic!("paired contract does not use batch dispatch")
    }
    fn time(&self) -> OutboundTime {
        self.time.pair()
    }
    fn validate_supported_options(
        &self,
        _request: &OutboundRequest,
        _mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        Ok(())
    }
}

async fn operation(
    client: &HttpClient,
    case: vectors::Case,
    time: &TestTime,
    reads: &Arc<AtomicUsize>,
    begin: bool,
) -> Value {
    reads.store(0, Ordering::SeqCst);
    let entered = time.now();
    let uri = format!("http://127.0.0.1:4001/transport/{}/core-token", case.name)
        .parse()
        .expect("controlled URI");
    let request = requests::make(
        case,
        uri,
        "core-token",
        if begin { "begin" } else { "send" },
        entered,
    )
    .expect("contract request");
    let parts = request.into_parts();
    let settings = requests::settings(&parts);
    let request = OutboundRequest::from_parts(parts).expect("request parts");
    let result = if begin {
        match client.begin(request).await {
            Ok(pending) => pending.collect().await,
            Err(error) => Err(error),
        }
    } else {
        client.send(request).await
    };
    capture::collect(
        result,
        time.pair().clock(),
        entered,
        settings,
        case.client_marker(),
        || json!({"reads":reads.load(Ordering::SeqCst),"fixture":"scripted-core"}),
    )
    .await
    .expect("bounded capture")
}

#[test]
fn every_original_policy_and_extended_buffered_upload_contract_has_begin_send_parity() {
    block_on(async {
        for case in vectors::all() {
            if case.cancels_response() {
                assert!(oracle::error_spec(case, "core").is_some(), "{}", case.name);
            }
            let time = TestTime::new(MonotonicInstant::now());
            let reads = Arc::new(AtomicUsize::new(0));
            let parts = Arc::new(Mutex::new(Vec::new()));
            let client = HttpClient::with_client(ScriptClient {
                parts: Arc::clone(&parts),
                reads: Arc::clone(&reads),
                time: time.clone(),
            })
            .with_proxy_marker(case.client_marker());
            let sent = operation(&client, case, &time, &reads, false).await;
            let begun = operation(&client, case, &time, &reads, true).await;
            let pair = json!({"send":sent,"begin":begun});
            if matches!(case.kind, vectors::Kind::Delay { .. }) {
                for mode in ["send", "begin"] {
                    assert_eq!(
                        pair[mode]["terminal_us"], 150_000_u64,
                        "{} {mode}",
                        case.name
                    );
                }
            }
            oracle::validate_pair(case, "core", "core-token", &pair).unwrap_or_else(|error| {
                panic!(
                    "{}: {error}; send={}; begin={}",
                    case.name, pair["send"]["outcome"], pair["begin"]["outcome"]
                )
            });
            let prepared = parts.lock().expect("prepared recorder");
            assert_eq!(
                prepared.len(),
                if case.expects_origin("core") { 2 } else { 0 },
                "{}",
                case.name
            );
            for request in prepared.iter() {
                let mut expected = sent["request"].clone();
                expected["proxy"] = json!(if matches!(
                    case.kind,
                    vectors::Kind::Marker(vectors::MarkerCase::On)
                ) || case.client_marker()
                    && !matches!(case.kind, vectors::Kind::Marker(vectors::MarkerCase::Off))
                {
                    "On"
                } else {
                    "Off"
                });
                expected["normalized_private"] = json!(false);
                assert_eq!(*request, expected, "{}", case.name);
            }
        }
    });
}
