//! Real-ingress forwarding controls; the adapter supplies the original request and client.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use edgezero_core::app::App;
use edgezero_core::context::{BodyKind, RequestContext};
use edgezero_core::extractor::State;
use edgezero_core::http::{Method, Response, Uri};
use edgezero_core::ingress::{AdmissionDecision, IngressGrant};
use edgezero_core::outbound::{
    OutboundBatchTermination, OutboundCollectedSlot, OutboundImageOptimizerOptions,
    OutboundRedirectAction, OutboundRedirectLimit, OutboundRequest, OutboundResponse,
};
use edgezero_core::response_egress::ResponseEgressCompletion;
use edgezero_core::router::RouterService;
use edgezero_core::{Body, BodyStream, EdgeError, PlatformMetadata, action};
use futures::Stream;
use serde_json::{Value, json};

#[derive(Default)]
struct Counts {
    polls: u64,
    bytes: u64,
    eofs: u64,
    drops: u64,
    nonempty: u64,
}

#[derive(Clone, Copy)]
pub struct BackendSnapshot(pub fn() -> Value);

struct Source {
    inner: BodyStream,
    counts: Arc<Mutex<Counts>>,
}

impl Stream for Source {
    type Item = Result<Bytes, EdgeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.counts.lock().unwrap().polls += 1;
        let polled = self.inner.as_mut().poll_next(cx);
        match &polled {
            Poll::Ready(Some(Ok(bytes))) => {
                let mut counts = self.counts.lock().unwrap();
                counts.bytes += bytes.len() as u64;
                counts.nonempty += u64::from(!bytes.is_empty());
            }
            Poll::Ready(None) => self.counts.lock().unwrap().eofs += 1,
            _ => {}
        }
        polled
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.counts.lock().unwrap().drops += 1;
    }
}

pub fn application(platform: PlatformMetadata, signal: Value) -> App {
    let mut router = RouterService::builder().with_state(signal);
    for method in [
        Method::GET,
        Method::HEAD,
        Method::POST,
        Method::PUT,
        Method::PATCH,
        Method::OPTIONS,
        Method::DELETE,
    ] {
        router = router.route("/progress/forward/{case}", method, forward);
    }
    let mut app = App::with_name_and_platform(router.build(), "ingress-forwarding", platform);
    app.set_ingress_admission_policy(|head| AdmissionDecision::Admit {
        completion: ResponseEgressCompletion::empty(),
        grant: IngressGrant::empty(),
        read_deadline: head.read_deadline_after(if head.target().path().ends_with("/late") {
            Duration::ZERO
        } else {
            Duration::from_secs(5)
        }),
    });
    app
}

fn failure() -> EdgeError {
    EdgeError::internal(std::io::Error::other("ingress forwarding proof failed"))
}

#[action]
async fn forward(ctx: RequestContext, State(signal): State<Value>) -> Result<Response, EdgeError> {
    let backend_snapshot = ctx.extensions().get::<BackendSnapshot>().copied();
    let backend_before = backend_snapshot.map(|snapshot| (snapshot.0)());
    let case = ctx
        .path_params()
        .get("case")
        .map(str::to_owned)
        .ok_or_else(failure)?;
    let token = ctx
        .headers()
        .get("x-request-token")
        .and_then(|v| v.to_str().ok())
        .filter(|v| {
            !v.is_empty()
                && v.len() <= 64
                && v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        .ok_or_else(failure)?
        .to_owned();
    let target: Uri = ctx
        .headers()
        .get("x-origin")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(failure)?
        .parse()
        .map_err(|_| failure())?;
    if !crate::is_loopback_http(&target) || target.port_u16().is_none() {
        return Err(failure());
    }
    if ![
        "send",
        "reject",
        "begin",
        "begin-post",
        "batch",
        "unsupported",
        "follow",
        "late",
    ]
    .contains(&case.as_str())
        || target.query().is_some()
        || target.path()
            != format!(
                "/forward/{}/{token}",
                if matches!(case.as_str(), "begin" | "follow") {
                    case.as_str()
                } else {
                    "send"
                }
            )
    {
        return Err(failure());
    }
    let client = ctx.http_client().ok_or_else(failure)?;
    let before = format!("{:?}", ctx.body_kind());
    let cached = if case == "batch" {
        if !ctx.body_bytes(8).await?.is_empty() || ctx.body_kind() != (BodyKind::Cached { len: 0 })
        {
            return Err(failure());
        }
        true
    } else {
        false
    };
    let mut incoming = ctx.into_request()?;
    let counts = Arc::new(Mutex::new(Counts::default()));
    let original = std::mem::replace(incoming.body_mut(), Body::empty());
    let streamed = original.is_stream();
    *incoming.body_mut() = match original.into_stream() {
        Ok(inner) => Body::from_stream(Source {
            inner,
            counts: Arc::clone(&counts),
        }),
        Err(buffered) => buffered,
    };
    let mut request = OutboundRequest::from_request(incoming, target.clone())?
        .timeout(Duration::from_secs(3))
        .max_request_body_bytes(8)
        .max_encoded_response_bytes(64)
        .max_decoded_response_bytes(64)
        .max_response_bytes(64);
    let mut begun = false;
    let mut redirects = 0;
    let outcome: Result<OutboundResponse, EdgeError> = match case.as_str() {
        "begin" | "begin-post" | "unsupported" => {
            if case == "unsupported" {
                request = request.image_optimizer(
                    OutboundImageOptimizerOptions::new()
                        .region(edgezero_core::OutboundImageOptimizerRegion::Asia),
                );
            }
            match client.begin(request).await {
                Err(error) => Err(error),
                Ok(pending) => {
                    begun = true;
                    let confirm = format!(
                        "http://{}/forward/confirm/{token}",
                        target.authority().ok_or_else(failure)?
                    );
                    let receipt = client
                        .send(
                            OutboundRequest::get(confirm)?
                                .header("x-request-token", token.clone())?
                                .timeout(Duration::from_secs(3))
                                .max_response_bytes(64),
                        )
                        .await?;
                    if receipt.status().as_u16() != 204 {
                        return Err(failure());
                    }
                    pending.collect().await
                }
            }
        }
        "batch" => {
            let result = client
                .send_all_until(
                    vec![request],
                    edgezero_core::Deadline::after(Duration::from_secs(3)),
                )
                .await
                .map_err(|_| failure())?;
            let (mut slots, termination) = result.into_parts();
            if termination != OutboundBatchTermination::Completed || slots.len() != 1 {
                return Err(failure());
            }
            match slots.pop().ok_or_else(failure)? {
                OutboundCollectedSlot::Resolved(slot) => slot.outcome,
                _ => return Err(failure()),
            }
        }
        "follow" => match client
            .follow(request, OutboundRedirectLimit::new(2)?, |_| {
                Ok::<_, Infallible>(OutboundRedirectAction::FollowPrepared)
            })
            .await
        {
            Ok(result) => {
                redirects = result.redirects_followed();
                Ok(result.into_parts().0)
            }
            Err(edgezero_core::OutboundFollowFailure::Hop { error, .. }) => Err(error),
            Err(edgezero_core::OutboundFollowFailure::Policy { error, .. }) => match error {},
        },
        _ => client.send(request).await,
    };
    let (mut response, error, rejection) = match outcome {
        Ok(response) => (response.into_response()?, None, None),
        Err(error) => {
            let rejection = match error.message().as_str() {
                "inbound body read deadline exceeded" => "inbound_timeout",
                "GET/HEAD request must not carry a body" => "get_body",
                "begin requires a buffered request body; use send for a streamed upload" => {
                    "streamed_begin"
                }
                "image_optimizer is not supported by fastly batches"
                | "image_optimizer is not supported by cloudflare"
                | "image_optimizer is not supported by spin" => "unsupported_option",
                _ => "other",
            };
            let mut response = Response::new(Body::empty());
            *response.status_mut() = error.status();
            (response, Some(error.kind()), Some(rejection))
        }
    };
    let counts = counts.lock().unwrap();
    let report = json!({"case":case,"token":token,"before":before,"cached":cached,
        "streamed":streamed,"source":{"polls":counts.polls,"bytes":counts.bytes,"eofs":counts.eofs,"drops":counts.drops,"nonempty":counts.nonempty},
        "begun":begun,"redirects":redirects,"error":error,"rejection":rejection,"host_signal":signal,
        "backend_before":backend_before,"backend_after":backend_snapshot.map(|snapshot| (snapshot.0)()),"qualification":false});
    let report = report.to_string();
    if report.len() > 2048 {
        return Err(failure());
    }
    response
        .headers_mut()
        .insert("x-forward-report", report.parse().map_err(|_| failure())?);
    Ok(response)
}
