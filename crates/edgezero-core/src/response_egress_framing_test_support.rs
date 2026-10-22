use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use super::ResponseEgressFramingError;
use crate::action;
use crate::app::App;
use crate::body::Body;
use crate::error::EdgeError;
use crate::extractor::State;
use crate::http::{HeaderValue, Method, Request, Response, StatusCode};
use crate::ingress::{
    AdmissionDecision, IngressDispatchOutcome, IngressFraming, IngressGrant, IngressHeadAccounting,
};
use crate::response_egress::{
    ResponseEgressCompletion, ResponseEgressEnvelope, ResponseEgressObserver, ResponseEgressReport,
};
use crate::router::RouterService;
use crate::time::{MonotonicClock, MonotonicInstant};

pub const FRAMING_RULES: [ResponseEgressFramingError; 6] = [
    ResponseEgressFramingError::ContentLengthMismatch,
    ResponseEgressFramingError::InvalidConnectionHeader,
    ResponseEgressFramingError::InvalidContentLength,
    ResponseEgressFramingError::UnsupportedStatus,
    ResponseEgressFramingError::UnsupportedTrailers,
    ResponseEgressFramingError::UnsupportedTunnel,
];

type Reports = Arc<Mutex<Vec<ResponseEgressReport>>>;

pub struct FramingFixture {
    pub completed: Reports,
    pub envelope: ResponseEgressEnvelope,
    pub observed: Reports,
}

struct Observer(Reports);

impl ResponseEgressObserver for Observer {
    #[inline]
    fn complete(&self, report: &ResponseEgressReport) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(report.clone());
    }
}

#[must_use]
#[inline]
pub fn framing_response(reason: ResponseEgressFramingError) -> (Method, Response) {
    let mut response = Response::new(Body::from("x"));
    let mut method = Method::GET;
    match reason {
        ResponseEgressFramingError::ContentLengthMismatch => {
            response
                .headers_mut()
                .insert("content-length", HeaderValue::from_static("2"));
        }
        ResponseEgressFramingError::InvalidConnectionHeader => {
            response.headers_mut().insert(
                "connection",
                HeaderValue::from_static("private token=secret"),
            );
        }
        ResponseEgressFramingError::InvalidContentLength => {
            response
                .headers_mut()
                .insert("content-length", HeaderValue::from_static("token=secret"));
        }
        ResponseEgressFramingError::UnsupportedStatus => {
            *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        }
        ResponseEgressFramingError::UnsupportedTrailers => {
            response
                .headers_mut()
                .insert("trailer", HeaderValue::from_static("private-token-secret"));
        }
        ResponseEgressFramingError::UnsupportedTunnel => method = Method::CONNECT,
    }
    (method, response)
}

#[action]
async fn invalid_response(
    State(reason): State<ResponseEgressFramingError>,
) -> Result<Response, EdgeError> {
    Ok(framing_response(reason).1)
}

/// Dispatches the same invalid response through a routed handler or admission refusal.
///
/// # Errors
/// Returns the unchanged framework dispatch error if the fixture cannot produce an envelope.
#[inline]
pub async fn framing_fixture(
    reason: ResponseEgressFramingError,
    detached: bool,
) -> Result<FramingFixture, EdgeError> {
    let now = MonotonicInstant::now();
    let (method, _) = framing_response(reason);
    let completed = Reports::default();
    let observed = Reports::default();
    let mut app = App::new(
        RouterService::builder()
            .with_state(reason)
            .route("/", method.clone(), invalid_response)
            .build(),
    );
    app.set_monotonic_clock(MonotonicClock::new(move || now));
    app.set_response_egress_observer(Observer(Arc::clone(&observed)));
    let completed_reports = Arc::clone(&completed);
    app.set_ingress_admission_policy(move |head| {
        let retained_reports = Arc::clone(&completed_reports);
        let completion = ResponseEgressCompletion::new(move |report| {
            retained_reports
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(report.clone());
        });
        if detached {
            AdmissionDecision::Refuse {
                completion,
                response: framing_response(reason).1,
            }
        } else {
            AdmissionDecision::Admit {
                completion,
                grant: IngressGrant::empty(),
                read_deadline: head.read_deadline_after(Duration::from_secs(1)),
            }
        }
    });
    let mut request = Request::new(Body::empty());
    *request.method_mut() = method;
    let outcome = app
        .dispatch_ingress(
            request,
            now,
            IngressHeadAccounting::HostManaged,
            IngressFraming::HostManaged,
        )
        .await?;
    let IngressDispatchOutcome::Response(envelope) = outcome else {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "framing fixture did not produce a response"
        )));
    };
    Ok(FramingFixture {
        completed,
        envelope: *envelope,
        observed,
    })
}
