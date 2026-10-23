use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use super::ResponseEgressFramingError;
use crate::action;
use crate::app::App;
use crate::body::Body;
use crate::error::EdgeError;
use crate::extractor::State;
use crate::http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use crate::ingress::{
    AdmissionDecision, IngressBeginOutcome, IngressDispatchOutcome, IngressFraming, IngressGrant,
    IngressHeadAccounting, IngressHeadParts,
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
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-synthesized-fixture",
        HeaderValue::from_static("configured"),
    );
    for _ in 0..30_u8 {
        headers.append("x-padding", HeaderValue::from_static("x"));
    }
    let used = headers.iter().try_fold(2_usize, |sum, (name, value)| {
        sum.checked_add(name.as_str().len())
            .and_then(|total| total.checked_add(value.as_bytes().len()))
            .and_then(|total| total.checked_add(4))
            .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("fixture header charge overflow")))
    })?;
    let padding = 4096_usize
        .checked_sub(used)
        .and_then(|remaining| remaining.checked_sub("x-padding".len()))
        .and_then(|remaining| remaining.checked_sub(4))
        .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("fixture header padding overflow")))?;
    headers.append(
        "x-padding",
        HeaderValue::from_str(&"a".repeat(padding)).map_err(EdgeError::internal)?,
    );
    app.set_synthesized_response_headers(headers)
        .map_err(EdgeError::internal)?;
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

/// Makes a typed source error after admission with competing renderer/configured retry fields.
///
/// # Errors
/// Returns the original construction/admission failure if the fixture cannot produce an envelope.
#[inline]
pub fn synthesized_error_fixture(error: EdgeError) -> Result<ResponseEgressEnvelope, EdgeError> {
    let app_router = RouterService::builder().build();
    let mut app = App::new(app_router);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-synthesized-fixture",
        HeaderValue::from_static("configured"),
    );
    headers.insert("retry-after", HeaderValue::from_static("180"));
    app.set_synthesized_response_headers(headers)
        .map_err(EdgeError::internal)?;
    app.set_error_response_renderer(|_| {
        let mut response = Response::new(Body::from("fixed framework error"));
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("240"));
        response
    });
    let request = Request::new(Body::empty());
    let head = IngressHeadParts::from_request(
        &request,
        IngressHeadAccounting::HostManaged,
        IngressFraming::HostManaged,
    );
    let IngressBeginOutcome::Admitted(prepared) = app.begin_ingress(head, app.monotonic_now())?
    else {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "source-error fixture was not admitted"
        )));
    };
    Ok(app.admitted_error_egress(prepared, error))
}

/// Verifies every configured value in the maximum-size fixture, ignoring converter-owned fields.
#[must_use]
#[inline]
pub fn has_full_synthesized_fixture_headers(headers: &HeaderMap) -> bool {
    let padding: Vec<_> = headers.get_all("x-padding").iter().collect();
    headers.get("x-synthesized-fixture") == Some(&HeaderValue::from_static("configured"))
        && headers.get_all("x-synthesized-fixture").iter().count() == 1
        && padding.len() == 31
        && padding
            .iter()
            .take(30)
            .all(|value| value.as_bytes() == b"x")
        && padding.last().is_some_and(|value| {
            value.as_bytes().len() == 3626 && value.as_bytes().iter().all(|byte| *byte == b'a')
        })
}
