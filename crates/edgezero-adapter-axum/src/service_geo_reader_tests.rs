//! Managed checked-IP dispatch retains the existing admission and terminal owner.

use super::tests::to_bytes;
use super::*;
use crate::geo::{NativeGeoSelection, ReaderIpExpectation};
use crate::proxy::ForwardingHeaderFamily;
use edgezero_core::Body;
use edgezero_core::context::RequestContext;
use edgezero_core::geo::{
    GeoInput, GeoIpSource, GeoProvider, GeoProviderId, GeoResolution, GeoStatus,
    GeoUnavailableReason,
};
use edgezero_core::http::Response as CoreResponse;
use edgezero_core::http::{HeaderValue, StatusCode, response_builder};
use edgezero_core::ingress::{AdmissionDecision, IngressGrant};
use edgezero_core::middleware::{Middleware, Next};
use edgezero_core::response_egress::ResponseEgressCompletion;
use std::sync::Mutex;
use std::time::Duration;

struct Counted(Arc<Mutex<Vec<GeoInput>>>);
impl GeoProvider for Counted {
    fn resolve(&self, input: &GeoInput) -> GeoResolution {
        self.0.lock().unwrap().push(input.clone());
        GeoResolution::NoMatch
    }
}

struct Mutate;
#[async_trait::async_trait(?Send)]
impl Middleware for Mutate {
    async fn handle(
        &self,
        mut ctx: RequestContext,
        next: Next<'_>,
    ) -> Result<CoreResponse, EdgeError> {
        let original = ctx.geo().clone();
        ctx.extensions_mut().insert(GeoOutcome::NoSelection);
        ctx.extensions_mut().insert(GeoInput::ReaderIp {
            address: "198.51.100.7".parse().unwrap(),
            source: GeoIpSource::DirectPeer,
        });
        ctx.headers_mut()
            .insert("x-forwarded-for", HeaderValue::from_static("198.51.100.7"));
        assert_eq!(ctx.geo(), &original);
        next.run(ctx).await
    }
}

#[edgezero_core::action]
async fn observe(ctx: RequestContext) -> Result<&'static str, EdgeError> {
    Ok(match ctx.geo() {
        GeoOutcome::Selected {
            status: GeoStatus::Unavailable(GeoUnavailableReason::NoMatch),
            ..
        } => "no_match",
        GeoOutcome::Selected {
            status: GeoStatus::Unavailable(GeoUnavailableReason::MissingReaderIp),
            ..
        } => "missing_reader",
        GeoOutcome::NoSelection | GeoOutcome::Selected { .. } => "unexpected",
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "one counted service scenario proves admission and terminal call counts for both reader expectations"
)]
#[tokio::test(flavor = "current_thread")]
async fn geo_reader_managed_dispatch_resolves_once_after_admission() {
    for expectation in [
        ReaderIpExpectation::PeerAllowed,
        ReaderIpExpectation::ForwardedRequired,
    ] {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let geo = NativeGeo::prepare(&NativeGeoSelection::SuppliedReaderIp {
            expectation,
            id: GeoProviderId::new("counted-reader").unwrap(),
            provider: Arc::new(Counted(Arc::clone(&calls))),
        })
        .unwrap();
        let router = RouterService::builder()
            .middleware(Mutate)
            .get("/{mode}", observe)
            .build();
        let mut app = App::new(router);
        app.set_ingress_admission_policy(|head| match head.target().path() {
            "/refuse" => AdmissionDecision::Refuse {
                completion: ResponseEgressCompletion::empty(),
                response: response_builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::empty())
                    .unwrap(),
            },
            "/abort" => AdmissionDecision::Abort,
            _ => AdmissionDecision::Admit {
                completion: ResponseEgressCompletion::empty(),
                grant: IngressGrant::empty(),
                read_deadline: head.read_deadline_after(Duration::from_secs(1)),
            },
        });
        let service = AxumServiceState::with_transport(
            app,
            AxumOutboundClient::try_transport().unwrap(),
            JsonBodyLimit::DEFAULT,
            NativeSession::new(NativeDiagnosticsHandle::new(), true).unwrap(),
            TrustedProxyPolicy::new(ForwardingHeaderFamily::XForwarded, ["127.0.0.1/32"]).unwrap(),
        )
        .with_geo(geo);
        let peer: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        for (actual_peer, forwarded, expected, invoked) in [
            (Some(peer), Some("203.0.113.9"), "no_match", true),
            (
                Some(peer),
                None,
                if expectation == ReaderIpExpectation::PeerAllowed {
                    "no_match"
                } else {
                    "missing_reader"
                },
                expectation == ReaderIpExpectation::PeerAllowed,
            ),
            (None, Some("203.0.113.9"), "missing_reader", false),
        ] {
            let before = calls.lock().unwrap().len();
            let mut request = Request::builder()
                .uri("/observe")
                .body(AxumBody::empty())
                .unwrap();
            request.extensions_mut().insert(ConnectInfo(peer));
            request.extensions_mut().insert(GeoInput::ReaderIp {
                address: "198.51.100.7".parse().unwrap(),
                source: GeoIpSource::DirectPeer,
            });
            if let Some(assertion) = forwarded {
                request
                    .headers_mut()
                    .insert("x-forwarded-for", HeaderValue::from_static(assertion));
            }
            let response = service
                .dispatch_request(request, &EgressConnection::default(), actual_peer)
                .await
                .unwrap();
            assert_eq!(to_bytes(response.into_body(), 128).await.unwrap(), expected);
            assert_eq!(calls.lock().unwrap().len(), before + usize::from(invoked));
        }
        assert_eq!(
            calls.lock().unwrap().first(),
            Some(&GeoInput::ReaderIp {
                address: "203.0.113.9".parse().unwrap(),
                source: GeoIpSource::TrustedXForwarded
            })
        );
        let before = calls.lock().unwrap().len();
        for path in ["/refuse", "/abort"] {
            let request = Request::builder()
                .uri(path)
                .header("x-forwarded-for", "203.0.113.9")
                .body(AxumBody::empty())
                .unwrap();
            let response = service
                .dispatch_request(request, &EgressConnection::default(), Some(peer))
                .await;
            if path == "/refuse" {
                assert_eq!(response.unwrap().status(), StatusCode::SERVICE_UNAVAILABLE);
            } else {
                assert!(response.is_err());
            }
            assert_eq!(calls.lock().unwrap().len(), before);
        }
        let (_phase, reader) = watch::channel(LifecyclePhase::Draining);
        let draining = service.with_phase(reader);
        let request = Request::builder()
            .uri("/observe")
            .header("x-forwarded-for", "203.0.113.9")
            .body(AxumBody::empty())
            .unwrap();
        assert!(
            draining
                .dispatch_request(request, &EgressConnection::default(), Some(peer))
                .await
                .is_err()
        );
        assert_eq!(calls.lock().unwrap().len(), before);
    }
}
