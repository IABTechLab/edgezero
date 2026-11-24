#![allow(
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    reason = "table-driven cases reuse request, result and fixture bindings"
)]

use std::cell::RefCell;
use std::future::pending;
use std::rc::Rc;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use futures::executor::block_on;
use futures::future::ready;
use futures_util::stream;
use sha2::{Digest as _, Sha256};

use super::{
    ClientCertificateChain, ClientIdentityError, ClientIdentityHosts, ClientIdentityMaterial,
    ClientIdentityName, ClientIdentityPolicy,
};
use crate::app::{App, Hooks};
use crate::body::Body;
use crate::context::RequestContext;
use crate::error::EdgeError;
use crate::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, request_builder};
use crate::ingress::{IngressFraming, IngressHeadAccounting};
use crate::manifest::{BakedManifest, ManifestLoader};
use crate::outbound::{
    HttpClient, IndependentOutboundProgress, OutboundBatch, OutboundBatchDriverEvent,
    OutboundCollectedSlot, OutboundDispatchRequest, OutboundExchangeOwner, OutboundFollowFailure,
    OutboundFuture, OutboundHead, OutboundHeaderFidelity, OutboundHeaderInput, OutboundHttpClient,
    OutboundPending, OutboundPreparedBatch, OutboundProgressRuntime, OutboundRedirectAction,
    OutboundRedirectContext, OutboundRedirectLimit, OutboundRequest, OutboundSendMode,
    OutboundTime,
};
use crate::platform::PlatformMetadata;
use crate::router::RouterService;
use crate::time::{Deadline, MonotonicClock};

const IDENTITY_MANIFEST: &str = r#"
[capabilities.outbound]
hosts = ["https://*:*"]

[capabilities.outbound.client-identities.mtls]
config-store = "certs"
secret-store = "keys"

[stores.config]
ids = ["certs"]

[stores.secrets]
ids = ["keys"]
"#;

const HOSTS: &str = "https://a.example\nhttps://b.example:8443\n";

static MANIFEST: LazyLock<ManifestLoader> =
    LazyLock::new(|| ManifestLoader::load_from_str(IDENTITY_MANIFEST));

type SentLog = Arc<Mutex<Vec<(Uri, Option<String>)>>>;

/// What the probe route saw on the request's `HttpClient`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Observed {
    NoClient,
    Policy { declares_identities: bool },
}

std::thread_local! {
    static OBSERVED_POLICY: RefCell<Option<Observed>> = const { RefCell::new(None) };
}

/// A fake adapter that runs the identity host check the way a real adapter does
/// and records every request it would have sent.
struct IdentityClient {
    hosts: &'static str,
    sent: SentLog,
}

struct IdentityHooks;

impl IdentityClient {
    fn respond(&self, request: OutboundDispatchRequest) -> Result<OutboundHead, EdgeError> {
        if let Some(identity) = request.client_identity() {
            let policy = request
                .client_identity_policy()
                .expect("policy with identity");
            ClientIdentityHosts::parse(self.hosts, policy)
                .and_then(|hosts| hosts.check(request.request().uri()))
                .map_err(|reason| EdgeError::client_identity(identity.name(), reason))?;
        }
        if let Ok(mut sent) = self.sent.lock() {
            sent.push((
                request.request().uri().clone(),
                request
                    .request()
                    .client_identity_name()
                    .map(|selected| selected.as_str().to_owned()),
            ));
        }
        match request.request().uri().path() {
            "/to-listed" => head(
                request,
                StatusCode::FOUND,
                Some("https://b.example:8443/end"),
            ),
            "/to-unlisted" => head(
                request,
                StatusCode::FOUND,
                Some("https://evil.example/end?q=1"),
            ),
            _ => head(request, StatusCode::OK, None),
        }
    }
}

impl OutboundHttpClient for IdentityClient {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        let outcome = self
            .respond(request)
            .and_then(|response_head| block_on(response_head.finish()));
        Ok(OutboundPending::new(
            Box::pin(ready(outcome)),
            OutboundExchangeOwner::new(|| {}),
        ))
    }

    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        Box::pin(ready(self.respond(request)))
    }

    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(IndependentOutboundProgress)
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        let (context, slots) = batch.into_slots();
        let items = slots
            .into_iter()
            .map(|(index, request)| OutboundBatchDriverEvent::Item {
                index,
                completed_at: context.clock().now(),
                outcome: self
                    .respond(request)
                    .and_then(|response_head| block_on(response_head.finish())),
            })
            .collect::<Vec<_>>();
        OutboundBatch::from_driver(
            context,
            stream::iter(items),
            OutboundExchangeOwner::new(|| {}),
        )
    }

    fn time(&self) -> OutboundTime {
        OutboundTime::new(MonotonicClock::default(), |_deadline, _clock| {
            Box::pin(pending())
        })
    }

    fn validate_supported_options(
        &self,
        _request: &OutboundRequest,
        _mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        Ok(())
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "the probe app needs only a manifest and one route"
)]
impl Hooks for IdentityHooks {
    fn manifest() -> BakedManifest {
        BakedManifest::Present(MANIFEST.manifest())
    }

    fn routes() -> Result<RouterService, EdgeError> {
        async fn probe(ctx: RequestContext) -> Result<String, EdgeError> {
            let observed =
                ctx.http_client()
                    .map_or(Observed::NoClient, |client| Observed::Policy {
                        declares_identities: client
                            .client_identity_policy()
                            .is_some_and(|attached| !attached.is_empty()),
                    });
            OBSERVED_POLICY.with(|slot| *slot.borrow_mut() = Some(observed));
            Ok("ok".to_owned())
        }
        Ok(RouterService::builder().get("/probe", probe).build())
    }
}

fn policy() -> Arc<ClientIdentityPolicy> {
    Arc::new(ClientIdentityPolicy::from_manifest(MANIFEST.manifest()))
}

fn name() -> ClientIdentityName {
    ClientIdentityName::new("mtls").expect("valid name")
}

fn identity_client(hosts: &'static str) -> (HttpClient, SentLog) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let client = HttpClient::with_client(IdentityClient {
        hosts,
        sent: Arc::clone(&sent),
    })
    .with_client_identity_policy(policy());
    (client, sent)
}

fn get(uri: &str) -> OutboundRequest {
    OutboundRequest::get(uri)
        .expect("request")
        .client_identity(name())
}

fn identity_reason(error: &EdgeError) -> &ClientIdentityError {
    error
        .client_identity_error()
        .unwrap_or_else(|| panic!("expected a client identity error, got {error:?}"))
}

fn head(
    request: OutboundDispatchRequest,
    status: StatusCode,
    location: Option<&'static str>,
) -> Result<OutboundHead, EdgeError> {
    let (_parts, context) = request.into_parts();
    let mut headers = HeaderMap::new();
    if let Some(target) = location {
        headers.insert("location", HeaderValue::from_static(target));
    }
    OutboundHead::new(
        status,
        OutboundHeaderInput::new(
            "test",
            headers,
            OutboundHeaderFidelity::AdapterVisibleFields,
        ),
        Box::pin(stream::empty()),
        context,
        OutboundExchangeOwner::new(|| {}),
    )
}

// --- names and policy -------------------------------------------------------

#[test]
fn names_follow_the_manifest_rules() {
    for valid in ["a", "prebid-server", "x9"] {
        let parsed = ClientIdentityName::new(valid).expect("valid name");
        assert_eq!(parsed.as_str(), valid);
        assert_eq!(parsed.to_string(), valid);
    }
    for invalid in ["", "Upper", "-lead", "trail-", "under_score", "dot.ted"] {
        let error = ClientIdentityName::new(invalid).expect_err("invalid name");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{invalid}");
    }
    let _too_long = ClientIdentityName::new(&"a".repeat(65)).expect_err("too long");

    let round_trip =
        OutboundRequest::from_parts(get("https://a.example/").into_parts()).expect("from parts");
    assert_eq!(round_trip.client_identity_name(), Some(&name()));
    assert_eq!(
        OutboundRequest::get("https://a.example/")
            .expect("request")
            .client_identity_name(),
        None
    );
}

#[test]
fn policy_resolves_stores_and_defaults_the_allowlist() {
    let policy = policy();
    let identity = policy.identity(&name()).expect("declared identity");
    assert_eq!(identity.name(), &name());
    assert_eq!(identity.config_store(), Some("certs"));
    assert_eq!(identity.secret_store(), Some("keys"));
    assert!(policy.allows_https_origin("anything.example", 1));

    let unlisted = ClientIdentityName::new("other").expect("valid name");
    assert!(policy.identity(&unlisted).is_none());

    let default_hosts =
        ManifestLoader::load_from_str("[capabilities.outbound.client-identities.mtls]\n");
    let default_policy = ClientIdentityPolicy::from_manifest(default_hosts.manifest());
    assert!(default_policy.allows_https_origin("pbs.example", 443));
    let identity = default_policy.identity(&name()).expect("declared");
    assert_eq!(identity.config_store(), None);
    assert_eq!(identity.secret_store(), None);

    assert!(ClientIdentityPolicy::from_baked(BakedManifest::Absent).is_empty());
    assert!(ClientIdentityPolicy::from_baked(BakedManifest::Malformed("corrupt")).is_empty());
    assert!(
        !ClientIdentityPolicy::from_baked(BakedManifest::Present(MANIFEST.manifest())).is_empty()
    );
}

// --- hosts entry --------------------------------------------------------------

#[test]
fn hosts_entry_parses_exact_https_origins() {
    let hosts = ClientIdentityHosts::parse(
        "\n  HTTPS://PBS.Example.com  \nhttps://b.example:8443\n\nhttps://[::1]:9443\nhttps://pbs.example.com:443\n",
        &policy(),
    )
    .expect("valid hosts");
    assert_eq!(
        hosts.origins().collect::<Vec<_>>(),
        [
            ("pbs.example.com", 443),
            ("b.example", 8443),
            ("[::1]", 9443)
        ]
    );
}

#[test]
fn hosts_entry_rejects_everything_outside_the_grammar() {
    let oversized = format!(
        "https://{}.example\n",
        "a".repeat(ClientIdentityHosts::MAX_BYTES)
    );
    let invalid = [
        "",
        " \n\n ",
        "http://pbs.example",
        "pbs.example",
        "https://pbs.example/path",
        "https://pbs.example?query=1",
        "https://user@pbs.example",
        "https://*.example",
        "https://pbs.example:*",
        "https://*",
        "*",
        "https://pbs.ex\u{e4}mple",
        oversized.as_str(),
    ];
    for entry in invalid {
        assert_eq!(
            ClientIdentityHosts::parse(entry, &policy()),
            Err(ClientIdentityError::MaterialInvalid {
                which: ClientIdentityMaterial::Hosts
            }),
            "{entry:?}"
        );
    }
}

#[test]
fn hosts_entry_must_stay_inside_the_outbound_allowlist() {
    let narrow = ManifestLoader::load_from_str(
        "[capabilities.outbound]\nhosts = [\"https://*.example.com\"]\n\
         [capabilities.outbound.client-identities.mtls]\n",
    );
    let policy = ClientIdentityPolicy::from_manifest(narrow.manifest());
    let _inside = ClientIdentityHosts::parse("https://pbs.example.com", &policy)
        .expect("inside the allowlist");
    for outside in [
        "https://example.com",
        "https://pbs.example.org",
        "https://pbs.example.com\nhttps://pbs.example.org",
    ] {
        assert_eq!(
            ClientIdentityHosts::parse(outside, &policy),
            Err(ClientIdentityError::MaterialInvalid {
                which: ClientIdentityMaterial::Hosts
            }),
            "{outside}"
        );
    }
}

#[test]
fn host_check_compares_scheme_host_and_port_only() {
    let hosts = ClientIdentityHosts::parse(HOSTS, &policy()).expect("hosts");
    for allowed in [
        "https://a.example/path?q=1",
        "https://A.EXAMPLE:443/",
        "https://b.example:8443/x",
    ] {
        assert_eq!(
            hosts.check(&allowed.parse::<Uri>().expect("uri")),
            Ok(()),
            "{allowed}"
        );
    }
    assert_eq!(
        hosts.check(&"https://b.example/x".parse::<Uri>().expect("uri")),
        Err(ClientIdentityError::HostNotAllowed {
            origin: "https://b.example:443".to_owned(),
            redirect: false
        })
    );
    assert_eq!(
        hosts.check(&"http://a.example/".parse::<Uri>().expect("uri")),
        Err(ClientIdentityError::RequiresHttps)
    );
    let ipv6 = ClientIdentityHosts::parse("https://[::1]:9443", &policy()).expect("ipv6");
    assert_eq!(
        ipv6.check(&"https://[0:0::1]:9443/".parse::<Uri>().expect("uri")),
        Ok(())
    );
}

// --- certificate chain ----------------------------------------------------------

fn pem_block(label: &str, der: &[u8]) -> String {
    format!(
        "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
        STANDARD.encode(der)
    )
}

#[test]
fn certificate_chain_fingerprints_the_leaf_and_the_whole_chain() {
    let leaf = b"leaf certificate der".as_slice();
    let intermediate = b"intermediate der".as_slice();
    let other_intermediate = b"replacement intermediate der".as_slice();
    let chain = ClientCertificateChain::parse(&format!(
        "subject=CN=leaf\n{}{}",
        pem_block("CERTIFICATE", leaf),
        pem_block("CERTIFICATE", intermediate)
    ))
    .expect("chain");
    let expected_leaf: [u8; 32] = Sha256::digest(leaf).into();
    assert_eq!(chain.leaf_fingerprint(), expected_leaf);

    let replaced = ClientCertificateChain::parse(&format!(
        "{}{}",
        pem_block("CERTIFICATE", leaf),
        pem_block("CERTIFICATE", other_intermediate)
    ))
    .expect("replaced chain");
    assert_eq!(replaced.leaf_fingerprint(), chain.leaf_fingerprint());
    assert_ne!(replaced.chain_fingerprint(), chain.chain_fingerprint());

    let leaf_only =
        ClientCertificateChain::parse(&pem_block("CERTIFICATE", leaf)).expect("leaf only");
    assert_ne!(leaf_only.chain_fingerprint(), chain.chain_fingerprint());

    let reparsed = ClientCertificateChain::parse(chain.pem()).expect("normalized pem");
    assert_eq!(reparsed, chain);
    assert!(!format!("{chain:?}").contains("BEGIN"));
}

#[test]
fn certificate_chain_keeps_only_certificate_blocks() {
    let chain = ClientCertificateChain::parse(&format!(
        "{}{}",
        pem_block("PRIVATE KEY", b"not for the certificate entry"),
        pem_block("CERTIFICATE", b"leaf")
    ))
    .expect("chain");
    assert!(!chain.pem().contains("PRIVATE KEY"));
    assert_eq!(chain.pem().matches("BEGIN CERTIFICATE").count(), 1);
}

#[test]
fn certificate_chain_rejects_malformed_entries() {
    let invalid = [
        String::new(),
        "no pem here".to_owned(),
        pem_block("PRIVATE KEY", b"key only"),
        "-----BEGIN CERTIFICATE-----\nAAAA\n".to_owned(),
        "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n".to_owned(),
        "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n".to_owned(),
        "-----BEGIN CERTIFICATE-----\nAAAA\n-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
            .to_owned(),
    ];
    for entry in &invalid {
        assert_eq!(
            ClientCertificateChain::parse(entry),
            Err(ClientIdentityError::MaterialInvalid {
                which: ClientIdentityMaterial::Certificate
            }),
            "{entry:?}"
        );
    }
}

// --- errors ---------------------------------------------------------------------

#[test]
fn errors_choose_status_and_kind_and_never_show_paths() {
    let cases = [
        (
            ClientIdentityError::Undeclared,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
        ),
        (
            ClientIdentityError::RequiresHttps,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
        ),
        (
            ClientIdentityError::Unsupported { adapter: "spin" },
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
        ),
        (
            ClientIdentityError::MaterialMissing {
                which: ClientIdentityMaterial::Key,
            },
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
        (
            ClientIdentityError::MaterialInvalid {
                which: ClientIdentityMaterial::Certificate,
            },
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
        (
            ClientIdentityError::HostNotAllowed {
                origin: "https://x.example:443".to_owned(),
                redirect: false,
            },
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
        (
            ClientIdentityError::HostNotAllowed {
                origin: "https://x.example:443".to_owned(),
                redirect: true,
            },
            StatusCode::BAD_GATEWAY,
            "bad_gateway",
        ),
    ];
    for (reason, status, kind) in cases {
        let error = EdgeError::client_identity(&name(), reason.clone());
        assert_eq!(error.status(), status, "{reason:?}");
        assert_eq!(error.kind(), kind, "{reason:?}");
        assert_eq!(identity_reason(&error), &reason);
        assert!(
            error.message().starts_with("client identity `mtls`: "),
            "{error}"
        );
    }

    let hosts = ClientIdentityHosts::parse(HOSTS, &policy()).expect("hosts");
    let reason = hosts
        .check(
            &"https://evil.example/secret/path?token=abc"
                .parse::<Uri>()
                .expect("uri"),
        )
        .expect_err("unlisted");
    let rendered = format!(
        "{reason} {reason:?} {}",
        EdgeError::client_identity(&name(), reason.clone())
    );
    assert!(rendered.contains("https://evil.example:443"), "{rendered}");
    assert!(!rendered.contains("secret"), "{rendered}");
    assert!(!rendered.contains("token"), "{rendered}");
    assert!(
        EdgeError::bad_request("plain")
            .client_identity_error()
            .is_none()
    );
}

// --- dispatch through HttpClient ----------------------------------------------------

#[test]
fn undeclared_and_non_https_requests_never_reach_the_adapter() {
    let (client, sent) = identity_client(HOSTS);
    let undeclared = ClientIdentityName::new("other").expect("valid name");
    let error = block_on(
        client.send(
            OutboundRequest::get("https://a.example/")
                .expect("request")
                .client_identity(undeclared),
        ),
    )
    .expect_err("undeclared");
    assert_eq!(identity_reason(&error), &ClientIdentityError::Undeclared);

    let error = block_on(client.send(get("http://a.example/"))).expect_err("http");
    assert_eq!(identity_reason(&error), &ClientIdentityError::RequiresHttps);

    let without_policy = HttpClient::with_client(IdentityClient {
        hosts: HOSTS,
        sent: Arc::clone(&sent),
    });
    let error = block_on(without_policy.send(get("https://a.example/"))).expect_err("no policy");
    assert_eq!(identity_reason(&error), &ClientIdentityError::Undeclared);

    assert!(sent.lock().expect("sent").is_empty());
}

#[test]
fn listed_origins_are_sent_with_the_identity_and_others_are_refused() {
    let (client, sent) = identity_client(HOSTS);
    let response = block_on(client.send(get("https://a.example/ok"))).expect("listed");
    assert_eq!(response.status(), StatusCode::OK);

    let overridden = get("https://a.example/ok")
        .host_authority_override("pbs.internal")
        .expect("override");
    block_on(client.send(overridden)).expect("override keeps the connection origin");

    let error = block_on(client.send(get("https://evil.example/"))).expect_err("unlisted");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        identity_reason(&error),
        &ClientIdentityError::HostNotAllowed {
            origin: "https://evil.example:443".to_owned(),
            redirect: false
        }
    );

    let _plain =
        block_on(client.send(OutboundRequest::get("https://evil.example/").expect("plain")))
            .expect("requests without an identity are unaffected");

    let recorded = sent.lock().expect("sent");
    assert_eq!(recorded.len(), 3);
    assert_eq!(recorded[0].1.as_deref(), Some("mtls"));
    assert_eq!(recorded[2].1, None);
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "redirect policies return a Result by contract"
)]
fn follow_all(_context: &mut OutboundRedirectContext<'_>) -> Result<OutboundRedirectAction, ()> {
    Ok(OutboundRedirectAction::FollowPrepared)
}

#[test]
fn redirects_keep_the_identity_and_recheck_every_hop() {
    let limit = OutboundRedirectLimit::new(5).expect("limit");

    let (client, sent) = identity_client(HOSTS);
    let followed = block_on(client.follow(get("https://a.example/to-listed"), limit, follow_all))
        .expect("followed");
    assert_eq!(followed.redirects_followed(), 1);
    let recorded = sent.lock().expect("sent").clone();
    assert_eq!(recorded.len(), 2);
    assert!(
        recorded
            .iter()
            .all(|(_, identity)| identity.as_deref() == Some("mtls"))
    );
    assert_eq!(recorded[1].0, "https://b.example:8443/end");

    let (client, sent) = identity_client(HOSTS);
    let failure = block_on(client.follow(get("https://a.example/to-unlisted"), limit, follow_all))
        .expect_err("unlisted redirect");
    let OutboundFollowFailure::Hop { index, error } = failure else {
        panic!("expected a hop failure");
    };
    assert_eq!(index, 1);
    assert_eq!(error.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(
        identity_reason(&error),
        &ClientIdentityError::HostNotAllowed {
            origin: "https://evil.example:443".to_owned(),
            redirect: true
        }
    );
    assert_eq!(
        sent.lock().expect("sent").len(),
        1,
        "the unlisted hop is never sent"
    );
}

#[test]
fn batch_slots_fail_alone() {
    let (client, sent) = identity_client(HOSTS);
    let cutoff = Deadline::after(Duration::from_secs(60));
    let results = block_on(client.send_all_until(
        vec![
            get("https://a.example/one"),
            get("https://evil.example/two"),
            OutboundRequest::get("https://a.example/three")
                .expect("request")
                .client_identity(ClientIdentityName::new("other").expect("name")),
            get("https://b.example:8443/four"),
        ],
        cutoff,
    ))
    .expect("batch");
    let (slots, _termination) = results.into_parts();
    let outcomes: Vec<_> = slots
        .into_iter()
        .map(|slot| match slot {
            OutboundCollectedSlot::Resolved(result) => result,
            OutboundCollectedSlot::AlreadyDelivered | OutboundCollectedSlot::Unresolved => {
                panic!("every slot resolves")
            }
        })
        .collect();
    let _first = outcomes[0].outcome.as_ref().expect("listed slot");
    assert!(matches!(
        outcomes[1].outcome.as_ref().map_err(identity_reason),
        Err(ClientIdentityError::HostNotAllowed { .. })
    ));
    assert!(matches!(
        outcomes[2].outcome.as_ref().map_err(identity_reason),
        Err(ClientIdentityError::Undeclared)
    ));
    let _fourth = outcomes[3].outcome.as_ref().expect("listed slot");
    assert_eq!(sent.lock().expect("sent").len(), 2);
}

// --- dispatch attaches the app policy --------------------------------------------

fn dispatch_probe(app: &App, client: Option<HttpClient>) -> Option<Observed> {
    OBSERVED_POLICY.with(|slot| *slot.borrow_mut() = None);
    let mut request = request_builder()
        .method(Method::GET)
        .uri("/probe")
        .body(Body::empty())
        .expect("request");
    if let Some(supplied) = client {
        request.extensions_mut().insert(supplied);
    }
    let _outcome = block_on(app.dispatch_ingress(
        request,
        app.monotonic_now(),
        IngressHeadAccounting::HostManaged,
        IngressFraming::HostManaged,
    ))
    .expect("dispatch");
    OBSERVED_POLICY.with(|slot| slot.borrow_mut().take())
}

#[test]
fn dispatch_attaches_the_app_policy_to_the_request_client() {
    let app = App::build::<IdentityHooks>(PlatformMetadata::default()).expect("app");
    assert!(!app.client_identity_policy().is_empty());

    let bare = HttpClient::with_client(IdentityClient {
        hosts: HOSTS,
        sent: Arc::new(Mutex::new(Vec::new())),
    });
    assert!(bare.client_identity_policy().is_none());
    assert_eq!(
        dispatch_probe(&app, Some(bare)),
        Some(Observed::Policy {
            declares_identities: true
        })
    );

    let explicit = HttpClient::with_client(IdentityClient {
        hosts: HOSTS,
        sent: Arc::new(Mutex::new(Vec::new())),
    })
    .with_client_identity_policy(Arc::new(ClientIdentityPolicy::default()));
    assert_eq!(
        dispatch_probe(&app, Some(explicit)),
        Some(Observed::Policy {
            declares_identities: false
        }),
        "an explicit policy is kept"
    );

    assert_eq!(
        dispatch_probe(&app, None),
        Some(Observed::NoClient),
        "no client is created"
    );

    assert_eq!(
        app.client_identity_policy().as_ref(),
        &ClientIdentityPolicy::from_manifest(MANIFEST.manifest())
    );
}
