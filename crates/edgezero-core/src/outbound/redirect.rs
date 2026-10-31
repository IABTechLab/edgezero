use std::cell::Cell;
use std::rc::Rc;
use std::str;

use futures_util::future::LocalBoxFuture;
use url::Url;

use super::dispatch::{self, DispatchBounds};
use super::{
    HttpClient, OutboundDispatchRequest, OutboundHead, OutboundRequest, OutboundResponse,
    OutboundScope, OutboundSendMode, OutboundTime, canonicalize_url, head, scope,
};
use crate::body::Body;
use crate::error::{BadGatewayReason, EdgeError};
use crate::http::header::{
    AUTHORIZATION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_LOCATION,
    CONTENT_TYPE, COOKIE, EXPECT, LOCATION,
};
use crate::http::{HeaderMap, Method, StatusCode, Uri};
use crate::time::{Deadline, MonotonicInstant};

struct FollowChain {
    client: HttpClient,
    index: Rc<Cell<usize>>,
    limit: OutboundRedirectLimit,
    original_deadline: Option<Deadline>,
    previous: MonotonicInstant,
    scope: OutboundScope,
    time: OutboundTime,
}

/// A failed request (initial hop zero) or an unchanged application policy rejection.
#[derive(Debug)]
pub enum OutboundFollowFailure<E> {
    Hop { index: usize, error: EdgeError },
    Policy { redirect_number: usize, error: E },
}

/// The final response and the actual URI and redirect count that produced it.
#[derive(Debug)]
pub struct OutboundFollowResult {
    final_uri: Uri,
    redirects_followed: usize,
    response: OutboundResponse,
    stop_reason: Option<OutboundRedirectStopReason>,
}

/// The synchronous policy's disposition of one redirect head.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the approved owned policy action avoids an allocation on every replacement request"
)]
pub enum OutboundRedirectAction {
    /// Send this complete replacement without implicitly inheriting options or stripping again.
    Follow(OutboundRequest),
    /// Follow the prepared request, unless the policy already took it or none was possible.
    FollowPrepared,
    /// Apply ordinary body handling and return the answering 3xx.
    Stop,
}

/// One answering head and an optional next request, after method and credential rules.
/// The initial request is never passed to this policy; authorize it before calling `follow`.
#[derive(Debug)]
pub struct OutboundRedirectContext<'head> {
    cross_origin: bool,
    headers: &'head HeaderMap,
    prepared_request: Option<OutboundRequest>,
    redirect_number: usize,
    request_uri: &'head Uri,
    resolved_target: &'head str,
    status: StatusCode,
}

/// An explicit maximum of 1 through 20 followed redirects, with no implicit default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboundRedirectLimit(usize);

/// Why a followable status was returned as the final response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundRedirectStopReason {
    BodyNotReplayable,
    LimitReached,
    MethodNotRewritable,
    NoLocation,
    StoppedByPolicy,
}

impl FollowChain {
    async fn finish<E>(
        &self,
        head: OutboundHead,
        final_uri: Uri,
        stop_reason: Option<OutboundRedirectStopReason>,
    ) -> Result<OutboundFollowResult, OutboundFollowFailure<E>> {
        let response = head
            .finish_from(self.previous)
            .await
            .map_err(|error| self.hop(error))?;
        Ok(OutboundFollowResult {
            final_uri,
            redirects_followed: self.index.get(),
            response,
            stop_reason,
        })
    }

    fn hop<E>(&self, error: EdgeError) -> OutboundFollowFailure<E> {
        OutboundFollowFailure::Hop {
            index: self.index.get(),
            error,
        }
    }

    async fn run<P, E>(
        mut self,
        mut prepared: OutboundDispatchRequest,
        mut policy: P,
    ) -> Result<OutboundFollowResult, OutboundFollowFailure<E>>
    where
        P: for<'head> FnMut(
            &mut OutboundRedirectContext<'head>,
        ) -> Result<OutboundRedirectAction, E>,
    {
        loop {
            let settled = dispatch::settle(prepared)
                .await
                .map_err(|error| self.hop(error))?;
            let budget = settled.budget();
            let (replay, replayable) = replay_request(settled.request());
            let uri = replay.uri.clone();
            let head = self
                .client
                .inner
                .dispatch_head(settled)
                .await
                .map_err(|error| self.hop(error))?;
            self.scope.check().map_err(|error| self.hop(error))?;
            head::checked_live(
                self.time.clock(),
                &mut self.previous,
                budget.deadline,
                budget.cause,
            )
            .map_err(|error| self.hop(error))?;
            self.scope.check().map_err(|error| self.hop(error))?;
            if !followable(head.status()) {
                return self.finish(head, uri, None).await;
            }
            if let Some(reason) = stop_reason(
                &head,
                &replay.method,
                replayable,
                self.index.get() == self.limit.get(),
            ) {
                return self.finish(head, uri, Some(reason)).await;
            }
            let target = resolve_location(&head, &uri).map_err(|error| self.hop(error))?;
            let base = Url::parse(&uri.to_string()).map_err(|_error| self.hop(protocol()))?;
            let cross_origin = target.origin() != base.origin();
            let automatic = prepare_redirect(replay, &target, cross_origin, head.status())
                .map_err(|error| self.hop(error))?;
            let redirect_number = self.index.get().saturating_add(1);
            let mut context = OutboundRedirectContext {
                cross_origin,
                headers: head.headers(),
                prepared_request: automatic,
                redirect_number,
                request_uri: &uri,
                resolved_target: target.as_str(),
                status: head.status(),
            };
            // Rejections are application values, including non-Send/non-Clone values. Do not
            // replace them with a later clock observation after synchronous policy execution.
            let action = policy(&mut context).map_err(|error| OutboundFollowFailure::Policy {
                redirect_number,
                error,
            })?;
            self.scope.check().map_err(|error| self.hop(error))?;
            let next = match action {
                OutboundRedirectAction::Stop => {
                    drop(context);
                    return self
                        .finish(head, uri, Some(OutboundRedirectStopReason::StoppedByPolicy))
                        .await;
                }
                OutboundRedirectAction::Follow(request) => request,
                OutboundRedirectAction::FollowPrepared => context
                    .take_prepared_request()
                    .ok_or_else(|| self.hop(protocol()))?,
            };
            drop(context);
            let status = head.status();
            head.discard();
            self.index.set(redirect_number);
            let entered_at = head::checked_sample(self.time.clock(), &mut self.previous)
                .map_err(|error| self.hop(error))?;
            prepared = dispatch::prepare(
                &self.client,
                next,
                OutboundSendMode::Follow,
                self.time.clone(),
                entered_at,
                DispatchBounds {
                    observation_cutoff: None,
                    original_chain_deadline: self.original_deadline,
                },
                self.scope.clone(),
            )
            .map_err(|error| self.hop(error))?;
            let sent = prepared.request();
            log::debug!(
                target: "edgezero::outbound::redirect",
                "redirect status={} scheme={} host={} port={} path={}",
                status.as_u16(),
                sent.uri().scheme_str().unwrap_or_default(),
                sent.host_name(),
                sent.resolved_port(),
                sent.uri().path(),
            );
        }
    }
}

impl OutboundFollowResult {
    #[must_use]
    #[inline]
    pub fn final_uri(&self) -> &Uri {
        &self.final_uri
    }

    #[must_use]
    #[inline]
    pub fn into_parts(
        self,
    ) -> (
        OutboundResponse,
        Uri,
        usize,
        Option<OutboundRedirectStopReason>,
    ) {
        (
            self.response,
            self.final_uri,
            self.redirects_followed,
            self.stop_reason,
        )
    }

    #[must_use]
    #[inline]
    pub fn redirects_followed(&self) -> usize {
        self.redirects_followed
    }

    #[must_use]
    #[inline]
    pub fn response(&self) -> &OutboundResponse {
        &self.response
    }

    #[must_use]
    #[inline]
    pub fn stop_reason(&self) -> Option<OutboundRedirectStopReason> {
        self.stop_reason
    }
}

impl OutboundRedirectContext<'_> {
    #[must_use]
    #[inline]
    pub fn headers(&self) -> &HeaderMap {
        self.headers
    }

    #[must_use]
    #[inline]
    pub fn is_cross_origin(&self) -> bool {
        self.cross_origin
    }

    #[must_use]
    #[inline]
    pub fn prepared_request(&self) -> Option<&OutboundRequest> {
        self.prepared_request.as_ref()
    }

    #[must_use]
    #[inline]
    pub fn redirect_number(&self) -> usize {
        self.redirect_number
    }

    #[must_use]
    #[inline]
    pub fn request_uri(&self) -> &Uri {
        self.request_uri
    }

    #[must_use]
    #[inline]
    pub fn resolved_target(&self) -> &str {
        self.resolved_target
    }

    #[must_use]
    #[inline]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    #[must_use]
    #[inline]
    pub fn take_prepared_request(&mut self) -> Option<OutboundRequest> {
        self.prepared_request.take()
    }
}

impl OutboundRedirectLimit {
    #[must_use]
    #[inline]
    pub fn get(&self) -> usize {
        self.0
    }

    /// Validates a maximum of 1 through 20 redirects (at most limit plus one sends).
    ///
    /// # Errors
    /// Returns a client error outside the supported range.
    #[inline]
    pub fn new(limit: usize) -> Result<Self, EdgeError> {
        if (1..=20).contains(&limit) {
            Ok(Self(limit))
        } else {
            Err(EdgeError::bad_request(
                "redirect limit must be between 1 and 20",
            ))
        }
    }
}

pub(super) fn follow<P, E>(
    client: &HttpClient,
    request: OutboundRequest,
    limit: OutboundRedirectLimit,
    policy: P,
) -> LocalBoxFuture<'static, Result<OutboundFollowResult, OutboundFollowFailure<E>>>
where
    P: for<'head> FnMut(&mut OutboundRedirectContext<'head>) -> Result<OutboundRedirectAction, E>
        + 'static,
    E: 'static,
{
    let (scope, time, entered_at, prepared) =
        match client.prepare(request, OutboundSendMode::Follow, None) {
            Ok(captured) => captured,
            Err(error) => {
                return Box::pin(
                    async move { Err(OutboundFollowFailure::Hop { index: 0, error }) },
                );
            }
        };
    let index = Rc::new(Cell::new(0));
    let chain = FollowChain {
        client: client.clone(),
        index: Rc::clone(&index),
        limit,
        original_deadline: prepared.context.original_chain_deadline,
        previous: entered_at,
        scope: scope.clone(),
        time: time.clone(),
    };
    // Reuse the exact scope binder; the inner Result retains the application's error type.
    let bound = scope::bind_future(
        scope,
        time,
        entered_at,
        Box::pin(async move { Ok(chain.run(prepared, policy).await) }),
    );
    Box::pin(async move {
        bound.await.map_err(|error| OutboundFollowFailure::Hop {
            index: index.get(),
            error,
        })?
    })
}

fn followable(status: StatusCode) -> bool {
    matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308)
}

fn prepare_redirect(
    mut replay: OutboundRequest,
    target: &Url,
    cross_origin: bool,
    status: StatusCode,
) -> Result<Option<OutboundRequest>, EdgeError> {
    if !matches!(target.scheme(), "http" | "https") {
        return Ok(None);
    }
    replay.uri = canonicalize_url(target.as_str()).map_err(|_error| protocol())?;
    rewrite(&mut replay, status);
    if cross_origin {
        replay.headers.remove(AUTHORIZATION);
        replay.headers.remove(COOKIE);
        replay.host_authority_override = None;
    }
    Ok(Some(replay))
}

fn protocol() -> EdgeError {
    EdgeError::bad_gateway_with_reason("invalid upstream redirect", BadGatewayReason::Protocol)
}

fn replay_request(request: &OutboundRequest) -> (OutboundRequest, bool) {
    // Exhaustive destructuring makes newly added options a compile-time migration obligation.
    // No streamed body is cloned or buffered, and no public request-cloning API is introduced.
    let OutboundRequest {
        body,
        cache_policy,
        connect_timeout,
        deadline,
        headers,
        host_authority_override,
        image_optimizer,
        inbound_read_deadline,
        max_brotli_window_bits,
        max_chunk_bytes,
        max_decoded_response_bytes,
        max_decoder_bytes,
        max_encoded_response_bytes,
        max_request_body_bytes,
        max_response_header_bytes,
        max_response_header_count,
        method,
        proxy_marker,
        response_decoding,
        response_mode,
        timeout,
        uri,
    } = request;
    let replayable = body.buffered_bytes().is_some();
    let replayed_body = body
        .buffered_bytes()
        .map_or_else(Body::empty, |bytes| Body::from(bytes.clone()));
    (
        OutboundRequest {
            body: replayed_body,
            cache_policy: *cache_policy,
            connect_timeout: *connect_timeout,
            deadline: *deadline,
            headers: headers.clone(),
            host_authority_override: host_authority_override.clone(),
            image_optimizer: image_optimizer.clone(),
            inbound_read_deadline: *inbound_read_deadline,
            max_brotli_window_bits: *max_brotli_window_bits,
            max_chunk_bytes: *max_chunk_bytes,
            max_decoded_response_bytes: *max_decoded_response_bytes,
            max_decoder_bytes: *max_decoder_bytes,
            max_encoded_response_bytes: *max_encoded_response_bytes,
            max_request_body_bytes: *max_request_body_bytes,
            max_response_header_bytes: *max_response_header_bytes,
            max_response_header_count: *max_response_header_count,
            method: method.clone(),
            proxy_marker: *proxy_marker,
            response_decoding: *response_decoding,
            response_mode: *response_mode,
            timeout: *timeout,
            uri: uri.clone(),
        },
        replayable,
    )
}

fn resolve_location(head: &OutboundHead, uri: &Uri) -> Result<Url, EdgeError> {
    let evidence = head.location_evidence();
    if evidence.observed_count() != 1 || evidence.has_non_text() || !evidence.retained_singleton() {
        return Err(protocol());
    }
    let raw = head.headers().get(LOCATION).ok_or_else(protocol)?;
    let location = str::from_utf8(raw.as_bytes()).map_err(|_error| protocol())?;
    let base = Url::parse(&uri.to_string()).map_err(|_error| protocol())?;
    let mut target = base.join(location).map_err(|_error| protocol())?;
    if !target.username().is_empty() || target.password().is_some() {
        return Err(protocol());
    }
    target.set_fragment(None);
    Ok(target)
}

fn rewrite(request: &mut OutboundRequest, status: StatusCode) {
    if matches!(status.as_u16(), 301..=303) {
        if request.method != Method::HEAD {
            request.method = Method::GET;
        }
        request.body = Body::empty();
        for name in [
            CONTENT_TYPE,
            CONTENT_LENGTH,
            CONTENT_ENCODING,
            CONTENT_LANGUAGE,
            CONTENT_LOCATION,
            EXPECT,
        ] {
            request.headers.remove(name);
        }
    }
}

fn stop_reason(
    head: &OutboundHead,
    method: &Method,
    replayable: bool,
    at_limit: bool,
) -> Option<OutboundRedirectStopReason> {
    if head.location_evidence().is_missing_or_empty() {
        Some(OutboundRedirectStopReason::NoLocation)
    } else if matches!(head.status().as_u16(), 301 | 302)
        && !matches!(*method, Method::GET | Method::HEAD | Method::POST)
    {
        Some(OutboundRedirectStopReason::MethodNotRewritable)
    } else if matches!(head.status().as_u16(), 307 | 308) && !replayable {
        Some(OutboundRedirectStopReason::BodyNotReplayable)
    } else if at_limit {
        Some(OutboundRedirectStopReason::LimitReached)
    } else {
        None
    }
}
