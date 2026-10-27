use std::fmt;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::stream::{self, LocalBoxStream, Stream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{EdgeError, RequestBodyOverflowOrigin};
use crate::outbound::{OutboundScope, OutboundScopeOwner, Revocable};

pub type BodyStream = LocalBoxStream<'static, Result<Bytes, EdgeError>>;

/// HTTP payload representation, independent of response-body lifetime ownership.
pub enum BodyPayload {
    Once(Bytes),
    Stream(BodyStream),
}

/// A buffered or locally streamed HTTP body with optional response-egress ownership.
pub struct Body {
    egress_owner: Option<Box<OutboundScopeOwner>>,
    payload: BodyPayload,
}

struct ManualBodyStream {
    owner: Option<OutboundScopeOwner>,
    source: Option<BodyStream>,
}

struct ScopedBodyStream {
    source: Option<Revocable<BodyStream>>,
}

impl Body {
    /// Returns the in-memory bytes for a buffered body, or `None` if this is
    /// a streaming body. To consume a streaming body into bytes, use
    /// [`Body::into_bytes_bounded`].
    #[inline]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.buffered_bytes().map(Bytes::as_ref)
    }

    pub(crate) fn attach_egress_owner(
        mut self,
        owner: OutboundScopeOwner,
    ) -> Result<Self, EdgeError> {
        if self.egress_owner.is_some() {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "response body already owns an outbound scope"
            )));
        }
        self.egress_owner = Some(Box::new(owner));
        Ok(self)
    }

    pub(crate) fn bind_outbound_scope(mut self, scope: &OutboundScope) -> Result<Self, EdgeError> {
        if let Err(error) = scope.check() {
            scope.with_cleanup(|| drop(self));
            return Err(error);
        }
        let payload = mem::replace(&mut self.payload, BodyPayload::Once(Bytes::new()));
        self.payload = match payload {
            BodyPayload::Once(bytes) => BodyPayload::Once(bytes),
            BodyPayload::Stream(source) => {
                let registered = Revocable::new(scope, source)?;
                BodyPayload::Stream(
                    ScopedBodyStream {
                        source: Some(registered),
                    }
                    .boxed_local(),
                )
            }
        };
        Ok(self)
    }

    pub(crate) fn buffered_bytes(&self) -> Option<&Bytes> {
        match &self.payload {
            BodyPayload::Once(bytes) => Some(bytes),
            BodyPayload::Stream(_) => None,
        }
    }

    #[must_use]
    #[inline]
    pub fn empty() -> Self {
        Self::from_bytes(Bytes::new())
    }

    #[inline]
    pub fn from_bytes<B>(bytes: B) -> Self
    where
        B: Into<Bytes>,
    {
        Self::from_parts(BodyPayload::Once(bytes.into()), None)
    }

    #[inline]
    pub fn from_external_stream<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + 'static,
        anyhow::Error: From<E>,
    {
        Self::from_stream(
            stream.map(|result| {
                result.map_err(|error| EdgeError::internal(anyhow::Error::from(error)))
            }),
        )
    }

    pub(crate) fn from_parts(
        payload: BodyPayload,
        egress_owner: Option<OutboundScopeOwner>,
    ) -> Self {
        Self {
            egress_owner: egress_owner.map(Box::new),
            payload,
        }
    }

    #[inline]
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, EdgeError>> + 'static,
    {
        Self::from_parts(BodyPayload::Stream(stream.boxed_local()), None)
    }

    pub(crate) fn has_egress_owner(&self) -> bool {
        self.egress_owner.is_some()
    }

    /// Consume a buffered body and return its bytes, or `None` if this is a
    /// streaming body. To collect a streaming body, use
    /// [`Body::into_bytes_bounded`].
    #[inline]
    pub fn into_bytes(self) -> Option<Bytes> {
        let (payload, owner) = self.into_parts();
        match payload {
            BodyPayload::Once(bytes) => {
                drop(owner);
                Some(bytes)
            }
            BodyPayload::Stream(source) => {
                drop(Self::from_parts(BodyPayload::Stream(source), owner));
                None
            }
        }
    }

    /// Drain the body into a single `Bytes` buffer, enforcing `max_size`.
    ///
    /// Works for both buffered and streaming variants.
    ///
    /// # Errors
    /// Returns [`EdgeError::PayloadTooLarge`] with inbound origin if the body exceeds
    /// `max_size` bytes. Source errors retain their original category.
    #[inline]
    pub async fn into_bytes_bounded(self, max_size: usize) -> Result<Bytes, EdgeError> {
        let (payload, owner) = self.into_parts();
        match payload {
            BodyPayload::Once(bytes) => {
                let _owner = owner;
                if bytes.len() > max_size {
                    return Err(EdgeError::payload_too_large(
                        RequestBodyOverflowOrigin::Inbound,
                    ));
                }
                Ok(bytes)
            }
            BodyPayload::Stream(source) => {
                let mut stream = manual_stream(source, owner);
                let mut buf = Vec::new();
                while let Some(result) = StreamExt::next(&mut stream).await {
                    let chunk = result?;
                    let next_len = buf.len().checked_add(chunk.len()).ok_or_else(|| {
                        EdgeError::payload_too_large(RequestBodyOverflowOrigin::Inbound)
                    })?;
                    if next_len > max_size {
                        return Err(EdgeError::payload_too_large(
                            RequestBodyOverflowOrigin::Inbound,
                        ));
                    }
                    buf.extend_from_slice(&chunk);
                }
                Ok(Bytes::from(buf))
            }
        }
    }

    /// Separates payload and its exact optional egress owner without copying bytes.
    /// The caller must retain the owner through terminal response egress.
    #[must_use]
    #[inline]
    pub fn into_parts(mut self) -> (BodyPayload, Option<OutboundScopeOwner>) {
        let payload = mem::replace(&mut self.payload, BodyPayload::Once(Bytes::new()));
        (payload, self.take_egress_owner())
    }

    /// Explicitly converts to a manually consumed stream. An attached owner closes at
    /// stream EOF, error, or drop; canonical egress extracts that owner before conversion.
    ///
    /// # Errors
    /// Returns the original buffered body, including its owner, rather than discarding it.
    #[inline]
    pub fn into_stream(self) -> Result<BodyStream, Self> {
        let (payload, owner) = self.into_parts();
        match payload {
            BodyPayload::Once(bytes) => Err(Self::from_parts(BodyPayload::Once(bytes), owner)),
            BodyPayload::Stream(source) => Ok(manual_stream(source, owner)),
        }
    }

    #[inline]
    pub fn is_stream(&self) -> bool {
        matches!(self.payload, BodyPayload::Stream(_))
    }

    /// # Errors
    /// Returns the underlying [`serde_json::Error`] if `value` cannot be serialized.
    #[inline]
    pub fn json<T>(value: &T) -> Result<Self, serde_json::Error>
    where
        T: Serialize,
    {
        serde_json::to_vec(value).map(Self::from_bytes)
    }

    #[inline]
    pub fn stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Bytes> + 'static,
    {
        Self::from_stream(stream.map(Ok::<Bytes, EdgeError>))
    }

    pub(crate) fn take_egress_owner(&mut self) -> Option<OutboundScopeOwner> {
        self.egress_owner.take().map(|owner| *owner)
    }

    #[inline]
    pub fn text<S>(text: S) -> Self
    where
        S: Into<String>,
    {
        Self::from_bytes(text.into().into_bytes())
    }

    /// # Errors
    /// Returns [`serde_json::Error`] if the body is streaming or its bytes are not valid JSON for `T`.
    #[inline]
    pub fn to_json<T>(&self) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        match &self.payload {
            BodyPayload::Once(bytes) => serde_json::from_slice(bytes.as_ref()),
            BodyPayload::Stream(_) => Err(serde_json::Error::io(io::Error::other(
                "streaming body cannot be materialised as JSON",
            ))),
        }
    }
}

impl ManualBodyStream {
    fn release(&mut self) {
        if let Some(owner) = self.owner.take() {
            owner.with(|| {
                let source = self.source.take();
                owner.close();
                drop(source);
            });
        }
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for ManualBodyStream {
    fn drop(&mut self) {
        self.release();
    }
}

impl Stream for ManualBodyStream {
    type Item = Result<Bytes, EdgeError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let Some(source) = &mut this.source else {
            return Poll::Ready(None);
        };
        let polled = if let Some(owner) = &this.owner {
            match owner.scope().check() {
                Ok(()) => owner.with(|| source.as_mut().poll_next(cx)),
                Err(error) => Poll::Ready(Some(Err(error))),
            }
        } else {
            source.as_mut().poll_next(cx)
        };
        if matches!(polled, Poll::Ready(None | Some(Err(_)))) {
            this.release();
        }
        polled
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.source.as_ref().map_or((0, Some(0)), Stream::size_hint)
    }
}

impl Stream for ScopedBodyStream {
    type Item = Result<Bytes, EdgeError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(source) = &self.source else {
            return Poll::Ready(None);
        };
        let observation = source
            .with(cx, |payload, context| {
                let terminal = payload.poll_next_unpin(context);
                if matches!(terminal, Poll::Ready(None | Some(Err(_)))) {
                    // Include terminal cleanup in the revocation check before exposing EOF.
                    drop(mem::replace(payload, stream::empty().boxed_local()));
                }
                terminal
            })
            .unwrap_or_else(|error| Poll::Ready(Some(Err(error))));
        if matches!(observation, Poll::Ready(None | Some(Err(_)))) {
            drop(self.source.take());
        }
        observation
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.source.is_none() {
            (0, Some(0))
        } else {
            (0, None)
        }
    }
}

impl Default for Body {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for Body {
    #[inline]
    fn drop(&mut self) {
        if let Some(owner) = self.egress_owner.take() {
            owner.with(|| {
                let payload = mem::replace(&mut self.payload, BodyPayload::Once(Bytes::new()));
                owner.close();
                drop(payload);
            });
        }
    }
}

impl fmt::Debug for Body {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Body")
            .field("payload", &self.payload)
            .field("egress_owner", &self.egress_owner.is_some())
            .finish()
    }
}

impl fmt::Debug for BodyPayload {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Once(bytes) => f.debug_struct("Once").field("len", &bytes.len()).finish(),
            Self::Stream(_) => f.debug_tuple("Stream").finish(),
        }
    }
}

impl From<Bytes> for Body {
    #[inline]
    fn from(value: Bytes) -> Self {
        Body::from_bytes(value)
    }
}

impl From<Vec<u8>> for Body {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Body::from_bytes(value)
    }
}

impl From<&[u8]> for Body {
    #[inline]
    fn from(value: &[u8]) -> Self {
        Body::from_bytes(Bytes::copy_from_slice(value))
    }
}

impl From<&str> for Body {
    #[inline]
    fn from(value: &str) -> Self {
        Body::text(value)
    }
}

impl From<String> for Body {
    #[inline]
    fn from(value: String) -> Self {
        Body::text(value)
    }
}

fn manual_stream(source: BodyStream, optional_owner: Option<OutboundScopeOwner>) -> BodyStream {
    match optional_owner {
        Some(owner) => ManualBodyStream {
            owner: Some(owner),
            source: Some(source),
        }
        .boxed_local(),
        None => source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ResponseLimitReason;
    use crate::http::StatusCode;
    use crate::outbound::{
        IndependentOutboundProgress, OutboundExchangeOwner, OutboundScope, OutboundScopeOwner,
        Revocable,
    };
    use futures::executor::block_on;
    use futures_util::stream;
    use std::cell::Cell;
    use std::io;
    use std::rc::Rc;

    #[cfg(panic = "unwind")]
    struct ClosingPanic;

    #[cfg(panic = "unwind")]
    struct ClosedSourceDrop(Rc<Cell<bool>>);

    #[cfg(panic = "unwind")]
    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for ClosingPanic {
        fn drop(&mut self) {
            panic!("fixture owner cleanup failure");
        }
    }

    #[cfg(panic = "unwind")]
    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for ClosedSourceDrop {
        fn drop(&mut self) {
            let ambient = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
            self.0.set(ambient.is_managed() && ambient.check().is_err());
        }
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn owner_cleanup_unwind_cannot_retain_or_unbind_payload() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        for manual in [false, true] {
            let owner = OutboundScopeOwner::response_body();
            drop(
                owner
                    .scope()
                    .provider_state(|| ClosingPanic)
                    .expect("provider"),
            );
            let closed_drop = Rc::new(Cell::new(false));
            let source_drop = ClosedSourceDrop(Rc::clone(&closed_drop));
            let polls = Rc::new(Cell::new(0_usize));
            let source_polls = Rc::clone(&polls);
            let body = Body::from_stream(stream::poll_fn(move |_| {
                let _held = &source_drop;
                source_polls.set(source_polls.get().saturating_add(1));
                Poll::Ready(None)
            }))
            .attach_egress_owner(owner)
            .expect("attached owner");
            if manual {
                let mut source = body.into_stream().expect("manual source");
                let failed_close = catch_unwind(AssertUnwindSafe(|| block_on(source.next())));
                assert!(failed_close.is_err(), "fixture exercises cleanup unwind");
                assert!(
                    closed_drop.get(),
                    "source drops while its closed owner is active"
                );
                assert!(block_on(source.next()).is_none());
                assert_eq!(polls.get(), 1, "source cannot be polled after unwind");
            } else {
                let failed_close = catch_unwind(AssertUnwindSafe(|| drop(body)));
                assert!(failed_close.is_err(), "fixture exercises cleanup unwind");
                assert!(
                    closed_drop.get(),
                    "payload drops before restoring ambient scope"
                );
                assert_eq!(polls.get(), 0);
            }
        }
    }

    #[test]
    fn as_bytes_returns_none_for_stream() {
        let body = Body::stream(stream::iter(vec![Bytes::from_static(b"data")]));
        assert!(body.as_bytes().is_none());
    }

    #[test]
    fn attached_buffered_parts_preserve_bytes_and_exact_owner() {
        let bytes = Bytes::from(vec![b'x'; 32]);
        let original = bytes.as_ptr();
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let body = Body::from(bytes)
            .attach_egress_owner(owner)
            .expect("attach")
            .bind_outbound_scope(&scope)
            .expect("bind buffered payload without copying or losing its owner");
        assert_eq!(body.as_bytes().expect("buffered").as_ptr(), original);
        assert!(!body.is_stream());
        let (payload, transferred_owner) = body.into_parts();
        let BodyPayload::Once(transferred_bytes) = payload else {
            panic!("buffered representation must remain Once");
        };
        assert_eq!(transferred_bytes.as_ptr(), original);
        assert_eq!(transferred_bytes.len(), 32);
        scope.check().expect("transferred owner remains live");
        drop(transferred_owner);
        assert!(scope.check().is_err());
    }

    #[test]
    fn manual_stream_conversion_retains_owner_until_consumption() {
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let body = Body::from_stream(stream::iter([Ok(Bytes::from_static(b"data"))]))
            .attach_egress_owner(owner)
            .expect("attach");
        let mut source = body.into_stream().expect("stream");
        scope.check().expect("unpolled stream owns scope");
        assert_eq!(
            block_on(source.next()).expect("chunk").expect("bytes"),
            "data"
        );
        scope.check().expect("nonterminal chunk retains scope");
        assert!(block_on(source.next()).is_none());
        assert!(scope.check().is_err());
    }

    #[test]
    fn manual_buffered_stream_conversion_returns_original_owner() {
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let body = Body::from("payload")
            .attach_egress_owner(owner)
            .expect("attach");
        let returned_body = body
            .into_stream()
            .err()
            .expect("buffered body returned intact");
        assert_eq!(returned_body.as_bytes(), Some(b"payload".as_slice()));
        scope.check().expect("returned body keeps ownership");
        drop(returned_body);
        assert!(scope.check().is_err());
    }

    #[test]
    fn dropping_attached_source_closes_roots_before_closed_scope_cleanup() {
        struct Release(Rc<Cell<usize>>);
        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.set(self.0.get().saturating_add(1));
            }
        }
        struct Cleanup(Rc<Cell<bool>>);
        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let ambient = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
                self.0.set(ambient.check().is_err());
            }
        }
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let released = Rc::new(Cell::new(0));
        let retained = Revocable::new(&scope, Release(Rc::clone(&released))).expect("root");
        let cleanup_closed = Rc::new(Cell::new(false));
        let cleanup = Cleanup(Rc::clone(&cleanup_closed));
        let body = Body::from_stream(stream::poll_fn(move |_| {
            let _keep = &cleanup;
            panic!("cleanup must not poll the source");
        }))
        .attach_egress_owner(owner)
        .expect("attach");
        drop(body);
        assert!(retained.is_revoked());
        assert_eq!(released.get(), 1);
        assert!(cleanup_closed.get());
    }

    #[test]
    fn manual_stream_error_and_abandonment_close_attached_scope() {
        for fail in [false, true] {
            let owner = OutboundScopeOwner::response_body();
            let scope = owner.scope();
            let body = Body::from_stream(stream::once(async {
                Err(EdgeError::gateway_timeout("source error"))
            }))
            .attach_egress_owner(owner)
            .expect("attach");
            let mut source = body.into_stream().expect("stream");
            if fail {
                assert!(matches!(
                    block_on(source.next()),
                    Some(Err(EdgeError::GatewayTimeout { .. }))
                ));
                assert!(scope.check().is_err());
            }
            drop(source);
            assert!(scope.check().is_err());
        }
    }

    #[test]
    fn collect_stream_body() {
        let body = Body::stream(stream::iter(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]));
        assert!(body.is_stream());
        let mut stream = body.into_stream().expect("stream");
        let collected = block_on(async {
            let mut data = Vec::new();
            while let Some(result) = stream.next().await {
                let chunk = result.expect("chunk");
                data.extend_from_slice(&chunk);
            }
            data
        });
        assert_eq!(collected, b"ab");
    }

    #[test]
    fn debug_formats_both_body_variants() {
        let buffered = Body::from("payload");
        let buffered_debug = format!("{buffered:?}");
        assert_eq!(
            buffered_debug,
            "Body { payload: Once { len: 7 }, egress_owner: false }"
        );

        let stream = Body::stream(stream::iter(vec![Bytes::from_static(b"chunk")]));
        let stream_debug = format!("{stream:?}");
        assert_eq!(
            stream_debug,
            "Body { payload: Stream, egress_owner: false }"
        );
    }

    #[test]
    fn default_body_is_empty() {
        let body = Body::default();
        assert!(body.as_bytes().expect("buffered").is_empty());
    }

    #[test]
    fn body_from_external_stream_maps_to_internal() {
        let source = stream::iter(vec![
            Ok(Bytes::from_static(b"ok")),
            Err(io::Error::other("boom")),
        ]);
        let body = Body::from_external_stream(source);
        let mut chunks = body.into_stream().expect("stream");
        let (first, second) = block_on(async {
            let first = chunks.next().await.expect("first").expect("ok");
            let second = chunks.next().await.expect("second");
            (first, second)
        });
        assert_eq!(first, Bytes::from_static(b"ok"));
        let err = second.expect_err("error");
        assert!(matches!(err, EdgeError::Internal { .. }));
        assert!(err.to_string().contains("boom"));
    }

    #[test]
    fn body_from_stream_preserves_edge_error() {
        let source = stream::iter([Err(EdgeError::response_too_large_with_reason(
            "encoded cap",
            ResponseLimitReason::EncodedBody,
        ))]);
        let body = Body::from_stream(source);
        let mut chunks = body.into_stream().expect("stream");
        let error = block_on(chunks.next())
            .expect("item")
            .expect_err("typed error");
        assert!(matches!(
            error,
            EdgeError::ResponseTooLarge {
                reason: ResponseLimitReason::EncodedBody,
                ..
            }
        ));
    }

    #[test]
    fn body_into_bytes_bounded_preserves_edge_error() {
        let source = stream::iter([Err(EdgeError::gateway_timeout("expired"))]);
        let error =
            block_on(Body::from_stream(source).into_bytes_bounded(100)).expect_err("typed error");
        assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
    }

    #[test]
    fn body_stream_accepts_infallible_bytes() {
        let body = Body::stream(stream::iter([
            Bytes::from_static(b"one"),
            Bytes::from_static(b"two"),
        ]));
        let bytes = block_on(body.into_bytes_bounded(6)).expect("body");
        assert_eq!(bytes, Bytes::from_static(b"onetwo"));
        assert_eq!(
            Body::from(Bytes::from_static(b"bytes")).as_bytes(),
            Some(b"bytes".as_slice())
        );
    }

    #[test]
    fn bounded_body_overflow_has_a_distinct_413_category() {
        for len in [7_usize, 8, 9] {
            for streamed in [false, true] {
                let bytes = Bytes::from(vec![b'x'; len]);
                let body = if streamed {
                    Body::stream(stream::iter([bytes]))
                } else {
                    Body::from(bytes)
                };
                match block_on(body.into_bytes_bounded(8)) {
                    Ok(collected) => {
                        assert!(len <= 8, "overflow must not truncate into success");
                        assert_eq!(collected.len(), len);
                    }
                    Err(error) => {
                        assert_eq!(len, 9);
                        assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
                        assert_eq!(error.kind(), "payload_too_large");
                    }
                }
            }
        }
    }

    fn assert_scoped_terminal_drop_is_not_completion(errored: bool) {
        let callback = Rc::new(OutboundScopeOwner::callback());
        let closing = Rc::clone(&callback);
        let drops = Rc::new(Cell::new(0_usize));
        let counted = Rc::clone(&drops);
        let release = OutboundExchangeOwner::new(move || {
            counted.set(counted.get().saturating_add(1));
            closing.close();
        });
        let scope = callback.with(|| OutboundScope::capture(Rc::new(IndependentOutboundProgress)));
        let raw = stream::poll_fn(move |_| {
            let _retained = &release;
            Poll::Ready(errored.then(|| Err(EdgeError::bad_request("fixture source error"))))
        });
        let body = Body::from_stream(raw)
            .bind_outbound_scope(&scope)
            .expect("bound source");
        let mut source = body.into_stream().expect("stream");
        let terminal = block_on(source.next());
        assert!(
            matches!(terminal, Some(Err(EdgeError::Internal { .. }))),
            "terminal source cleanup must not hide scope closure"
        );
        assert!(block_on(source.next()).is_none());
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn scoped_eof_cleanup_closure_is_reported_as_internal() {
        assert_scoped_terminal_drop_is_not_completion(false);
    }

    #[test]
    fn scoped_error_cleanup_closure_is_reported_as_internal() {
        assert_scoped_terminal_drop_is_not_completion(true);
    }

    #[test]
    fn body_bounded_checks_before_append() {
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let source = stream::iter([
            Ok(Bytes::from_static(b"too-large")),
            Ok(Bytes::from_static(b"must-not-poll")),
        ])
        .inspect(move |_| observed.set(observed.get().saturating_add(1)));
        let error =
            block_on(Body::from_stream(source).into_bytes_bounded(3)).expect_err("over limit");
        assert!(matches!(
            error,
            EdgeError::PayloadTooLarge {
                origin: RequestBodyOverflowOrigin::Inbound
            }
        ));
        assert_eq!(polls.get(), 1);
    }

    #[test]
    fn from_vec_u8_builds_buffered_body() {
        let body = Body::from(vec![1_u8, 2_u8, 3_u8]);
        assert_eq!(body.as_bytes().expect("buffered"), &[1_u8, 2_u8, 3_u8]);
    }

    #[test]
    fn into_bytes_bounded_buffered_ok() {
        let body = Body::from("hello");
        let result = block_on(body.into_bytes_bounded(100));
        assert_eq!(result.unwrap(), Bytes::from("hello"));
    }

    #[test]
    fn into_bytes_bounded_buffered_too_large() {
        let body = Body::from("hello");
        block_on(body.into_bytes_bounded(3)).expect_err("body exceeds max_size");
    }

    #[test]
    fn into_bytes_bounded_stream_ok() {
        let body = Body::stream(stream::iter(vec![
            Bytes::from_static(b"ab"),
            Bytes::from_static(b"cd"),
        ]));
        let result = block_on(body.into_bytes_bounded(100));
        assert_eq!(result.unwrap(), Bytes::from("abcd"));
    }

    #[test]
    fn into_bytes_bounded_stream_too_large() {
        let body = Body::stream(stream::iter(vec![
            Bytes::from_static(b"ab"),
            Bytes::from_static(b"cd"),
        ]));
        block_on(body.into_bytes_bounded(3)).expect_err("stream exceeds max_size");
    }

    #[test]
    fn into_bytes_returns_none_for_stream() {
        let body = Body::stream(stream::iter(vec![Bytes::from_static(b"data")]));
        assert!(body.into_bytes().is_none());
    }

    #[test]
    fn into_stream_returns_buffered_body() {
        let body = Body::from("payload");
        let returned_body = body.into_stream().err().expect("buffered body");
        assert_eq!(returned_body.as_bytes(), Some(b"payload".as_slice()));
    }

    #[test]
    fn is_stream_returns_false_for_buffered_body() {
        let body = Body::from("payload");
        assert!(!body.is_stream());
    }

    #[test]
    fn to_json_fails_for_streaming_body() {
        let body = Body::stream(stream::iter(vec![
            Bytes::from_static(b"{"),
            Bytes::from_static(b"}"),
        ]));
        body.to_json::<serde_json::Value>()
            .expect_err("streaming body cannot deserialize as JSON");
    }
}
