use std::collections::BTreeMap;
use std::num::NonZeroU64;

use edgezero_core::outbound::{
    HttpClient, OutboundCollectedSlot, OutboundProxyMarker, OutboundRequest, ResponseDecoding,
    ResponseMode,
};

use super::*;

type FixtureHead = Box<dyn FnOnce(OutboundResponseContext) -> Result<OutboundHead, EdgeError>>;

thread_local! {
    static RESPONSES: RefCell<BTreeMap<u64, FixtureHead>> = const { RefCell::new(BTreeMap::new()) };
    static NEXT_RESPONSE: Cell<u64> = const { Cell::new(0) };
}

struct ResponseClient {
    id: u64,
    time: OutboundTime,
}

#[expect(
    clippy::missing_trait_methods,
    reason = "Drop::pin_drop is unstable and conflicts with drop"
)]
impl Drop for ResponseClient {
    fn drop(&mut self) {
        let response = RESPONSES.with(|responses| responses.borrow_mut().remove(&self.id));
        drop(response);
    }
}

impl OutboundHttpClient for ResponseClient {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        let head = self.dispatch_head(request);
        schedule_response(Box::pin(async move { head.await?.finish().await }))
    }

    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        let response = RESPONSES.with(|responses| responses.borrow_mut().remove(&self.id));
        let (_, context) = request.into_parts();
        Box::pin(async move {
            response
                .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("fixture response missing")))?(
                context,
            )
        })
    }

    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(IndependentOutboundProgress)
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        schedule_batch(self, batch)
    }

    fn time(&self) -> OutboundTime {
        self.time.clone()
    }

    fn validate_supported_options(
        &self,
        request: &OutboundRequest,
        _mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        super::super::validate_supported_options(request)
    }
}

pub(super) fn fixture_client<Head>(time: OutboundTime, head: Head) -> Result<HttpClient, EdgeError>
where
    Head: FnOnce(OutboundResponseContext) -> Result<OutboundHead, EdgeError> + 'static,
{
    let id = NEXT_RESPONSE
        .with(|next| {
            let id = next.get();
            id.checked_add(1).map(|next_id| {
                next.set(next_id);
                id
            })
        })
        .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("fixture identity overflow")))?;
    RESPONSES.with(|responses| responses.borrow_mut().insert(id, Box::new(head)));
    Ok(HttpClient::with_client(ResponseClient { id, time }))
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing response fixtures exercise independent policy boundaries"
)]
pub(super) async fn finish_fixture_response(
    response: WorkerResponse,
    abort_guard: AbortGuard,
    request_method: Method,
    proxy_marker: OutboundProxyMarker,
    response_decoding: ResponseDecoding,
    response_mode: ResponseMode,
    budget: DispatchBudget,
    max_decoder_bytes: u64,
    max_brotli_window_bits: u8,
    max_chunk_bytes: Option<NonZeroU64>,
    max_decoded_response_bytes: Option<u64>,
    max_encoded_response_bytes: Option<u64>,
    max_response_header_bytes: Option<u64>,
    max_response_header_count: Option<u64>,
    time: OutboundTime,
) -> Result<OutboundResponse, EdgeError> {
    let mut parts = OutboundRequest::get("https://fixture.invalid/")?.into_parts();
    parts.method = request_method;
    parts.proxy_marker = proxy_marker;
    parts.response_decoding = response_decoding;
    parts.response_mode = response_mode;
    parts.max_decoder_bytes = max_decoder_bytes;
    parts.max_brotli_window_bits = max_brotli_window_bits;
    parts.max_chunk_bytes = max_chunk_bytes;
    parts.max_decoded_response_bytes = max_decoded_response_bytes;
    parts.max_encoded_response_bytes = max_encoded_response_bytes;
    parts.max_response_header_bytes = max_response_header_bytes;
    parts.max_response_header_count = max_response_header_count;
    match budget.cause {
        BudgetSource::PerCallTimeout => parts.timeout = Some(budget.duration),
        BudgetSource::RequestDeadline => parts.deadline = Some(budget.deadline),
        BudgetSource::BatchCutoff | BudgetSource::Default => {}
        BudgetSource::ConnectTimeout
        | BudgetSource::IdleTimeout
        | BudgetSource::Unspecified
        | _ => {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "unsupported fixture governing source"
            )));
        }
    }
    let request = OutboundRequest::from_parts(parts)?;
    let client = fixture_client(time, move |context| {
        response_head(response, abort_guard, context)
    })?;
    if budget.cause == BudgetSource::BatchCutoff {
        let results = client
            .start_batch_until(vec![request], budget.deadline)
            .collect()
            .await
            .map_err(|failure| failure.into_parts().0)?;
        let (mut slots, _) = results.into_parts();
        match slots
            .pop()
            .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("fixture slot missing")))?
        {
            OutboundCollectedSlot::Resolved(slot) => slot.outcome,
            OutboundCollectedSlot::Unresolved => Err(timeout_error(BudgetSource::BatchCutoff)),
            OutboundCollectedSlot::AlreadyDelivered => Err(EdgeError::internal(anyhow::anyhow!(
                "fixture result was already delivered"
            ))),
        }
    } else {
        client.send(request).await
    }
}

pub(super) async fn finish_fixture_stream(
    source: BodyStream,
    guard: AbortGuard,
    budget: DispatchBudget,
    time: OutboundTime,
) -> Result<BodyStream, EdgeError> {
    let mut parts = OutboundRequest::get("https://fixture.invalid/")?
        .stream_response()
        .into_parts();
    match budget.cause {
        BudgetSource::PerCallTimeout => parts.timeout = Some(budget.duration),
        BudgetSource::RequestDeadline => parts.deadline = Some(budget.deadline),
        BudgetSource::Default => {}
        BudgetSource::BatchCutoff
        | BudgetSource::ConnectTimeout
        | BudgetSource::IdleTimeout
        | BudgetSource::Unspecified
        | _ => {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "unsupported streamed fixture budget"
            )));
        }
    }
    let client = fixture_client(time, move |context| {
        OutboundHead::new(
            StatusCode::OK,
            OutboundHeaderInput::new(
                "cloudflare",
                HeaderMap::new(),
                OutboundHeaderFidelity::ProviderFieldBoundariesUnknown,
            ),
            guarded_response_stream(source, guard),
            context,
            OutboundExchangeOwner::new(|| {}),
        )
    })?;
    client
        .send(OutboundRequest::from_parts(parts)?)
        .await?
        .into_body()
        .into_stream()
        .map_err(|_buffered| {
            EdgeError::internal(anyhow::anyhow!("streamed fixture returned a buffered body"))
        })
}
