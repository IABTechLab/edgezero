#![allow(
    clippy::too_many_lines,
    reason = "each contract vector's options stay in one shared request-construction table"
)]

use super::core_api as api;
use super::vectors::{Case, Coding, ErrorCase, HeaderCase, Kind, MarkerCase};
use api::http::{Method, Uri};
use api::{
    Deadline, EdgeError, MonotonicInstant, OutboundCachePolicy, OutboundProxyMarker,
    OutboundRequest, OutboundRequestParts, ResponseDecoding,
};
use serde_json::{Value, json};
use std::{num::NonZeroU64, time::Duration};

pub fn make(
    case: Case,
    uri: Uri,
    token: &str,
    step: &str,
    entered: MonotonicInstant,
) -> Result<OutboundRequest, EdgeError> {
    let mut request = OutboundRequest::new(
        if case.request_method() == "POST" {
            Method::POST
        } else {
            Method::GET
        },
        uri,
    )?
    .header("x-request-token", token)?
    .header("x-transport-step", step)?
    .timeout(Duration::from_secs(3))
    .max_response_bytes(64)
    .max_encoded_response_bytes(256)
    .max_decoded_response_bytes(64)
    .max_decoder_bytes(32 * 1024 * 1024)
    .max_brotli_window_bits(30)
    .max_request_body_bytes(64);
    if !case.request_body().is_empty() {
        request = request.body(case.request_body());
    }
    request = match case.kind {
        Kind::Policy {
            coding, declared, ..
        } => {
            request = request.max_response_bytes(10);
            if case.streamed {
                request = request.max_decoded_response_bytes(10);
            }
            if declared || (case.streamed && coding == Coding::Passthrough) {
                request = request.max_encoded_response_bytes(10);
            }
            request
        }
        Kind::Coding {
            original, partial, ..
        } => {
            request = request.header("accept-encoding", "gzip, deflate, br")?;
            if original {
                request = request.response_decoding(ResponseDecoding::Original);
            }
            if original || partial {
                request = request.max_decoder_bytes(0).max_decoded_response_bytes(1);
            }
            request
        }
        Kind::Rechunk { original, .. } => {
            request = request.max_chunk_bytes(
                NonZeroU64::new(2)
                    .ok_or_else(|| EdgeError::bad_request("invalid parity chunk cap"))?,
            );
            if original {
                request = request.response_decoding(ResponseDecoding::Original);
            }
            request
        }
        Kind::Passthrough => request.max_decoded_response_bytes(1),
        Kind::OriginalIdentity { .. } => request
            .response_decoding(ResponseDecoding::Original)
            .max_decoded_response_bytes(1),
        Kind::Delay { deadline, .. } => {
            if deadline {
                request
                    .timeout(Duration::from_secs(1))
                    .deadline(Deadline::at_instant(
                        entered
                            .checked_add(Duration::from_millis(150))
                            .ok_or_else(|| EdgeError::bad_request("parity deadline overflow"))?,
                    ))
            } else {
                request.timeout(Duration::from_millis(150))
            }
        }
        Kind::Error(ErrorCase::DecoderMemory) => request.max_decoder_bytes(0),
        Kind::Error(ErrorCase::BrotliWindow) => request.max_brotli_window_bits(10),
        Kind::Header(HeaderCase::Bytes) => request.max_response_header_bytes(0),
        Kind::Header(HeaderCase::Count) => request.max_response_header_count(0),
        Kind::Header(HeaderCase::Tie) => request
            .max_response_header_bytes(0)
            .max_response_header_count(0),
        Kind::Header(HeaderCase::MarkerCountOff | HeaderCase::MarkerCountOn) => {
            request.max_response_header_count(5)
        }
        Kind::Header(HeaderCase::MarkerBytesOff | HeaderCase::MarkerBytesOn) => {
            let reply = case.reply(token);
            let raw_bytes = reply
                .fields
                .iter()
                .map(|(name, value)| name.len().saturating_add(value.len()))
                .sum::<usize>()
                .saturating_add("content-length".len())
                .saturating_add(1);
            request
                .max_response_header_bytes(u64::try_from(raw_bytes).map_err(EdgeError::internal)?)
        }
        Kind::Marker(MarkerCase::Off) => request
            .proxy_marker(OutboundProxyMarker::Off)
            .max_response_header_count(6),
        Kind::Marker(MarkerCase::On) => request.proxy_marker(OutboundProxyMarker::On),
        Kind::Upload { cap } => request.max_request_body_bytes(cap),
        Kind::Authority => request.host_authority_override("virtual.example:8443")?,
        Kind::Cache => request.cache_policy(OutboundCachePolicy::Bypass),
        Kind::Connect => request
            .connect_timeout(Duration::from_millis(200))
            .timeout(Duration::from_secs(2)),
        Kind::RequestNormalize => request
            .header("connection", "x-private")?
            .header("x-private", "removed")?
            .header("proxy-connection", "keep-alive")?,
        Kind::Bodyless205
        | Kind::Error(ErrorCase::Body | ErrorCase::Gzip | ErrorCase::Head)
        | Kind::GetBody
        | Kind::StreamLive
        | Kind::Header(HeaderCase::Duplicate | HeaderCase::Normalize)
        | Kind::Marker(MarkerCase::InheritOff)
        | Kind::Upstream413 => request,
    };
    if case.streamed {
        request = request.stream_response();
    }
    Ok(request)
}

pub fn settings(parts: &OutboundRequestParts) -> Value {
    json!({"authority":parts.host_authority_override.as_ref().map(ToString::to_string),
        "body_bytes":parts.body.as_bytes(),"cache":format!("{:?}",parts.cache_policy),
        "chunk_bytes":parts.max_chunk_bytes.map(NonZeroU64::get),"connect_ms":parts.connect_timeout.map(|value|value.as_millis()),
        "decoding":format!("{:?}",parts.response_decoding),"deadline":parts.deadline.is_some(),
        "decoded_cap":parts.max_decoded_response_bytes,"decoder_cap":parts.max_decoder_bytes,
        "encoded_cap":parts.max_encoded_response_bytes,"header_bytes":parts.max_response_header_bytes,
        "header_count":parts.max_response_header_count,"method":parts.method.as_str(),
        "mode":format!("{:?}",parts.response_mode),"proxy":format!("{:?}",parts.proxy_marker),
        "response_cap":match parts.response_mode {api::ResponseMode::Buffered{max_bytes}=>Some(max_bytes),api::ResponseMode::Streamed=>None},
        "timeout_ms":parts.timeout.map(|value|value.as_millis()),"upload_cap":parts.max_request_body_bytes,"window_bits":parts.max_brotli_window_bits})
}
