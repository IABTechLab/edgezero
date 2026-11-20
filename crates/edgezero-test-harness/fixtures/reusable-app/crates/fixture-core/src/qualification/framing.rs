//! Literal response sources for adapter framing and SDK-reference probes.

use bytes::Bytes;
use edgezero_core::context::RequestContext;
use edgezero_core::http::{Response, StatusCode, response_builder};
use edgezero_core::response_egress::ResponseEgressDeadline;
use edgezero_core::time::Deadline;
use edgezero_core::{Body, EdgeError, action};
use futures::stream;

pub fn source(case: &str) -> Result<Response, EdgeError> {
    let mut builder = response_builder();
    let body = match case {
        "buffered" | "head" | "expired" => Body::from("abcdef"),
        "empty" => Body::empty(),
        "explicit" => {
            builder = builder.header("content-length", "6");
            Body::from("abcdef")
        }
        "mismatch" => {
            builder = builder.header("content-length", "7");
            Body::from("abcdef")
        }
        "trailers" => {
            builder = builder.header("trailer", "private-probe-field");
            Body::from("must-not-escape")
        }
        "head-explicit" | "not-modified-explicit" => {
            builder = builder.header("content-length", "6");
            if case == "not-modified-explicit" {
                builder = builder.status(StatusCode::NOT_MODIFIED);
            }
            Body::from("abcdef")
        }
        "not-modified" | "no-content" | "reset-content" => {
            builder = builder.status(match case {
                "not-modified" => StatusCode::NOT_MODIFIED,
                "no-content" => StatusCode::NO_CONTENT,
                _ => StatusCode::RESET_CONTENT,
            });
            Body::from("abcdef")
        }
        "streamed" | "declared-stream" | "short-stream" => {
            if case != "streamed" {
                builder = builder.header(
                    "content-length",
                    if case == "short-stream" { "7" } else { "6" },
                );
            }
            Body::stream(stream::iter([
                Bytes::from_static(b"abc"),
                Bytes::from_static(b"def"),
            ]))
        }
        "compress-buffered" | "compress-declared-stream" => {
            builder = builder
                .header("content-type", "text/plain")
                .header("x-compress-hint", "on");
            let bytes = Bytes::from("compressible-source-".repeat(512));
            if case == "compress-declared-stream" {
                builder = builder.header("content-length", bytes.len());
                Body::stream(stream::iter([bytes]))
            } else {
                Body::from_bytes(bytes)
            }
        }
        _ => return Err(EdgeError::bad_request("unknown controlled framing case")),
    };
    builder.body(body).map_err(EdgeError::internal)
}

#[action]
pub(super) async fn probe(ctx: RequestContext) -> Result<Response, EdgeError> {
    let case = ctx
        .path_params()
        .get("case")
        .ok_or_else(|| EdgeError::bad_request("missing controlled framing case"))?;
    if case == "rendered-not-found" {
        return Err(EdgeError::not_found("/controlled-framing-missing"));
    }
    let mut response = source(case)?;
    if case == "expired" {
        response
            .extensions_mut()
            .insert(ResponseEgressDeadline::at(Deadline::at_instant(
                ctx.monotonic_clock().now(),
            )));
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sources_do_not_prepopulate_implicit_framing() {
        for case in [
            "buffered",
            "empty",
            "head",
            "not-modified",
            "no-content",
            "reset-content",
            "streamed",
            "compress-buffered",
        ] {
            let response = source(case).unwrap();
            assert!(!response.headers().contains_key("content-length"), "{case}");
            assert!(
                !response.headers().contains_key("transfer-encoding"),
                "{case}"
            );
        }
        for case in [
            "explicit",
            "head-explicit",
            "not-modified-explicit",
            "declared-stream",
        ] {
            assert_eq!(source(case).unwrap().headers()["content-length"], "6");
        }
        assert!(source("uncontrolled").is_err());
    }
}
