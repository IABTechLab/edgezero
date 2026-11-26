//! Suite-only inputs for the canonical synthesized-header delivery paths.

use edgezero_core::app::App;
use edgezero_core::context::RequestContext;
use edgezero_core::http::{HeaderMap, HeaderValue, Response};
use edgezero_core::ingress::{AdmissionDecision, IngressGrant};
use edgezero_core::response_egress::{DetachedResponseEgressDecision, ResponseEgressCompletion};
use edgezero_core::{Body, EdgeError, action};
use serde_json::json;
use std::time::Duration;

pub fn enabled() -> bool {
    option_env!("EDGEZERO_QUALIFICATION_SUITE") == Some("synthesized-headers")
}

pub fn fields(source: &'static str) -> HeaderMap {
    let mut fields = HeaderMap::new();
    fields.insert("x-synthesized-fixture", HeaderValue::from_static(source));
    for index in 0..30 {
        fields.append(
            "set-cookie",
            format!("slot-{index:02}=value; Path=/").parse().unwrap(),
        );
    }
    let used = 2
        + fields
            .iter()
            .map(|(name, value)| name.as_str().len() + value.len() + 4)
            .sum::<usize>()
        + "set-cookie".len()
        + 4;
    let padding = 4096_usize
        .checked_sub(used + "padding=".len())
        .expect("fixture header budget");
    fields.append(
        "set-cookie",
        format!("padding={}", "a".repeat(padding)).parse().unwrap(),
    );
    fields
}

pub fn token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-request-token")
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .map(str::to_owned)
}

fn completion(token: Option<String>) -> ResponseEgressCompletion {
    ResponseEgressCompletion::new(move |report| {
        eprintln!(
            "{}",
            json!({"event":"synthesized-terminal","token":token,
            "body_kind":format!("{:?}", report.body_kind),"outcome":format!("{:?}",report.outcome),"bytes_written":report.bytes_written,
            "fallback":report.fallback_disposition.map(|value|format!("{value:?}")),"qualification":false})
        );
    })
}

pub fn detached_witness(app: &mut App, token: Option<String>) {
    app.set_detached_response_egress_decision_factory(move |head| {
        eprintln!("{}", json!({"event":"synthesized-detached","token":token,"status":head.status().as_u16(),"qualification":false}));
        DetachedResponseEgressDecision::Send { completion: completion(token.clone()), deadline: head.write_deadline_after(Duration::from_secs(2)) }
    });
}

pub fn configure(app: &mut App) -> Result<(), EdgeError> {
    if !enabled() {
        return Ok(());
    }
    app.set_synthesized_response_headers(fields("app"))
        .map_err(EdgeError::internal)?;
    app.set_ingress_admission_policy(|head| {
        let token = token(head.headers());
        eprintln!(
            "{}",
            json!({"event":"synthesized-admission","token":token,"qualification":false})
        );
        let completion = completion(token);
        if head.target().path() == "/progress/synthesized/refused" {
            let mut response = Response::new(Body::from("refused by probe"));
            *response.status_mut() = edgezero_core::http::StatusCode::SERVICE_UNAVAILABLE;
            return AdmissionDecision::Refuse {
                completion,
                response,
            };
        }
        AdmissionDecision::Admit {
            completion,
            grant: IngressGrant::empty(),
            read_deadline: head.read_deadline_after(Duration::from_secs(2)),
        }
    });
    Ok(())
}

#[action]
pub(super) async fn probe(ctx: RequestContext) -> Result<Response, EdgeError> {
    eprintln!(
        "{}",
        json!({"event":"synthesized-handler","token":token(ctx.headers()),"qualification":false})
    );
    match ctx.path_params().get("case") {
        Some("success") => Ok(Response::new(Body::from("routed success"))),
        Some("handler-error") => Err(EdgeError::bad_request("controlled handler error")),
        Some("fallback") => super::framing::source("trailers"),
        _ => Err(EdgeError::bad_request("unknown synthesized header case")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::SynthesizedResponseHeaders;

    #[test]
    fn synthesized_fixture_fills_the_inclusive_policy_bounds() {
        for source in ["app", "request"] {
            let fields = fields(source);
            assert_eq!(fields.len(), 32);
            assert_eq!(
                2 + fields
                    .iter()
                    .map(|(name, value)| name.as_str().len() + value.len() + 4)
                    .sum::<usize>(),
                4096
            );
            assert_eq!(fields["x-synthesized-fixture"], source);
            assert_eq!(fields.get_all("set-cookie").iter().count(), 31);
            SynthesizedResponseHeaders::new(fields).unwrap();
        }
    }
}
