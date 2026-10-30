use edgezero_core::{EdgeError, context::RequestContext, http::Response};
use std::sync::OnceLock;

pub const HEADER: &str = "x-edgezero-fixture-launch";
pub const NONCE: &str = "EDGEZERO_FIXTURE_LAUNCH_NONCE";
static IDENTITY: OnceLock<String> = OnceLock::new();

pub fn initialize(nonce: Option<String>, mode: &str) {
    if let (Some(nonce), Some(build)) = (nonce, option_env!("EDGEZERO_FIXTURE_BUILD_ID")) {
        let valid = |value: &str| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        };
        if valid(&nonce) && valid(build) && valid(mode) {
            let _ = IDENTITY.set(format!("v1;{nonce};{build};{mode}"));
        }
    }
}

pub fn initialize_environment(mode: &str) {
    initialize(std::env::var(NONCE).ok(), mode);
}

pub fn value() -> Option<&'static str> {
    IDENTITY.get().map(String::as_str)
}

pub fn stamp(response: &mut Response) {
    if let Some(identity) = value() {
        response
            .headers_mut()
            .insert(HEADER, identity.parse().expect("validated launch identity"));
    }
}

pub struct Identity;
#[async_trait::async_trait(?Send)]
impl edgezero_core::middleware::Middleware for Identity {
    async fn handle(
        &self,
        ctx: RequestContext,
        next: edgezero_core::middleware::Next<'_>,
    ) -> Result<Response, EdgeError> {
        let mut response = next.run(ctx).await?;
        stamp(&mut response);
        Ok(response)
    }
}
