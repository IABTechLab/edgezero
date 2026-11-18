//! Separate operator component; never include this route in a serving application.
#[cfg(any(test, all(target_arch = "wasm32", target_os = "wasi")))]
use edgezero_core::http::HeaderMap;
#[cfg(any(test, all(target_arch = "wasm32", target_os = "wasi")))]
use subtle::ConstantTimeEq as _;

#[cfg(any(test, all(target_arch = "wasm32", target_os = "wasi")))]
fn authenticated(headers: &HeaderMap, expected: &[u8]) -> bool {
    let valid =
        |value: &[u8]| (32..=128).contains(&value.len()) && value.iter().all(u8::is_ascii_graphic);
    let mut values = headers.get_all("x-edgezero-loader-token").iter();
    values.next().is_some_and(|value| {
        values.next().is_none()
            && valid(value.as_bytes())
            && valid(expected)
            && bool::from(value.as_bytes().ct_eq(expected))
    })
}

#[cfg(all(target_arch = "wasm32", target_os = "wasi"))]
mod component {
    use super::authenticated;
    use edgezero_core::app::Hooks;
    use edgezero_core::context::RequestContext;
    use edgezero_core::http::response_builder;
    use edgezero_core::http::{Response, StatusCode};
    use edgezero_core::key_value_store::KvHandle;
    use edgezero_core::router::RouterService;
    use edgezero_core::{Body, EdgeError, action};
    use geo_kv_prototype::publication::{Artifact, publish, verify};
    use std::{path::Path, sync::Arc};

    struct Loader;
    impl Hooks for Loader {
        fn routes() -> Result<RouterService, EdgeError> {
            Ok(RouterService::builder().post("/publish", load).build())
        }
    }
    fn response(status: StatusCode, body: String) -> Result<Response, EdgeError> {
        response_builder()
            .status(status)
            .header("content-type", "application/json")
            .header("cache-control", "no-store")
            .body(Body::from(body))
            .map_err(EdgeError::internal)
    }
    #[action]
    async fn load(ctx: RequestContext) -> Result<Response, EdgeError> {
        let expected = spin_sdk::variables::get("loader_token").await;
        if !expected
            .as_ref()
            .is_ok_and(|value| authenticated(ctx.headers(), value.as_bytes()))
        {
            return response(
                StatusCode::UNAUTHORIZED,
                "{\"error\":\"unauthorized\"}".into(),
            );
        }
        let result = async {
            let artifact = Artifact::open(Path::new("/artifact")).await?;
            let label = spin_sdk::variables::get("loader_store").await?;
            let store = edgezero_adapter_spin::key_value_store::SpinKvStore::open(&label).await?;
            let kv = KvHandle::new(Arc::new(store));
            let report = publish(&artifact, &kv).await?;
            verify(&artifact, kv).await?;
            Ok::<_, anyhow::Error>(serde_json::json!({
                "objects": report.objects, "written": report.written, "identical": report.identical,
                "pin": format!("{}@{}", artifact.receipt.manifest_key.split('/').nth(2).unwrap_or(""), geo_kv_prototype::index::hex(&artifact.receipt.manifest_hash)),
            }))
        }.await;
        match result {
            Ok(report) => response(StatusCode::OK, report.to_string()),
            Err(_) => response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "{\"error\":\"publication failed\"}".into(),
            ),
        }
    }
    #[spin_sdk::http_service]
    async fn handle(
        req: spin_sdk::http::Request,
    ) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
        edgezero_adapter_spin::run_app::<Loader>(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::http::HeaderValue;
    #[test]
    fn loader_requires_one_valid_credential() {
        let token = b"0123456789abcdef0123456789abcdef";
        let mut headers = HeaderMap::new();
        assert!(!authenticated(&headers, token));
        headers.insert(
            "x-edgezero-loader-token",
            HeaderValue::from_bytes(token).unwrap(),
        );
        assert!(authenticated(&headers, token));
        assert!(!authenticated(
            &headers,
            b"ffffffffffffffffffffffffffffffff"
        ));
        assert!(!authenticated(&headers, b"short"));
        headers.append(
            "x-edgezero-loader-token",
            HeaderValue::from_bytes(token).unwrap(),
        );
        assert!(!authenticated(&headers, token));
    }
}
