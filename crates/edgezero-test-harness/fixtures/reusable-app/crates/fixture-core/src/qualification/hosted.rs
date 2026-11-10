use edgezero_core::{
    Deadline, EdgeError, OutboundCachePolicy, OutboundImageFormat, OutboundImageOptimizerOptions,
    OutboundImageOptimizerRegion, OutboundProxyMarker, OutboundRequest, ResponseDecoding,
    http::{HeaderMap, Method, Uri},
};
use std::time::Duration;

pub struct Plan {
    pub case: String,
    pub token: String,
    pub target: Uri,
    pub method: Method,
    pub timeout: Option<Duration>,
    pub connect: Option<Duration>,
    pub deadline: Option<Duration>,
    pub batch: bool,
    pub pair: bool,
    pub optimizer: bool,
    pub stream: bool,
    pub preserve_query: bool,
    pub region: OutboundImageOptimizerRegion,
}

fn invalid() -> EdgeError {
    EdgeError::bad_request("invalid hosted qualification probe")
}

impl Plan {
    pub fn parse(case: &str, method: Method, headers: &HeaderMap) -> Result<Self, EdgeError> {
        if ![
            "long-budget",
            "head-timeout",
            "body-buffered",
            "body-streamed",
            "connect",
            "connect-default",
            "refused",
            "cache-send",
            "cache-batch",
            "reuse",
            "image",
            "image-query-on",
            "image-query-off",
            "image-short-deadline",
            "image-deadline-1ms",
            "image-deadline-1025ms",
        ]
        .contains(&case)
            || !matches!(method, Method::GET | Method::HEAD)
        {
            return Err(invalid());
        }
        let field = |name| {
            let mut values = headers.get_all(name).iter();
            let value = values
                .next()
                .and_then(|value| value.to_str().ok())
                .ok_or_else(invalid)?;
            if values.next().is_some() {
                return Err(invalid());
            }
            Ok(value)
        };
        let token = field("x-request-token")?;
        if token.is_empty()
            || token.len() > 64
            || !token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(invalid());
        }
        let input = field("x-qualification-target")?;
        if input.len() > 2048 || input.contains('#') {
            return Err(invalid());
        }
        let mut target: Uri = input.parse().map_err(|_| invalid())?;
        if !super::origins::allows(&target) {
            return Err(invalid());
        }
        if case.starts_with("image-query-") {
            if target.query().is_some() {
                return Err(invalid());
            }
            target = Uri::builder()
                .scheme(target.scheme_str().ok_or_else(invalid)?)
                .authority(target.authority().ok_or_else(invalid)?.clone())
                .path_and_query(format!(
                    "{}?width=100&unrelated=qualification",
                    target.path()
                ))
                .build()
                .map_err(|_| invalid())?;
        }
        let region = if case.starts_with("image") {
            OutboundImageOptimizerRegion::parse(field("x-qualification-region")?)
                .map_err(|_| invalid())?
        } else {
            OutboundImageOptimizerRegion::UsEast
        };
        let timeout = (!case.starts_with("image-deadline-")).then(|| {
            Duration::from_millis(match case {
                "long-budget" => 90_000,
                "head-timeout" => 100,
                "body-buffered" | "body-streamed" => 500,
                "connect" | "connect-default" | "refused" => 2000,
                _ => 1000,
            })
        });
        let deadline = match case {
            "image-short-deadline" => Some(Duration::from_millis(250)),
            "image-deadline-1ms" => Some(Duration::from_millis(1)),
            "image-deadline-1025ms" => Some(Duration::from_millis(1025)),
            _ => None,
        };
        Ok(Self {
            case: case.into(),
            token: token.into(),
            target,
            method,
            timeout,
            connect: matches!(case, "connect" | "refused").then_some(Duration::from_millis(200)),
            deadline,
            batch: case == "cache-batch",
            pair: matches!(case, "cache-send" | "cache-batch"),
            optimizer: case.starts_with("image"),
            stream: case.starts_with("image") || case == "body-streamed",
            preserve_query: case != "image-query-off",
            region,
        })
    }
    pub fn request(&self, headers: &HeaderMap) -> Result<OutboundRequest, EdgeError> {
        let mut request = OutboundRequest::new(self.method.clone(), self.target.clone())?
            .header("x-request-token", self.token.clone())?
            .proxy_marker(OutboundProxyMarker::Off)
            .response_decoding(ResponseDecoding::Original)
            .max_response_bytes(8 * 1024 * 1024)
            .max_encoded_response_bytes(8 * 1024 * 1024)
            .max_decoded_response_bytes(8 * 1024 * 1024);
        if let Some(timeout) = self.timeout {
            request = request.timeout(timeout);
        }
        if let Some(deadline) = self.deadline {
            request = request.deadline(Deadline::after(deadline));
        }
        if let Some(connect) = self.connect {
            request = request.connect_timeout(connect);
        }
        if self.case == "reuse" {
            request = request.cache_policy(OutboundCachePolicy::Bypass);
        }
        if let Some(accept) = headers.get("accept") {
            request = request.header("accept", accept.clone())?;
        }
        if self.optimizer {
            request = request.image_optimizer(
                OutboundImageOptimizerOptions::new()
                    .region(self.region)
                    .width(100)?
                    .format(OutboundImageFormat::WebP)
                    .preserve_origin_query(self.preserve_query),
            );
        }
        if self.stream {
            request = request.stream_response();
        }
        Ok(request)
    }

    pub fn phase_millis(&self) -> u64 {
        let anchor = self.timeout.or(self.deadline).unwrap_or(Duration::ZERO);
        (anchor.as_millis() as u64).max(1).next_power_of_two()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> HeaderMap {
        let mut fields = HeaderMap::new();
        fields.insert("x-request-token", "run-1".parse().unwrap());
        fields.insert(
            "x-qualification-target",
            "http://127.0.0.1:1234/image.jpg".parse().unwrap(),
        );
        fields.insert("x-qualification-region", "us_east".parse().unwrap());
        fields
    }

    #[test]
    fn hosted_presets_supply_exact_long_head_body_connect_and_cache_settings() {
        for (case, millis, connect, stream, batch, pair) in [
            ("long-budget", 90_000, None, false, false, false),
            ("head-timeout", 100, None, false, false, false),
            ("body-buffered", 500, None, false, false, false),
            ("body-streamed", 500, None, true, false, false),
            ("connect", 2000, Some(200), false, false, false),
            ("connect-default", 2000, None, false, false, false),
            ("refused", 2000, Some(200), false, false, false),
            ("cache-send", 1000, None, false, false, true),
            ("cache-batch", 1000, None, false, true, true),
            ("reuse", 1000, None, false, false, false),
        ] {
            let plan = Plan::parse(case, Method::GET, &headers()).unwrap();
            assert_eq!(plan.timeout, Some(Duration::from_millis(millis)));
            assert_eq!(plan.connect, connect.map(Duration::from_millis));
            assert_eq!(plan.stream, stream);
            assert_eq!(plan.batch, batch);
            assert_eq!(plan.pair, pair);
            let request = plan.request(&headers()).unwrap();
            let parts = request.into_parts();
            assert_eq!(parts.timeout, plan.timeout);
            assert_eq!(parts.connect_timeout, plan.connect);
        }
    }

    #[test]
    fn optimizer_presets_keep_typed_options_streaming_query_controls_and_original_deadline() {
        for case in [
            "image",
            "image-query-on",
            "image-query-off",
            "image-short-deadline",
            "image-deadline-1ms",
            "image-deadline-1025ms",
        ] {
            let plan = Plan::parse(case, Method::HEAD, &headers()).unwrap();
            let request = plan.request(&headers()).unwrap();
            let options = request.image_optimizer_options().unwrap();
            assert_eq!(options.width_setting(), Some(100));
            assert_eq!(
                options.format_setting(),
                Some(edgezero_core::OutboundImageFormat::WebP)
            );
            assert_eq!(
                options.preserve_origin_query_setting(),
                case != "image-query-off"
            );
            assert!(plan.stream);
            assert_eq!(request.method(), Method::HEAD);
            if case.starts_with("image-query-") {
                assert_eq!(
                    request.uri().query(),
                    Some("width=100&unrelated=qualification")
                );
            }
            let parts = request.into_parts();
            assert_eq!(parts.deadline.is_some(), case.contains("deadline"));
            assert_eq!(parts.timeout.is_none(), case.starts_with("image-deadline-"));
            assert_eq!(
                plan.deadline,
                match case {
                    "image-short-deadline" => Some(Duration::from_millis(250)),
                    "image-deadline-1ms" => Some(Duration::from_millis(1)),
                    "image-deadline-1025ms" => Some(Duration::from_millis(1025)),
                    _ => None,
                }
            );
        }
    }

    #[test]
    fn probe_input_cannot_add_timeouts_change_origins_or_echo_secrets() {
        let mut duplicate = headers();
        duplicate.append("x-request-token", "other".parse().unwrap());
        assert!(Plan::parse("image", Method::GET, &duplicate).is_err());
        for (case, method) in [("unknown", Method::GET), ("image", Method::POST)] {
            assert!(Plan::parse(case, method, &headers()).is_err());
        }
        for (field, value) in [
            ("x-request-token", "private-token!"),
            (
                "x-qualification-target",
                "https://unapproved.example/private-token",
            ),
            ("x-qualification-region", "private-token"),
        ] {
            let mut values = headers();
            values.insert(field, value.parse().unwrap());
            let error = Plan::parse("image", Method::GET, &values)
                .err()
                .expect("invalid probe");
            assert_eq!(error.message(), "invalid hosted qualification probe");
        }
    }
}
