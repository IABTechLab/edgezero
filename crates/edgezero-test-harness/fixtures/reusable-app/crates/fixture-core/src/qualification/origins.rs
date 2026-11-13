use edgezero_core::{EdgeError, http::Uri};
use std::sync::OnceLock;

pub struct OriginPolicy(Vec<(String, String)>);

fn invalid() -> EdgeError {
    EdgeError::bad_request("invalid qualification origin configuration")
}

fn authority(uri: &Uri) -> Option<(String, String)> {
    let scheme = uri
        .scheme_str()
        .filter(|scheme| matches!(*scheme, "http" | "https"))?;
    let authority = uri.authority()?.as_str();
    if authority.contains('@')
        || authority.contains('*')
        || uri.host()?.is_empty()
        || uri.port_u16() == Some(0)
        || (uri.port_u16().is_none() && authority != uri.host()?)
    {
        return None;
    }
    Some((scheme.to_owned(), authority.to_ascii_lowercase()))
}

fn configured() -> &'static Result<OriginPolicy, EdgeError> {
    static POLICY: OnceLock<Result<OriginPolicy, EdgeError>> = OnceLock::new();
    POLICY.get_or_init(|| {
        OriginPolicy::parse(option_env!("EDGEZERO_QUALIFICATION_ORIGINS").unwrap_or(""))
    })
}

pub fn validate_configuration() -> Result<(), EdgeError> {
    configured().as_ref().map(|_| ()).map_err(|_| {
        EdgeError::internal(std::io::Error::other(
            "invalid qualification origin configuration",
        ))
    })
}

pub fn allows(uri: &Uri) -> bool {
    configured().as_ref().is_ok_and(|policy| policy.allows(uri))
}

impl OriginPolicy {
    pub fn parse(input: &str) -> Result<Self, EdgeError> {
        if input.len() > 16 * 1024 {
            return Err(invalid());
        }
        if input.is_empty() {
            return Ok(Self(Vec::new()));
        }
        let entries: Vec<String> = serde_json::from_str(input).map_err(|_| invalid())?;
        if entries.len() > 16 {
            return Err(invalid());
        }
        let mut origins = Vec::new();
        for entry in entries {
            if entry.len() > 2048 || entry.contains('#') {
                return Err(invalid());
            }
            let uri: Uri = entry.parse().map_err(|_| invalid())?;
            if uri.path() != "/" || uri.query().is_some() {
                return Err(invalid());
            }
            let origin = authority(&uri).ok_or_else(invalid)?;
            if origins.contains(&origin) {
                return Err(invalid());
            }
            origins.push(origin);
        }
        Ok(Self(origins))
    }

    pub fn allows(&self, uri: &Uri) -> bool {
        let Some(origin) = authority(uri) else {
            return false;
        };
        (crate::is_loopback_http(uri) && uri.port_u16().is_some_and(|port| port > 0))
            || self.0.contains(&origin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_are_loopback_only_and_hosted_origins_are_exact_build_time_facts() {
        let local = OriginPolicy::parse("").unwrap();
        for target in ["http://127.0.0.1:1234/a", "http://localhost:1234/a"] {
            assert!(local.allows(&target.parse().unwrap()), "{target}");
        }
        for target in [
            "https://origin.example/a",
            "http://127.0.0.1/a",
            "http://127.0.0.1:0/a",
            "http://user@127.0.0.1:1234/a",
        ] {
            assert!(!local.allows(&target.parse().unwrap()), "{target}");
        }
        let hosted = OriginPolicy::parse(
            &json!(["https://origin.example", "http://other.example:8080"]).to_string(),
        )
        .unwrap();
        assert!(hosted.allows(&"https://origin.example/any/path?a=1".parse().unwrap()));
        for target in [
            "http://origin.example/a",
            "https://origin.example:444/a",
            "https://origin.example.evil/a",
            "https://user@origin.example/a",
        ] {
            assert!(!hosted.allows(&target.parse().unwrap()), "{target}");
        }
    }

    #[test]
    fn malformed_oversized_duplicate_or_credentialled_allowlists_fail_without_echoing_input() {
        for value in [
            json!({}),
            json!(["https://user:private-token@origin.example"]),
            json!(["https://origin.example/path"]),
            json!(["https://origin.example/?private-token=1"]),
            json!(["https://origin.example/#private-token"]),
            json!(["*.example"]),
            json!(["https://origin.example", "https://origin.example"]),
            json!(vec!["https://origin.example"; 17]),
            json!(["https://origin.example:0"]),
            json!(["https://origin.example:abc"]),
            json!(["https://origin.example:65536"]),
        ] {
            let error = OriginPolicy::parse(&value.to_string())
                .err()
                .expect("invalid allowlist");
            assert_eq!(
                error.message(),
                "invalid qualification origin configuration"
            );
        }
        assert!(OriginPolicy::parse(&" ".repeat(16 * 1024 + 1)).is_err());
        assert!(OriginPolicy::parse("not-json").is_err());
    }
}
