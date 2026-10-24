use edgezero_core::http::Uri;
use edgezero_core::request::{
    CapturedTarget, HeaderFidelity, InboundOrigin, OriginSource, Preservation, RequestIngress,
    TargetSource,
};

pub(crate) fn capture_request_ingress(uri: &Uri) -> RequestIngress {
    let origin = uri
        .scheme_str()
        .zip(uri.authority())
        .and_then(|(scheme, authority)| {
            InboundOrigin::parse(scheme, authority.as_str(), OriginSource::RuntimeUri).ok()
        });
    let source = if uri.scheme().is_some() {
        TargetSource::RuntimeUrl
    } else {
        TargetSource::RuntimePathAndQuery
    };
    let target = CapturedTarget::capture(&uri.to_string(), source, Preservation::Unknown);
    let headers = HeaderFidelity::RUNTIME_HEADER_MAP;
    RequestIngress::new(target, origin, headers)
}

#[cfg(test)]
mod ingress_tests {
    use super::capture_request_ingress;
    use edgezero_core::http::Uri;
    use edgezero_core::request::{CapturedTarget, OriginSource, Preservation};

    #[test]
    fn ingress_captures_runtime_uri_and_origin_only() {
        let uri: Uri = "https://example.com/reserved/%2e//%2F?q=a%2Fb"
            .parse()
            .expect("should parse URI");
        let ingress = capture_request_ingress(&uri);
        let CapturedTarget::Complete(target) = ingress.target() else {
            panic!("should capture target")
        };
        assert_eq!(target.value(), uri.to_string());
        let origin = ingress.origin().expect("should capture runtime origin");
        assert_eq!(origin.scheme(), "https");
        assert_eq!(origin.authority(), "example.com");
        assert_eq!(origin.source(), OriginSource::RuntimeUri);
        assert_eq!(
            ingress
                .header_fidelity(&"cookie".parse().expect("should parse name"))
                .global_field_order(),
            Preservation::Unavailable
        );
        assert!(
            capture_request_ingress(&"/reserved".parse().expect("should parse path"))
                .origin()
                .is_none()
        );
        assert!(
            capture_request_ingress(
                &"ftp://example.com/reserved"
                    .parse()
                    .expect("should parse URI")
            )
            .origin()
            .is_none()
        );
    }
}
