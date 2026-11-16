//! Test-only diagnostic metadata at the existing response-commit boundary.

use std::borrow::Cow;
use std::cell::RefCell;

use edgezero_core::http::{HeaderMap, HeaderName, HeaderValue};
use edgezero_core::{SynthesizedResponseHeaders, SynthesizedResponseHeadersError};

thread_local! {
    static DIAGNOSTIC: RefCell<Option<SynthesizedResponseHeaders>> = const { RefCell::new(None) };
}

struct Scope(Option<SynthesizedResponseHeaders>);

#[expect(
    clippy::missing_trait_methods,
    reason = "Drop::pin_drop is unstable and conflicts with drop"
)]
impl Drop for Scope {
    fn drop(&mut self) {
        DIAGNOSTIC.with(|current| *current.borrow_mut() = self.0.take());
    }
}

/// Adds one bounded diagnostic extension field to emissions in this synchronous thread scope.
///
/// Prepared response fields are never replaced, including conflicting or duplicate values.
/// Nested scopes replace (not accumulate) snapshots, restoring the previous one on return or
/// unwind. No request, body, clock or application callback is retained; status, body, framing
/// and delivery ownership are not configurable here.
/// This instruments a fixture, not an uninstrumented production build or hosted qualification.
///
/// # Errors
/// Rejects non-extension or host-control fields and nonportable/oversized values before running
/// `operation`. The existing synthesized-header validator supplies the inclusive byte bound.
#[inline]
pub fn with_diagnostic_header<T, Operation>(
    name: HeaderName,
    value: HeaderValue,
    operation: Operation,
) -> Result<T, SynthesizedResponseHeadersError>
where
    Operation: FnOnce() -> T,
{
    if !name.as_str().starts_with("x-")
        || matches!(name.as_str(), "x-compress-hint" | "x-edgezero-proxy")
    {
        return Err(SynthesizedResponseHeadersError::ForbiddenName);
    }
    let mut fields = HeaderMap::new();
    fields.insert(name, value);
    let snapshot = SynthesizedResponseHeaders::new(fields)?;
    let _scope = Scope(DIAGNOSTIC.with(|current| current.replace(Some(snapshot))));
    Ok(operation())
}

pub(super) fn headers(original: &HeaderMap) -> Cow<'_, HeaderMap> {
    DIAGNOSTIC.with(|slot| {
        let current = slot.borrow();
        let Some(snapshot) = current.as_ref() else {
            return Cow::Borrowed(original);
        };
        let mut headers = original.clone();
        snapshot.apply(&mut headers);
        Cow::Owned(headers)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(target_arch = "wasm32"))]
    use std::{panic, thread};

    fn scoped<T>(value: &'static str, operation: impl FnOnce() -> T) -> T {
        with_diagnostic_header(
            HeaderName::from_static("x-test-launch"),
            HeaderValue::from_static(value),
            operation,
        )
        .expect("valid diagnostic")
    }

    #[test]
    fn diagnostics_add_only_the_scoped_field_and_restore_after_return() {
        let mut original = HeaderMap::new();
        original.insert("content-length", HeaderValue::from_static("7"));
        original.insert("retry-after", HeaderValue::from_static("60"));
        assert!(matches!(headers(&original), Cow::Borrowed(_)));
        scoped("outer", || {
            let output = headers(&original);
            assert_eq!(output["x-test-launch"], "outer");
            assert_eq!(output["content-length"], "7");
            assert_eq!(output["retry-after"], "60");
            assert_eq!(output.len(), original.len() + 1);
            scoped("inner", || {
                assert_eq!(headers(&original)["x-test-launch"], "inner");
            });
            assert_eq!(headers(&original)["x-test-launch"], "outer");
        });
        assert!(matches!(headers(&original), Cow::Borrowed(_)));
        assert!(!original.contains_key("x-test-launch"));
    }

    #[test]
    fn diagnostics_never_repair_conflicting_or_duplicate_identity() {
        for values in [
            vec!["expected"],
            vec!["wrong"],
            vec!["expected", "expected"],
        ] {
            let mut original = HeaderMap::new();
            for value in &values {
                original.append("x-test-launch", HeaderValue::from_str(value).unwrap());
            }
            scoped("expected", || {
                assert_eq!(headers(&original).as_ref(), &original);
            });
        }
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn diagnostics_are_thread_local() {
        scoped("parent", || {
            assert!(
                thread::spawn(|| headers(&HeaderMap::new()).is_empty())
                    .join()
                    .unwrap()
            );
        });
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn diagnostics_restore_after_unwind() {
        scoped("outer", || {
            assert!(panic::catch_unwind(|| scoped("inner", || panic!("fixture panic"))).is_err());
            assert_eq!(headers(&HeaderMap::new())["x-test-launch"], "outer");
        });
        assert!(headers(&HeaderMap::new()).is_empty());
    }

    #[test]
    fn invalid_nested_diagnostic_keeps_the_outer_scope() {
        scoped("outer", || {
            assert!(
                with_diagnostic_header(
                    HeaderName::from_static("content-length"),
                    HeaderValue::from_static("1"),
                    || panic!("invalid inner scope"),
                )
                .is_err()
            );
            assert_eq!(headers(&HeaderMap::new())["x-test-launch"], "outer");
        });
        assert!(headers(&HeaderMap::new()).is_empty());
    }

    #[test]
    fn diagnostics_reject_semantic_fields_and_invalid_values_before_operation() {
        for name in [
            "content-length",
            "retry-after",
            "set-cookie",
            "x-compress-hint",
            "x-edgezero-proxy",
        ] {
            assert_eq!(
                with_diagnostic_header(
                    HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    HeaderValue::from_static("1"),
                    || panic!("invalid operation")
                ),
                Err(SynthesizedResponseHeadersError::ForbiddenName)
            );
        }
        for value in [b" leading".to_vec(), vec![0x80], vec![b'a'; 4096]] {
            assert!(
                with_diagnostic_header(
                    HeaderName::from_static("x-test-launch"),
                    HeaderValue::from_bytes(&value).unwrap(),
                    || panic!("invalid operation")
                )
                .is_err()
            );
        }
        // 2 final CRLF + 13 name + 4 delimiters leaves 4077 bytes at the inclusive limit.
        with_diagnostic_header(
            HeaderName::from_static("x-test-launch"),
            HeaderValue::from_str(&"a".repeat(4077)).unwrap(),
            || (),
        )
        .unwrap();
        assert!(headers(&HeaderMap::new()).is_empty());
    }
}
