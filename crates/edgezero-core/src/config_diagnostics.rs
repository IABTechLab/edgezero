//! Bounded, explicitly approved typed app-config validation diagnostics.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "approval declarations, safe reports, and bounded traversal are grouped in data-flow order"
)]

use validator::{ValidationErrors, ValidationErrorsKind};

/// Maximum number of declarations accepted from a registry.
pub const MAX_REGISTRY_ENTRIES: usize = 64;
/// Maximum number of approved diagnostics retained per failure.
pub const MAX_DIAGNOSTICS: usize = 8;
/// Maximum approved code length in bytes.
pub const MAX_CODE_BYTES: usize = 64;
/// Maximum fixed explanation length in bytes.
pub const MAX_EXPLANATION_BYTES: usize = 512;
/// Maximum rendered structural path length in bytes.
pub const MAX_PATH_BYTES: usize = 256;
/// Maximum validation-tree path depth inspected.
pub const MAX_DEPTH: usize = 32;
/// Maximum validation containers, entries and field errors inspected.
pub const MAX_VISITED_NODES: usize = 1_024;

/// A trusted structural path component. Wildcards match list indices only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationPathSegment {
    /// A fixed field name from the application's schema, never a map key value.
    Field(&'static str),
    /// Any numeric list position.
    Each,
}

/// Explicit approval of a fixed diagnostic at an exact path and error code.
///
/// All strings must come from compiled declarations, never configuration values.
/// Validator messages and parameters are ignored, even for approved codes.
#[derive(Clone, Copy, Debug)]
pub struct ApprovedDiagnostic {
    /// Full structural path, with `Each` for list positions.
    pub path: &'static [ValidationPathSegment],
    /// Fixed validator error code.
    pub code: &'static str,
    /// Fixed, single-line operator explanation (including remediation).
    pub explanation: &'static str,
}

/// An approved diagnostic, containing no validator-owned strings or values.
#[derive(Clone, Debug)]
pub struct ValidationDiagnostic {
    field_path: String,
    code: &'static str,
    explanation: &'static str,
}

impl ValidationDiagnostic {
    /// Approved structural path with numeric list positions.
    #[must_use]
    #[inline]
    pub fn field_path(&self) -> &str {
        &self.field_path
    }

    /// Approved fixed error code.
    #[must_use]
    #[inline]
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// Approved fixed operator explanation.
    #[must_use]
    #[inline]
    pub fn explanation(&self) -> &'static str {
        self.explanation
    }
}

/// Bounded safe summary of a validation failure. It does not retain its source.
#[derive(Clone, Debug, Default)]
pub struct ValidationReport {
    field_path: String,
    diagnostics: Vec<ValidationDiagnostic>,
}

impl ValidationReport {
    /// Inspect an existing validation result without revalidating or reading data.
    ///
    /// Invalid/oversized registries and unknown errors retain generic reporting.
    /// Exhausting any limit only stops diagnostic collection; callers must still
    /// reject the configuration. Names in the fallback path are redacted.
    #[must_use]
    #[inline]
    pub fn from_errors(errors: &ValidationErrors, registry: &[ApprovedDiagnostic]) -> Self {
        let rules: Vec<&ApprovedDiagnostic> = if registry.len() <= MAX_REGISTRY_ENTRIES {
            registry.iter().filter(|rule| valid_rule(rule)).collect()
        } else {
            Vec::new()
        };
        let mut walker = Walker {
            report: Self::default(),
            rules,
            remaining: MAX_VISITED_NODES,
        };
        walker.walk(errors, &mut Vec::new());
        walker.report
    }

    /// First failing path, with unapproved names redacted.
    #[must_use]
    #[inline]
    pub fn field_path(&self) -> &str {
        &self.field_path
    }

    /// Explicitly approved operator diagnostics, in deterministic path order.
    #[must_use]
    #[inline]
    pub fn diagnostics(&self) -> &[ValidationDiagnostic] {
        &self.diagnostics
    }

    /// Coarse public explanation; approved diagnostic details are omitted.
    #[must_use]
    #[inline]
    pub fn public_message(&self) -> String {
        if self.field_path.is_empty() {
            "app config failed validation".to_owned()
        } else {
            format!(
                "app config failed validation for field `{}`",
                self.field_path
            )
        }
    }

    /// Safe operator text suitable for CLI errors and application logs.
    #[must_use]
    #[inline]
    pub fn operator_message(&self) -> String {
        let mut output = self.public_message();
        for diagnostic in &self.diagnostics {
            output.push_str("; ");
            output.push_str(&diagnostic.field_path);
            output.push_str(" [");
            output.push_str(diagnostic.code);
            output.push_str("]: ");
            output.push_str(diagnostic.explanation);
        }
        output
    }
}

fn valid_rule(rule: &ApprovedDiagnostic) -> bool {
    !rule.code.is_empty()
        && rule.code.len() <= MAX_CODE_BYTES
        && rule
            .code
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && !rule.explanation.is_empty()
        && rule.explanation.len() <= MAX_EXPLANATION_BYTES
        && !rule.explanation.chars().any(char::is_control)
        && rule.path.len() <= MAX_DEPTH
        && rule.path.iter().all(|segment| match segment {
            ValidationPathSegment::Field(name) => {
                !name.is_empty()
                    && name.len() <= MAX_PATH_BYTES
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
            }
            ValidationPathSegment::Each => true,
        })
}

#[derive(Clone, Copy)]
enum ActualSegment<'tree> {
    Field(&'tree str),
    Index(usize),
}

struct Walker<'tree> {
    report: ValidationReport,
    rules: Vec<&'tree ApprovedDiagnostic>,
    remaining: usize,
}

impl Walker<'_> {
    fn spend(&mut self, count: usize) -> bool {
        if count > self.remaining {
            self.remaining = 0;
            false
        } else {
            self.remaining = self.remaining.saturating_sub(count);
            true
        }
    }

    fn walk<'tree>(
        &mut self,
        errors: &'tree ValidationErrors,
        path: &mut Vec<ActualSegment<'tree>>,
    ) {
        if path.len() >= MAX_DEPTH
            || self.report.diagnostics.len() >= MAX_DIAGNOSTICS
            || !self.spend(1)
            || !self.spend(errors.errors().len())
        {
            return;
        }
        // Bound the collection BEFORE allocating/sorting attacker-sized maps.
        let mut keys: Vec<&str> = errors
            .errors()
            .keys()
            .map(AsRef::as_ref)
            .filter(|key: &&str| key.len() <= MAX_PATH_BYTES)
            .collect();
        keys.sort_unstable();
        for key in keys {
            if self.remaining == 0 || self.report.diagnostics.len() >= MAX_DIAGNOSTICS {
                break;
            }
            path.push(ActualSegment::Field(key));
            match errors.errors().get(key) {
                Some(ValidationErrorsKind::Field(items)) => {
                    if self.report.field_path.is_empty() {
                        self.report.field_path = render_path(path, None).unwrap_or_default();
                    }
                    for error in items {
                        if !self.spend(1) || self.report.diagnostics.len() >= MAX_DIAGNOSTICS {
                            break;
                        }
                        for rule in &self.rules {
                            if error.code.as_ref() == rule.code
                                && path_matches(path, rule.path)
                                && let Some(field_path) = render_path(path, Some(rule.path))
                            {
                                self.report.diagnostics.push(ValidationDiagnostic {
                                    field_path,
                                    code: rule.code,
                                    explanation: rule.explanation,
                                });
                                break;
                            }
                        }
                    }
                }
                Some(ValidationErrorsKind::Struct(inner)) => self.walk(inner, path),
                Some(ValidationErrorsKind::List(items))
                    if path.len() < MAX_DEPTH && self.spend(items.len()) =>
                {
                    // validator's list is a BTreeMap, already in index order.
                    for (index, inner) in items {
                        if self.remaining == 0 || self.report.diagnostics.len() >= MAX_DIAGNOSTICS {
                            break;
                        }
                        path.push(ActualSegment::Index(*index));
                        self.walk(inner, path);
                        path.pop();
                    }
                }
                Some(ValidationErrorsKind::List(_)) | None => {}
            }
            path.pop();
        }
    }
}

fn path_matches(actual: &[ActualSegment<'_>], approved: &[ValidationPathSegment]) -> bool {
    actual.len() == approved.len()
        && actual
            .iter()
            .zip(approved)
            .all(
                |(actual_part, approved_part)| match (actual_part, approved_part) {
                    (ActualSegment::Field(found), ValidationPathSegment::Field(expected)) => {
                        found == expected
                    }
                    (ActualSegment::Index(_), ValidationPathSegment::Each) => true,
                    _ => false,
                },
            )
}

fn render_path(
    actual: &[ActualSegment<'_>],
    approved: Option<&[ValidationPathSegment]>,
) -> Option<String> {
    let mut result = String::new();
    for (position, segment) in actual.iter().enumerate() {
        match segment {
            ActualSegment::Field(_) => {
                let name = match approved.and_then(|parts| parts.get(position)) {
                    Some(ValidationPathSegment::Field(name)) => name,
                    Some(ValidationPathSegment::Each) | None => "<redacted>",
                };
                if !result.is_empty() {
                    result.push('.');
                }
                if result.len().saturating_add(name.len()) > MAX_PATH_BYTES {
                    return None;
                }
                result.push_str(name);
            }
            ActualSegment::Index(index) => {
                result.push('[');
                result.push_str(&index.to_string());
                result.push(']');
            }
        }
        if result.len() > MAX_PATH_BYTES {
            return None;
        }
    }
    Some(result)
}

/// Shared extraction fixture for adapter contracts; not part of the application API.
#[cfg(feature = "test-utils")]
#[doc(hidden)]
pub mod contract_fixture {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde::Deserialize;
    use validator::{Validate, ValidationError, ValidationErrors};

    use super::{ApprovedDiagnostic, ValidationPathSegment};
    use crate::app_config::{AppConfigMeta, SecretField};
    use crate::blob_envelope::BlobEnvelope;
    use crate::body::Body;
    use crate::config_store::{ConfigStore, ConfigStoreError, ConfigStoreHandle};
    use crate::context::RequestContext;
    use crate::error::EdgeError;
    use crate::extractor::AppConfig;
    use crate::http::request_builder;
    use crate::params::PathParams;
    use crate::store_registry::{ConfigStoreBinding, StoreRegistry};

    /// Approved explanation must stay out of public responses.
    pub const EXPLANATION: &str = "Enable feature bidder-x and rebuild CLI and guest.";
    /// Validator value/message must stay out of all reports.
    pub const CANARY: &str = "UNTRUSTED_CONFIG_VALUE_CANARY";

    #[derive(Deserialize)]
    struct RefusedConfig {
        _unused: Option<String>,
    }

    impl AppConfigMeta for RefusedConfig {
        fn secret_fields() -> Vec<SecretField> {
            Vec::new()
        }
        fn validation_diagnostics() -> &'static [ApprovedDiagnostic] {
            &[ApprovedDiagnostic {
                path: &[ValidationPathSegment::Field("implementation")],
                code: "implementation_unavailable",
                explanation: EXPLANATION,
            }]
        }
    }

    impl Validate for RefusedConfig {
        fn validate(&self) -> Result<(), ValidationErrors> {
            let mut error = ValidationError::new("implementation_unavailable");
            error.message = Some(CANARY.into());
            error.add_param("value".into(), &CANARY);
            let mut errors = ValidationErrors::new();
            errors.add("implementation", error);
            Err(errors)
        }
    }

    struct FixtureStore;
    #[async_trait(?Send)]
    impl ConfigStore for FixtureStore {
        async fn get(&self, _key: &str) -> Result<Option<String>, ConfigStoreError> {
            let envelope = BlobEnvelope::new(serde_json::json!({}), "2026-11-13T00:00:00Z".into());
            serde_json::to_string(&envelope)
                .map(Some)
                .map_err(ConfigStoreError::internal)
        }
    }

    /// Exercise the real store reader, typed validation and safe error mapping.
    #[inline]
    pub async fn extraction_error() -> Result<EdgeError, EdgeError> {
        let binding = ConfigStoreBinding {
            handle: ConfigStoreHandle::new(Arc::new(FixtureStore)),
            default_key: "config".to_owned(),
        };
        let registry = StoreRegistry::single_id("default".to_owned(), binding);
        let mut request = request_builder()
            .uri("/")
            .body(Body::empty())
            .map_err(EdgeError::internal)?;
        request.extensions_mut().insert(registry);
        let ctx = RequestContext::new(request, PathParams::default());
        match AppConfig::<RefusedConfig>::from_store(&ctx, "default", None).await {
            Err(error) => Ok(error),
            Ok(_) => Err(EdgeError::internal(anyhow::anyhow!(
                "contract fixture unexpectedly accepted"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use validator::{ValidationError, ValidationErrors, ValidationErrorsKind};

    const RULES: &[ApprovedDiagnostic] = &[ApprovedDiagnostic {
        path: &[
            ValidationPathSegment::Field("bidders"),
            ValidationPathSegment::Each,
            ValidationPathSegment::Field("implementation"),
        ],
        code: "implementation_unavailable",
        explanation: "Enable Cargo feature bidder-x and rebuild both CLI and serving guest.",
    }];

    fn refusal(code: &'static str) -> ValidationErrors {
        let mut error = ValidationError::new(code);
        error.message = Some(Cow::Borrowed("UNTRUSTED_MESSAGE_CANARY"));
        error.add_param(Cow::Borrowed("value"), &"SECRET_URL_VALUE_CANARY");
        let mut leaf = ValidationErrors::new();
        leaf.add("implementation", error);
        let mut root = ValidationErrors::new();
        root.errors_mut().insert(
            Cow::Borrowed("bidders"),
            ValidationErrorsKind::List([(2, Box::new(leaf))].into_iter().collect()),
        );
        root
    }

    #[test]
    fn approves_exact_path_and_code_without_retaining_values() {
        let report = ValidationReport::from_errors(&refusal("implementation_unavailable"), RULES);
        assert_eq!(report.diagnostics().len(), 1);
        let diagnostic = &report.diagnostics()[0];
        assert_eq!(diagnostic.field_path(), "bidders[2].implementation");
        assert_eq!(diagnostic.code(), "implementation_unavailable");
        assert_eq!(diagnostic.explanation(), RULES[0].explanation);
        let output = format!("{report:?} {}", report.operator_message());
        for canary in ["UNTRUSTED_MESSAGE_CANARY", "SECRET_URL_VALUE_CANARY"] {
            assert!(!output.contains(canary));
        }
    }

    #[test]
    fn unknown_codes_and_different_paths_are_generic() {
        let unknown = ValidationReport::from_errors(&refusal("unknown"), RULES);
        assert!(unknown.diagnostics().is_empty());
        let mut errors = ValidationErrors::new();
        errors.add(
            "SECRET_MAP_KEY_CANARY",
            ValidationError::new("implementation_unavailable"),
        );
        let report = ValidationReport::from_errors(&errors, RULES);
        assert!(report.diagnostics().is_empty());
        assert!(!format!("{report:?}").contains("SECRET_MAP_KEY_CANARY"));
    }

    #[test]
    fn bounds_codes_explanations_paths_depth_and_diagnostic_count() {
        for length in [MAX_CODE_BYTES, MAX_CODE_BYTES + 1] {
            let code = "x".repeat(length).leak();
            let rule = ApprovedDiagnostic { code, ..RULES[0] };
            assert_eq!(
                ValidationReport::from_errors(&refusal(code), &[rule])
                    .diagnostics()
                    .len(),
                usize::from(length == MAX_CODE_BYTES)
            );
        }
        for length in [MAX_EXPLANATION_BYTES, MAX_EXPLANATION_BYTES + 1] {
            let rule = ApprovedDiagnostic {
                explanation: "x".repeat(length).leak(),
                ..RULES[0]
            };
            assert_eq!(
                ValidationReport::from_errors(&refusal("implementation_unavailable"), &[rule])
                    .diagnostics()
                    .len(),
                usize::from(length == MAX_EXPLANATION_BYTES)
            );
        }
        let mut many = ValidationErrors::new();
        let mut leaf = ValidationErrors::new();
        for _ in 0..=MAX_DIAGNOSTICS {
            leaf.add(
                "implementation",
                ValidationError::new("implementation_unavailable"),
            );
        }
        many.errors_mut().insert(
            Cow::Borrowed("bidders"),
            ValidationErrorsKind::List([(0, Box::new(leaf))].into_iter().collect()),
        );
        assert_eq!(
            ValidationReport::from_errors(&many, RULES)
                .diagnostics()
                .len(),
            MAX_DIAGNOSTICS
        );

        let long_name = "x".repeat(MAX_PATH_BYTES).leak();
        let long_path = [ApprovedDiagnostic {
            path: Box::leak(
                vec![
                    ValidationPathSegment::Field("bidders"),
                    ValidationPathSegment::Each,
                    ValidationPathSegment::Field(long_name),
                ]
                .into_boxed_slice(),
            ),
            ..RULES[0]
        }];
        let mut long_leaf = ValidationErrors::new();
        long_leaf.add(
            long_name,
            ValidationError::new("implementation_unavailable"),
        );
        let mut root = ValidationErrors::new();
        root.errors_mut().insert(
            Cow::Borrowed("bidders"),
            ValidationErrorsKind::List([(0, Box::new(long_leaf))].into_iter().collect()),
        );
        assert!(
            ValidationReport::from_errors(&root, &long_path)
                .diagnostics()
                .is_empty()
        );

        let mut deep = refusal("implementation_unavailable");
        for _ in 0..=MAX_DEPTH {
            let mut parent = ValidationErrors::new();
            parent.errors_mut().insert(
                Cow::Borrowed("NESTED_CANARY"),
                ValidationErrorsKind::Struct(Box::new(deep)),
            );
            deep = parent;
        }
        let report = ValidationReport::from_errors(&deep, RULES);
        assert!(report.diagnostics().is_empty());
        assert!(!format!("{report:?}").contains("NESTED_CANARY"));
    }

    #[test]
    fn list_wildcard_does_not_approve_string_keys_or_prefixes() {
        let mut leaf = ValidationErrors::new();
        leaf.add(
            "implementation",
            ValidationError::new("implementation_unavailable"),
        );
        let mut map = ValidationErrors::new();
        map.errors_mut().insert(
            Cow::Borrowed("0"),
            ValidationErrorsKind::Struct(Box::new(leaf)),
        );
        let mut root = ValidationErrors::new();
        root.errors_mut().insert(
            Cow::Borrowed("bidders"),
            ValidationErrorsKind::Struct(Box::new(map)),
        );
        assert!(
            ValidationReport::from_errors(&root, RULES)
                .diagnostics()
                .is_empty()
        );
        let prefix = [ApprovedDiagnostic {
            path: &[ValidationPathSegment::Field("bidders")],
            ..RULES[0]
        }];
        assert!(
            ValidationReport::from_errors(&refusal("implementation_unavailable"), &prefix)
                .diagnostics()
                .is_empty()
        );
    }

    #[test]
    fn accepts_exact_registry_path_and_depth_limits() {
        let registry = vec![RULES[0]; MAX_REGISTRY_ENTRIES];
        assert_eq!(
            ValidationReport::from_errors(&refusal("implementation_unavailable"), &registry)
                .diagnostics()
                .len(),
            1
        );
        for length in [MAX_PATH_BYTES, MAX_PATH_BYTES + 1] {
            let name = "x".repeat(length).leak();
            let path = Box::leak(vec![ValidationPathSegment::Field(name)].into_boxed_slice());
            let rule = ApprovedDiagnostic { path, ..RULES[0] };
            let mut errors = ValidationErrors::new();
            errors.add(name, ValidationError::new(rule.code));
            assert_eq!(
                ValidationReport::from_errors(&errors, &[rule])
                    .diagnostics()
                    .len(),
                usize::from(length == MAX_PATH_BYTES)
            );
        }
        for depth in [MAX_DEPTH, MAX_DEPTH + 1] {
            let path = Box::leak(vec![ValidationPathSegment::Field("n"); depth].into_boxed_slice());
            let rule = ApprovedDiagnostic { path, ..RULES[0] };
            let mut errors = ValidationErrors::new();
            errors.add("n", ValidationError::new(rule.code));
            for _ in 1..depth {
                let mut parent = ValidationErrors::new();
                parent.errors_mut().insert(
                    Cow::Borrowed("n"),
                    ValidationErrorsKind::Struct(Box::new(errors)),
                );
                errors = parent;
            }
            assert_eq!(
                ValidationReport::from_errors(&errors, &[rule])
                    .diagnostics()
                    .len(),
                usize::from(depth == MAX_DEPTH)
            );
        }
    }

    #[test]
    fn field_and_list_budgets_bound_collection() {
        let mut leaf = ValidationErrors::new();
        for _ in 0..=MAX_VISITED_NODES {
            leaf.add(
                "implementation",
                ValidationError::new("implementation_unavailable"),
            );
        }
        let mut errors = ValidationErrors::new();
        errors.errors_mut().insert(
            Cow::Borrowed("bidders"),
            ValidationErrorsKind::List([(0, Box::new(leaf))].into_iter().collect()),
        );
        assert_eq!(
            ValidationReport::from_errors(&errors, RULES)
                .diagnostics()
                .len(),
            MAX_DIAGNOSTICS
        );
        // An approved error beyond the field budget must not be visited.
        let mut exhausted_leaf = ValidationErrors::new();
        for _ in 0..MAX_VISITED_NODES {
            exhausted_leaf.add("implementation", ValidationError::new("unknown"));
        }
        exhausted_leaf.add(
            "implementation",
            ValidationError::new("implementation_unavailable"),
        );
        errors.errors_mut().insert(
            Cow::Borrowed("bidders"),
            ValidationErrorsKind::List([(0, Box::new(exhausted_leaf))].into_iter().collect()),
        );
        assert!(
            ValidationReport::from_errors(&errors, RULES)
                .diagnostics()
                .is_empty()
        );
        let items = (0..=MAX_VISITED_NODES)
            .map(|index| (index, Box::new(ValidationErrors::new())))
            .collect();
        errors
            .errors_mut()
            .insert(Cow::Borrowed("bidders"), ValidationErrorsKind::List(items));
        assert!(
            ValidationReport::from_errors(&errors, RULES)
                .diagnostics()
                .is_empty()
        );
    }

    #[test]
    fn oversized_declarations_and_trees_stay_generic_and_bounded() {
        let oversized = [ApprovedDiagnostic {
            explanation: "bad\nexplanation",
            ..RULES[0]
        }];
        assert!(
            ValidationReport::from_errors(&refusal("implementation_unavailable"), &oversized)
                .diagnostics()
                .is_empty()
        );
        let many = vec![RULES[0]; MAX_REGISTRY_ENTRIES + 1];
        assert!(
            ValidationReport::from_errors(&refusal("implementation_unavailable"), &many)
                .diagnostics()
                .is_empty()
        );
        let mut errors = ValidationErrors::new();
        for index in 0..=MAX_VISITED_NODES {
            errors.errors_mut().insert(
                Cow::Owned(format!("CANARY{index}")),
                ValidationErrorsKind::Field(vec![ValidationError::new("x")]),
            );
        }
        let report = ValidationReport::from_errors(&errors, RULES);
        assert!(report.diagnostics().is_empty());
        assert!(report.field_path().len() <= MAX_PATH_BYTES);
        assert!(!format!("{report:?}").contains("CANARY"));
    }
}
