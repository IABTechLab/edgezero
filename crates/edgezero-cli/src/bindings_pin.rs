//! Untyped portable bindings preflight and capped file loading.

#[cfg(feature = "bindings-pin")]
mod enabled {
    use super::{check_portable_size, parse_portable_bindings, read_portable_bindings};
    use crate::args::BindingsPinArgs;
    use edgezero_adapter::store_bindings::{KvBinding, StoreBindingsDocument, valid_portable_id};
    use edgezero_core::time::MonotonicClock;
    use edgezero_store_aws::settings::{
        AwsPreparationLimits, DynamoDbKvLimits, DynamoDbPinnedIdentity, DynamoDbRecordsSettings,
    };
    use edgezero_store_aws::{AwsPreparation, PreparationError as AwsPreparationError};
    use edgezero_store_celld::settings::{
        CelldAccessToken, CelldPinnedIdentity, CelldPreparationLimits, CelldRecordsSettings,
        celld_token_key,
    };
    use edgezero_store_celld::{CelldPreparation, PreparationError as CelldPreparationError};
    use std::collections::BTreeMap;
    use std::env;
    use std::fs;
    use std::io::Write as _;
    use std::path::Path;
    use tokio::runtime::{Builder, Runtime};
    use toml_edit::{DocumentMut, InlineTable, Item, TableLike, value};

    #[derive(Debug, thiserror::Error)]
    enum PreflightFailed {
        #[error(transparent)]
        Aws(AwsPreparationError),
        #[error(transparent)]
        Celld(CelldPreparationError),
    }

    trait Preflight {
        fn celld(
            &mut self,
            settings: &CelldRecordsSettings,
            token_env: &str,
            token: &CelldAccessToken,
        ) -> Result<CelldPinnedIdentity, PreflightFailed>;

        fn dynamodb(
            &mut self,
            settings: &DynamoDbRecordsSettings,
        ) -> Result<DynamoDbPinnedIdentity, PreflightFailed>;
    }

    struct NativePreflight {
        aws: AwsPreparation,
        celld: CelldPreparation,
        runtime: Runtime,
    }

    impl NativePreflight {
        fn new() -> Result<Self, String> {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_error| "failed to initialize pin preflight runtime".to_owned())?;
            let clock = MonotonicClock::default();
            let aws = AwsPreparation::new(clock.clone(), AwsPreparationLimits::default())
                .map_err(|_error| "failed to initialize AWS preflight".to_owned())?;
            let celld = CelldPreparation::new(clock, CelldPreparationLimits::default())
                .map_err(|_error| "failed to initialize celld preflight".to_owned())?;
            Ok(Self {
                aws,
                celld,
                runtime,
            })
        }
    }

    impl Preflight for NativePreflight {
        fn celld(
            &mut self,
            settings: &CelldRecordsSettings,
            token_env: &str,
            token: &CelldAccessToken,
        ) -> Result<CelldPinnedIdentity, PreflightFailed> {
            self.runtime
                .block_on(
                    self.celld
                        .preflight_record_store(settings, token_env, token),
                )
                .map_err(PreflightFailed::Celld)
        }

        fn dynamodb(
            &mut self,
            settings: &DynamoDbRecordsSettings,
        ) -> Result<DynamoDbPinnedIdentity, PreflightFailed> {
            self.runtime
                .block_on(self.aws.preflight_dynamodb_records(settings))
                .map(|identity| identity.pinned)
                .map_err(PreflightFailed::Aws)
        }
    }

    pub(super) fn run(args: &BindingsPinArgs) -> Result<(), String> {
        let ids = pin_file_with(&args.store_bindings, &mut NativePreflight::new()?, |key| {
            env::var(key).ok()
        })?;
        for id in ids {
            log::info!("[edgezero] pinned store binding {id}");
        }
        Ok(())
    }

    fn validate_preflight_input(bindings: &StoreBindingsDocument) -> Result<(), String> {
        if bindings.version != 1
            || !bindings.config.is_empty()
            || !bindings.secrets.is_empty()
            || bindings.limits.is_some()
        {
            return Err("bindings pin requires a portable version 1 KV document".to_owned());
        }
        for (id, binding) in &bindings.kv {
            if !valid_portable_id(id) {
                return Err(
                    "portable binding IDs must contain only lowercase ASCII, digits and underscore"
                        .to_owned(),
                );
            }
            match binding {
                KvBinding::DynamoDbRecords(settings) => {
                    settings
                        .validate(DynamoDbKvLimits::PORTABLE)
                        .and_then(|()| settings.portable_endpoint())
                        .map_err(|_error| "invalid portable DynamoDB binding".to_owned())?;
                    if settings.credentials.is_none() {
                        return Err(
                            "portable DynamoDB binding requires a credentials reference".to_owned()
                        );
                    }
                }
                KvBinding::CelldRecords(settings) => {
                    settings
                        .portable_endpoint()
                        .map_err(|_error| "invalid portable celld binding".to_owned())?;
                }
                KvBinding::DynamoDb(_) | KvBinding::Celld(_) => {
                    return Err("bindings pin supports only portable record providers".to_owned());
                }
            }
        }
        Ok(())
    }

    fn pin_file_with(
        path: &Path,
        preflight: &mut impl Preflight,
        mut env: impl FnMut(&str) -> Option<String>,
    ) -> Result<Vec<String>, String> {
        let input = read_portable_bindings(path)?;
        let bindings = parse_portable_bindings(&input)?;
        validate_preflight_input(&bindings)?;
        let mut document = input
            .parse::<DocumentMut>()
            .map_err(|_error| "malformed store bindings".to_owned())?;
        // Capture every token before the first network call, including when an
        // earlier alphabetic binding would otherwise run an AWS preflight first.
        let mut tokens = BTreeMap::new();
        for (id, binding) in &bindings.kv {
            if matches!(binding, KvBinding::CelldRecords(_)) {
                let key = celld_token_key(id);
                let raw = env(&key)
                    .ok_or_else(|| format!("missing preflight token environment variable {key}"))?;
                let token = CelldAccessToken::new(raw).map_err(|_error| {
                    format!("invalid preflight token environment variable {key}")
                })?;
                tokens.insert(id.as_str(), (key, token));
            }
        }
        let mut ids = Vec::new();
        for (id, binding) in &bindings.kv {
            let pinned = document
                .get_mut("kv")
                .and_then(|kv| kv.get_mut(id))
                .and_then(Item::as_table_like_mut)
                .ok_or_else(|| "invalid KV binding table".to_owned())?
                .entry("pinned")
                .or_insert_with(|| value(InlineTable::new()));
            let pin_fields = pinned
                .as_table_like_mut()
                .ok_or_else(|| "invalid pinned identity table".to_owned())?;
            let failed = |error: PreflightFailed| {
                format!("preflight failed for store binding {id}: {error}")
            };
            match binding {
                KvBinding::DynamoDbRecords(settings) => {
                    let identity = preflight.dynamodb(settings).map_err(failed)?;
                    set_pin_field(pin_fields, "table_arn", identity.table_arn);
                    if let Some(table_id) = identity.table_id {
                        set_pin_field(pin_fields, "table_id", table_id);
                    } else {
                        pin_fields.remove("table_id");
                    }
                }
                KvBinding::CelldRecords(settings) => {
                    let (key, token) = tokens
                        .get(id.as_str())
                        .ok_or_else(|| "missing captured celld preflight token".to_owned())?;
                    let identity = preflight.celld(settings, key, token).map_err(failed)?;
                    set_pin_field(pin_fields, "instance", identity.instance);
                }
                KvBinding::DynamoDb(_) | KvBinding::Celld(_) => {
                    return Err("bindings pin supports only portable record providers".to_owned());
                }
            }
            ids.push(id.clone());
        }
        let output = document.to_string();
        check_portable_size(&output)?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut temporary = tempfile::NamedTempFile::new_in(parent)
            .map_err(|_error| "failed to stage pinned bindings".to_owned())?;
        let permissions = fs::metadata(path)
            .map_err(|_error| "failed to read bindings permissions".to_owned())?
            .permissions();
        temporary
            .as_file()
            .set_permissions(permissions)
            .map_err(|_error| "failed to preserve bindings permissions".to_owned())?;
        temporary
            .write_all(output.as_bytes())
            .map_err(|_error| "failed to stage pinned bindings".to_owned())?;
        temporary
            .persist(path)
            .map_err(|_error| "failed to replace pinned bindings".to_owned())?;
        Ok(ids)
    }

    fn set_pin_field(table: &mut dyn TableLike, key: &str, content: String) {
        let mut replacement = value(content);
        if let Some(previous) = table.get(key).and_then(Item::as_value)
            && let Some(next) = replacement.as_value_mut()
        {
            *next.decor_mut() = previous.decor().clone();
        }
        table.insert(key, replacement);
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::bindings_pin::MAX_BINDINGS_CHARS;
        use edgezero_adapter::store_bindings::{PortableDeclarations, PortableRecordProviders};
        use std::path::PathBuf;

        const INPUT: &str = r#"# Deployment note stays at the top.
version = 1 # schema

[kv.identity] # keep this
provider = "aws-dynamodb-records"
region = "us-east-1"
table = "identity-table"
namespace = "example/identity"
credentials = { secrets = "stores" } # runtime source, not AWS env
# Endpoint must remain absent.

[kv.sessions]
provider = "celld-records"
endpoint = "https://unreachable.invalid"
store = "sessions"
access = { mode = "authenticated", token_secret = { secrets = "stores" } } # keep source
"#;
        const TOKEN: &str = "a-preflight-secret-never-written-or-printed";

        #[derive(Default)]
        struct FakePreflight {
            calls: Vec<String>,
            endpoint: Option<String>,
            fail_aws: bool,
            fail_celld: bool,
            table_id: Option<String>,
        }

        impl Preflight for FakePreflight {
            fn celld(
                &mut self,
                _settings: &CelldRecordsSettings,
                token_env: &str,
                token: &CelldAccessToken,
            ) -> Result<CelldPinnedIdentity, PreflightFailed> {
                if token_env != "KV__SESSIONS__CELLD_TOKEN" || token.expose() != TOKEN {
                    return Err(PreflightFailed::Celld(CelldPreparationError::Credentials));
                }
                self.calls.push("celld".to_owned());
                if self.fail_celld {
                    return Err(PreflightFailed::Celld(CelldPreparationError::Forbidden));
                }
                Ok(CelldPinnedIdentity {
                    instance: "AAAAAAAAAAAAAAAAAAAAAA".to_owned(),
                })
            }

            fn dynamodb(
                &mut self,
                settings: &DynamoDbRecordsSettings,
            ) -> Result<DynamoDbPinnedIdentity, PreflightFailed> {
                if settings.endpoint != self.endpoint {
                    return Err(PreflightFailed::Aws(AwsPreparationError::InvalidEndpoint));
                }
                self.calls.push("dynamodb".to_owned());
                if self.fail_aws {
                    return Err(PreflightFailed::Aws(AwsPreparationError::AccessDenied));
                }
                Ok(DynamoDbPinnedIdentity {
                    table_arn: "arn:aws:dynamodb:us-east-1:111122223333:table/identity-table"
                        .to_owned(),
                    table_id: self.table_id.clone(),
                })
            }
        }

        fn pin_fixture(
            input: &str,
            preflight: &mut FakePreflight,
            env: impl FnMut(&str) -> Option<String>,
        ) -> (tempfile::TempDir, PathBuf, Result<Vec<String>, String>) {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("bindings.toml");
            fs::write(&path, input).unwrap();
            let result = pin_file_with(&path, preflight, env);
            (directory, path, result)
        }

        #[test]
        fn bindings_pin_success_changes_only_pins_and_keeps_comments_and_sources() {
            let mut preflight = FakePreflight::default();
            let (_directory, path, result) = pin_fixture(INPUT, &mut preflight, |key| {
                assert_eq!(key, "KV__SESSIONS__CELLD_TOKEN");
                Some(TOKEN.to_owned())
            });
            let ids = result.unwrap();
            assert_eq!(ids, ["identity", "sessions"]);
            assert_eq!(preflight.calls, ["dynamodb", "celld"]);
            let output = fs::read_to_string(path).unwrap();
            let mut without_pins = output
                .lines()
                .filter(|line| !line.starts_with("pinned ="))
                .collect::<Vec<_>>()
                .join("\n");
            without_pins.push('\n');
            assert_eq!(without_pins, INPUT);
            assert!(!output.contains(TOKEN));
            assert!(!output.contains("table_id"));
            assert!(!output.contains("KV__SESSIONS__CELLD_TOKEN"));
            assert!(!ids.join(",").contains(TOKEN));
            let bindings = parse_portable_bindings(&output).unwrap();
            bindings
                .validate_portable(
                    &PortableDeclarations {
                        kv: &["identity", "sessions"],
                        secrets: &["stores"],
                    },
                    PortableRecordProviders {
                        celld: true,
                        dynamodb: true,
                    },
                )
                .unwrap();
        }

        #[test]
        fn bindings_pin_late_failure_leaves_input_byte_for_byte_unchanged() {
            let mut preflight = FakePreflight {
                fail_celld: true,
                ..FakePreflight::default()
            };
            let (_directory, path, result) =
                pin_fixture(INPUT, &mut preflight, |_key| Some(TOKEN.to_owned()));
            assert_eq!(preflight.calls, ["dynamodb", "celld"]);
            assert_eq!(fs::read_to_string(path).unwrap(), INPUT);
            let error = result.unwrap_err();
            assert_eq!(
                error,
                "preflight failed for store binding sessions: forbidden"
            );
            assert!(!error.contains(TOKEN));
            assert!(!error.contains("unreachable.invalid"));
        }

        #[test]
        fn bindings_pin_retains_the_closed_aws_failure_category() {
            let mut preflight = FakePreflight {
                fail_aws: true,
                ..FakePreflight::default()
            };
            let (_directory, path, result) =
                pin_fixture(INPUT, &mut preflight, |_key| Some(TOKEN.to_owned()));
            assert_eq!(
                result.unwrap_err(),
                "preflight failed for store binding identity: access denied"
            );
            assert_eq!(preflight.calls, ["dynamodb"]);
            assert_eq!(fs::read_to_string(path).unwrap(), INPUT);
        }

        #[test]
        fn bindings_pin_missing_or_invalid_token_fails_before_any_preflight() {
            for token in [None, Some("invalid-secret".to_owned())] {
                let mut preflight = FakePreflight::default();
                let (_directory, path, result) =
                    pin_fixture(INPUT, &mut preflight, |_key| token.clone());
                let error = result.unwrap_err();
                assert!(error.contains("KV__SESSIONS__CELLD_TOKEN"));
                assert!(!error.contains("invalid-secret"));
                assert!(preflight.calls.is_empty());
                assert_eq!(fs::read_to_string(path).unwrap(), INPUT);
            }
        }

        #[test]
        fn bindings_pin_replaces_existing_pin_without_dropping_its_comment() {
            let input = INPUT.replace(
                "region =",
                "pinned = { table_arn = 'old', table_id = 'old-id' } # preserve pin note\nregion =",
            );
            let mut preflight = FakePreflight::default();
            let (_directory, path, result) =
                pin_fixture(&input, &mut preflight, |_key| Some(TOKEN.to_owned()));
            result.unwrap();
            let output = fs::read_to_string(path).unwrap();
            assert!(output.contains("# preserve pin note"));
            assert!(!output.contains("old-id"));
            assert!(!output.contains("table_id"));
            assert!(output.contains("arn:aws:dynamodb:"));
        }

        #[test]
        fn bindings_pin_preserves_an_explicit_endpoint_and_exact_table_id() {
            let endpoint = "http://127.0.0.1:18000";
            let input = INPUT.replace(
                "region =",
                &format!("endpoint = '{endpoint}' # custom endpoint\nregion ="),
            );
            let mut preflight = FakePreflight {
                endpoint: Some(endpoint.to_owned()),
                table_id: Some("observed-table-id".to_owned()),
                ..FakePreflight::default()
            };
            let (_directory, path, result) =
                pin_fixture(&input, &mut preflight, |_key| Some(TOKEN.to_owned()));
            result.unwrap();
            let output = fs::read_to_string(path).unwrap();
            assert!(output.contains(&format!("endpoint = '{endpoint}' # custom endpoint")));
            let document = parse_portable_bindings(&output).unwrap();
            let KvBinding::DynamoDbRecords(settings) = document
                .kv
                .get("identity")
                .expect("pinned DynamoDB binding")
            else {
                panic!("expected DynamoDB");
            };
            assert_eq!(
                settings.pinned.as_ref().unwrap().table_id.as_deref(),
                Some("observed-table-id")
            );
        }

        #[test]
        fn bindings_pin_preserves_table_form_pin_comments() {
            let input = INPUT.replace(
                "[kv.sessions]",
                "[kv.identity.pinned] # table note\ntable_arn = 'old' # ARN note\n\n[kv.sessions]",
            );
            let mut preflight = FakePreflight::default();
            let (_directory, path, result) =
                pin_fixture(&input, &mut preflight, |_key| Some(TOKEN.to_owned()));
            result.unwrap();
            let output = fs::read_to_string(path).unwrap();
            assert!(output.contains("[kv.identity.pinned] # table note"));
            assert!(output.contains("# ARN note"));
            assert!(output.contains("arn:aws:dynamodb:"));
        }

        #[test]
        fn bindings_pin_output_over_the_entry_limit_leaves_input_unchanged() {
            let input = format!(
                "{}#{}\n",
                INPUT,
                "x".repeat(MAX_BINDINGS_CHARS - INPUT.chars().count() - 2)
            );
            let mut preflight = FakePreflight::default();
            let (_directory, path, result) =
                pin_fixture(&input, &mut preflight, |_key| Some(TOKEN.to_owned()));
            assert!(result.unwrap_err().contains("entry limit"));
            assert_eq!(preflight.calls, ["dynamodb", "celld"]);
            assert_eq!(fs::read_to_string(path).unwrap(), input);
        }

        #[test]
        fn bindings_pin_invalid_documents_never_preflight_or_change_the_file() {
            for input in [
                INPUT.replace("version = 1", "version = 2"),
                INPUT.replace("[kv.identity]", "[kv.Identity]"),
                INPUT.replace("credentials = { secrets = \"stores\" }", ""),
                INPUT.replace(
                    "mode = \"authenticated\", token_secret = { secrets = \"stores\" }",
                    "mode = \"trusted-network\"",
                ),
                INPUT.replace("aws-dynamodb-records", "aws-dynamodb"),
                format!("{INPUT}\n[limits.aws]\npreparation_timeout_ms = 1"),
            ] {
                let mut preflight = FakePreflight::default();
                let (_directory, path, result) =
                    pin_fixture(&input, &mut preflight, |_key| panic!("no token read"));
                let _error = result.expect_err("invalid document must fail");
                assert!(preflight.calls.is_empty());
                assert_eq!(fs::read_to_string(path).unwrap(), input);
            }
        }
    }
}

use crate::args::BindingsPinArgs;
use edgezero_adapter::store_bindings::StoreBindingsDocument;
use std::fs::File;
use std::io::Read as _;
use std::path::Path;

// Fastly Config Store entries hold at most 8,000 characters. Bound the read
// before decoding UTF-8, allowing four bytes per character.
const MAX_BINDINGS_BYTES: u16 = 32_000;
const MAX_BINDINGS_CHARS: usize = 8_000;

/// Preflight every selected binding and write only its pinned identity.
///
/// # Errors
/// Returns a redacted error on unsupported targets, invalid files, missing
/// credentials, failed preflight or failed atomic replacement. Without the
/// `bindings-pin` feature this command reports that support is not compiled.
#[inline]
pub fn run_config_bindings_pin(args: &BindingsPinArgs) -> Result<(), String> {
    if args.adapter != "fastly" {
        return Err("bindings pin supports only `--adapter fastly`".to_owned());
    }
    #[cfg(feature = "bindings-pin")]
    {
        enabled::run(args)
    }
    #[cfg(not(feature = "bindings-pin"))]
    {
        Err(
            "bindings pin support is not compiled; rebuild the CLI with `--features bindings-pin`"
                .to_owned(),
        )
    }
}

pub(crate) fn read_portable_bindings(path: &Path) -> Result<String, String> {
    let file = File::open(path).map_err(|_error| "failed to read store bindings".to_owned())?;
    let mut bytes = Vec::new();
    file.take(u64::from(MAX_BINDINGS_BYTES) + 1)
        .read_to_end(&mut bytes)
        .map_err(|_error| "failed to read store bindings".to_owned())?;
    let input =
        String::from_utf8(bytes).map_err(|_error| "invalid store bindings UTF-8".to_owned())?;
    check_portable_size(&input)?;
    Ok(input)
}

pub(crate) fn parse_portable_bindings(input: &str) -> Result<StoreBindingsDocument, String> {
    check_portable_size(input)?;
    StoreBindingsDocument::parse(input, usize::from(MAX_BINDINGS_BYTES))
        .map_err(|error| error.to_string())
}

fn check_portable_size(input: &str) -> Result<(), String> {
    if input.len() > usize::from(MAX_BINDINGS_BYTES) || input.chars().count() > MAX_BINDINGS_CHARS {
        return Err("store bindings exceed the Fastly Config Store entry limit".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[cfg(not(feature = "bindings-pin"))]
    #[test]
    fn bindings_pin_feature_off_reports_support_not_compiled() {
        let args = BindingsPinArgs {
            adapter: "fastly".to_owned(),
            store_bindings: "absent.toml".into(),
        };
        assert!(
            run_config_bindings_pin(&args)
                .unwrap_err()
                .contains("not compiled")
        );
    }

    #[test]
    fn bindings_pin_rejects_other_targets_before_any_file_access() {
        let args = BindingsPinArgs {
            adapter: "axum".to_owned(),
            store_bindings: "absent.toml".into(),
        };
        assert_eq!(
            run_config_bindings_pin(&args).unwrap_err(),
            "bindings pin supports only `--adapter fastly`"
        );
    }

    #[test]
    fn bindings_files_are_capped_and_malformed_input_is_redacted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bindings.toml");
        fs::write(&path, "#".repeat(MAX_BINDINGS_CHARS + 1)).unwrap();
        assert!(read_portable_bindings(&path).unwrap_err().contains("limit"));
        assert_eq!(
            parse_portable_bindings("not TOML with a secret").unwrap_err(),
            "malformed store bindings"
        );
    }
}
