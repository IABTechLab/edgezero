//! Existing native preparation APIs only. Provisioning belongs to the operator.
use anyhow::{Result, ensure};
use edgezero_core::{key_value_store::KvHandle, time::MonotonicClock};
use serde::Deserialize;
use std::{fs, path::Path};

#[derive(Deserialize)]
#[serde(tag = "backend", rename_all = "kebab-case", deny_unknown_fields)]
enum Settings {
    #[cfg(feature = "celld")]
    Celld {
        settings: edgezero_store_celld::settings::CelldKvSettings,
        #[serde(default)]
        limits: edgezero_store_celld::settings::CelldPreparationLimits,
    },
    #[cfg(feature = "dynamodb")]
    Dynamodb {
        settings: edgezero_store_aws::settings::DynamoDbKvSettings,
        #[serde(default)]
        limits: edgezero_store_aws::settings::AwsPreparationLimits,
    },
}

pub async fn prepare(path: &Path, required_bytes: usize) -> Result<KvHandle> {
    let settings: Settings = serde_json::from_slice(&fs::read(path)?)?;
    match settings {
        #[cfg(feature = "celld")]
        Settings::Celld { settings, limits } => {
            use edgezero_store_celld::{CelldPreparation, settings::CelldAccessToken};
            settings.validate()?;
            ensure!(
                required_bytes as u64 <= limits.max_value_bytes,
                "artifact exceeds backend budget"
            );
            let token = settings
                .access
                .token_env()
                .map(|name| Ok::<_, anyhow::Error>(CelldAccessToken::new(std::env::var(name)?)?))
                .transpose()?;
            Ok(CelldPreparation::new(MonotonicClock::default(), limits)?
                .prepare_kv(&settings, token.as_ref())
                .await?)
        }
        #[cfg(feature = "dynamodb")]
        Settings::Dynamodb { settings, limits } => {
            ensure!(
                required_bytes <= settings.limits.max_value_bytes,
                "artifact exceeds backend budget"
            );
            Ok(
                edgezero_store_aws::AwsPreparation::new(MonotonicClock::default(), limits)?
                    .prepare_dynamodb(&settings)
                    .await?,
            )
        }
    }
}
