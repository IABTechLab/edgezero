//! Per-KV-ID portable record providers selected by the runtime bindings document.
//!
//! The optional Config Store `edgezero_store_bindings`, entry `EDGEZERO__STORE_BINDINGS`,
//! holds the shared `version = 1` document. It is read once when the application is
//! built, with no network I/O and no secret reads. Each bound KV ID is then served by
//! its portable provider over the request's own `HttpClient`; every other declared ID
//! stays on Fastly KV. A document naming a provider this binary lacks fails assembly.
//! Nothing falls back to Fastly KV.

#[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
use std::collections::BTreeMap;
#[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
use std::sync::Arc;

#[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
use edgezero_adapter::store_bindings::KvBinding;
use edgezero_adapter::store_bindings::{
    PORTABLE_BINDINGS_ENTRY, PORTABLE_BINDINGS_STORE, PortableDeclarations,
    PortableRecordProviders, StoreBindingsDocument,
};
use edgezero_core::app::{App, StoresMetadata};
use edgezero_core::error::EdgeError;
use edgezero_core::key_value_store::{KvError, KvHandle};
use edgezero_core::outbound::HttpClient;
#[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
use edgezero_core::secret_store::SecretHandle;
#[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
use edgezero_core::store_registry::BoundSecretStore;
#[cfg(feature = "aws-dynamodb-records")]
use edgezero_store_aws::portable_dynamodb::PreparedDynamoDbRecords;
#[cfg(feature = "celld-records")]
use edgezero_store_celld::portable::PreparedCelldRecords;

use crate::config_store::portable_bindings_document;
#[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
use crate::secret_store::FastlySecretStore;

/// The Fastly Config Store entry value cap: 8,000 Unicode scalars of up to 4 bytes.
const MAX_DOCUMENT_BYTES: usize = 32_000;

/// Record providers compiled into this binary.
const COMPILED: PortableRecordProviders = PortableRecordProviders {
    celld: cfg!(feature = "celld-records"),
    dynamodb: cfg!(feature = "aws-dynamodb-records"),
};

/// One bound KV ID's prepared provider. Settings and pins only; no request state.
#[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
enum Prepared {
    #[cfg(feature = "celld-records")]
    Celld(PreparedCelldRecords),
    #[cfg(feature = "aws-dynamodb-records")]
    DynamoDb(PreparedDynamoDbRecords),
}

/// Prepared providers for every bound KV ID, retained on the application.
#[derive(Clone)]
pub(crate) struct PortableProviders {
    #[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
    by_id: Arc<BTreeMap<String, Prepared>>,
}

impl PortableProviders {
    /// A request-bound store for `id` over the request's effective client, if bound.
    #[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
    pub(crate) fn bind(&self, id: &str, client: &HttpClient) -> Option<KvHandle> {
        Some(match self.by_id.get(id)? {
            #[cfg(feature = "celld-records")]
            Prepared::Celld(provider) => KvHandle::new(Arc::new(provider.bind(client.clone()))),
            #[cfg(feature = "aws-dynamodb-records")]
            Prepared::DynamoDb(provider) => KvHandle::new(Arc::new(provider.bind(client.clone()))),
        })
    }

    /// Without portable providers no ID is ever bound.
    #[cfg(not(any(feature = "aws-dynamodb-records", feature = "celld-records")))]
    #[expect(
        clippy::unused_self,
        reason = "keep one binding call site whether or not providers are compiled"
    )]
    pub(crate) const fn bind(&self, _id: &str, _client: &HttpClient) -> Option<KvHandle> {
        None
    }
}

/// Reads and validates the bindings document and installs the prepared providers.
///
/// A missing store or entry means every declared KV ID stays on Fastly KV.
///
/// # Errors
/// Returns a typed validation error for an unreadable, malformed or unsupported
/// document, an undeclared or unlayouted ID, or a provider this binary lacks.
pub(crate) fn install(app: &mut App, stores: StoresMetadata) -> Result<(), EdgeError> {
    let Some(text) = portable_bindings_document(PORTABLE_BINDINGS_STORE, PORTABLE_BINDINGS_ENTRY)
        .map_err(|_error| failure("store bindings unavailable"))?
    else {
        return Ok(());
    };
    let document = StoreBindingsDocument::parse(&text, MAX_DOCUMENT_BYTES)
        .map_err(|category| failure(&category.to_string()))?;
    document
        .validate_portable(
            &PortableDeclarations {
                kv: stores.kv.map_or(&[][..], |meta| meta.ids),
                secrets: stores.secrets.map_or(&[][..], |meta| meta.ids),
            },
            COMPILED,
        )
        .map_err(|category| failure(&category.to_string()))?;
    #[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
    {
        let providers = prepare(app, &document)?;
        if !providers.by_id.is_empty() {
            app.insert_state(providers);
        }
    }
    // Validation rejects every KV binding when no portable provider is compiled.
    #[cfg(not(any(feature = "aws-dynamodb-records", feature = "celld-records")))]
    let _: (&mut App, StoreBindingsDocument) = (app, document);
    Ok(())
}

#[cfg(any(feature = "aws-dynamodb-records", feature = "celld-records"))]
fn prepare(app: &App, document: &StoreBindingsDocument) -> Result<PortableProviders, EdgeError> {
    let secrets = |logical: &str| {
        BoundSecretStore::new(
            SecretHandle::new(Arc::new(FastlySecretStore)),
            logical.to_owned(),
        )
    };
    let mut by_id = BTreeMap::new();
    for (id, binding) in &document.kv {
        let layout = app
            .record_store_requirements()
            .get(id)
            .ok_or_else(|| failure("record binding requires an application layout"))?;
        let prepared = match binding {
            #[cfg(feature = "aws-dynamodb-records")]
            KvBinding::DynamoDbRecords(settings) => {
                let logical = settings
                    .credentials
                    .as_ref()
                    .map(|reference| reference.secrets.as_str())
                    .ok_or_else(|| failure("missing credentials reference"))?;
                Prepared::DynamoDb(
                    PreparedDynamoDbRecords::new(id, settings, layout, secrets(logical))
                        .map_err(|category| failure(&category.to_string()))?,
                )
            }
            #[cfg(feature = "celld-records")]
            KvBinding::CelldRecords(settings) => {
                let logical = settings
                    .token_secret()
                    .ok_or_else(|| failure("missing token reference"))?;
                Prepared::Celld(
                    PreparedCelldRecords::new(id, settings, layout, secrets(logical))
                        .map_err(|category| failure(&category.to_string()))?,
                )
            }
            // Portable validation already rejected native-only and uncompiled providers.
            _ => return Err(failure("provider unsupported on this target")),
        };
        by_id.insert(id.clone(), prepared);
    }
    Ok(PortableProviders {
        by_id: Arc::new(by_id),
    })
}

fn failure(category: &str) -> EdgeError {
    KvError::Validation(format!("store bindings: {category}")).into()
}
