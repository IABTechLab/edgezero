//! Adapter-owned descriptions of local CLI inputs and outputs.

use std::path::PathBuf;

/// Declared project-relative inputs to an adapter's pure layout resolver.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct AdapterLocalContext<'ctx> {
    component: Option<&'ctx str>,
    crate_path: Option<&'ctx str>,
    manifest: Option<&'ctx str>,
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "the constructor precedes its related read-only accessors"
)]
impl<'ctx> AdapterLocalContext<'ctx> {
    /// Describe the selected adapter without introducing core manifest types.
    #[must_use]
    #[inline]
    pub fn new(
        manifest: Option<&'ctx str>,
        crate_path: Option<&'ctx str>,
        component: Option<&'ctx str>,
    ) -> Self {
        Self {
            component,
            crate_path,
            manifest,
        }
    }

    /// Declared adapter manifest, relative to the application root.
    #[must_use]
    #[inline]
    pub fn manifest(&self) -> Option<&'ctx str> {
        self.manifest
    }

    /// Declared adapter crate, relative to the application root.
    #[must_use]
    #[inline]
    pub fn crate_path(&self) -> Option<&'ctx str> {
        self.crate_path
    }

    /// Optional component selected inside a provider manifest.
    #[must_use]
    #[inline]
    pub fn component(&self) -> Option<&'ctx str> {
        self.component
    }
}

/// How shared reporting handles an adapter-declared artifact's contents.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LocalArtifactKind {
    /// Environment assignments whose values must be redacted in previews.
    Env,
    /// TOML whose scalar values and comments must be removed from previews.
    SensitiveToml,
    /// Ordinary text, such as a generated TOML document.
    Text,
}

/// A local provision output, relative to the application root.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct LocalArtifact {
    pub kind: LocalArtifactKind,
    pub path: PathBuf,
}

impl LocalArtifact {
    /// Declare an artifact and its provider-independent content kind.
    #[must_use]
    #[inline]
    pub fn new<P>(path: P, kind: LocalArtifactKind) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            path: path.into(),
            kind,
        }
    }
}

/// Pure local CLI layout supplied by an adapter.
///
/// Paths are project-relative. The CLI validates them in the original tree
/// before staging and uses the same artifacts for snapshots and diffs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct AdapterLocalLayout {
    pub artifacts: Vec<LocalArtifact>,
    pub manifest: Option<PathBuf>,
    pub serve_env: Option<PathBuf>,
}

impl AdapterLocalLayout {
    /// Start with a manifest which is also a text provision output.
    #[must_use]
    #[inline]
    pub fn new<P>(manifest: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self::new_with_kind(manifest, LocalArtifactKind::Text)
    }

    /// Start with an adapter-selected preview policy for its manifest.
    #[must_use]
    #[inline]
    pub fn new_with_kind<P>(manifest: P, kind: LocalArtifactKind) -> Self
    where
        P: Into<PathBuf>,
    {
        let path = manifest.into();
        Self {
            artifacts: vec![LocalArtifact::new(path.clone(), kind)],
            manifest: Some(path),
            serve_env: None,
        }
    }

    /// Add a provider-owned sidecar to shared snapshots and reporting.
    #[must_use]
    #[inline]
    pub fn with_artifact<P>(mut self, path: P, kind: LocalArtifactKind) -> Self
    where
        P: Into<PathBuf>,
    {
        self.artifacts.push(LocalArtifact::new(path, kind));
        self
    }

    /// Select an env file to preload when serving this adapter.
    #[must_use]
    #[inline]
    pub fn with_serve_env<P>(mut self, path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        self.serve_env = Some(path.into());
        self
    }
}
