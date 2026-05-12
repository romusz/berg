//! Error types for `berg-core`.

use thiserror::Error;

/// Top-level error type returned by `berg-core` operations.
///
/// Marked `#[non_exhaustive]` so additional variants can be added without it
/// being a semver-breaking change for downstream `match` expressions.
/// Specific iceberg failure modes (e.g., `TableNotFound`,
/// `CatalogConnection`) will be promoted to dedicated variants as engine
/// functions land; until then, [`BergError::Iceberg`] is the catch-all.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BergError {
    /// An application name was empty or whitespace-only.
    #[error("application name cannot be empty")]
    EmptyAppName,

    /// The catalog URI required to contact an Iceberg REST catalog was missing.
    #[error("catalog URI is required; pass --catalog-uri or set BERG_CATALOG_URI")]
    MissingCatalogUri,

    /// A table identifier could not be parsed.
    #[error("invalid table identifier `{value}`: expected catalog.namespace.table")]
    InvalidTableIdentifier { value: String },

    /// A catalog property could not be parsed.
    #[error("invalid catalog property `{value}`: expected key=value")]
    InvalidCatalogProperty { value: String },

    /// The requested table does not have a current snapshot.
    #[error("table `{table}` does not have a current snapshot")]
    NoCurrentSnapshot { table: String },

    /// A manifest list entry reported an invalid manifest file length.
    #[error("manifest `{path}` has invalid length `{length}`")]
    InvalidManifestLength { path: String, length: i64 },

    /// A requested manifest file ID was not present in the current manifest list.
    #[error("manifest file id `{id}` not found; available ids: {}", available.join(", "))]
    UnknownManifestFileId { id: String, available: Vec<String> },

    /// A snapshot timestamp could not be represented.
    #[error("snapshot `{snapshot_id}` has invalid timestamp `{timestamp_ms}`")]
    InvalidSnapshotTimestamp { snapshot_id: i64, timestamp_ms: i64 },

    /// An error originating from `iceberg-rust`.
    #[error(transparent)]
    Iceberg(#[from] iceberg::Error),
}

/// Convenience alias used throughout `berg-core`.
pub type Result<T> = std::result::Result<T, BergError>;
