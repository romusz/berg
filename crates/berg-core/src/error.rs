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

    /// An error originating from `iceberg-rust`.
    #[error(transparent)]
    Iceberg(#[from] iceberg::Error),
}

/// Convenience alias used throughout `berg-core`.
pub type Result<T> = std::result::Result<T, BergError>;
