//! Shared core for Berg — Apache Iceberg tooling.
//!
//! This crate is consumed by the `berg-cli` and `berg-tui` frontends and houses
//! all logic that is not specific to a single user interface.
//!
//! ## Iceberg surface
//!
//! `berg-core` is the deliberate re-export point for the subset of
//! `iceberg-rust` that frontends are expected to consume directly. Frontends
//! depend on `berg-core`, not on `iceberg`. See the project rationale section
//! "Abstraction boundary: operations yes, types no" for the reasoning.

pub mod engine;
pub mod error;
pub mod view;

pub use error::{BergError, Result};

/// Iceberg specification types — `Schema`, `Snapshot`, `PartitionSpec`,
/// `TableMetadata`, and so on.
///
/// Re-exported verbatim from [`iceberg::spec`]. Frontends consume these as
/// plain data; `berg-core::engine` returns them, and `berg-core::view`
/// derives presentation-independent intermediates from them.
pub use iceberg::spec;

/// Iceberg identifier types — `NamespaceIdent`, `TableIdent`.
///
/// Re-exported from the `iceberg` crate root. These are the canonical way to
/// address namespaces and tables across the Iceberg ecosystem.
pub use iceberg::{NamespaceIdent, TableIdent};

/// Returns the version string of the `berg-core` crate.
#[must_use]
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Build a welcome banner for an application.
///
/// # Errors
///
/// Returns [`BergError::EmptyAppName`] if `app_name` is empty or whitespace-only.
pub fn welcome_message(app_name: &str) -> Result<String> {
    let app_name = app_name.trim();

    if app_name.is_empty() {
        return Err(BergError::EmptyAppName);
    }

    Ok(format!("Welcome to {app_name} {}.", version()))
}

#[cfg(test)]
mod tests {
    use super::{BergError, welcome_message};

    #[test]
    fn welcome_message_includes_app_name() {
        let message = welcome_message("berg").expect("valid app name");

        assert!(message.starts_with("Welcome to berg "));
    }

    #[test]
    fn welcome_message_rejects_empty_names() {
        let err = welcome_message("  ").unwrap_err();

        assert!(matches!(err, BergError::EmptyAppName));
    }
}
