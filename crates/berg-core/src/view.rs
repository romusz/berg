//! Presentation-independent views over Iceberg data.
//!
//! Berg-shaped intermediate representations derived from Iceberg spec types.
//! Roughly analogous to an AST: structured, semantic, presentation-agnostic.
//! Frontends consume these views and decide how to render them
//! (CLI text, TUI widgets, future GUI components).
//!
//! ## What goes here
//!
//! Types and pure functions that derive **information** from
//! [`crate::spec`] types. Examples of likely future contents:
//!
//! - `SchemaSummary` — fields rolled up with partition flags, nullability,
//!   stats hints.
//! - `SnapshotTimeline` — ordered traversal of a snapshot history.
//! - `PartitionLayout` — partition spec viewed alongside the columns it touches.
//! - `ManifestDigest` — manifest contents summarized for inspection.
//!
//! ## What does *not* go here
//!
//! - Final presentation: text strings, ANSI escapes, ratatui widgets, HTML.
//!   Those live in the frontends.
//! - Async I/O or catalog calls. Those live in [`crate::engine`].
//! - Mirrors of Iceberg spec types. If [`crate::spec::Schema`] is enough for
//!   both frontends, pass it through directly — don't introduce a wrapper.
//!
//! ## Pass-through default
//!
//! A view type is justified only when it removes real duplication or carries
//! semantics frontends would otherwise compute themselves. When the iceberg
//! spec type already conveys what the frontend needs, frontends consume
//! [`crate::spec`] types directly.
//!
//! Currently a placeholder; content lands as features are implemented.
