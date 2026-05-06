//! Async operations against Apache Iceberg catalogs and tables.
//!
//! This module wraps [`iceberg-rust`](https://crates.io/crates/iceberg) and is
//! the home for catalog clients, table inspection, snapshot navigation, and
//! manifest reading. Code in this module is expected to be async.
//!
//! ## Wired-in backends
//!
//! `berg-core` declares the iceberg ecosystem crates needed to talk to a real
//! Iceberg deployment, so this module can grow features without further Cargo
//! changes:
//!
//! - [`iceberg`] — core types, traits, and table loader machinery.
//! - [`iceberg_catalog_rest`] — Apache Iceberg REST catalog protocol client.
//! - [`iceberg_storage_opendal`] — file IO via `OpenDAL`, with default
//!   features for in-memory, local filesystem, and S3 storage. Additional
//!   backends (GCS, Azure, OSS) are available behind the upstream feature
//!   flags but not enabled by default.
//!
//! Frontend crates do **not** depend on the catalog or storage crates
//! directly; backend selection is a `berg-core` concern.
//!
//! Currently a placeholder; functionality lands as features are implemented.
