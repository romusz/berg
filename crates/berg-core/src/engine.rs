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
use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::StorageFactory;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;

use crate::{BergError, Result, spec};

/// A fully-qualified table identifier accepted by the CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedTableIdent {
    catalog: String,
    table: TableIdent,
}

impl QualifiedTableIdent {
    /// Parse `catalog.namespace.table` into a REST catalog prefix plus Iceberg table ident.
    ///
    /// # Errors
    ///
    /// Returns [`BergError::InvalidTableIdentifier`] when the value has fewer
    /// than three dot-separated segments or contains an empty segment.
    pub fn parse(value: &str) -> Result<Self> {
        let parts = value.split('.').map(str::trim).collect::<Vec<_>>();

        if parts.len() < 3 || parts.iter().any(|part| part.is_empty()) {
            return Err(BergError::InvalidTableIdentifier {
                value: value.to_string(),
            });
        }

        let catalog = parts[0].to_string();
        let namespace = NamespaceIdent::from_strs(&parts[1..parts.len() - 1])?;
        let table = TableIdent::new(namespace, parts[parts.len() - 1].to_string());

        Ok(Self { catalog, table })
    }

    /// REST catalog prefix selected by the leading identifier segment.
    #[must_use]
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    /// Iceberg table identifier without the catalog/prefix segment.
    #[must_use]
    pub fn table(&self) -> &TableIdent {
        &self.table
    }

    /// Full identifier as provided by the user.
    #[must_use]
    pub fn display_name(&self) -> String {
        format!("{}.{}", self.catalog, self.table)
    }
}

/// Connection settings for an Iceberg REST catalog.
#[derive(Debug, Clone)]
pub struct RestCatalogConfig {
    uri: String,
    prefix: String,
    warehouse: Option<String>,
    properties: HashMap<String, String>,
}

impl RestCatalogConfig {
    /// Build a REST catalog config.
    ///
    /// # Errors
    ///
    /// Returns [`BergError::MissingCatalogUri`] when `uri` is empty after
    /// trimming trailing slashes.
    pub fn new(
        uri: impl Into<String>,
        prefix: impl Into<String>,
        warehouse: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Self> {
        let uri = uri.into().trim_end_matches('/').to_string();
        let prefix = prefix.into().trim_matches('/').to_string();

        if uri.is_empty() {
            return Err(BergError::MissingCatalogUri);
        }

        Ok(Self {
            uri,
            prefix,
            warehouse,
            properties,
        })
    }

    /// REST endpoint used to load this table's current metadata.
    #[must_use]
    pub fn table_endpoint(&self, table: &TableIdent) -> String {
        format!(
            "{}/v1/{}/namespaces/{}/tables/{}",
            self.uri,
            self.prefix,
            table.namespace().to_url_string(),
            table.name()
        )
    }

    fn catalog_properties(&self) -> HashMap<String, String> {
        let mut properties = self.properties.clone();
        properties.insert(REST_CATALOG_PROP_URI.to_string(), self.uri.clone());
        properties.insert("prefix".to_string(), self.prefix.clone());

        if let Some(warehouse) = &self.warehouse {
            properties.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
        }

        properties
    }
}

/// Load the current schema for a table through an Iceberg REST catalog.
///
/// # Errors
///
/// Returns an Iceberg-backed error when the catalog cannot be constructed,
/// contacted, or cannot load the requested table.
pub async fn load_current_schema(
    config: &RestCatalogConfig,
    table_ident: &TableIdent,
) -> Result<spec::SchemaRef> {
    let storage_factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: None,
    });
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load("berg", config.catalog_properties())
        .await?;

    let table = catalog.load_table(table_ident).await?;

    Ok(table.metadata().current_schema().clone())
}

/// Parse `key=value` catalog property strings.
///
/// # Errors
///
/// Returns [`BergError::InvalidCatalogProperty`] when the value does not contain
/// `=` or the property key is empty.
pub fn parse_catalog_property(value: &str) -> Result<(String, String)> {
    let Some((key, property_value)) = value.split_once('=') else {
        return Err(BergError::InvalidCatalogProperty {
            value: value.to_string(),
        });
    };

    let key = key.trim();
    if key.is_empty() {
        return Err(BergError::InvalidCatalogProperty {
            value: value.to_string(),
        });
    }

    Ok((key.to_string(), property_value.trim().to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{QualifiedTableIdent, RestCatalogConfig, parse_catalog_property};

    #[test]
    fn parses_catalog_namespace_table() {
        let table = QualifiedTableIdent::parse("lakehouse.redapl_v3.k8s_pod_blue")
            .expect("valid table ident");

        assert_eq!("lakehouse", table.catalog());
        assert_eq!("redapl_v3.k8s_pod_blue", table.table().to_string());
        assert_eq!("lakehouse.redapl_v3.k8s_pod_blue", table.display_name());
    }

    #[test]
    fn parses_nested_namespaces() {
        let table = QualifiedTableIdent::parse("lakehouse.a.b.c")
            .expect("valid nested namespace table ident");

        assert_eq!("lakehouse", table.catalog());
        assert_eq!("a.b.c", table.table().to_string());
    }

    #[test]
    fn rejects_missing_catalog_segment() {
        assert!(QualifiedTableIdent::parse("redapl_v3.k8s_pod_blue").is_err());
    }

    #[test]
    fn builds_table_endpoint() {
        let table = QualifiedTableIdent::parse("lakehouse.redapl_v3.k8s_pod_blue")
            .expect("valid table ident");
        let config = RestCatalogConfig::new(
            "https://lakehouse-gateway.us1.staging.dog/internal/catalog/",
            table.catalog(),
            None,
            HashMap::default(),
        )
        .expect("valid config");

        assert_eq!(
            "https://lakehouse-gateway.us1.staging.dog/internal/catalog/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            config.table_endpoint(table.table())
        );
    }

    #[test]
    fn parses_catalog_property() {
        assert_eq!(
            (
                "header.Authorization".to_string(),
                "Bearer token".to_string()
            ),
            parse_catalog_property("header.Authorization=Bearer token").expect("valid property")
        );
    }
}
