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
use std::process::{Command, Stdio};
use std::sync::Arc;

use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use iceberg::io::StorageFactory;
use iceberg::spec::{DataContentType, ManifestContentType};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::{
    AwsCredential, AwsCredentialLoad, CustomAwsCredentialLoader, OpenDalStorageFactory,
};
use reqwest::Client;
use time::OffsetDateTime;

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
    s3_credentials: Option<S3CredentialSource>,
}

#[derive(Debug)]
struct AwsProfileCredentialLoader {
    profile: String,
}

#[derive(Debug)]
struct AwsVaultCredentialLoader {
    profile: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum S3CredentialSource {
    AwsProfile(String),
    AwsVault(String),
}

/// Statistics for the current Iceberg table snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTableStats {
    /// Snapshot these statistics were computed from.
    pub snapshot_id: i64,
    /// Snapshot commit/update timestamp.
    pub snapshot_updated_at: OffsetDateTime,
    /// Current table metadata JSON location.
    pub metadata_json_path: String,
    /// Whether the current table metadata JSON object is compressed.
    pub metadata_json_compressed: bool,
    /// Current snapshot manifest list location.
    pub manifest_list_path: String,
    /// Total bytes across live data files and live delete files.
    pub total_table_file_size_bytes: u64,
    /// Number of live data files.
    pub data_file_count: u64,
    /// Number of live position delete files.
    pub position_delete_file_count: u64,
    /// Number of position delete records across live position delete files.
    pub position_delete_record_count: u64,
    /// Number of live equality delete files.
    pub equality_delete_file_count: u64,
    /// Number of equality delete records across live equality delete files.
    pub equality_delete_record_count: u64,
    /// Number of records in live data files.
    pub record_count: u64,
    /// Number of manifest files in the current snapshot manifest list.
    pub manifest_file_count: u64,
    /// Size of the current snapshot manifest list file.
    pub manifest_list_size_bytes: u64,
    /// Total size of manifest files referenced by the current snapshot manifest list.
    pub manifest_files_size_bytes: u64,
    /// Size of the current table metadata JSON file.
    pub metadata_json_size_bytes: u64,
}

/// Data file size statistics for the current Iceberg table snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentDataFileSizeStats {
    /// Snapshot these statistics were computed from.
    pub snapshot_id: i64,
    /// Snapshot commit/update timestamp.
    pub snapshot_updated_at: OffsetDateTime,
    /// Current snapshot manifest list location.
    pub manifest_list_path: String,
    /// Target data file size from table properties, or Iceberg's default.
    pub target_file_size_bytes: u64,
    /// Number of live data files.
    pub data_file_count: u64,
    /// Average live data file size, rounded to the nearest byte.
    pub avg_data_file_size_bytes: Option<u64>,
    /// Distribution of live data file sizes.
    pub distribution: Option<DataFileSizeDistribution>,
    /// Data file size bucket summaries.
    pub buckets: Vec<DataFileSizeBucketStats>,
}

/// Data file size bucket summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileSizeBucketStats {
    /// Human-readable bucket range label.
    pub label: String,
    /// Number of live data files in this bucket.
    pub file_count: u64,
    /// Total bytes across live data files in this bucket.
    pub total_size_bytes: u64,
    /// File-count share stored as thousandths of one percent.
    pub file_percentage_millis: u64,
    /// Byte-size share stored as thousandths of one percent.
    pub size_percentage_millis: u64,
}

/// Percentile distribution for a set of data file sizes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileSizeDistribution {
    /// Smallest data file size.
    pub min: u64,
    /// 25th percentile data file size.
    pub p25: u64,
    /// 50th percentile data file size.
    pub p50: u64,
    /// 75th percentile data file size.
    pub p75: u64,
    /// 95th percentile data file size.
    pub p95: u64,
    /// Largest data file size.
    pub max: u64,
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
            s3_credentials: None,
        })
    }

    /// Use AWS SDK profile credentials for S3 table metadata and data files.
    #[must_use]
    pub fn with_s3_profile(mut self, profile: Option<String>) -> Self {
        self.s3_credentials = profile.map(S3CredentialSource::AwsProfile);
        self
    }

    /// Use `aws-vault export` credentials for S3 table metadata and data files.
    #[must_use]
    pub fn with_aws_vault_profile(mut self, profile: Option<String>) -> Self {
        if let Some(profile) = profile {
            self.s3_credentials = Some(S3CredentialSource::AwsVault(profile));
        }

        self
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
    let table = load_table(config, table_ident).await?;

    Ok(table.metadata().current_schema().clone())
}

/// Load current snapshot statistics for a table through an Iceberg REST catalog.
///
/// # Errors
///
/// Returns an Iceberg-backed error when catalog, metadata, manifest list, or
/// manifest reads fail. Returns [`BergError::NoCurrentSnapshot`] when the table
/// has no current snapshot.
pub async fn load_current_table_stats(
    config: &RestCatalogConfig,
    table_ident: &TableIdent,
) -> Result<CurrentTableStats> {
    let table = load_table(config, table_ident).await?;
    let metadata = table.metadata();
    let snapshot = metadata
        .current_snapshot()
        .ok_or_else(|| BergError::NoCurrentSnapshot {
            table: table_ident.to_string(),
        })?;
    let manifest_list_path = snapshot.manifest_list().to_string();
    let metadata_json_path = table.metadata_location_result()?.to_string();
    let manifest_list_size_bytes = table
        .file_io()
        .new_input(&manifest_list_path)?
        .metadata()
        .await?
        .size;
    let metadata_json_size_bytes = table
        .file_io()
        .new_input(&metadata_json_path)?
        .metadata()
        .await?
        .size;
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await?;
    let snapshot_updated_at = snapshot_updated_at(snapshot.snapshot_id(), snapshot.timestamp_ms())?;
    let mut stats = CurrentTableStats {
        snapshot_id: snapshot.snapshot_id(),
        snapshot_updated_at,
        metadata_json_compressed: is_compressed_metadata_json(&metadata_json_path),
        metadata_json_path,
        manifest_list_path,
        total_table_file_size_bytes: 0,
        data_file_count: 0,
        position_delete_file_count: 0,
        position_delete_record_count: 0,
        equality_delete_file_count: 0,
        equality_delete_record_count: 0,
        record_count: 0,
        manifest_file_count: manifest_list.entries().len() as u64,
        manifest_list_size_bytes,
        manifest_files_size_bytes: 0,
        metadata_json_size_bytes,
    };

    for manifest_file in manifest_list.entries() {
        stats.manifest_files_size_bytes +=
            u64::try_from(manifest_file.manifest_length).map_err(|_| {
                BergError::InvalidManifestLength {
                    path: manifest_file.manifest_path.clone(),
                    length: manifest_file.manifest_length,
                }
            })?;

        if manifest_file.content == ManifestContentType::Data
            && !manifest_file.has_added_files()
            && !manifest_file.has_existing_files()
        {
            continue;
        }

        if manifest_file.content == ManifestContentType::Deletes
            && !manifest_file.has_added_files()
            && !manifest_file.has_existing_files()
        {
            continue;
        }

        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            stats.total_table_file_size_bytes += entry.file_size_in_bytes();

            match entry.content_type() {
                DataContentType::Data => {
                    stats.data_file_count += 1;
                    stats.record_count += entry.record_count();
                }
                DataContentType::PositionDeletes => {
                    stats.position_delete_file_count += 1;
                    stats.position_delete_record_count += entry.record_count();
                }
                DataContentType::EqualityDeletes => {
                    stats.equality_delete_file_count += 1;
                    stats.equality_delete_record_count += entry.record_count();
                }
            }
        }
    }

    Ok(stats)
}

/// Load current snapshot data file size statistics for a table through an Iceberg REST catalog.
///
/// # Errors
///
/// Returns an Iceberg-backed error when catalog, metadata, or manifest reads
/// fail. Returns [`BergError::NoCurrentSnapshot`] when the table has no current
/// snapshot.
pub async fn load_current_data_file_size_stats(
    config: &RestCatalogConfig,
    table_ident: &TableIdent,
) -> Result<CurrentDataFileSizeStats> {
    let table = load_table(config, table_ident).await?;
    let metadata = table.metadata();
    let snapshot = metadata
        .current_snapshot()
        .ok_or_else(|| BergError::NoCurrentSnapshot {
            table: table_ident.to_string(),
        })?;
    let manifest_list_path = snapshot.manifest_list().to_string();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await?;
    let snapshot_updated_at = snapshot_updated_at(snapshot.snapshot_id(), snapshot.timestamp_ms())?;
    let target_file_size_bytes = target_file_size_bytes(metadata.properties());
    let mut data_file_sizes = Vec::new();

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }

        if !manifest_file.has_added_files() && !manifest_file.has_existing_files() {
            continue;
        }

        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        data_file_sizes.extend(
            manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive() && entry.content_type() == DataContentType::Data)
                .map(|entry| entry.file_size_in_bytes()),
        );
    }

    data_file_sizes.sort_unstable();
    let data_file_count = data_file_sizes.len() as u64;
    let avg_data_file_size_bytes = rounded_average(&data_file_sizes);
    let distribution = data_file_size_distribution(&data_file_sizes);
    let buckets = data_file_size_buckets(&data_file_sizes, target_file_size_bytes);

    Ok(CurrentDataFileSizeStats {
        snapshot_id: snapshot.snapshot_id(),
        snapshot_updated_at,
        manifest_list_path,
        target_file_size_bytes,
        data_file_count,
        avg_data_file_size_bytes,
        distribution,
        buckets,
    })
}

fn target_file_size_bytes(properties: &HashMap<String, String>) -> u64 {
    properties
        .get(spec::TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(spec::TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT as u64)
}

fn snapshot_updated_at(snapshot_id: i64, timestamp_ms: i64) -> Result<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000).map_err(|_| {
        BergError::InvalidSnapshotTimestamp {
            snapshot_id,
            timestamp_ms,
        }
    })
}

fn rounded_average(values: &[u64]) -> Option<u64> {
    let count = values.len() as u128;
    if count == 0 {
        return None;
    }

    let total = values
        .iter()
        .fold(0_u128, |total, value| total + u128::from(*value));
    let average = (total + count / 2) / count;

    Some(average.min(u128::from(u64::MAX)) as u64)
}

fn data_file_size_distribution(sorted_values: &[u64]) -> Option<DataFileSizeDistribution> {
    Some(DataFileSizeDistribution {
        min: *sorted_values.first()?,
        p25: percentile(sorted_values, 1, 4),
        p50: percentile(sorted_values, 1, 2),
        p75: percentile(sorted_values, 3, 4),
        p95: percentile(sorted_values, 95, 100),
        max: *sorted_values.last()?,
    })
}

fn data_file_size_buckets(
    sorted_values: &[u64],
    target_file_size_bytes: u64,
) -> Vec<DataFileSizeBucketStats> {
    let total_file_count = sorted_values.len() as u64;
    let total_size_bytes = sorted_values
        .iter()
        .fold(0_u64, |total, size| total.saturating_add(*size));
    let bucket_specs = data_file_size_bucket_specs(target_file_size_bytes);
    let mut buckets = bucket_specs
        .iter()
        .map(|spec| DataFileSizeBucketStats {
            label: spec.label.clone(),
            file_count: 0,
            total_size_bytes: 0,
            file_percentage_millis: 0,
            size_percentage_millis: 0,
        })
        .collect::<Vec<_>>();

    for size_bytes in sorted_values {
        let bucket_index = bucket_specs
            .iter()
            .position(|bucket| bucket.contains(*size_bytes))
            .unwrap_or(bucket_specs.len() - 1);
        buckets[bucket_index].file_count += 1;
        buckets[bucket_index].total_size_bytes = buckets[bucket_index]
            .total_size_bytes
            .saturating_add(*size_bytes);
    }

    for bucket in &mut buckets {
        bucket.file_percentage_millis =
            ratio_percentage_millis(bucket.file_count, total_file_count);
        bucket.size_percentage_millis =
            ratio_percentage_millis(bucket.total_size_bytes, total_size_bytes);
    }

    buckets
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DataFileSizeBucketSpec {
    label: String,
    lower_bound_inclusive: u64,
    upper_bound_exclusive: Option<u64>,
}

impl DataFileSizeBucketSpec {
    fn contains(&self, size_bytes: u64) -> bool {
        size_bytes >= self.lower_bound_inclusive
            && self
                .upper_bound_exclusive
                .is_none_or(|upper_bound| size_bytes < upper_bound)
    }
}

fn data_file_size_bucket_specs(target_file_size_bytes: u64) -> Vec<DataFileSizeBucketSpec> {
    const MIB: u64 = 1024 * 1024;

    let target_25 = target_file_size_bytes / 4;
    let target_75 = target_file_size_bytes.saturating_mul(3) / 4;
    let target_125 = target_file_size_bytes.saturating_mul(5) / 4;
    let target_200 = target_file_size_bytes.saturating_mul(2);
    let candidates = [
        ("< 16 MiB".to_string(), 0, Some(16 * MIB)),
        ("16-64 MiB".to_string(), 16 * MIB, Some(64 * MIB)),
        ("64 MiB-25% target".to_string(), 64 * MIB, Some(target_25)),
        ("25-75% target".to_string(), target_25, Some(target_75)),
        ("75-125% target".to_string(), target_75, Some(target_125)),
        ("125-200% target".to_string(), target_125, Some(target_200)),
        ("> 200% target".to_string(), target_200, None),
    ];

    candidates
        .into_iter()
        .filter_map(|(label, lower_bound_inclusive, upper_bound_exclusive)| {
            if upper_bound_exclusive.is_some_and(|upper_bound| upper_bound <= lower_bound_inclusive)
            {
                return None;
            }

            Some(DataFileSizeBucketSpec {
                label,
                lower_bound_inclusive,
                upper_bound_exclusive,
            })
        })
        .collect()
}

fn ratio_percentage_millis(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }

    let numerator = u128::from(numerator);
    let denominator = u128::from(denominator);
    let rounded = (numerator * 100_000 + denominator / 2) / denominator;

    rounded.min(u128::from(u64::MAX)) as u64
}

fn percentile(sorted_values: &[u64], numerator: usize, denominator: usize) -> u64 {
    debug_assert!(!sorted_values.is_empty());
    debug_assert!(denominator > 0);

    let last_index = sorted_values.len() - 1;
    let scaled_index = last_index * numerator;
    let lower_index = scaled_index / denominator;
    let upper_index = lower_index.saturating_add(1).min(last_index);
    let remainder = scaled_index % denominator;
    let lower_value = u128::from(sorted_values[lower_index]);
    let upper_value = u128::from(sorted_values[upper_index]);
    let denominator = denominator as u128;
    let remainder = remainder as u128;
    let interpolated =
        lower_value * denominator + (upper_value - lower_value) * remainder + denominator / 2;

    (interpolated / denominator).min(u128::from(u64::MAX)) as u64
}

fn is_compressed_metadata_json(path: &str) -> bool {
    let lower_path = path.to_ascii_lowercase();
    let has_gzip_extension = std::path::Path::new(&lower_path)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("gz"));

    has_gzip_extension || lower_path.contains(".gz.")
}

async fn load_table(config: &RestCatalogConfig, table_ident: &TableIdent) -> Result<Table> {
    let catalog = load_rest_catalog(config).await?;

    Ok(catalog.load_table(table_ident).await?)
}

async fn load_rest_catalog(config: &RestCatalogConfig) -> Result<RestCatalog> {
    let customized_credential_load =
        config
            .s3_credentials
            .as_ref()
            .map(|credentials| match credentials {
                S3CredentialSource::AwsProfile(profile) => {
                    CustomAwsCredentialLoader::new(Arc::new(AwsProfileCredentialLoader {
                        profile: profile.clone(),
                    }))
                }
                S3CredentialSource::AwsVault(profile) => {
                    CustomAwsCredentialLoader::new(Arc::new(AwsVaultCredentialLoader {
                        profile: profile.clone(),
                    }))
                }
            });
    let storage_factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load,
    });

    Ok(RestCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load("berg", config.catalog_properties())
        .await?)
}

#[async_trait]
impl AwsCredentialLoad for AwsProfileCredentialLoader {
    async fn load_credential(&self, _client: Client) -> anyhow::Result<Option<AwsCredential>> {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .profile_name(&self.profile)
            .load()
            .await;
        let Some(provider) = config.credentials_provider() else {
            return Ok(None);
        };
        let credentials = provider.provide_credentials().await?;

        Ok(Some(AwsCredential {
            access_key_id: credentials.access_key_id().to_string(),
            secret_access_key: credentials.secret_access_key().to_string(),
            session_token: credentials.session_token().map(ToString::to_string),
            expires_in: None,
        }))
    }
}

#[async_trait]
impl AwsCredentialLoad for AwsVaultCredentialLoader {
    async fn load_credential(&self, _client: Client) -> anyhow::Result<Option<AwsCredential>> {
        let output = Command::new("aws-vault")
            .args(["export", "--format=env", &self.profile])
            .stdin(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()?;

        if !output.status.success() {
            anyhow::bail!("aws-vault export failed with status {}", output.status);
        }

        Ok(Some(credential_from_env_output(&output.stdout)?))
    }
}

fn credential_from_env_output(output: &[u8]) -> anyhow::Result<AwsCredential> {
    let output = std::str::from_utf8(output)?;
    let mut access_key_id = None;
    let mut secret_access_key = None;
    let mut session_token = None;

    for line in output.lines() {
        let line = line.trim().strip_prefix("export ").unwrap_or(line.trim());
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = unquote_env_value(value.trim()).to_string();

        match key.trim() {
            "AWS_ACCESS_KEY_ID" => access_key_id = Some(value),
            "AWS_SECRET_ACCESS_KEY" => secret_access_key = Some(value),
            "AWS_SESSION_TOKEN" | "AWS_SECURITY_TOKEN" => session_token = Some(value),
            _ => {}
        }
    }

    let access_key_id = access_key_id
        .ok_or_else(|| anyhow::anyhow!("aws-vault export did not return AWS_ACCESS_KEY_ID"))?;
    let secret_access_key = secret_access_key
        .ok_or_else(|| anyhow::anyhow!("aws-vault export did not return AWS_SECRET_ACCESS_KEY"))?;

    Ok(AwsCredential {
        access_key_id,
        secret_access_key,
        session_token,
        expires_in: None,
    })
}

fn unquote_env_value(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
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

    use super::{
        DataFileSizeDistribution, QualifiedTableIdent, RestCatalogConfig,
        credential_from_env_output, data_file_size_buckets, data_file_size_distribution,
        parse_catalog_property, rounded_average,
    };

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

    #[test]
    fn parses_aws_vault_env_output() {
        let credential = credential_from_env_output(
            br#"AWS_ACCESS_KEY_ID=access
AWS_SECRET_ACCESS_KEY="secret"
AWS_SESSION_TOKEN='token'
"#,
        )
        .expect("valid aws-vault export output");

        assert_eq!("access", credential.access_key_id);
        assert_eq!("secret", credential.secret_access_key);
        assert_eq!(Some("token".to_string()), credential.session_token);
    }

    #[test]
    fn computes_data_file_size_distribution() {
        let sizes = [100, 200, 300, 400, 500];

        let distribution = data_file_size_distribution(&sizes).expect("distribution");

        assert_eq!(
            DataFileSizeDistribution {
                min: 100,
                p25: 200,
                p50: 300,
                p75: 400,
                p95: 480,
                max: 500,
            },
            distribution
        );
    }

    #[test]
    fn computes_rounded_data_file_size_average() {
        assert_eq!(Some(151), rounded_average(&[100, 201]));
        assert_eq!(None, rounded_average(&[]));
    }

    #[test]
    fn computes_data_file_size_buckets() {
        let mib = 1024 * 1024;
        let sizes = [
            8 * mib,
            32 * mib,
            80 * mib,
            400 * mib,
            700 * mib,
            1100 * mib,
        ];

        let buckets = data_file_size_buckets(&sizes, 512 * mib);

        assert_eq!("< 16 MiB", buckets[0].label);
        assert_eq!(1, buckets[0].file_count);
        assert_eq!(8 * mib, buckets[0].total_size_bytes);
        assert_eq!(16_667, buckets[0].file_percentage_millis);
        assert_eq!(345, buckets[0].size_percentage_millis);
        assert_eq!("75-125% target", buckets[4].label);
        assert_eq!(1, buckets[4].file_count);
        assert_eq!(400 * mib, buckets[4].total_size_bytes);
        assert_eq!("125-200% target", buckets[5].label);
        assert_eq!(1, buckets[5].file_count);
        assert_eq!(700 * mib, buckets[5].total_size_bytes);
        assert_eq!("> 200% target", buckets[6].label);
        assert_eq!(1, buckets[6].file_count);
        assert_eq!(1100 * mib, buckets[6].total_size_bytes);
    }
}
