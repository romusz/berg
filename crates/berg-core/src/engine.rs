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
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;

use arrow_array::{Array, LargeStringArray, StringArray, StringViewArray};
use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use flate2::write::GzDecoder;
use iceberg::io::{InputFile, StorageFactory};
use iceberg::spec::{DataContentType, DataFileFormat, ManifestContentType};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::{
    AwsCredential, AwsCredentialLoad, CustomAwsCredentialLoader, OpenDalStorageFactory,
};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use reqwest::Client;
use time::OffsetDateTime;

use crate::{BergError, Result, spec};

// Keep compressed metadata accounting memory-bounded: range-read and decode in chunks.
const METADATA_JSON_READ_CHUNK_SIZE_BYTES: u64 = 64 * 1024;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetadataJsonDecodedSize {
    stored_file_compressed: bool,
    decoded_size_bytes: u64,
}

#[derive(Debug, Default)]
struct CountingWriter {
    bytes_written: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum S3CredentialSource {
    AwsProfile(String),
    AwsVault(String),
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = u64::try_from(buf.len())
            .map_err(|_| io::Error::other("buffer length does not fit in u64"))?;
        self.bytes_written = self
            .bytes_written
            .checked_add(len)
            .ok_or_else(|| io::Error::other("byte count overflow"))?;

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Statistics for the current Iceberg table snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTableStats {
    /// Snapshot these statistics were computed from.
    pub snapshot_id: i64,
    /// Snapshot commit/update timestamp.
    pub snapshot_updated_at: OffsetDateTime,
    /// Number of snapshots retained in the current table metadata.
    pub retained_snapshot_count: usize,
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
    /// Stored size of the current table metadata JSON file.
    pub metadata_json_size_bytes: u64,
    /// Uncompressed size of the current table metadata JSON content.
    pub metadata_json_uncompressed_size_bytes: u64,
}

/// Properties and metadata identifiers from the current Iceberg table metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTableProperties {
    /// Current table metadata JSON location.
    pub metadata_json_path: String,
    /// Table metadata last update timestamp.
    pub last_updated_at: OffsetDateTime,
    /// Iceberg table format version.
    pub format_version: spec::FormatVersion,
    /// Table UUID.
    pub table_uuid: String,
    /// Table base location.
    pub location: String,
    /// Current snapshot ID, when the table has a current snapshot.
    pub current_snapshot_id: Option<i64>,
    /// Current schema ID.
    pub current_schema_id: i32,
    /// Default partition spec ID.
    pub default_partition_spec_id: i32,
    /// Default sort order ID.
    pub default_sort_order_id: i64,
    /// Table properties sorted by key for stable output.
    pub properties: Vec<TablePropertyEntry>,
}

/// One table property key/value pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePropertyEntry {
    /// Property key.
    pub key: String,
    /// Property value exactly as stored in table metadata.
    pub value: String,
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
    /// Total bytes across live data files.
    pub total_data_file_size_bytes: u64,
    /// Number of live data files.
    pub data_file_count: u64,
    /// Average live data file size, rounded to the nearest byte.
    pub avg_data_file_size_bytes: Option<u64>,
    /// Distribution of live data file sizes.
    pub distribution: Option<DataFileSizeDistribution>,
    /// Data file size bucket summaries.
    pub buckets: Vec<DataFileSizeBucketStats>,
}

/// Metadata-derived max for one column in the current Iceberg table snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTableMax {
    /// Snapshot analyzed for this result.
    pub snapshot_id: i64,
    /// Snapshot commit/update timestamp.
    pub snapshot_updated_at: OffsetDateTime,
    /// Current snapshot manifest list location.
    pub manifest_list_path: String,
    /// User-requested column path.
    pub column: String,
    /// Resolved current-schema field path.
    pub field_path: String,
    /// Resolved current-schema field ID.
    pub field_id: i32,
    /// Current field type.
    pub field_type: String,
    /// Unsupported target reason. When present, no max result is reported.
    pub unsupported_reason: Option<String>,
    /// Metadata-derived max, rendered for display.
    pub metadata_max: Option<String>,
    /// Confidence of `metadata_max` for the current snapshot.
    pub max_confidence: MaxConfidence,
    /// Reasons supporting the confidence state.
    pub max_confidence_reasons: Vec<String>,
    /// Precision of the displayed metadata max.
    pub max_precision: BoundPrecision,
    /// Number of live data file metadata entries examined.
    pub data_file_metadata_entries_scanned: u64,
    /// Zero-record live data file metadata entries ignored for max coverage.
    pub zero_record_data_file_metadata_entries: u64,
    /// Non-empty data files whose manifest schema did not contain the current field ID.
    pub data_files_field_absent: u64,
    /// Field-absent data files resolved with the current field initial-default.
    pub data_files_using_initial_default: u64,
    /// Data files proven to have only null/NaN values for the requested field.
    pub data_files_with_no_non_null_values: u64,
    /// Data files whose NaN counts report one or more NaN values.
    pub data_files_with_nan_values: u64,
    /// Data files that may contain values but did not have an upper bound.
    pub data_files_without_upper_bound: u64,
    /// Float/double upper bounds that decoded as NaN.
    pub nan_upper_bounds: u64,
    /// Upper-bound decode or compatibility failures attributable to the max side.
    pub upper_bound_decode_failures: u64,
    /// Manifest read/decode failures encountered after the current snapshot was known.
    pub manifest_decode_failures: u64,
    /// Live non-empty equality delete file metadata entries.
    pub equality_delete_files: u64,
    /// Live zero-record delete file metadata entries.
    pub zero_record_delete_files: u64,
    /// Max candidate files that may be affected by applicable equality deletes.
    pub max_candidate_files_with_applicable_equality_deletes: usize,
    /// Total max candidate file count.
    pub max_candidate_file_count: usize,
    /// Smallest sequence number among max candidate data files, when available.
    pub max_candidate_data_sequence_number_min: Option<i64>,
    /// Largest sequence number among max candidate data files, when available.
    pub max_candidate_data_sequence_number_max: Option<i64>,
    /// Max candidate data files without inherited sequence numbers.
    pub max_candidate_files_without_sequence_number: usize,
    /// Equality-delete impact on max confidence.
    pub max_equality_delete_impact: DeleteImpact,
    /// Live non-empty position delete file metadata entries.
    pub position_delete_files: u64,
    /// Smallest sequence number among live non-empty position delete files, when available.
    pub position_delete_sequence_number_min: Option<i64>,
    /// Largest sequence number among live non-empty position delete files, when available.
    pub position_delete_sequence_number_max: Option<i64>,
    /// Live non-empty position delete files without inherited sequence numbers.
    pub position_delete_files_without_sequence_number: u64,
    /// Position delete files with `referenced_data_file` metadata.
    pub position_delete_files_with_referenced_data_file: u64,
    /// Position delete files pruned from max candidates by delete/data sequence numbers.
    pub position_delete_files_not_applicable_by_sequence: u64,
    /// Position delete files pruned from max candidates by partition metadata.
    pub position_delete_files_not_applicable_by_partition: u64,
    /// Position delete files pruned from max candidates by `referenced_data_file` mismatch.
    pub position_delete_files_not_applicable_by_referenced_data_file: u64,
    /// Position delete files whose max-candidate applicability could not be determined.
    pub position_delete_files_with_unknown_applicability: u64,
    /// Position delete files that may apply to at least one max candidate.
    pub position_delete_files_applicable_to_max_candidates: u64,
    /// Unreferenced Parquet position delete files requiring `file_path` reads.
    pub position_delete_files_requiring_file_path_reads: u64,
    /// Unreferenced Parquet position delete files whose `file_path` column was read.
    pub position_delete_files_read_for_file_path: u64,
    /// Applicable unreferenced non-Parquet position delete files.
    pub unsupported_position_delete_files: u64,
    /// Max candidate files touched by applicable position deletes.
    pub max_candidate_files_touched_by_position_deletes: usize,
    /// Position-delete impact on max confidence.
    pub max_position_delete_impact: DeleteImpact,
    /// Completeness of max-side position delete analysis.
    pub max_position_delete_analysis: DeleteAnalysisCompleteness,
    /// Metadata and delete read completeness for the reported result.
    pub read_completeness: ReadCompleteness,
    /// Type compatibility across compared upper bounds and synthetic defaults.
    pub type_compatibility: TypeCompatibility,
    /// Detail for promoted or incompatible type compatibility states.
    pub type_compatibility_detail: Option<String>,
    /// Source of current/default metrics mode evidence.
    pub metrics_mode_evidence: String,
    /// Current/default metrics mode used to reason about precision.
    pub current_metrics_mode: String,
    /// Additional precision explanation.
    pub precision_detail: Option<String>,
    /// Caveats that qualify the max result.
    pub caveats: Vec<String>,
}

/// Confidence of a metadata-derived bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxConfidence {
    /// Complete metadata supports the result within Berg's metadata-only scope.
    High,
    /// A value was computed but relevant upper bounds are missing.
    Partial,
    /// A value was computed but deletes may have removed extrema rows.
    Lowered,
    /// Berg cannot safely determine current-table representativeness.
    Unknown,
    /// No metadata-derived value could be computed.
    Unavailable,
}

/// Precision of a displayed metadata bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundPrecision {
    /// Metrics truncation does not apply, or exactness is otherwise supported.
    Exact,
    /// Current config says full metrics, but historical per-file mode is unavailable.
    ProbablyExact,
    /// Truncation may apply to the displayed bound.
    PossiblyTruncated,
    /// Berg cannot reason safely about precision.
    Unknown,
    /// No bound was computed.
    Unavailable,
}

/// Delete impact for max candidate files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteImpact {
    /// No candidate is affected, or at least one candidate is proven unaffected.
    Unaffected,
    /// Some candidates are affected and at least one is proven unaffected.
    PartiallyAffected,
    /// Every candidate may be affected by equality deletes.
    AllCandidatesPossiblyAffected,
    /// Every candidate is touched by position deletes.
    AllCandidatesTouched,
    /// Applicability or touch status could not be determined.
    Unknown,
    /// No candidate files were available for this analysis.
    NotApplicable,
}

/// Completeness of position-delete candidate analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteAnalysisCompleteness {
    /// All required metadata/delete information was analyzed.
    Complete,
    /// Required delete information was missing or unsupported.
    Incomplete,
    /// Analysis was unnecessary because there were no max candidates.
    NotApplicable,
}

/// Overall read completeness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadCompleteness {
    /// All metadata reads required for the reported state completed.
    Complete,
    /// One or more required metadata reads failed or were unsupported.
    Incomplete,
}

/// Type compatibility across upper-bound values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeCompatibility {
    /// All compared values used the current primitive type exactly.
    Exact,
    /// Values were promoted by a safe V1 rule.
    SafelyPromoted,
    /// Values were incompatible with the current primitive type.
    Incompatible,
    /// Compatibility could not be determined.
    Unknown,
}

/// Manifest files in the current Iceberg table snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentManifestFileList {
    /// Snapshot this list was read from.
    pub snapshot_id: i64,
    /// Snapshot commit/update timestamp.
    pub snapshot_updated_at: OffsetDateTime,
    /// Current snapshot manifest list location.
    pub manifest_list_path: String,
    /// Manifest files in current snapshot manifest list order.
    pub files: Vec<ManifestFileListEntry>,
}

/// One entry from the current snapshot manifest list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestFileListEntry {
    /// Short generated ID for selecting this manifest file.
    pub id: String,
    /// Manifest file basename.
    pub name: String,
    /// Full manifest file path.
    pub path: String,
    /// Manifest content type.
    pub content: ManifestContentType,
    /// Manifest file length in bytes.
    pub size_bytes: u64,
    /// Partition spec ID used to write files in this manifest.
    pub partition_spec_id: i32,
    /// Number of added files when reported.
    pub added_files_count: Option<u32>,
    /// Number of existing files when reported.
    pub existing_files_count: Option<u32>,
    /// Number of deleted files when reported.
    pub deleted_files_count: Option<u32>,
}

/// One selected manifest file from the current Iceberg table snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentManifestFileDetail {
    /// Snapshot this detail was read from.
    pub snapshot_id: i64,
    /// Snapshot commit/update timestamp.
    pub snapshot_updated_at: OffsetDateTime,
    /// Current snapshot manifest list location.
    pub manifest_list_path: String,
    /// Number of manifest files in the current snapshot manifest list.
    pub manifest_file_count: u64,
    /// Short generated ID used to select this manifest file.
    pub manifest_file_id: String,
    /// Selected manifest file.
    pub manifest_file: spec::ManifestFile,
    /// Metadata fields available for each partition field summary in the selected manifest file.
    pub partition_metadata: Vec<ManifestPartitionMetadataSummary>,
    /// Column metric fields available across live entries in the selected manifest file.
    pub column_metadata: Vec<ManifestColumnMetadataSummary>,
}

/// Metadata entries available for one manifest partition field summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestPartitionMetadataSummary {
    /// Partition field name from the manifest file's partition spec, or a synthetic placeholder.
    pub field_name: String,
    /// Partition field ID from the manifest file's partition spec when known.
    pub field_id: Option<i32>,
    /// Whether the optional `contains_nan` metadata field is present.
    pub has_contains_nan: bool,
    /// Whether a lower bound exists. The bound value itself is intentionally not exposed here.
    pub has_lower_bound: bool,
    /// Whether an upper bound exists. The bound value itself is intentionally not exposed here.
    pub has_upper_bound: bool,
}

/// Metadata entries available for one table column in a selected manifest file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestColumnMetadataSummary {
    /// Column name from the table schema, or a synthetic placeholder.
    pub column_name: String,
    /// Iceberg field ID for the column.
    pub field_id: i32,
    /// Column metadata field names present for this column. Bound values themselves are intentionally not exposed here.
    pub metadata_fields: Vec<String>,
}

/// Current snapshot partition layout and per-partition data file statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTablePartitions {
    /// Snapshot these statistics were computed from.
    pub snapshot_id: i64,
    /// Snapshot commit/update timestamp.
    pub snapshot_updated_at: OffsetDateTime,
    /// Current table metadata JSON location.
    pub metadata_json_path: String,
    /// Current snapshot manifest list location.
    pub manifest_list_path: String,
    /// Current table schema used to describe the default partition spec.
    pub current_schema: spec::SchemaRef,
    /// Default partition spec for new writes.
    pub partition_spec: spec::PartitionSpecRef,
    /// Target data file size from table properties, or Iceberg's default.
    pub target_file_size_bytes: u64,
    /// Total bytes across live data files.
    pub total_data_file_size_bytes: u64,
    /// Number of live data files.
    pub data_file_count: u64,
    /// Data file size bucket labels, matching table data file size statistics.
    pub bucket_labels: Vec<String>,
    /// Live data files grouped by partition spec ID and partition value.
    pub partitions: Vec<CurrentTablePartitionStats>,
}

/// Data file statistics for one current snapshot partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTablePartitionStats {
    /// Partition spec ID used to write files in this partition.
    pub partition_spec_id: i32,
    /// Human-readable partition path, or `unpartitioned` for unpartitioned specs.
    pub partition: String,
    /// Number of live data files in this partition.
    pub file_count: u64,
    /// Total bytes across live data files in this partition.
    pub total_size_bytes: u64,
    /// Data file size bucket summaries for this partition.
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

/// Load properties from the current table metadata through an Iceberg REST catalog.
///
/// # Errors
///
/// Returns an Iceberg-backed error when the catalog cannot be constructed,
/// contacted, or cannot load the requested table. Returns
/// [`BergError::InvalidTableMetadataTimestamp`] when the table metadata update
/// timestamp cannot be represented.
pub async fn load_current_table_properties(
    config: &RestCatalogConfig,
    table_ident: &TableIdent,
) -> Result<CurrentTableProperties> {
    let table = load_table(config, table_ident).await?;
    let metadata = table.metadata();
    let metadata_json_path = table.metadata_location_result()?.to_string();
    let last_updated_at = table_metadata_updated_at(metadata.last_updated_ms())?;
    let mut properties = metadata
        .properties()
        .iter()
        .map(|(key, value)| TablePropertyEntry {
            key: key.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();

    properties.sort_unstable_by(|left, right| left.key.cmp(&right.key));

    Ok(CurrentTableProperties {
        metadata_json_path,
        last_updated_at,
        format_version: metadata.format_version(),
        table_uuid: metadata.uuid().to_string(),
        location: metadata.location().to_string(),
        current_snapshot_id: metadata.current_snapshot_id(),
        current_schema_id: metadata.current_schema_id(),
        default_partition_spec_id: metadata.default_partition_spec_id(),
        default_sort_order_id: metadata.default_sort_order_id(),
        properties,
    })
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
    let metadata_json_input = table.file_io().new_input(&metadata_json_path)?;
    let metadata_json_size_bytes = metadata_json_input.metadata().await?.size;
    let metadata_json_compressed = is_compressed_metadata_json(&metadata_json_path);
    let metadata_json_size = metadata_json_size(
        &metadata_json_input,
        metadata_json_size_bytes,
        metadata_json_compressed,
    )
    .await?;
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await?;
    let manifest_files_size_bytes = manifest_files_size_bytes(manifest_list.entries())?;
    let snapshot_updated_at = snapshot_updated_at(snapshot.snapshot_id(), snapshot.timestamp_ms())?;
    let mut stats = CurrentTableStats {
        snapshot_id: snapshot.snapshot_id(),
        snapshot_updated_at,
        retained_snapshot_count: metadata.snapshots().len(),
        metadata_json_compressed: metadata_json_size.stored_file_compressed,
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
        manifest_files_size_bytes,
        metadata_json_size_bytes,
        metadata_json_uncompressed_size_bytes: metadata_json_size.decoded_size_bytes,
    };

    visit_live_manifest_files(
        &table,
        &manifest_list,
        |_| true,
        |live_manifest| {
            for entry in live_manifest_entries(&live_manifest.manifest) {
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

            Ok(())
        },
    )
    .await?;

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

    visit_live_manifest_files(
        &table,
        &manifest_list,
        |content| content == ManifestContentType::Data,
        |live_manifest| {
            data_file_sizes.extend(
                live_data_file_entries(&live_manifest.manifest)
                    .map(spec::ManifestEntry::file_size_in_bytes),
            );

            Ok(())
        },
    )
    .await?;

    data_file_sizes.sort_unstable();
    let data_file_count = data_file_sizes.len() as u64;
    let total_data_file_size_bytes = total_size_bytes(&data_file_sizes);
    let avg_data_file_size_bytes = rounded_average(&data_file_sizes);
    let distribution = data_file_size_distribution(&data_file_sizes);
    let buckets = data_file_size_buckets(&data_file_sizes, target_file_size_bytes);

    Ok(CurrentDataFileSizeStats {
        snapshot_id: snapshot.snapshot_id(),
        snapshot_updated_at,
        manifest_list_path,
        target_file_size_bytes,
        total_data_file_size_bytes,
        data_file_count,
        avg_data_file_size_bytes,
        distribution,
        buckets,
    })
}

/// Load a metadata-derived max for one current-schema column in the current table snapshot.
///
/// # Errors
///
/// Returns an Iceberg-backed error when catalog, table metadata, or the manifest
/// list cannot be loaded. Returns [`BergError::NoCurrentSnapshot`] when the
/// table has no current snapshot, and [`BergError::UnknownColumnPath`] when the
/// requested column path is not present in the current schema.
pub async fn load_current_table_max(
    config: &RestCatalogConfig,
    table_ident: &TableIdent,
    column_path: &str,
) -> Result<CurrentTableMax> {
    let table = load_table(config, table_ident).await?;
    let metadata = table.metadata();
    let snapshot = metadata
        .current_snapshot()
        .ok_or_else(|| BergError::NoCurrentSnapshot {
            table: table_ident.to_string(),
        })?;
    let manifest_list_path = snapshot.manifest_list().to_string();
    let snapshot_updated_at = snapshot_updated_at(snapshot.snapshot_id(), snapshot.timestamp_ms())?;
    let resolution = resolve_current_column_path(metadata.current_schema(), column_path)?;
    let metrics_mode = current_metrics_mode(metadata.properties(), &resolution.field_path);
    let mut analysis = CurrentTableMaxAnalysis::new(
        snapshot.snapshot_id(),
        snapshot_updated_at,
        manifest_list_path.clone(),
        column_path.to_string(),
        resolution,
        metrics_mode,
    );

    if analysis.unsupported_reason.is_some() {
        analysis.finish_without_manifest_analysis();
        return Ok(analysis.into_result());
    }

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await?;
    let mut delete_files = Vec::new();

    for manifest_file in manifest_list.entries() {
        if !manifest_file_has_live_files(manifest_file) {
            continue;
        }

        match manifest_file.load_manifest(table.file_io()).await {
            Ok(manifest) => match manifest_file.content {
                ManifestContentType::Data => analysis.analyze_data_manifest(&manifest),
                ManifestContentType::Deletes => collect_delete_files(
                    &manifest,
                    manifest_file.partition_spec_id,
                    &mut analysis,
                    &mut delete_files,
                ),
            },
            Err(_) => analysis.record_manifest_decode_failure(),
        }
    }

    analysis.analyze_delete_files(&table, &delete_files).await;
    analysis.finish();

    Ok(analysis.into_result())
}

/// Load current snapshot manifest files for a table through an Iceberg REST catalog.
///
/// # Errors
///
/// Returns an Iceberg-backed error when catalog, metadata, or manifest list reads
/// fail. Returns [`BergError::NoCurrentSnapshot`] when the table has no current
/// snapshot.
pub async fn load_current_manifest_file_list(
    config: &RestCatalogConfig,
    table_ident: &TableIdent,
) -> Result<CurrentManifestFileList> {
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

    Ok(CurrentManifestFileList {
        snapshot_id: snapshot.snapshot_id(),
        snapshot_updated_at,
        manifest_list_path,
        files: manifest_file_list_entries(manifest_list.entries())?,
    })
}

/// Load one current snapshot manifest file for a table through an Iceberg REST catalog.
///
/// # Errors
///
/// Returns an Iceberg-backed error when catalog, metadata, manifest list, or
/// manifest reads fail. Returns [`BergError::NoCurrentSnapshot`] when the table
/// has no current snapshot. Returns [`BergError::UnknownManifestFileId`] when
/// `manifest_file_id` is not in the current manifest list.
pub async fn load_current_manifest_file_detail(
    config: &RestCatalogConfig,
    table_ident: &TableIdent,
    manifest_file_id: &str,
) -> Result<CurrentManifestFileDetail> {
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
    let (manifest_file_id, manifest_file) =
        find_manifest_file_by_id(manifest_list.entries(), manifest_file_id)
            .map(|(id, manifest_file)| (id, manifest_file.clone()))
            .ok_or_else(|| BergError::UnknownManifestFileId {
                id: manifest_file_id.to_string(),
                available: manifest_file_ids(manifest_list.entries()),
            })?;
    let partition_spec = metadata
        .partition_spec_by_id(manifest_file.partition_spec_id)
        .map(std::convert::AsRef::as_ref);
    let partition_metadata = manifest_partition_metadata(&manifest_file, partition_spec);
    let manifest = manifest_file.load_manifest(table.file_io()).await?;
    let column_metadata = manifest_column_metadata(&manifest, metadata.current_schema());

    Ok(CurrentManifestFileDetail {
        snapshot_id: snapshot.snapshot_id(),
        snapshot_updated_at,
        manifest_list_path,
        manifest_file_count: manifest_list.entries().len() as u64,
        manifest_file_id,
        manifest_file,
        partition_metadata,
        column_metadata,
    })
}

/// Load the current partition spec and current snapshot per-partition file statistics.
///
/// # Errors
///
/// Returns an Iceberg-backed error when catalog, metadata, or manifest reads
/// fail. Returns [`BergError::NoCurrentSnapshot`] when the table has no current
/// snapshot.
pub async fn load_current_table_partitions(
    config: &RestCatalogConfig,
    table_ident: &TableIdent,
) -> Result<CurrentTablePartitions> {
    let table = load_table(config, table_ident).await?;
    let metadata = table.metadata();
    let snapshot = metadata
        .current_snapshot()
        .ok_or_else(|| BergError::NoCurrentSnapshot {
            table: table_ident.to_string(),
        })?;
    let metadata_json_path = table.metadata_location_result()?.to_string();
    let manifest_list_path = snapshot.manifest_list().to_string();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await?;
    let snapshot_updated_at = snapshot_updated_at(snapshot.snapshot_id(), snapshot.timestamp_ms())?;
    let current_schema = metadata.current_schema().clone();
    let partition_spec = metadata.default_partition_spec().clone();
    let target_file_size_bytes = target_file_size_bytes(metadata.properties());
    let bucket_labels = data_file_size_bucket_specs(target_file_size_bytes)
        .into_iter()
        .map(|bucket| bucket.label)
        .collect::<Vec<_>>();
    let mut partition_accumulators = BTreeMap::<(i32, String), PartitionAccumulator>::new();
    let mut data_file_count = 0_u64;
    let mut total_data_file_size_bytes = 0_u64;

    visit_live_manifest_files(
        &table,
        &manifest_list,
        |content| content == ManifestContentType::Data,
        |live_manifest| {
            let manifest_spec = live_manifest.manifest.metadata().partition_spec();
            let manifest_schema = live_manifest.manifest.metadata().schema();
            let partition_type = manifest_spec.partition_type(manifest_schema)?;

            for entry in live_data_file_entries(&live_manifest.manifest) {
                let file_size_bytes = entry.file_size_in_bytes();
                let partition = partition_path(
                    manifest_spec,
                    &partition_type,
                    entry.data_file().partition(),
                );

                data_file_count += 1;
                total_data_file_size_bytes =
                    total_data_file_size_bytes.saturating_add(file_size_bytes);
                partition_accumulators
                    .entry((live_manifest.partition_spec_id, partition))
                    .or_default()
                    .add_file(file_size_bytes);
            }

            Ok(())
        },
    )
    .await?;

    let partitions =
        partition_stats_from_accumulators(partition_accumulators, target_file_size_bytes);

    Ok(CurrentTablePartitions {
        snapshot_id: snapshot.snapshot_id(),
        snapshot_updated_at,
        metadata_json_path,
        manifest_list_path,
        current_schema,
        partition_spec,
        target_file_size_bytes,
        total_data_file_size_bytes,
        data_file_count,
        bucket_labels,
        partitions,
    })
}

struct LiveManifest {
    partition_spec_id: i32,
    manifest: spec::Manifest,
}

#[derive(Debug, Clone)]
struct CurrentColumnResolution {
    field_path: String,
    field_id: i32,
    field_type: String,
    primitive_type: Option<spec::PrimitiveType>,
    required: bool,
    initial_default: Option<spec::Literal>,
    unsupported_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct CurrentMetricsMode {
    evidence: String,
    value: String,
}

#[derive(Debug, Clone)]
struct CurrentTableMaxAnalysis {
    snapshot_id: i64,
    snapshot_updated_at: OffsetDateTime,
    manifest_list_path: String,
    column: String,
    field_path: String,
    field_id: i32,
    field_type: String,
    primitive_type: Option<spec::PrimitiveType>,
    required: bool,
    initial_default: Option<spec::Literal>,
    unsupported_reason: Option<String>,
    metrics_mode: CurrentMetricsMode,
    metadata_max: Option<BoundValue>,
    max_candidates: Vec<CandidateFile>,
    data_file_metadata_entries_scanned: u64,
    zero_record_data_file_metadata_entries: u64,
    data_files_field_absent: u64,
    data_files_using_initial_default: u64,
    data_files_with_no_non_null_values: u64,
    data_files_with_nan_values: u64,
    data_files_with_unknown_nan_counts: u64,
    data_files_without_upper_bound: u64,
    nan_upper_bounds: u64,
    upper_bound_decode_failures: u64,
    required_field_absent_without_default: u64,
    manifest_decode_failures: u64,
    equality_delete_files: u64,
    zero_record_delete_files: u64,
    max_candidate_files_with_applicable_equality_deletes: usize,
    max_equality_delete_impact: DeleteImpact,
    position_delete_files: u64,
    position_delete_sequence_number_min: Option<i64>,
    position_delete_sequence_number_max: Option<i64>,
    position_delete_files_without_sequence_number: u64,
    position_delete_files_with_referenced_data_file: u64,
    position_delete_files_not_applicable_by_sequence: u64,
    position_delete_files_not_applicable_by_partition: u64,
    position_delete_files_not_applicable_by_referenced_data_file: u64,
    position_delete_files_with_unknown_applicability: u64,
    position_delete_files_applicable_to_max_candidates: u64,
    position_delete_files_requiring_file_path_reads: u64,
    position_delete_files_read_for_file_path: u64,
    unsupported_position_delete_files: u64,
    position_delete_read_failures: u64,
    max_candidate_files_touched_by_position_deletes: usize,
    max_position_delete_impact: DeleteImpact,
    max_position_delete_analysis: DeleteAnalysisCompleteness,
    read_completeness: ReadCompleteness,
    type_compatibility: TypeCompatibility,
    type_compatibility_detail: Option<String>,
    max_confidence: MaxConfidence,
    max_confidence_reasons: Vec<String>,
    max_precision: BoundPrecision,
    precision_detail: Option<String>,
    caveats: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct BoundValue {
    kind: BoundValueKind,
    display: String,
}

#[derive(Debug, Clone, PartialEq)]
enum BoundValueKind {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    Decimal { unscaled: i128, scale: u32 },
    Date(i32),
    Time(i64),
    Timestamp(i64),
    Timestamptz(i64),
    TimestampNs(i64),
    TimestamptzNs(i64),
    String(String),
    Uuid(u128),
    Fixed(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundCompatibility {
    Exact,
    SafelyPromoted,
    Incompatible,
}

#[derive(Debug, Clone)]
struct CandidateFile {
    path: String,
    sequence_number: Option<i64>,
    partition_spec_id: i32,
    partition: spec::Struct,
}

#[derive(Debug, Clone)]
struct DeleteFileInfo {
    content_type: DataContentType,
    path: String,
    file_format: DataFileFormat,
    sequence_number: Option<i64>,
    partition_spec_id: i32,
    partition_spec_is_unpartitioned: bool,
    partition: spec::Struct,
    referenced_data_file: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct CandidateDeleteStatus {
    equality: DeleteSideStatus,
    position: DeleteSideStatus,
}

#[derive(Debug, Clone, Default)]
struct DeleteSideStatus {
    has_effect: bool,
    has_unknown: bool,
}

impl DeleteSideStatus {
    fn record_effect(&mut self) {
        self.has_effect = true;
    }

    fn record_unknown(&mut self) {
        self.has_unknown = true;
    }

    fn is_unaffected(&self) -> bool {
        !self.has_effect && !self.has_unknown
    }
}

impl CandidateDeleteStatus {
    fn record_equality_may_affect(&mut self) {
        self.equality.record_effect();
    }

    fn record_equality_unknown(&mut self) {
        self.equality.record_unknown();
    }

    fn record_position_touched(&mut self) {
        self.position.record_effect();
    }

    fn record_position_unknown(&mut self) {
        self.position.record_unknown();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteApplicability {
    NotApplicable,
    Applicable,
    Unknown,
}

impl CurrentTableMaxAnalysis {
    fn new(
        snapshot_id: i64,
        snapshot_updated_at: OffsetDateTime,
        manifest_list_path: String,
        column: String,
        resolution: CurrentColumnResolution,
        metrics_mode: CurrentMetricsMode,
    ) -> Self {
        Self {
            snapshot_id,
            snapshot_updated_at,
            manifest_list_path,
            column,
            field_path: resolution.field_path,
            field_id: resolution.field_id,
            field_type: resolution.field_type,
            primitive_type: resolution.primitive_type,
            required: resolution.required,
            initial_default: resolution.initial_default,
            unsupported_reason: resolution.unsupported_reason,
            metrics_mode,
            metadata_max: None,
            max_candidates: Vec::new(),
            data_file_metadata_entries_scanned: 0,
            zero_record_data_file_metadata_entries: 0,
            data_files_field_absent: 0,
            data_files_using_initial_default: 0,
            data_files_with_no_non_null_values: 0,
            data_files_with_nan_values: 0,
            data_files_with_unknown_nan_counts: 0,
            data_files_without_upper_bound: 0,
            nan_upper_bounds: 0,
            upper_bound_decode_failures: 0,
            required_field_absent_without_default: 0,
            manifest_decode_failures: 0,
            equality_delete_files: 0,
            zero_record_delete_files: 0,
            max_candidate_files_with_applicable_equality_deletes: 0,
            max_equality_delete_impact: DeleteImpact::NotApplicable,
            position_delete_files: 0,
            position_delete_sequence_number_min: None,
            position_delete_sequence_number_max: None,
            position_delete_files_without_sequence_number: 0,
            position_delete_files_with_referenced_data_file: 0,
            position_delete_files_not_applicable_by_sequence: 0,
            position_delete_files_not_applicable_by_partition: 0,
            position_delete_files_not_applicable_by_referenced_data_file: 0,
            position_delete_files_with_unknown_applicability: 0,
            position_delete_files_applicable_to_max_candidates: 0,
            position_delete_files_requiring_file_path_reads: 0,
            position_delete_files_read_for_file_path: 0,
            unsupported_position_delete_files: 0,
            position_delete_read_failures: 0,
            max_candidate_files_touched_by_position_deletes: 0,
            max_position_delete_impact: DeleteImpact::NotApplicable,
            max_position_delete_analysis: DeleteAnalysisCompleteness::NotApplicable,
            read_completeness: ReadCompleteness::Complete,
            type_compatibility: TypeCompatibility::Exact,
            type_compatibility_detail: None,
            max_confidence: MaxConfidence::Unavailable,
            max_confidence_reasons: Vec::new(),
            max_precision: BoundPrecision::Unavailable,
            precision_detail: None,
            caveats: Vec::new(),
        }
    }

    fn analyze_data_manifest(&mut self, manifest: &spec::Manifest) {
        let Some(current_type) = self.primitive_type.clone() else {
            return;
        };
        let manifest_schema = manifest.metadata().schema();

        for entry in live_data_file_entries(manifest) {
            self.data_file_metadata_entries_scanned += 1;

            if entry.record_count() == 0 {
                self.zero_record_data_file_metadata_entries += 1;
                continue;
            }

            let candidate_file = CandidateFile {
                path: entry.file_path().to_string(),
                sequence_number: entry.sequence_number(),
                partition_spec_id: manifest.metadata().partition_spec().spec_id(),
                partition: entry.data_file().partition().clone(),
            };

            let Some(manifest_field) = manifest_schema.field_by_id(self.field_id) else {
                self.analyze_field_absent_data_file(&current_type, candidate_file);
                continue;
            };
            let Some(manifest_type) = manifest_field.field_type.as_primitive_type() else {
                self.upper_bound_decode_failures += 1;
                self.record_type_compatibility(BoundCompatibility::Incompatible, None);
                continue;
            };

            if self.file_has_no_non_null_non_nan_values(entry.data_file(), &current_type) {
                self.data_files_with_no_non_null_values += 1;
                continue;
            }

            let Some(bound) = entry.data_file().upper_bounds().get(&self.field_id) else {
                self.data_files_without_upper_bound += 1;
                continue;
            };
            let primitive_literal: spec::PrimitiveLiteral = bound.clone().into();
            if let Ok((value, compatibility)) = BoundValue::from_primitive_literal(
                manifest_type,
                &current_type,
                primitive_literal,
                Some(|| bound.to_string()),
            ) {
                if value.is_nan() {
                    self.nan_upper_bounds += 1;
                } else {
                    self.record_type_compatibility(
                        compatibility,
                        promoted_type_detail(manifest_type, &current_type),
                    );
                    self.add_max_candidate(value, candidate_file);
                }
            } else {
                self.upper_bound_decode_failures += 1;
                self.record_type_compatibility(BoundCompatibility::Incompatible, None);
            }
        }
    }

    fn analyze_field_absent_data_file(
        &mut self,
        current_type: &spec::PrimitiveType,
        candidate_file: CandidateFile,
    ) {
        self.data_files_field_absent += 1;

        if let Some(initial_default) = self.initial_default.clone() {
            let spec::Literal::Primitive(primitive_literal) = initial_default else {
                self.upper_bound_decode_failures += 1;
                return;
            };
            match BoundValue::from_primitive_literal(
                current_type,
                current_type,
                primitive_literal,
                None::<fn() -> String>,
            ) {
                Ok((value, compatibility)) => {
                    self.data_files_using_initial_default += 1;
                    self.record_type_compatibility(compatibility, None);
                    if value.is_nan() {
                        self.nan_upper_bounds += 1;
                    } else {
                        self.add_max_candidate(value, candidate_file);
                    }
                }
                Err(()) => self.upper_bound_decode_failures += 1,
            }
        } else if self.required {
            self.required_field_absent_without_default += 1;
        } else {
            self.data_files_with_no_non_null_values += 1;
        }
    }

    fn file_has_no_non_null_non_nan_values(
        &mut self,
        data_file: &spec::DataFile,
        current_type: &spec::PrimitiveType,
    ) -> bool {
        let value_count = data_file.value_counts().get(&self.field_id).copied();
        let null_count = data_file.null_value_counts().get(&self.field_id).copied();
        let nan_count = data_file.nan_value_counts().get(&self.field_id).copied();

        if let (Some(value_count), Some(null_count)) = (value_count, null_count) {
            if value_count == null_count {
                return true;
            }

            if is_float_or_double(current_type) {
                if let Some(nan_count) = nan_count {
                    if nan_count > 0 {
                        self.data_files_with_nan_values += 1;
                    }
                    if value_count == null_count.saturating_add(nan_count) {
                        return true;
                    }
                } else {
                    self.data_files_with_unknown_nan_counts += 1;
                }
            }
        } else if is_float_or_double(current_type) {
            self.data_files_with_unknown_nan_counts += 1;
        }

        false
    }

    fn add_max_candidate(&mut self, value: BoundValue, candidate_file: CandidateFile) {
        match &self.metadata_max {
            None => {
                self.metadata_max = Some(value);
                self.max_candidates = vec![candidate_file];
            }
            Some(current_max) => match value.compare(current_max) {
                Some(Ordering::Greater) => {
                    self.metadata_max = Some(value);
                    self.max_candidates = vec![candidate_file];
                }
                Some(Ordering::Equal) => self.max_candidates.push(candidate_file),
                Some(Ordering::Less) => {}
                None => {
                    self.upper_bound_decode_failures += 1;
                    self.record_type_compatibility(BoundCompatibility::Incompatible, None);
                }
            },
        }
    }

    fn record_manifest_decode_failure(&mut self) {
        self.manifest_decode_failures += 1;
        self.read_completeness = ReadCompleteness::Incomplete;
    }

    fn record_type_compatibility(
        &mut self,
        compatibility: BoundCompatibility,
        detail: Option<String>,
    ) {
        match compatibility {
            BoundCompatibility::Exact => {}
            BoundCompatibility::SafelyPromoted => {
                if self.type_compatibility == TypeCompatibility::Exact {
                    self.type_compatibility = TypeCompatibility::SafelyPromoted;
                }
                if self.type_compatibility_detail.is_none() {
                    self.type_compatibility_detail = detail;
                }
            }
            BoundCompatibility::Incompatible => {
                self.type_compatibility = TypeCompatibility::Incompatible;
                if self.type_compatibility_detail.is_none() {
                    self.type_compatibility_detail = Some(
                        "one or more upper bounds could not be compared with the current field type"
                            .to_string(),
                    );
                }
            }
        }
    }

    async fn analyze_delete_files(&mut self, table: &Table, delete_files: &[DeleteFileInfo]) {
        self.count_delete_file_inventory(delete_files);

        if self.max_candidates.is_empty() {
            return;
        }

        let mut statuses = vec![CandidateDeleteStatus::default(); self.max_candidates.len()];

        self.analyze_equality_delete_files(delete_files, &mut statuses);
        self.analyze_position_delete_files(table, delete_files, &mut statuses)
            .await;
        self.finish_delete_impact(&statuses);
    }

    fn count_delete_file_inventory(&mut self, delete_files: &[DeleteFileInfo]) {
        for delete_file in delete_files {
            match delete_file.content_type {
                DataContentType::EqualityDeletes => self.equality_delete_files += 1,
                DataContentType::PositionDeletes => {
                    self.position_delete_files += 1;
                    self.record_position_delete_sequence(delete_file.sequence_number);
                    if delete_file.referenced_data_file.is_some() {
                        self.position_delete_files_with_referenced_data_file += 1;
                    }
                }
                DataContentType::Data => {}
            }
        }
    }

    fn analyze_equality_delete_files(
        &mut self,
        delete_files: &[DeleteFileInfo],
        statuses: &mut [CandidateDeleteStatus],
    ) {
        for delete_file in delete_files
            .iter()
            .filter(|delete_file| delete_file.content_type == DataContentType::EqualityDeletes)
        {
            for (candidate, status) in self.max_candidates.iter().zip(statuses.iter_mut()) {
                match equality_delete_applicability(delete_file, candidate) {
                    DeleteApplicability::Applicable => status.record_equality_may_affect(),
                    DeleteApplicability::Unknown => status.record_equality_unknown(),
                    DeleteApplicability::NotApplicable => {}
                }
            }
        }
    }

    async fn analyze_position_delete_files(
        &mut self,
        table: &Table,
        delete_files: &[DeleteFileInfo],
        statuses: &mut [CandidateDeleteStatus],
    ) {
        for delete_file in delete_files
            .iter()
            .filter(|delete_file| delete_file.content_type == DataContentType::PositionDeletes)
        {
            if let Some(referenced_data_file) = &delete_file.referenced_data_file {
                self.analyze_referenced_position_delete_file(
                    delete_file,
                    referenced_data_file,
                    statuses,
                );
                continue;
            }

            self.analyze_unreferenced_position_delete_file(table, delete_file, statuses)
                .await;
        }
    }

    fn record_position_delete_sequence(&mut self, sequence_number: Option<i64>) {
        let Some(sequence_number) = sequence_number else {
            self.position_delete_files_without_sequence_number += 1;
            return;
        };

        self.position_delete_sequence_number_min = Some(
            self.position_delete_sequence_number_min
                .map_or(sequence_number, |current| current.min(sequence_number)),
        );
        self.position_delete_sequence_number_max = Some(
            self.position_delete_sequence_number_max
                .map_or(sequence_number, |current| current.max(sequence_number)),
        );
    }

    fn analyze_referenced_position_delete_file(
        &mut self,
        delete_file: &DeleteFileInfo,
        referenced_data_file: &str,
        statuses: &mut [CandidateDeleteStatus],
    ) {
        let mut sequence_excluded = 0_usize;
        let mut reference_excluded = false;
        let mut applicable = false;
        let mut unknown = false;

        for (candidate, status) in self.max_candidates.iter().zip(statuses.iter_mut()) {
            match position_delete_sequence_applicability(delete_file, candidate) {
                DeleteApplicability::NotApplicable => sequence_excluded += 1,
                DeleteApplicability::Unknown => {
                    if referenced_data_file == candidate.path {
                        unknown = true;
                        status.record_position_unknown();
                    } else {
                        reference_excluded = true;
                    }
                }
                DeleteApplicability::Applicable => {
                    if referenced_data_file == candidate.path {
                        applicable = true;
                        status.record_position_touched();
                    } else {
                        reference_excluded = true;
                    }
                }
            }
        }

        if applicable {
            self.position_delete_files_applicable_to_max_candidates += 1;
        } else if unknown {
            self.position_delete_files_with_unknown_applicability += 1;
        } else if sequence_excluded == self.max_candidates.len() {
            self.position_delete_files_not_applicable_by_sequence += 1;
        } else if reference_excluded {
            self.position_delete_files_not_applicable_by_referenced_data_file += 1;
        }
    }

    async fn analyze_unreferenced_position_delete_file(
        &mut self,
        table: &Table,
        delete_file: &DeleteFileInfo,
        statuses: &mut [CandidateDeleteStatus],
    ) {
        let applicable_candidate_indexes =
            self.prefilter_unreferenced_position_delete_file(delete_file, statuses);

        if applicable_candidate_indexes.is_empty() {
            return;
        }

        if delete_file.file_format != DataFileFormat::Parquet {
            self.unsupported_position_delete_files += 1;
            for index in applicable_candidate_indexes {
                statuses[index].record_position_unknown();
            }
            return;
        }

        self.position_delete_files_requiring_file_path_reads += 1;
        if let Ok(paths) = read_parquet_position_delete_file_paths(table, &delete_file.path).await {
            self.position_delete_files_read_for_file_path += 1;
            for index in applicable_candidate_indexes {
                if paths.contains(&self.max_candidates[index].path) {
                    statuses[index].record_position_touched();
                }
            }
        } else {
            self.position_delete_read_failures += 1;
            for index in applicable_candidate_indexes {
                statuses[index].record_position_unknown();
            }
        }
    }

    fn prefilter_unreferenced_position_delete_file(
        &mut self,
        delete_file: &DeleteFileInfo,
        statuses: &mut [CandidateDeleteStatus],
    ) -> Vec<usize> {
        let mut applicable_candidate_indexes = Vec::new();
        let mut sequence_excluded = 0_usize;
        let mut partition_excluded = false;
        let mut unknown = false;

        for (index, candidate) in self.max_candidates.iter().enumerate() {
            match position_delete_sequence_applicability(delete_file, candidate) {
                DeleteApplicability::NotApplicable => sequence_excluded += 1,
                DeleteApplicability::Unknown => {
                    unknown = true;
                    statuses[index].record_position_unknown();
                }
                DeleteApplicability::Applicable => {
                    if delete_file.partition_spec_id == candidate.partition_spec_id
                        && delete_file.partition != candidate.partition
                    {
                        partition_excluded = true;
                    } else {
                        applicable_candidate_indexes.push(index);
                    }
                }
            }
        }

        if applicable_candidate_indexes.is_empty() {
            if unknown {
                self.position_delete_files_with_unknown_applicability += 1;
            } else if sequence_excluded == self.max_candidates.len() {
                self.position_delete_files_not_applicable_by_sequence += 1;
            } else if partition_excluded {
                self.position_delete_files_not_applicable_by_partition += 1;
            }
            return applicable_candidate_indexes;
        }

        if unknown {
            self.position_delete_files_with_unknown_applicability += 1;
        }
        self.position_delete_files_applicable_to_max_candidates += 1;
        applicable_candidate_indexes
    }

    fn finish_delete_impact(&mut self, statuses: &[CandidateDeleteStatus]) {
        self.max_candidate_files_with_applicable_equality_deletes = statuses
            .iter()
            .filter(|status| status.equality.has_effect)
            .count();
        self.max_equality_delete_impact = equality_delete_impact(statuses);

        self.max_candidate_files_touched_by_position_deletes = statuses
            .iter()
            .filter(|status| status.position.has_effect)
            .count();
        self.max_position_delete_impact = position_delete_impact(statuses);
        self.max_position_delete_analysis =
            if self.max_position_delete_impact == DeleteImpact::Unknown {
                DeleteAnalysisCompleteness::Incomplete
            } else {
                DeleteAnalysisCompleteness::Complete
            };
    }

    fn finish_without_manifest_analysis(&mut self) {
        self.max_precision = BoundPrecision::Unavailable;
        self.max_confidence = MaxConfidence::Unavailable;
        self.max_confidence_reasons = Vec::new();
    }

    fn finish(&mut self) {
        if self.manifest_decode_failures > 0 {
            self.read_completeness = ReadCompleteness::Incomplete;
            self.caveats.push("Phase 1 uses Iceberg's whole-manifest decoder; a manifest bound decode failure can block max analysis even when the failed bound is lower-side only.".to_string());
        }
        if self.position_delete_read_failures > 0 || self.unsupported_position_delete_files > 0 {
            self.read_completeness = ReadCompleteness::Incomplete;
        }

        self.max_precision = max_precision(
            self.metadata_max.is_some(),
            self.primitive_type.as_ref(),
            &self.metrics_mode.value,
        );
        self.precision_detail = Some(max_precision_detail(
            self.primitive_type.as_ref(),
            &self.metrics_mode.value,
        ));
        self.max_confidence = self.determine_max_confidence();
        self.max_confidence_reasons = self.max_confidence_reasons();
    }

    fn determine_max_confidence(&self) -> MaxConfidence {
        if self.metadata_max.is_none() {
            return MaxConfidence::Unavailable;
        }
        if self.read_completeness == ReadCompleteness::Incomplete {
            return MaxConfidence::Unknown;
        }
        if !matches!(
            self.type_compatibility,
            TypeCompatibility::Exact | TypeCompatibility::SafelyPromoted
        ) {
            return MaxConfidence::Unknown;
        }
        if self.upper_bound_decode_failures > 0
            || self.nan_upper_bounds > 0
            || self.data_files_with_unknown_nan_counts > 0
            || self.required_field_absent_without_default > 0
            || self.max_equality_delete_impact == DeleteImpact::Unknown
            || self.max_position_delete_impact == DeleteImpact::Unknown
        {
            return MaxConfidence::Unknown;
        }
        if self.data_files_without_upper_bound > 0 {
            return MaxConfidence::Partial;
        }
        if self.max_equality_delete_impact == DeleteImpact::AllCandidatesPossiblyAffected
            || self.max_position_delete_impact == DeleteImpact::AllCandidatesTouched
        {
            return MaxConfidence::Lowered;
        }

        MaxConfidence::High
    }

    fn max_confidence_reasons(&self) -> Vec<String> {
        let mut reasons = Vec::new();

        if self.metadata_max.is_none() {
            reasons.push(if self.data_file_metadata_entries_scanned == 0 {
                "current snapshot has no live data file metadata entries".to_string()
            } else if self.data_file_metadata_entries_scanned
                == self.zero_record_data_file_metadata_entries
            {
                "current snapshot has no rows in live data files".to_string()
            } else {
                "no usable upper-bound values were available for relevant live data files"
                    .to_string()
            });
        }
        if self.read_completeness == ReadCompleteness::Incomplete {
            reasons.push(
                "metadata or delete-file reads required for max analysis were incomplete"
                    .to_string(),
            );
        }
        if !matches!(
            self.type_compatibility,
            TypeCompatibility::Exact | TypeCompatibility::SafelyPromoted
        ) {
            reasons.push("upper-bound type compatibility could not be established".to_string());
        }
        if self.upper_bound_decode_failures > 0 {
            reasons.push(format!(
                "{} relevant upper bound(s) could not be decoded or compared",
                self.upper_bound_decode_failures
            ));
        }
        if self.nan_upper_bounds > 0 {
            reasons.push(format!(
                "{} float/double upper bound(s) decoded as NaN",
                self.nan_upper_bounds
            ));
        }
        if self.data_files_with_unknown_nan_counts > 0 {
            reasons.push(format!(
                "NaN count metadata is incomplete for {} float/double data file(s)",
                self.data_files_with_unknown_nan_counts
            ));
        }
        if self.required_field_absent_without_default > 0 {
            reasons.push(format!(
                "{} required field-absent data file(s) had no usable initial-default",
                self.required_field_absent_without_default
            ));
        }
        if self.data_files_without_upper_bound > 0 {
            reasons.push(format!(
                "{} data file metadata entr{} may contain values but do not have upper bounds",
                self.data_files_without_upper_bound,
                if self.data_files_without_upper_bound == 1 {
                    "y"
                } else {
                    "ies"
                }
            ));
        }
        match self.max_equality_delete_impact {
            DeleteImpact::AllCandidatesPossiblyAffected => reasons.push(
                "all max candidate files may be affected by applicable equality deletes"
                    .to_string(),
            ),
            DeleteImpact::Unknown => reasons.push(
                "equality-delete applicability for max candidate files is unknown".to_string(),
            ),
            DeleteImpact::Unaffected | DeleteImpact::PartiallyAffected => reasons.push(
                "at least one max candidate is proven unaffected by equality deletes".to_string(),
            ),
            DeleteImpact::NotApplicable => reasons
                .push("no max candidates were available for equality-delete analysis".to_string()),
            DeleteImpact::AllCandidatesTouched => {}
        }
        match self.max_position_delete_impact {
            DeleteImpact::AllCandidatesTouched => reasons.push(
                "all max candidate files are touched by applicable position deletes".to_string(),
            ),
            DeleteImpact::Unknown => reasons.push(
                "position-delete touch status for max candidate files is unknown".to_string(),
            ),
            DeleteImpact::Unaffected | DeleteImpact::PartiallyAffected => reasons.push(
                "at least one max candidate is proven untouched by position deletes".to_string(),
            ),
            DeleteImpact::NotApplicable => reasons
                .push("no max candidates were available for position-delete analysis".to_string()),
            DeleteImpact::AllCandidatesPossiblyAffected => {}
        }

        if self.max_confidence == MaxConfidence::High {
            reasons.insert(0, "complete upper-bound coverage".to_string());
        }

        reasons
    }

    fn into_result(self) -> CurrentTableMax {
        CurrentTableMax {
            snapshot_id: self.snapshot_id,
            snapshot_updated_at: self.snapshot_updated_at,
            manifest_list_path: self.manifest_list_path,
            column: self.column,
            field_path: self.field_path,
            field_id: self.field_id,
            field_type: self.field_type,
            unsupported_reason: self.unsupported_reason,
            metadata_max: self.metadata_max.map(|value| value.display),
            max_confidence: self.max_confidence,
            max_confidence_reasons: self.max_confidence_reasons,
            max_precision: self.max_precision,
            data_file_metadata_entries_scanned: self.data_file_metadata_entries_scanned,
            zero_record_data_file_metadata_entries: self.zero_record_data_file_metadata_entries,
            data_files_field_absent: self.data_files_field_absent,
            data_files_using_initial_default: self.data_files_using_initial_default,
            data_files_with_no_non_null_values: self.data_files_with_no_non_null_values,
            data_files_with_nan_values: self.data_files_with_nan_values,
            data_files_without_upper_bound: self.data_files_without_upper_bound,
            nan_upper_bounds: self.nan_upper_bounds,
            upper_bound_decode_failures: self.upper_bound_decode_failures,
            manifest_decode_failures: self.manifest_decode_failures,
            equality_delete_files: self.equality_delete_files,
            zero_record_delete_files: self.zero_record_delete_files,
            max_candidate_files_with_applicable_equality_deletes: self
                .max_candidate_files_with_applicable_equality_deletes,
            max_candidate_file_count: self.max_candidates.len(),
            max_candidate_data_sequence_number_min: sequence_number_min(&self.max_candidates),
            max_candidate_data_sequence_number_max: sequence_number_max(&self.max_candidates),
            max_candidate_files_without_sequence_number: self
                .max_candidates
                .iter()
                .filter(|candidate| candidate.sequence_number.is_none())
                .count(),
            max_equality_delete_impact: self.max_equality_delete_impact,
            position_delete_files: self.position_delete_files,
            position_delete_sequence_number_min: self.position_delete_sequence_number_min,
            position_delete_sequence_number_max: self.position_delete_sequence_number_max,
            position_delete_files_without_sequence_number: self
                .position_delete_files_without_sequence_number,
            position_delete_files_with_referenced_data_file: self
                .position_delete_files_with_referenced_data_file,
            position_delete_files_not_applicable_by_sequence: self
                .position_delete_files_not_applicable_by_sequence,
            position_delete_files_not_applicable_by_partition: self
                .position_delete_files_not_applicable_by_partition,
            position_delete_files_not_applicable_by_referenced_data_file: self
                .position_delete_files_not_applicable_by_referenced_data_file,
            position_delete_files_with_unknown_applicability: self
                .position_delete_files_with_unknown_applicability,
            position_delete_files_applicable_to_max_candidates: self
                .position_delete_files_applicable_to_max_candidates,
            position_delete_files_requiring_file_path_reads: self
                .position_delete_files_requiring_file_path_reads,
            position_delete_files_read_for_file_path: self.position_delete_files_read_for_file_path,
            unsupported_position_delete_files: self.unsupported_position_delete_files,
            max_candidate_files_touched_by_position_deletes: self
                .max_candidate_files_touched_by_position_deletes,
            max_position_delete_impact: self.max_position_delete_impact,
            max_position_delete_analysis: self.max_position_delete_analysis,
            read_completeness: self.read_completeness,
            type_compatibility: self.type_compatibility,
            type_compatibility_detail: self.type_compatibility_detail,
            metrics_mode_evidence: self.metrics_mode.evidence,
            current_metrics_mode: self.metrics_mode.value,
            precision_detail: self.precision_detail,
            caveats: self.caveats,
        }
    }
}

impl BoundValue {
    #[expect(
        clippy::too_many_lines,
        reason = "primitive bound normalization is intentionally exhaustive"
    )]
    fn from_primitive_literal<D>(
        manifest_type: &spec::PrimitiveType,
        current_type: &spec::PrimitiveType,
        literal: spec::PrimitiveLiteral,
        display: Option<D>,
    ) -> std::result::Result<(Self, BoundCompatibility), ()>
    where
        D: FnOnce() -> String,
    {
        let (kind, compatibility) = match (manifest_type, current_type, literal) {
            (
                spec::PrimitiveType::Boolean,
                spec::PrimitiveType::Boolean,
                spec::PrimitiveLiteral::Boolean(value),
            ) => (BoundValueKind::Boolean(value), BoundCompatibility::Exact),
            (
                spec::PrimitiveType::Int,
                spec::PrimitiveType::Int,
                spec::PrimitiveLiteral::Int(value),
            ) => (BoundValueKind::Int(value), BoundCompatibility::Exact),
            (
                spec::PrimitiveType::Int,
                spec::PrimitiveType::Long,
                spec::PrimitiveLiteral::Int(value),
            ) => (
                BoundValueKind::Long(i64::from(value)),
                BoundCompatibility::SafelyPromoted,
            ),
            (
                spec::PrimitiveType::Long,
                spec::PrimitiveType::Long,
                spec::PrimitiveLiteral::Long(value),
            ) => (BoundValueKind::Long(value), BoundCompatibility::Exact),
            (
                spec::PrimitiveType::Float,
                spec::PrimitiveType::Float,
                spec::PrimitiveLiteral::Float(value),
            ) => (
                BoundValueKind::Float(value.0.to_bits()),
                BoundCompatibility::Exact,
            ),
            (
                spec::PrimitiveType::Float,
                spec::PrimitiveType::Double,
                spec::PrimitiveLiteral::Float(value),
            ) => (
                BoundValueKind::Double(f64::from(value.0).to_bits()),
                BoundCompatibility::SafelyPromoted,
            ),
            (
                spec::PrimitiveType::Double,
                spec::PrimitiveType::Double,
                spec::PrimitiveLiteral::Double(value),
            ) => (
                BoundValueKind::Double(value.0.to_bits()),
                BoundCompatibility::Exact,
            ),
            (
                spec::PrimitiveType::Decimal {
                    precision: manifest_precision,
                    scale: manifest_scale,
                },
                spec::PrimitiveType::Decimal {
                    precision: current_precision,
                    scale: current_scale,
                },
                spec::PrimitiveLiteral::Int128(value),
            ) if manifest_scale == current_scale && manifest_precision <= current_precision => (
                BoundValueKind::Decimal {
                    unscaled: value,
                    scale: *current_scale,
                },
                if manifest_precision == current_precision {
                    BoundCompatibility::Exact
                } else {
                    BoundCompatibility::SafelyPromoted
                },
            ),
            (
                spec::PrimitiveType::Date,
                spec::PrimitiveType::Date,
                spec::PrimitiveLiteral::Int(value),
            ) => (BoundValueKind::Date(value), BoundCompatibility::Exact),
            (
                spec::PrimitiveType::Time,
                spec::PrimitiveType::Time,
                spec::PrimitiveLiteral::Long(value),
            ) => (BoundValueKind::Time(value), BoundCompatibility::Exact),
            (
                spec::PrimitiveType::Timestamp,
                spec::PrimitiveType::Timestamp,
                spec::PrimitiveLiteral::Long(value),
            ) => (BoundValueKind::Timestamp(value), BoundCompatibility::Exact),
            (
                spec::PrimitiveType::Timestamptz,
                spec::PrimitiveType::Timestamptz,
                spec::PrimitiveLiteral::Long(value),
            ) => (
                BoundValueKind::Timestamptz(value),
                BoundCompatibility::Exact,
            ),
            (
                spec::PrimitiveType::TimestampNs,
                spec::PrimitiveType::TimestampNs,
                spec::PrimitiveLiteral::Long(value),
            ) => (
                BoundValueKind::TimestampNs(value),
                BoundCompatibility::Exact,
            ),
            (
                spec::PrimitiveType::TimestamptzNs,
                spec::PrimitiveType::TimestamptzNs,
                spec::PrimitiveLiteral::Long(value),
            ) => (
                BoundValueKind::TimestamptzNs(value),
                BoundCompatibility::Exact,
            ),
            (
                spec::PrimitiveType::String,
                spec::PrimitiveType::String,
                spec::PrimitiveLiteral::String(value),
            ) => (BoundValueKind::String(value), BoundCompatibility::Exact),
            (
                spec::PrimitiveType::Uuid,
                spec::PrimitiveType::Uuid,
                spec::PrimitiveLiteral::UInt128(value),
            ) => (BoundValueKind::Uuid(value), BoundCompatibility::Exact),
            (
                spec::PrimitiveType::Fixed(manifest_len),
                spec::PrimitiveType::Fixed(current_len),
                spec::PrimitiveLiteral::Binary(value),
            ) if manifest_len == current_len => {
                (BoundValueKind::Fixed(value), BoundCompatibility::Exact)
            }
            _ => return Err(()),
        };
        let display = display.map_or_else(|| display_bound_value(&kind), |display| display());

        Ok((Self { kind, display }, compatibility))
    }

    fn compare(&self, other: &Self) -> Option<Ordering> {
        match (&self.kind, &other.kind) {
            (BoundValueKind::Boolean(left), BoundValueKind::Boolean(right)) => {
                left.partial_cmp(right)
            }
            (BoundValueKind::Int(left), BoundValueKind::Int(right))
            | (BoundValueKind::Date(left), BoundValueKind::Date(right)) => left.partial_cmp(right),
            (BoundValueKind::Long(left), BoundValueKind::Long(right))
            | (BoundValueKind::Time(left), BoundValueKind::Time(right))
            | (BoundValueKind::Timestamp(left), BoundValueKind::Timestamp(right))
            | (BoundValueKind::Timestamptz(left), BoundValueKind::Timestamptz(right))
            | (BoundValueKind::TimestampNs(left), BoundValueKind::TimestampNs(right))
            | (BoundValueKind::TimestamptzNs(left), BoundValueKind::TimestamptzNs(right)) => {
                left.partial_cmp(right)
            }
            (BoundValueKind::Float(left), BoundValueKind::Float(right)) => {
                Some(f32::from_bits(*left).total_cmp(&f32::from_bits(*right)))
            }
            (BoundValueKind::Double(left), BoundValueKind::Double(right)) => {
                Some(f64::from_bits(*left).total_cmp(&f64::from_bits(*right)))
            }
            (
                BoundValueKind::Decimal {
                    unscaled: left,
                    scale: left_scale,
                },
                BoundValueKind::Decimal {
                    unscaled: right,
                    scale: right_scale,
                },
            ) if left_scale == right_scale => left.partial_cmp(right),
            (BoundValueKind::String(left), BoundValueKind::String(right)) => {
                left.partial_cmp(right)
            }
            (BoundValueKind::Uuid(left), BoundValueKind::Uuid(right)) => left.partial_cmp(right),
            (BoundValueKind::Fixed(left), BoundValueKind::Fixed(right)) => left.partial_cmp(right),
            _ => None,
        }
    }

    fn is_nan(&self) -> bool {
        match self.kind {
            BoundValueKind::Float(bits) => f32::from_bits(bits).is_nan(),
            BoundValueKind::Double(bits) => f64::from_bits(bits).is_nan(),
            _ => false,
        }
    }
}

fn resolve_current_column_path(
    schema: &spec::Schema,
    column_path: &str,
) -> Result<CurrentColumnResolution> {
    let segments = column_path.split('.').collect::<Vec<_>>();
    if segments.is_empty() || segments.iter().any(|segment| segment.is_empty()) {
        return Err(BergError::UnknownColumnPath {
            path: column_path.to_string(),
        });
    }

    let mut fields = schema.as_struct().fields();
    for (index, segment) in segments.iter().enumerate() {
        if let Some(base_name) = collection_marker_segment_base(segment) {
            let Some(field) = fields.iter().find(|field| field.name == base_name) else {
                return Err(BergError::UnknownColumnPath {
                    path: column_path.to_string(),
                });
            };
            return Ok(unsupported_nested_collection_resolution(
                column_path,
                field,
                "primitive fields inside lists or maps are not supported by metadata max phase 1",
            ));
        }

        let Some(field) = fields.iter().find(|field| field.name == *segment) else {
            return Err(BergError::UnknownColumnPath {
                path: column_path.to_string(),
            });
        };
        let is_last = index == segments.len() - 1;

        if is_last {
            return Ok(column_resolution_from_field(column_path, field));
        }

        match field.field_type.as_ref() {
            spec::Type::Struct(struct_type) => fields = struct_type.fields(),
            spec::Type::List(_) => {
                return Ok(unsupported_nested_collection_resolution(
                    column_path,
                    field,
                    "primitive fields inside lists are not supported by metadata max phase 1",
                ));
            }
            spec::Type::Map(_) => {
                return Ok(unsupported_nested_collection_resolution(
                    column_path,
                    field,
                    "primitive fields inside maps are not supported by metadata max phase 1",
                ));
            }
            spec::Type::Primitive(_) => {
                return Err(BergError::UnknownColumnPath {
                    path: column_path.to_string(),
                });
            }
        }
    }

    Err(BergError::UnknownColumnPath {
        path: column_path.to_string(),
    })
}

fn collection_marker_segment_base(segment: &str) -> Option<&str> {
    let marker_index = segment.find("[]").or_else(|| segment.find("{}"))?;
    (marker_index > 0).then(|| &segment[..marker_index])
}

fn column_resolution_from_field(
    column_path: &str,
    field: &spec::NestedFieldRef,
) -> CurrentColumnResolution {
    match field.field_type.as_ref() {
        spec::Type::Primitive(spec::PrimitiveType::Binary) => CurrentColumnResolution {
            field_path: column_path.to_string(),
            field_id: field.id,
            field_type: field.field_type.to_string(),
            primitive_type: None,
            required: field.required,
            initial_default: field.initial_default.clone(),
            unsupported_reason: Some(
                "binary fields are not supported by metadata max phase 1".to_string(),
            ),
        },
        spec::Type::Primitive(primitive_type) => CurrentColumnResolution {
            field_path: column_path.to_string(),
            field_id: field.id,
            field_type: primitive_type.to_string(),
            primitive_type: Some(primitive_type.clone()),
            required: field.required,
            initial_default: field.initial_default.clone(),
            unsupported_reason: None,
        },
        spec::Type::Struct(_) => CurrentColumnResolution {
            field_path: column_path.to_string(),
            field_id: field.id,
            field_type: "struct".to_string(),
            primitive_type: None,
            required: field.required,
            initial_default: field.initial_default.clone(),
            unsupported_reason: Some(
                "struct fields are not supported by metadata max phase 1".to_string(),
            ),
        },
        spec::Type::List(_) => CurrentColumnResolution {
            field_path: column_path.to_string(),
            field_id: field.id,
            field_type: "list".to_string(),
            primitive_type: None,
            required: field.required,
            initial_default: field.initial_default.clone(),
            unsupported_reason: Some(
                "list fields are not supported by metadata max phase 1".to_string(),
            ),
        },
        spec::Type::Map(_) => CurrentColumnResolution {
            field_path: column_path.to_string(),
            field_id: field.id,
            field_type: "map".to_string(),
            primitive_type: None,
            required: field.required,
            initial_default: field.initial_default.clone(),
            unsupported_reason: Some(
                "map fields are not supported by metadata max phase 1".to_string(),
            ),
        },
    }
}

fn unsupported_nested_collection_resolution(
    column_path: &str,
    field: &spec::NestedFieldRef,
    reason: &str,
) -> CurrentColumnResolution {
    CurrentColumnResolution {
        field_path: column_path.to_string(),
        field_id: field.id,
        field_type: field.field_type.to_string(),
        primitive_type: None,
        required: field.required,
        initial_default: field.initial_default.clone(),
        unsupported_reason: Some(reason.to_string()),
    }
}

fn current_metrics_mode(
    properties: &HashMap<String, String>,
    field_path: &str,
) -> CurrentMetricsMode {
    let column_key = format!("write.metadata.metrics.column.{field_path}");
    if let Some(value) = properties.get(&column_key) {
        return CurrentMetricsMode {
            evidence: "current table properties; per-file historical metrics mode is not available"
                .to_string(),
            value: normalize_metrics_mode(value),
        };
    }

    if let Some(value) = properties.get("write.metadata.metrics.default") {
        return CurrentMetricsMode {
            evidence: "current table properties; per-file historical metrics mode is not available"
                .to_string(),
            value: normalize_metrics_mode(value),
        };
    }

    CurrentMetricsMode {
        evidence: "Iceberg default; per-file historical metrics mode is not available".to_string(),
        value: "truncate(16)".to_string(),
    }
}

fn normalize_metrics_mode(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(' ', "")
}

fn collect_delete_files(
    manifest: &spec::Manifest,
    partition_spec_id: i32,
    analysis: &mut CurrentTableMaxAnalysis,
    delete_files: &mut Vec<DeleteFileInfo>,
) {
    let partition_spec_is_unpartitioned = manifest.metadata().partition_spec().is_unpartitioned();

    for entry in live_manifest_entries(manifest) {
        if !matches!(
            entry.content_type(),
            DataContentType::EqualityDeletes | DataContentType::PositionDeletes
        ) {
            continue;
        }

        if entry.record_count() == 0 {
            analysis.zero_record_delete_files += 1;
            continue;
        }

        delete_files.push(DeleteFileInfo {
            content_type: entry.content_type(),
            path: entry.file_path().to_string(),
            file_format: entry.file_format(),
            sequence_number: entry.sequence_number(),
            partition_spec_id,
            partition_spec_is_unpartitioned,
            partition: entry.data_file().partition().clone(),
            referenced_data_file: entry.data_file().referenced_data_file(),
        });
    }
}

fn sequence_number_min(candidates: &[CandidateFile]) -> Option<i64> {
    candidates
        .iter()
        .filter_map(|candidate| candidate.sequence_number)
        .min()
}

fn sequence_number_max(candidates: &[CandidateFile]) -> Option<i64> {
    candidates
        .iter()
        .filter_map(|candidate| candidate.sequence_number)
        .max()
}

fn equality_delete_applicability(
    delete_file: &DeleteFileInfo,
    candidate: &CandidateFile,
) -> DeleteApplicability {
    match (delete_file.sequence_number, candidate.sequence_number) {
        (Some(delete_sequence), Some(candidate_sequence))
            if delete_sequence <= candidate_sequence =>
        {
            return DeleteApplicability::NotApplicable;
        }
        (Some(_), Some(_)) => {}
        _ => return DeleteApplicability::Unknown,
    }

    if delete_file.partition_spec_is_unpartitioned {
        return DeleteApplicability::Applicable;
    }

    if delete_file.partition_spec_id == candidate.partition_spec_id {
        if delete_file.partition == candidate.partition {
            DeleteApplicability::Applicable
        } else {
            DeleteApplicability::NotApplicable
        }
    } else {
        DeleteApplicability::Unknown
    }
}

fn position_delete_sequence_applicability(
    delete_file: &DeleteFileInfo,
    candidate: &CandidateFile,
) -> DeleteApplicability {
    match (delete_file.sequence_number, candidate.sequence_number) {
        (Some(delete_sequence), Some(candidate_sequence))
            if delete_sequence < candidate_sequence =>
        {
            DeleteApplicability::NotApplicable
        }
        (Some(_), Some(_)) => DeleteApplicability::Applicable,
        _ => DeleteApplicability::Unknown,
    }
}

fn equality_delete_impact(statuses: &[CandidateDeleteStatus]) -> DeleteImpact {
    if statuses.is_empty() {
        return DeleteImpact::NotApplicable;
    }

    let unaffected = statuses
        .iter()
        .filter(|status| status.equality.is_unaffected())
        .count();
    let affected = statuses
        .iter()
        .filter(|status| status.equality.has_effect)
        .count();
    let unknown = statuses
        .iter()
        .any(|status| status.equality.has_unknown);

    if unaffected > 0 && affected > 0 {
        DeleteImpact::PartiallyAffected
    } else if unaffected > 0 {
        DeleteImpact::Unaffected
    } else if unknown {
        DeleteImpact::Unknown
    } else if affected == statuses.len() {
        DeleteImpact::AllCandidatesPossiblyAffected
    } else {
        DeleteImpact::Unaffected
    }
}

fn position_delete_impact(statuses: &[CandidateDeleteStatus]) -> DeleteImpact {
    if statuses.is_empty() {
        return DeleteImpact::NotApplicable;
    }

    let untouched = statuses
        .iter()
        .filter(|status| status.position.is_unaffected())
        .count();
    let touched = statuses
        .iter()
        .filter(|status| status.position.has_effect)
        .count();
    let unknown = statuses
        .iter()
        .any(|status| status.position.has_unknown);

    if untouched > 0 && touched > 0 {
        DeleteImpact::PartiallyAffected
    } else if untouched > 0 {
        DeleteImpact::Unaffected
    } else if unknown {
        DeleteImpact::Unknown
    } else if touched == statuses.len() {
        DeleteImpact::AllCandidatesTouched
    } else {
        DeleteImpact::Unaffected
    }
}

async fn read_parquet_position_delete_file_paths(
    table: &Table,
    delete_file_path: &str,
) -> std::result::Result<HashSet<String>, ()> {
    let input = table
        .file_io()
        .new_input(delete_file_path)
        .map_err(|_| ())?
        .read()
        .await
        .map_err(|_| ())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(input).map_err(|_| ())?;
    let file_path_index = builder
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == "file_path")
        .ok_or(())?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), [file_path_index]);
    let reader = builder
        .with_projection(projection)
        .build()
        .map_err(|_| ())?;
    let mut paths = HashSet::new();

    for batch in reader {
        let batch = batch.map_err(|_| ())?;
        let column = batch.column(0).as_ref();
        extend_string_values(column, &mut paths)?;
    }

    Ok(paths)
}

fn extend_string_values(
    array: &dyn Array,
    values: &mut HashSet<String>,
) -> std::result::Result<(), ()> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        for index in 0..array.len() {
            if array.is_null(index) {
                return Err(());
            }
            values.insert(array.value(index).to_string());
        }
        return Ok(());
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        for index in 0..array.len() {
            if array.is_null(index) {
                return Err(());
            }
            values.insert(array.value(index).to_string());
        }
        return Ok(());
    }
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        for index in 0..array.len() {
            if array.is_null(index) {
                return Err(());
            }
            values.insert(array.value(index).to_string());
        }
        return Ok(());
    }

    Err(())
}

fn promoted_type_detail(
    manifest_type: &spec::PrimitiveType,
    current_type: &spec::PrimitiveType,
) -> Option<String> {
    match (manifest_type, current_type) {
        (spec::PrimitiveType::Int, spec::PrimitiveType::Long) => {
            Some("int upper bounds promoted to long".to_string())
        }
        (spec::PrimitiveType::Float, spec::PrimitiveType::Double) => {
            Some("float upper bounds promoted to double".to_string())
        }
        (
            spec::PrimitiveType::Decimal {
                precision: manifest_precision,
                scale: manifest_scale,
            },
            spec::PrimitiveType::Decimal {
                precision: current_precision,
                scale: current_scale,
            },
        ) if manifest_scale == current_scale && manifest_precision < current_precision => {
            Some("decimal upper bounds use same scale with widened precision".to_string())
        }
        _ => None,
    }
}

fn is_float_or_double(primitive_type: &spec::PrimitiveType) -> bool {
    matches!(
        primitive_type,
        spec::PrimitiveType::Float | spec::PrimitiveType::Double
    )
}

fn is_truncatable_type(primitive_type: Option<&spec::PrimitiveType>) -> bool {
    matches!(
        primitive_type,
        Some(spec::PrimitiveType::String | spec::PrimitiveType::Fixed(_))
    )
}

fn max_precision(
    has_metadata_max: bool,
    primitive_type: Option<&spec::PrimitiveType>,
    metrics_mode: &str,
) -> BoundPrecision {
    if !has_metadata_max {
        return BoundPrecision::Unavailable;
    }
    if !is_truncatable_type(primitive_type) {
        return BoundPrecision::Exact;
    }

    match metrics_mode {
        "full" => BoundPrecision::ProbablyExact,
        mode if mode.starts_with("truncate(") && mode.ends_with(')') => {
            BoundPrecision::PossiblyTruncated
        }
        _ => BoundPrecision::Unknown,
    }
}

fn max_precision_detail(
    primitive_type: Option<&spec::PrimitiveType>,
    metrics_mode: &str,
) -> String {
    if !is_truncatable_type(primitive_type) {
        return "metrics truncation does not apply to this field type".to_string();
    }

    match metrics_mode {
        "full" => {
            "current full metrics config does not prove older live files were written with full bounds"
                .to_string()
        }
        mode if mode.starts_with("truncate(") && mode.ends_with(')') => {
            "displayed metadata max may not be an exact row value; truncation length is current/default config evidence, not per-file metadata".to_string()
        }
        "none" | "counts" => {
            "bounds exist but current metrics config does not explain them; files may have been written under older config".to_string()
        }
        _ => "current metrics mode could not be interpreted".to_string(),
    }
}

fn display_bound_value(kind: &BoundValueKind) -> String {
    match kind {
        BoundValueKind::Boolean(value) => value.to_string(),
        BoundValueKind::Int(value) => value.to_string(),
        BoundValueKind::Long(value) => value.to_string(),
        BoundValueKind::Float(bits) => f32::from_bits(*bits).to_string(),
        BoundValueKind::Double(bits) => f64::from_bits(*bits).to_string(),
        BoundValueKind::Decimal { unscaled, scale } => display_decimal(*unscaled, *scale),
        BoundValueKind::Date(value) => spec::Datum::date(*value).to_string(),
        BoundValueKind::Time(value) => spec::Datum::time_micros(*value)
            .map_or_else(|_| value.to_string(), |value| value.to_string()),
        BoundValueKind::Timestamp(value) => spec::Datum::timestamp_micros(*value).to_string(),
        BoundValueKind::Timestamptz(value) => spec::Datum::timestamptz_micros(*value).to_string(),
        BoundValueKind::TimestampNs(value) => spec::Datum::timestamp_nanos(*value).to_string(),
        BoundValueKind::TimestamptzNs(value) => spec::Datum::timestamptz_nanos(*value).to_string(),
        BoundValueKind::String(value) => format!(r#""{value}""#),
        BoundValueKind::Uuid(value) => display_uuid(*value),
        BoundValueKind::Fixed(value) => display_hex_bytes(value),
    }
}

fn display_uuid(value: u128) -> String {
    let hex = format!("{value:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn display_hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0F) as usize]));
    }
    output
}

fn display_decimal(unscaled: i128, scale: u32) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }

    let negative = unscaled.is_negative();
    let digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let mut output = String::new();
    if negative {
        output.push('-');
    }
    if digits.len() <= scale {
        output.push_str("0.");
        output.push_str(&"0".repeat(scale - digits.len()));
        output.push_str(&digits);
    } else {
        let split = digits.len() - scale;
        output.push_str(&digits[..split]);
        output.push('.');
        output.push_str(&digits[split..]);
    }

    output
}

async fn visit_live_manifest_files<F, V>(
    table: &Table,
    manifest_list: &spec::ManifestList,
    include_content: F,
    mut visit: V,
) -> Result<()>
where
    F: Fn(ManifestContentType) -> bool,
    V: FnMut(LiveManifest) -> Result<()>,
{
    for manifest_file in manifest_list.entries() {
        if !include_content(manifest_file.content) || !manifest_file_has_live_files(manifest_file) {
            continue;
        }

        visit(LiveManifest {
            partition_spec_id: manifest_file.partition_spec_id,
            manifest: manifest_file.load_manifest(table.file_io()).await?,
        })?;
    }

    Ok(())
}

fn manifest_file_has_live_files(manifest_file: &spec::ManifestFile) -> bool {
    manifest_file.has_added_files() || manifest_file.has_existing_files()
}

fn manifest_file_list_entries(
    manifest_files: &[spec::ManifestFile],
) -> Result<Vec<ManifestFileListEntry>> {
    manifest_files
        .iter()
        .enumerate()
        .map(|(index, manifest_file)| manifest_file_list_entry(index, manifest_file))
        .collect()
}

fn manifest_file_list_entry(
    index: usize,
    manifest_file: &spec::ManifestFile,
) -> Result<ManifestFileListEntry> {
    let size_bytes = u64::try_from(manifest_file.manifest_length).map_err(|_| {
        BergError::InvalidManifestLength {
            path: manifest_file.manifest_path.clone(),
            length: manifest_file.manifest_length,
        }
    })?;

    Ok(ManifestFileListEntry {
        id: manifest_file_id(index),
        name: manifest_file_name(&manifest_file.manifest_path),
        path: manifest_file.manifest_path.clone(),
        content: manifest_file.content,
        size_bytes,
        partition_spec_id: manifest_file.partition_spec_id,
        added_files_count: manifest_file.added_files_count,
        existing_files_count: manifest_file.existing_files_count,
        deleted_files_count: manifest_file.deleted_files_count,
    })
}

fn find_manifest_file_by_id<'a>(
    manifest_files: &'a [spec::ManifestFile],
    requested_id: &str,
) -> Option<(String, &'a spec::ManifestFile)> {
    manifest_files
        .iter()
        .enumerate()
        .find_map(|(index, manifest_file)| {
            manifest_file_matches_id(index, manifest_file, requested_id)
                .then(|| (manifest_file_id(index), manifest_file))
        })
}

fn manifest_file_matches_id(
    index: usize,
    manifest_file: &spec::ManifestFile,
    requested_id: &str,
) -> bool {
    let id = manifest_file_id(index);
    let name = manifest_file_name(&manifest_file.manifest_path);
    let stem = manifest_file_stem(&name);

    requested_id == id || requested_id == name || requested_id == stem
}

fn manifest_file_ids(manifest_files: &[spec::ManifestFile]) -> Vec<String> {
    manifest_files
        .iter()
        .enumerate()
        .map(|(index, _)| manifest_file_id(index))
        .collect()
}

fn manifest_file_id(index: usize) -> String {
    format!("m{}", index + 1)
}

fn manifest_file_name(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn manifest_file_stem(name: &str) -> &str {
    name.strip_suffix(".avro").unwrap_or(name)
}

fn manifest_partition_metadata(
    manifest_file: &spec::ManifestFile,
    partition_spec: Option<&spec::PartitionSpec>,
) -> Vec<ManifestPartitionMetadataSummary> {
    manifest_file
        .partitions
        .as_ref()
        .map_or_else(Vec::new, |fields| {
            fields
                .iter()
                .enumerate()
                .map(|(index, field_summary)| {
                    let partition_field = partition_spec.and_then(|spec| spec.fields().get(index));

                    ManifestPartitionMetadataSummary {
                        field_name: partition_field
                            .map_or_else(|| format!("<field:{index}>"), |field| field.name.clone()),
                        field_id: partition_field.map(|field| field.field_id),
                        has_contains_nan: field_summary.contains_nan.is_some(),
                        has_lower_bound: field_summary.lower_bound.is_some(),
                        has_upper_bound: field_summary.upper_bound.is_some(),
                    }
                })
                .collect()
        })
}

fn manifest_column_metadata(
    manifest: &spec::Manifest,
    schema: &spec::Schema,
) -> Vec<ManifestColumnMetadataSummary> {
    let mut accumulators = BTreeMap::<i32, ColumnMetadataAccumulator>::new();
    let column_paths = schema_field_paths(schema);

    for entry in live_manifest_entries(manifest) {
        let data_file = entry.data_file();
        add_column_metadata_keys(
            &mut accumulators,
            data_file.column_sizes().keys(),
            COLUMN_METADATA_COLUMN_SIZES,
        );
        add_column_metadata_keys(
            &mut accumulators,
            data_file.value_counts().keys(),
            COLUMN_METADATA_VALUE_COUNTS,
        );
        add_column_metadata_keys(
            &mut accumulators,
            data_file.null_value_counts().keys(),
            COLUMN_METADATA_NULL_VALUE_COUNTS,
        );
        add_column_metadata_keys(
            &mut accumulators,
            data_file.nan_value_counts().keys(),
            COLUMN_METADATA_NAN_VALUE_COUNTS,
        );
        add_column_metadata_keys(
            &mut accumulators,
            data_file.lower_bounds().keys(),
            COLUMN_METADATA_LOWER_BOUNDS,
        );
        add_column_metadata_keys(
            &mut accumulators,
            data_file.upper_bounds().keys(),
            COLUMN_METADATA_UPPER_BOUNDS,
        );
    }

    accumulators
        .into_iter()
        .map(|(field_id, accumulator)| accumulator.into_summary(field_id, &column_paths))
        .collect()
}

fn schema_field_paths(schema: &spec::Schema) -> BTreeMap<i32, String> {
    let mut paths = BTreeMap::new();

    for field in schema.as_struct().fields() {
        collect_schema_field_paths(&mut paths, field, &field.name);
    }

    paths
}

fn collect_schema_field_paths(
    paths: &mut BTreeMap<i32, String>,
    field: &spec::NestedFieldRef,
    path: &str,
) {
    paths.insert(field.id, path.to_string());

    match field.field_type.as_ref() {
        spec::Type::Struct(struct_type) => {
            for child in struct_type.fields() {
                collect_schema_field_paths(paths, child, &format!("{path}.{}", child.name));
            }
        }
        spec::Type::List(list_type) => {
            collect_schema_field_paths(paths, &list_type.element_field, &format!("{path}[]"));
        }
        spec::Type::Map(map_type) => {
            collect_schema_field_paths(paths, &map_type.key_field, &format!("{path}{{}}.key"));
            collect_schema_field_paths(
                paths,
                &map_type.value_field,
                &map_value_schema_path(path, map_type.value_field.field_type.as_ref()),
            );
        }
        spec::Type::Primitive(_) => {}
    }
}

fn map_value_schema_path(path: &str, value_type: &spec::Type) -> String {
    match value_type {
        spec::Type::Struct(_) | spec::Type::List(_) | spec::Type::Map(_) => format!("{path}{{}}"),
        spec::Type::Primitive(_) => format!("{path}{{}}.value"),
    }
}

fn add_column_metadata_keys<'a, I>(
    accumulators: &mut BTreeMap<i32, ColumnMetadataAccumulator>,
    field_ids: I,
    field: u8,
) where
    I: IntoIterator<Item = &'a i32>,
{
    for field_id in field_ids {
        accumulators.entry(*field_id).or_default().fields |= field;
    }
}

const COLUMN_METADATA_COLUMN_SIZES: u8 = 1 << 0;
const COLUMN_METADATA_VALUE_COUNTS: u8 = 1 << 1;
const COLUMN_METADATA_NULL_VALUE_COUNTS: u8 = 1 << 2;
const COLUMN_METADATA_NAN_VALUE_COUNTS: u8 = 1 << 3;
const COLUMN_METADATA_LOWER_BOUNDS: u8 = 1 << 4;
const COLUMN_METADATA_UPPER_BOUNDS: u8 = 1 << 5;

#[derive(Debug, Default)]
struct ColumnMetadataAccumulator {
    fields: u8,
}

impl ColumnMetadataAccumulator {
    fn into_summary(
        self,
        field_id: i32,
        column_paths: &BTreeMap<i32, String>,
    ) -> ManifestColumnMetadataSummary {
        ManifestColumnMetadataSummary {
            column_name: column_paths
                .get(&field_id)
                .map_or_else(|| format!("<field:{field_id}>"), ToString::to_string),
            field_id,
            metadata_fields: column_metadata_field_names(self.fields),
        }
    }
}

fn column_metadata_field_names(fields: u8) -> Vec<String> {
    [
        (COLUMN_METADATA_COLUMN_SIZES, "column_sizes"),
        (COLUMN_METADATA_VALUE_COUNTS, "value_counts"),
        (COLUMN_METADATA_NULL_VALUE_COUNTS, "null_value_counts"),
        (COLUMN_METADATA_NAN_VALUE_COUNTS, "nan_value_counts"),
        (COLUMN_METADATA_LOWER_BOUNDS, "lower_bounds"),
        (COLUMN_METADATA_UPPER_BOUNDS, "upper_bounds"),
    ]
    .into_iter()
    .filter_map(|(field, name)| ((fields & field) != 0).then_some(name.to_string()))
    .collect()
}

fn live_manifest_entries(
    manifest: &spec::Manifest,
) -> impl Iterator<Item = &spec::ManifestEntry> + '_ {
    manifest
        .entries()
        .iter()
        .map(std::convert::AsRef::as_ref)
        .filter(|entry| entry.is_alive())
}

fn live_data_file_entries(
    manifest: &spec::Manifest,
) -> impl Iterator<Item = &spec::ManifestEntry> + '_ {
    live_manifest_entries(manifest).filter(|entry| entry.content_type() == DataContentType::Data)
}

fn manifest_files_size_bytes(manifest_files: &[spec::ManifestFile]) -> Result<u64> {
    manifest_files
        .iter()
        .try_fold(0_u64, |total, manifest_file| {
            let manifest_length = u64::try_from(manifest_file.manifest_length).map_err(|_| {
                BergError::InvalidManifestLength {
                    path: manifest_file.manifest_path.clone(),
                    length: manifest_file.manifest_length,
                }
            })?;

            Ok(total.saturating_add(manifest_length))
        })
}

fn partition_stats_from_accumulators(
    partition_accumulators: BTreeMap<(i32, String), PartitionAccumulator>,
    target_file_size_bytes: u64,
) -> Vec<CurrentTablePartitionStats> {
    partition_accumulators
        .into_iter()
        .map(|((partition_spec_id, partition), mut accumulator)| {
            accumulator.file_sizes.sort_unstable();
            let buckets = data_file_size_buckets(&accumulator.file_sizes, target_file_size_bytes);

            CurrentTablePartitionStats {
                partition_spec_id,
                partition,
                file_count: accumulator.file_sizes.len() as u64,
                total_size_bytes: accumulator.total_size_bytes,
                buckets,
            }
        })
        .collect()
}

#[derive(Debug, Default)]
struct PartitionAccumulator {
    file_sizes: Vec<u64>,
    total_size_bytes: u64,
}

impl PartitionAccumulator {
    fn add_file(&mut self, file_size_bytes: u64) {
        self.file_sizes.push(file_size_bytes);
        self.total_size_bytes = self.total_size_bytes.saturating_add(file_size_bytes);
    }
}

fn partition_path(
    partition_spec: &spec::PartitionSpec,
    partition_type: &spec::StructType,
    partition: &spec::Struct,
) -> String {
    if partition_spec.is_unpartitioned() {
        return "unpartitioned".to_string();
    }

    let field_types = partition_type.fields();
    let mut path_parts = Vec::with_capacity(partition_spec.fields().len());
    for (index, field) in partition_spec.fields().iter().enumerate() {
        path_parts.push(format!(
            "{}={}",
            field.name,
            field
                .transform
                .to_human_string(&field_types[index].field_type, partition[index].as_ref())
        ));
    }

    let partition_path = path_parts.join("/");
    if partition_path.is_empty() {
        "unpartitioned".to_string()
    } else {
        partition_path
    }
}

fn total_size_bytes(values: &[u64]) -> u64 {
    values
        .iter()
        .fold(0_u64, |total, size| total.saturating_add(*size))
}

fn target_file_size_bytes(properties: &HashMap<String, String>) -> u64 {
    properties
        .get(spec::TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(spec::TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT as u64)
}

fn table_metadata_updated_at(timestamp_ms: i64) -> Result<OffsetDateTime> {
    timestamp_ms_to_utc(timestamp_ms)
        .map_err(|()| BergError::InvalidTableMetadataTimestamp { timestamp_ms })
}

fn snapshot_updated_at(snapshot_id: i64, timestamp_ms: i64) -> Result<OffsetDateTime> {
    timestamp_ms_to_utc(timestamp_ms).map_err(|()| BergError::InvalidSnapshotTimestamp {
        snapshot_id,
        timestamp_ms,
    })
}

fn timestamp_ms_to_utc(timestamp_ms: i64) -> std::result::Result<OffsetDateTime, ()> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000).map_err(|_| ())
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

    Some(u128_to_u64_saturating(average))
}

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
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
    let total_size_bytes = total_size_bytes(sorted_values);
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

    let below_target_start = target_file_size_bytes / 4;
    let near_target_start = target_file_size_bytes.saturating_mul(3) / 4;
    let above_target_start = target_file_size_bytes.saturating_mul(5) / 4;
    let oversized_start = target_file_size_bytes.saturating_mul(2);
    let candidates = [
        ("< 16 MiB".to_string(), 0, Some(16 * MIB)),
        ("16-64 MiB".to_string(), 16 * MIB, Some(64 * MIB)),
        (
            "64 MiB-25% target".to_string(),
            64 * MIB,
            Some(below_target_start),
        ),
        (
            "25-75% target".to_string(),
            below_target_start,
            Some(near_target_start),
        ),
        (
            "75-125% target".to_string(),
            near_target_start,
            Some(above_target_start),
        ),
        (
            "125-200% target".to_string(),
            above_target_start,
            Some(oversized_start),
        ),
        ("> 200% target".to_string(), oversized_start, None),
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

    u128_to_u64_saturating(rounded)
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

    u128_to_u64_saturating(interpolated / denominator)
}

async fn metadata_json_size(
    input_file: &InputFile,
    stored_size_bytes: u64,
    stored_file_compressed: bool,
) -> Result<MetadataJsonDecodedSize> {
    if !stored_file_compressed {
        return Ok(MetadataJsonDecodedSize {
            stored_file_compressed,
            decoded_size_bytes: stored_size_bytes,
        });
    }

    let reader = input_file.reader().await?;
    let mut decoder = GzDecoder::new(CountingWriter::default());

    let mut chunk_start = 0;
    while chunk_start < stored_size_bytes {
        let chunk_end = chunk_start
            .saturating_add(METADATA_JSON_READ_CHUNK_SIZE_BYTES)
            .min(stored_size_bytes);
        let chunk = reader.read(chunk_start..chunk_end).await?;
        write_gzip_chunk(&mut decoder, &chunk)?;
        chunk_start = chunk_end;
    }

    Ok(MetadataJsonDecodedSize {
        stored_file_compressed,
        decoded_size_bytes: finish_gzip_uncompressed_size(decoder)?,
    })
}

fn write_gzip_chunk(decoder: &mut GzDecoder<CountingWriter>, chunk: &[u8]) -> Result<()> {
    decoder.write_all(chunk).map_err(iceberg::Error::from)?;

    Ok(())
}

fn finish_gzip_uncompressed_size(decoder: GzDecoder<CountingWriter>) -> Result<u64> {
    Ok(decoder
        .finish()
        .map_err(iceberg::Error::from)?
        .bytes_written)
}

fn is_compressed_metadata_json(path: &str) -> bool {
    let path = path.to_ascii_lowercase();

    path.ends_with(".gz.metadata.json") || path.ends_with(".metadata.json.gz")
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
    use std::collections::{BTreeMap, HashMap};
    use std::io::Write;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use iceberg::TableIdent;
    use iceberg::io::FileIO;
    use iceberg::table::Table;
    use parquet::arrow::ArrowWriter;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, FieldSummary, FormatVersion,
        Literal, Manifest, ManifestContentType, ManifestEntry, ManifestFile, ManifestMetadata,
        ManifestStatus, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema,
        SortOrder, Struct, TableMetadataBuilder, Transform, Type,
    };
    use time::OffsetDateTime;

    use super::{
        BoundPrecision, COLUMN_METADATA_COLUMN_SIZES, COLUMN_METADATA_LOWER_BOUNDS,
        COLUMN_METADATA_NULL_VALUE_COUNTS, COLUMN_METADATA_VALUE_COUNTS, CandidateDeleteStatus,
        CandidateFile, ColumnMetadataAccumulator, CurrentMetricsMode, CurrentTableMaxAnalysis,
        DataFileSizeDistribution, DeleteFileInfo, DeleteImpact, MaxConfidence,
        PartitionAccumulator, QualifiedTableIdent, ReadCompleteness, RestCatalogConfig,
        TypeCompatibility, collect_delete_files, credential_from_env_output,
        data_file_size_buckets, data_file_size_distribution, find_manifest_file_by_id,
        is_compressed_metadata_json, manifest_file_list_entries, manifest_partition_metadata,
        max_precision, metadata_json_size, parse_catalog_property, partition_path,
        partition_stats_from_accumulators, read_parquet_position_delete_file_paths,
        resolve_current_column_path, rounded_average, schema_field_paths,
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

    #[tokio::test]
    async fn computes_metadata_json_uncompressed_size_from_stream() {
        let metadata = br#"{"format-version":2}"#;
        let metadata_len = u64::try_from(metadata.len()).expect("metadata length fits in u64");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(metadata).expect("write gzip metadata");
        let compressed = encoder.finish().expect("finish gzip metadata");
        let file_io = FileIO::new_with_memory();
        let plain_path = "memory://table/metadata/00001.metadata.json";
        let compressed_path = "memory://table/metadata/00002.gz.metadata.json";

        file_io
            .new_output(plain_path)
            .expect("plain output file")
            .write(metadata.to_vec().into())
            .await
            .expect("write plain metadata");
        file_io
            .new_output(compressed_path)
            .expect("compressed output file")
            .write(compressed.clone().into())
            .await
            .expect("write compressed metadata");

        let plain_input = file_io.new_input(plain_path).expect("plain input file");
        let plain_stored_size = plain_input.metadata().await.expect("plain metadata").size;
        let plain_size = metadata_json_size(
            &plain_input,
            plain_stored_size,
            is_compressed_metadata_json(plain_path),
        )
        .await
        .expect("plain metadata size");

        let compressed_input = file_io
            .new_input(compressed_path)
            .expect("compressed input file");
        let compressed_stored_size = compressed_input
            .metadata()
            .await
            .expect("compressed metadata")
            .size;
        let compressed_size = metadata_json_size(
            &compressed_input,
            compressed_stored_size,
            is_compressed_metadata_json(compressed_path),
        )
        .await
        .expect("compressed metadata size");

        assert_eq!(Some(&0x1F), compressed.first());
        assert_eq!(Some(&0x8B), compressed.get(1));
        assert!(is_compressed_metadata_json(
            "s3://bucket/table/metadata/00001.gz.metadata.json"
        ));
        assert!(is_compressed_metadata_json(
            "s3://bucket/table/metadata/00001.metadata.json.gz"
        ));
        assert!(!is_compressed_metadata_json(
            "s3://bucket/table/metadata/00001.metadata.json"
        ));
        assert!(!plain_size.stored_file_compressed);
        assert_eq!(metadata_len, plain_size.decoded_size_bytes);
        assert!(compressed_size.stored_file_compressed);
        assert_eq!(metadata_len, compressed_size.decoded_size_bytes);
    }

    #[test]
    fn builds_manifest_file_list_entries() {
        let manifest_files = [manifest_file(
            "s3://bucket/path/manifest.avro",
            Some(3),
            Some(2),
        )];

        let entries = manifest_file_list_entries(&manifest_files).expect("manifest entries");

        assert_eq!(1, entries.len());
        assert_eq!("m1", entries[0].id);
        assert_eq!("manifest.avro", entries[0].name);
        assert_eq!("s3://bucket/path/manifest.avro", entries[0].path);
        assert_eq!(1024, entries[0].size_bytes);
        assert_eq!(Some(3), entries[0].added_files_count);
        assert_eq!(Some(2), entries[0].existing_files_count);
    }

    #[test]
    fn finds_manifest_file_by_short_id_or_name() {
        let first_manifest = manifest_file("s3://bucket/first.avro", Some(0), Some(0));
        let second_manifest = manifest_file("s3://bucket/second.avro", Some(0), Some(0));
        let manifest_files = [first_manifest, second_manifest];

        let (id, manifest_file) =
            find_manifest_file_by_id(&manifest_files, "m2").expect("manifest by id");
        assert_eq!("m2", id);
        assert_eq!("s3://bucket/second.avro", manifest_file.manifest_path);

        let (id, manifest_file) =
            find_manifest_file_by_id(&manifest_files, "first").expect("manifest by stem");
        assert_eq!("m1", id);
        assert_eq!("s3://bucket/first.avro", manifest_file.manifest_path);
    }

    #[test]
    fn summarizes_manifest_partition_metadata_presence() {
        let schema = Arc::new(partition_test_schema());
        let partition_spec = PartitionSpec::builder(schema)
            .with_spec_id(7)
            .add_partition_field("org_id", "org_id", Transform::Identity)
            .expect("valid identity partition field")
            .add_partition_field("day", "day_bucket", Transform::Bucket(16))
            .expect("valid bucket partition field")
            .build()
            .expect("valid partition spec");
        let mut manifest_file = manifest_file("live.avro", Some(1), Some(0));
        manifest_file.partition_spec_id = 7;
        manifest_file.partitions = Some(vec![
            FieldSummary {
                contains_null: false,
                contains_nan: Some(false),
                lower_bound: Some(vec![1].into()),
                upper_bound: None,
            },
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: None,
                upper_bound: Some(vec![2].into()),
            },
        ]);

        let metadata = manifest_partition_metadata(&manifest_file, Some(&partition_spec));

        assert_eq!(2, metadata.len());
        assert_eq!("org_id", metadata[0].field_name);
        assert_eq!(Some(1000), metadata[0].field_id);
        assert!(metadata[0].has_contains_nan);
        assert!(metadata[0].has_lower_bound);
        assert!(!metadata[0].has_upper_bound);
        assert_eq!("day_bucket", metadata[1].field_name);
        assert_eq!(Some(1001), metadata[1].field_id);
        assert!(!metadata[1].has_contains_nan);
        assert!(!metadata[1].has_lower_bound);
        assert!(metadata[1].has_upper_bound);
    }

    #[test]
    fn summarizes_manifest_column_metadata_presence() {
        let schema = partition_test_schema();
        let column_paths = schema_field_paths(&schema);

        let summary = ColumnMetadataAccumulator {
            fields: COLUMN_METADATA_COLUMN_SIZES
                | COLUMN_METADATA_VALUE_COUNTS
                | COLUMN_METADATA_NULL_VALUE_COUNTS
                | COLUMN_METADATA_LOWER_BOUNDS,
        }
        .into_summary(1, &column_paths);
        let unknown_summary = ColumnMetadataAccumulator::default().into_summary(99, &column_paths);

        assert_eq!("org_id", summary.column_name);
        assert_eq!(1, summary.field_id);
        assert_eq!(
            vec![
                "column_sizes".to_string(),
                "value_counts".to_string(),
                "null_value_counts".to_string(),
                "lower_bounds".to_string()
            ],
            summary.metadata_fields
        );
        assert_eq!("<field:99>", unknown_summary.column_name);
    }

    #[test]
    fn formats_unpartitioned_partition_path() {
        let schema = partition_test_schema();
        let partition_spec = PartitionSpec::unpartition_spec();
        let partition_type = partition_spec
            .partition_type(&schema)
            .expect("valid partition type");

        assert_eq!(
            "unpartitioned",
            partition_path(&partition_spec, &partition_type, &Struct::empty())
        );
    }

    #[test]
    fn formats_multi_field_partition_path() {
        let schema = partition_test_schema();
        let partition_spec = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(7)
            .add_partition_field("org_id", "org_id", Transform::Identity)
            .expect("valid identity partition field")
            .add_partition_field("day", "day_bucket", Transform::Bucket(16))
            .expect("valid bucket partition field")
            .add_partition_field("level", "level_prefix", Transform::Truncate(3))
            .expect("valid truncate partition field")
            .build()
            .expect("valid partition spec");
        let partition_type = partition_spec
            .partition_type(&schema)
            .expect("valid partition type");
        let partition = Struct::from_iter([
            Some(Literal::Primitive(PrimitiveLiteral::Long(123))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(7))),
            Some(Literal::Primitive(PrimitiveLiteral::String(
                "pro".to_string(),
            ))),
        ]);

        assert_eq!(
            "org_id=123/day_bucket=7/level_prefix=pro",
            partition_path(&partition_spec, &partition_type, &partition)
        );
    }

    #[test]
    fn builds_partition_stats_grouped_by_spec_and_partition() {
        let mib = 1024 * 1024;
        let mut accumulators = BTreeMap::<(i32, String), PartitionAccumulator>::new();
        accumulators
            .entry((7, "org_id=123".to_string()))
            .or_default()
            .add_file(8 * mib);
        accumulators
            .entry((7, "org_id=123".to_string()))
            .or_default()
            .add_file(32 * mib);
        accumulators
            .entry((8, "org_id=123".to_string()))
            .or_default()
            .add_file(400 * mib);

        let partitions = partition_stats_from_accumulators(accumulators, 512 * mib);

        assert_eq!(2, partitions.len());
        assert_eq!(7, partitions[0].partition_spec_id);
        assert_eq!("org_id=123", partitions[0].partition);
        assert_eq!(2, partitions[0].file_count);
        assert_eq!(40 * mib, partitions[0].total_size_bytes);
        assert_eq!(1, partitions[0].buckets[0].file_count);
        assert_eq!(1, partitions[0].buckets[1].file_count);
        assert_eq!(8, partitions[1].partition_spec_id);
        assert_eq!("org_id=123", partitions[1].partition);
        assert_eq!(1, partitions[1].file_count);
        assert_eq!(400 * mib, partitions[1].total_size_bytes);
        assert_eq!(1, partitions[1].buckets[4].file_count);
    }

    #[test]
    fn resolves_table_max_column_paths_through_structs_only() {
        let schema = Schema::builder()
            .with_fields([
                NestedField::optional(
                    1,
                    "profile",
                    Type::Struct(crate::spec::StructType::new(vec![
                        NestedField::optional(2, "email", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::optional(
                    3,
                    "tags",
                    Type::List(crate::spec::ListType::new(
                        NestedField::list_element(4, Type::Primitive(PrimitiveType::String), false)
                            .into(),
                    )),
                )
                .into(),
                NestedField::optional(5, "payload", Type::Primitive(PrimitiveType::Binary)).into(),
            ])
            .build()
            .expect("valid schema");

        let resolved = resolve_current_column_path(&schema, "profile.email")
            .expect("nested struct primitive resolves");
        assert_eq!(2, resolved.field_id);
        assert_eq!(Some(PrimitiveType::String), resolved.primitive_type);
        assert!(resolved.unsupported_reason.is_none());

        let list_result =
            resolve_current_column_path(&schema, "tags").expect("list target resolves");
        assert!(
            list_result
                .unsupported_reason
                .expect("list unsupported reason")
                .contains("list fields")
        );

        let binary_result =
            resolve_current_column_path(&schema, "payload").expect("binary target resolves");
        assert!(
            binary_result
                .unsupported_reason
                .expect("binary unsupported reason")
                .contains("binary fields")
        );

        assert!(resolve_current_column_path(&schema, "email").is_err());
    }

    #[test]
    fn table_max_ignores_missing_lower_bounds_for_confidence() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields([NestedField::required(
                    1,
                    "event_id",
                    Type::Primitive(PrimitiveType::Long),
                )
                .into()])
                .build()
                .expect("valid schema"),
        );
        let manifest = Manifest::new(
            ManifestMetadata {
                schema_id: 0,
                schema: schema.clone(),
                partition_spec: PartitionSpec::builder(schema.clone())
                    .build()
                    .expect("valid partition spec"),
                format_version: FormatVersion::V2,
                content: ManifestContentType::Data,
            },
            vec![
                data_manifest_entry("s3://warehouse/table/data/1.parquet", 10),
                data_manifest_entry("s3://warehouse/table/data/2.parquet", 20),
            ],
        );
        let resolution =
            resolve_current_column_path(&schema, "event_id").expect("event_id resolves");
        let mut analysis = CurrentTableMaxAnalysis::new(
            42,
            OffsetDateTime::from_unix_timestamp(1_777_999_300).expect("valid timestamp"),
            "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            "event_id".to_string(),
            resolution,
            CurrentMetricsMode {
                evidence: "Iceberg default".to_string(),
                value: "truncate(16)".to_string(),
            },
        );

        analysis.analyze_data_manifest(&manifest);
        analysis.finish_delete_impact(&vec![
            CandidateDeleteStatus::default();
            analysis.max_candidates.len()
        ]);
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(Some("20".to_string()), result.metadata_max);
        assert_eq!(MaxConfidence::High, result.max_confidence);
        assert_eq!(0, result.data_files_without_upper_bound);
        assert_eq!(0, result.upper_bound_decode_failures);
    }

    #[test]
    fn table_max_explains_unreferenced_position_deletes_pruned_by_sequence() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields([NestedField::required(
                    1,
                    "event_id",
                    Type::Primitive(PrimitiveType::Long),
                )
                .into()])
                .build()
                .expect("valid schema"),
        );
        let resolution =
            resolve_current_column_path(&schema, "event_id").expect("event_id resolves");
        let mut analysis = CurrentTableMaxAnalysis::new(
            42,
            OffsetDateTime::from_unix_timestamp(1_777_999_300).expect("valid timestamp"),
            "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            "event_id".to_string(),
            resolution,
            CurrentMetricsMode {
                evidence: "Iceberg default".to_string(),
                value: "truncate(16)".to_string(),
            },
        );
        analysis.max_candidates.push(CandidateFile {
            path: "s3://warehouse/table/data/max.parquet".to_string(),
            sequence_number: Some(10),
            partition_spec_id: 0,
            partition: Struct::empty(),
        });
        let delete_file = DeleteFileInfo {
            content_type: DataContentType::PositionDeletes,
            path: "s3://warehouse/table/delete/old-delete.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            sequence_number: Some(9),
            partition_spec_id: 0,
            partition_spec_is_unpartitioned: true,
            partition: Struct::empty(),
            referenced_data_file: None,
        };
        let mut statuses = vec![CandidateDeleteStatus::default(); analysis.max_candidates.len()];

        let applicable =
            analysis.prefilter_unreferenced_position_delete_file(&delete_file, &mut statuses);

        assert!(applicable.is_empty());
        assert_eq!(1, analysis.position_delete_files_not_applicable_by_sequence);
        assert_eq!(0, analysis.position_delete_files_requiring_file_path_reads);
        assert_eq!(
            0,
            analysis.position_delete_files_applicable_to_max_candidates
        );
    }

    #[tokio::test]
    async fn table_max_reports_delete_inventory_even_when_max_is_unavailable() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let mut analysis = table_max_analysis(&schema);
        let manifest = test_manifest(
            schema.clone(),
            vec![test_data_entry(
                "s3://warehouse/table/data/no-bound.parquet",
                1,
                None,
            )],
        );
        let delete_files = [
            equality_delete_file(Some(1), 0, Struct::empty()),
            position_delete_file(
                Some(2),
                DataFileFormat::Puffin,
                Some("s3://warehouse/table/data/no-bound.parquet".to_string()),
            ),
            position_delete_file(None, DataFileFormat::Parquet, None),
        ];

        analysis.analyze_data_manifest(&manifest);
        analysis.analyze_delete_files(&table, &delete_files).await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(None, result.metadata_max);
        assert_eq!(MaxConfidence::Unavailable, result.max_confidence);
        assert_eq!(1, result.data_files_without_upper_bound);
        assert_eq!(1, result.equality_delete_files);
        assert_eq!(2, result.position_delete_files);
        assert_eq!(1, result.position_delete_files_with_referenced_data_file);
        assert_eq!(Some(2), result.position_delete_sequence_number_min);
        assert_eq!(Some(2), result.position_delete_sequence_number_max);
        assert_eq!(1, result.position_delete_files_without_sequence_number);
        assert_eq!(0, result.position_delete_files_requiring_file_path_reads);
        assert_eq!(
            DeleteImpact::NotApplicable,
            result.max_equality_delete_impact
        );
        assert_eq!(
            DeleteImpact::NotApplicable,
            result.max_position_delete_impact
        );
    }

    #[tokio::test]
    async fn table_max_reports_delete_inventory_with_no_live_data_files() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let mut analysis = table_max_analysis(&schema);
        let delete_files = [
            equality_delete_file(Some(1), 0, Struct::empty()),
            position_delete_file(Some(1), DataFileFormat::Parquet, None),
        ];

        analysis.analyze_delete_files(&table, &delete_files).await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(None, result.metadata_max);
        assert_eq!(MaxConfidence::Unavailable, result.max_confidence);
        assert_eq!(0, result.data_file_metadata_entries_scanned);
        assert_eq!(1, result.equality_delete_files);
        assert_eq!(1, result.position_delete_files);
        assert_eq!(0, result.position_delete_files_requiring_file_path_reads);
        assert_eq!(
            DeleteImpact::NotApplicable,
            result.max_equality_delete_impact
        );
        assert_eq!(
            DeleteImpact::NotApplicable,
            result.max_position_delete_impact
        );
    }

    #[tokio::test]
    async fn table_max_reports_delete_inventory_with_only_zero_record_data_files() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let mut analysis = table_max_analysis(&schema);
        let manifest = test_manifest(
            schema.clone(),
            vec![test_data_entry_with_record_count(
                "s3://warehouse/table/data/zero-record.parquet",
                1,
                0,
                None,
            )],
        );
        let delete_files = [
            equality_delete_file(Some(1), 0, Struct::empty()),
            position_delete_file(Some(1), DataFileFormat::Parquet, None),
        ];

        analysis.analyze_data_manifest(&manifest);
        analysis.analyze_delete_files(&table, &delete_files).await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(None, result.metadata_max);
        assert_eq!(MaxConfidence::Unavailable, result.max_confidence);
        assert_eq!(1, result.zero_record_data_file_metadata_entries);
        assert_eq!(1, result.equality_delete_files);
        assert_eq!(1, result.position_delete_files);
        assert_eq!(0, result.position_delete_files_requiring_file_path_reads);
        assert_eq!(
            DeleteImpact::NotApplicable,
            result.max_equality_delete_impact
        );
        assert_eq!(
            DeleteImpact::NotApplicable,
            result.max_position_delete_impact
        );
    }

    #[test]
    fn table_max_reports_zero_record_delete_inventory_when_max_is_unavailable() {
        let schema = long_test_schema(true, None);
        let mut analysis = table_max_analysis(&schema);
        let manifest = test_delete_manifest(
            schema.clone(),
            vec![test_delete_entry(DataContentType::PositionDeletes, 0)],
        );
        let mut delete_files = Vec::new();

        collect_delete_files(&manifest, 0, &mut analysis, &mut delete_files);
        analysis.finish();
        let result = analysis.into_result();

        assert!(delete_files.is_empty());
        assert_eq!(None, result.metadata_max);
        assert_eq!(MaxConfidence::Unavailable, result.max_confidence);
        assert_eq!(1, result.zero_record_delete_files);
        assert_eq!(0, result.position_delete_files);
    }

    #[test]
    fn table_max_missing_upper_bound_is_partial() {
        let schema = long_test_schema(true, None);
        let mut analysis = table_max_analysis(&schema);
        let manifest = test_manifest(
            schema.clone(),
            vec![
                test_data_entry(
                    "s3://warehouse/table/data/with-bound.parquet",
                    1,
                    Some(Datum::long(10)),
                ),
                test_data_entry("s3://warehouse/table/data/no-bound.parquet", 1, None),
            ],
        );

        analysis.analyze_data_manifest(&manifest);
        finish_without_deletes(&mut analysis);
        let result = analysis.into_result();

        assert_eq!(Some("10".to_string()), result.metadata_max);
        assert_eq!(MaxConfidence::Partial, result.max_confidence);
        assert_eq!(1, result.data_files_without_upper_bound);
    }

    #[test]
    fn table_max_optional_absent_field_does_not_weaken_max() {
        let schema = long_test_schema(false, None);
        let mut analysis = table_max_analysis(&schema);
        let present_manifest = test_manifest(
            schema.clone(),
            vec![test_data_entry(
                "s3://warehouse/table/data/with-bound.parquet",
                1,
                Some(Datum::long(10)),
            )],
        );
        let absent_manifest = test_manifest(
            Arc::new(other_test_schema()),
            vec![test_data_entry(
                "s3://warehouse/table/data/field-absent.parquet",
                1,
                None,
            )],
        );

        analysis.analyze_data_manifest(&present_manifest);
        analysis.analyze_data_manifest(&absent_manifest);
        finish_without_deletes(&mut analysis);
        let result = analysis.into_result();

        assert_eq!(Some("10".to_string()), result.metadata_max);
        assert_eq!(MaxConfidence::High, result.max_confidence);
        assert_eq!(1, result.data_files_field_absent);
        assert_eq!(1, result.data_files_with_no_non_null_values);
        assert_eq!(0, result.data_files_without_upper_bound);
    }

    #[test]
    fn table_max_required_absent_field_with_existing_max_is_unknown() {
        let schema = long_test_schema(true, None);
        let mut analysis = table_max_analysis(&schema);
        let present_manifest = test_manifest(
            schema.clone(),
            vec![test_data_entry(
                "s3://warehouse/table/data/with-bound.parquet",
                1,
                Some(Datum::long(10)),
            )],
        );
        let absent_manifest = test_manifest(
            Arc::new(other_test_schema()),
            vec![test_data_entry(
                "s3://warehouse/table/data/field-absent.parquet",
                1,
                None,
            )],
        );

        analysis.analyze_data_manifest(&present_manifest);
        analysis.analyze_data_manifest(&absent_manifest);
        finish_without_deletes(&mut analysis);
        let result = analysis.into_result();

        assert_eq!(Some("10".to_string()), result.metadata_max);
        assert_eq!(MaxConfidence::Unknown, result.max_confidence);
        assert!(
            result
                .max_confidence_reasons
                .iter()
                .any(|reason| reason.contains("required field-absent"))
        );
    }

    #[test]
    fn table_max_initial_default_contributes_synthetic_bound() {
        let schema = long_test_schema(true, Some(Literal::long(100)));
        let mut analysis = table_max_analysis(&schema);
        let absent_manifest = test_manifest(
            Arc::new(other_test_schema()),
            vec![test_data_entry(
                "s3://warehouse/table/data/field-absent.parquet",
                1,
                None,
            )],
        );

        analysis.analyze_data_manifest(&absent_manifest);
        finish_without_deletes(&mut analysis);
        let result = analysis.into_result();

        assert_eq!(Some("100".to_string()), result.metadata_max);
        assert_eq!(MaxConfidence::High, result.max_confidence);
        assert_eq!(1, result.data_files_using_initial_default);
    }

    #[test]
    fn table_max_nan_initial_default_is_not_a_usable_synthetic_bound() {
        let schema = initial_default_schema(PrimitiveType::Double, Literal::double(f64::NAN), true);
        let mut analysis = table_max_analysis(&schema);
        let absent_manifest = test_manifest(
            Arc::new(other_test_schema()),
            vec![test_data_entry(
                "s3://warehouse/table/data/field-absent.parquet",
                1,
                None,
            )],
        );

        analysis.analyze_data_manifest(&absent_manifest);
        finish_without_deletes(&mut analysis);
        let result = analysis.into_result();

        assert_eq!(None, result.metadata_max);
        assert_eq!(MaxConfidence::Unavailable, result.max_confidence);
        assert_eq!(1, result.data_files_using_initial_default);
        assert_eq!(1, result.nan_upper_bounds);
        assert_eq!(0, result.upper_bound_decode_failures);
        assert!(
            result
                .max_confidence_reasons
                .iter()
                .any(|reason| reason.contains("no usable upper-bound"))
        );
        assert!(
            result
                .max_confidence_reasons
                .iter()
                .any(|reason| reason.contains("decoded as NaN"))
        );
    }

    #[test]
    fn table_max_initial_defaults_render_with_current_type_semantics() {
        for (primitive_type, initial_default, expected) in [
            (PrimitiveType::Date, Literal::date(19_723), "2024-01-01"),
            (
                PrimitiveType::Timestamp,
                Literal::timestamp(1_000),
                "1970-01-01 00:00:00.001",
            ),
            (
                PrimitiveType::Timestamptz,
                Literal::timestamptz(1_000),
                "1970-01-01 00:00:00.001 UTC",
            ),
            (
                PrimitiveType::Uuid,
                Literal::uuid_from_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").expect("valid uuid"),
                "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
            ),
        ] {
            let schema = initial_default_schema(primitive_type, initial_default, true);
            let mut analysis = table_max_analysis(&schema);
            let absent_manifest = test_manifest(
                Arc::new(other_test_schema()),
                vec![test_data_entry(
                    "s3://warehouse/table/data/field-absent.parquet",
                    1,
                    None,
                )],
            );

            analysis.analyze_data_manifest(&absent_manifest);
            finish_without_deletes(&mut analysis);
            let result = analysis.into_result();

            assert_eq!(Some(expected.to_string()), result.metadata_max);
            assert_eq!(MaxConfidence::High, result.max_confidence);
            assert_eq!(1, result.data_files_using_initial_default);
        }
    }

    #[test]
    fn table_max_nan_upper_bound_makes_computed_max_unknown() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields([NestedField::required(
                    1,
                    "event_id",
                    Type::Primitive(PrimitiveType::Double),
                )
                .into()])
                .build()
                .expect("valid schema"),
        );
        let mut analysis = max_analysis_for_column(&schema, "event_id");
        let manifest = test_manifest(
            schema.clone(),
            vec![
                test_data_entry(
                    "s3://warehouse/table/data/finite.parquet",
                    1,
                    Some(Datum::double(5.0)),
                ),
                test_data_entry(
                    "s3://warehouse/table/data/nan.parquet",
                    1,
                    Some(Datum::double(f64::NAN)),
                ),
            ],
        );

        analysis.analyze_data_manifest(&manifest);
        finish_without_deletes(&mut analysis);
        let result = analysis.into_result();

        assert_eq!(Some("5".to_string()), result.metadata_max);
        assert_eq!(MaxConfidence::Unknown, result.max_confidence);
        assert_eq!(1, result.nan_upper_bounds);
    }

    #[test]
    fn table_max_missing_nan_counts_for_float_column_is_not_silent_zero() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields([NestedField::required(
                    1,
                    "event_id",
                    Type::Primitive(PrimitiveType::Double),
                )
                .into()])
                .build()
                .expect("valid schema"),
        );
        let mut analysis = max_analysis_for_column(&schema, "event_id");
        let manifest = test_manifest(
            schema.clone(),
            vec![test_data_entry(
                "s3://warehouse/table/data/missing-nan-counts.parquet",
                1,
                Some(Datum::double(5.0)),
            )],
        );

        analysis.analyze_data_manifest(&manifest);
        finish_without_deletes(&mut analysis);
        let result = analysis.into_result();

        assert_eq!(Some("5".to_string()), result.metadata_max);
        assert_eq!(MaxConfidence::Unknown, result.max_confidence);
        assert_eq!(0, result.data_files_with_nan_values);
        assert_eq!(0, result.nan_upper_bounds);
        assert!(
            result
                .max_confidence_reasons
                .iter()
                .any(|reason| reason.contains("NaN count"))
        );
    }

    #[test]
    fn table_max_tracks_tied_candidate_files() {
        let schema = long_test_schema(true, None);
        let mut analysis = table_max_analysis(&schema);
        let manifest = test_manifest(
            schema.clone(),
            vec![
                test_data_entry(
                    "s3://warehouse/table/data/first.parquet",
                    1,
                    Some(Datum::long(10)),
                ),
                test_data_entry(
                    "s3://warehouse/table/data/second.parquet",
                    1,
                    Some(Datum::long(10)),
                ),
            ],
        );

        analysis.analyze_data_manifest(&manifest);
        finish_without_deletes(&mut analysis);
        let result = analysis.into_result();

        assert_eq!(Some("10".to_string()), result.metadata_max);
        assert_eq!(2, result.max_candidate_file_count);
    }

    #[test]
    fn table_max_safely_promotes_manifest_int_to_current_long() {
        let current_schema = long_test_schema(true, None);
        let manifest_schema = Arc::new(
            Schema::builder()
                .with_fields([NestedField::required(
                    1,
                    "event_id",
                    Type::Primitive(PrimitiveType::Int),
                )
                .into()])
                .build()
                .expect("valid schema"),
        );
        let mut analysis = table_max_analysis(&current_schema);
        let manifest = test_manifest(
            manifest_schema,
            vec![test_data_entry(
                "s3://warehouse/table/data/int-bound.parquet",
                1,
                Some(Datum::int(10)),
            )],
        );

        analysis.analyze_data_manifest(&manifest);
        finish_without_deletes(&mut analysis);
        let result = analysis.into_result();

        assert_eq!(Some("10".to_string()), result.metadata_max);
        assert_eq!(TypeCompatibility::SafelyPromoted, result.type_compatibility);
        assert_eq!(MaxConfidence::High, result.max_confidence);
    }

    #[test]
    fn table_max_unknown_collection_like_path_is_an_input_error() {
        let schema = long_test_schema(true, None);

        assert!(resolve_current_column_path(&schema, "missing[]").is_err());
        assert!(resolve_current_column_path(&schema, "missing{}.value").is_err());
    }

    #[test]
    fn table_max_precision_is_type_and_metrics_mode_aware() {
        assert_eq!(
            BoundPrecision::PossiblyTruncated,
            max_precision(true, Some(&PrimitiveType::String), "truncate(16)")
        );
        assert_eq!(
            BoundPrecision::ProbablyExact,
            max_precision(true, Some(&PrimitiveType::String), "full")
        );
        assert_eq!(
            BoundPrecision::Unknown,
            max_precision(true, Some(&PrimitiveType::String), "counts")
        );
        assert_eq!(
            BoundPrecision::Exact,
            max_precision(true, Some(&PrimitiveType::Long), "truncate(16)")
        );
        assert_eq!(
            BoundPrecision::Unavailable,
            max_precision(false, Some(&PrimitiveType::String), "full")
        );
    }

    #[test]
    fn table_max_equality_delete_affecting_all_candidates_lowers_confidence() {
        let schema = long_test_schema(true, None);
        let mut analysis = table_max_analysis_with_single_candidate(&schema);
        let mut statuses = vec![CandidateDeleteStatus::default(); analysis.max_candidates.len()];
        let delete_files = [equality_delete_file(Some(2), 0, Struct::empty())];

        analysis.analyze_equality_delete_files(&delete_files, &mut statuses);
        analysis.finish_delete_impact(&statuses);
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(
            DeleteImpact::AllCandidatesPossiblyAffected,
            result.max_equality_delete_impact
        );
        assert_eq!(MaxConfidence::Lowered, result.max_confidence);
    }

    #[test]
    fn table_max_equality_delete_unknown_makes_confidence_unknown() {
        let schema = long_test_schema(true, None);
        let mut analysis = table_max_analysis_with_single_candidate(&schema);
        analysis.max_candidates[0].sequence_number = None;
        let mut statuses = vec![CandidateDeleteStatus::default(); analysis.max_candidates.len()];
        let delete_files = [equality_delete_file(Some(2), 0, Struct::empty())];

        analysis.analyze_equality_delete_files(&delete_files, &mut statuses);
        analysis.finish_delete_impact(&statuses);
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(DeleteImpact::Unknown, result.max_equality_delete_impact);
        assert_eq!(MaxConfidence::Unknown, result.max_confidence);
    }

    #[test]
    fn table_max_equality_delete_unknown_applicability_is_not_overwritten_by_later_affect() {
        let schema = long_test_schema(true, None);
        let unknown_delete = equality_delete_file(Some(2), 1, Struct::empty());
        let applicable_delete = equality_delete_file(Some(2), 0, Struct::empty());

        for (case, delete_files) in [
            (
                "unknown before applicable",
                vec![unknown_delete.clone(), applicable_delete.clone()],
            ),
            (
                "unknown after applicable",
                vec![applicable_delete, unknown_delete],
            ),
        ] {
            let mut analysis = table_max_analysis_with_single_candidate(&schema);
            let mut statuses =
                vec![CandidateDeleteStatus::default(); analysis.max_candidates.len()];

            analysis.analyze_equality_delete_files(&delete_files, &mut statuses);
            analysis.finish_delete_impact(&statuses);
            analysis.finish();
            let result = analysis.into_result();

            assert_eq!(
                1, result.max_candidate_files_with_applicable_equality_deletes,
                "{case}"
            );
            assert_eq!(
                DeleteImpact::Unknown,
                result.max_equality_delete_impact,
                "{case}"
            );
            assert_eq!(MaxConfidence::Unknown, result.max_confidence, "{case}");
        }
    }

    #[test]
    fn table_max_equality_delete_unknown_missing_sequence_is_not_overwritten_by_later_affect() {
        let schema = long_test_schema(true, None);
        let unknown_delete = equality_delete_file(None, 0, Struct::empty());
        let applicable_delete = equality_delete_file(Some(2), 0, Struct::empty());

        for (case, delete_files) in [
            (
                "unknown sequence before applicable",
                vec![unknown_delete.clone(), applicable_delete.clone()],
            ),
            (
                "unknown sequence after applicable",
                vec![applicable_delete, unknown_delete],
            ),
        ] {
            let mut analysis = table_max_analysis_with_single_candidate(&schema);
            let mut statuses =
                vec![CandidateDeleteStatus::default(); analysis.max_candidates.len()];

            analysis.analyze_equality_delete_files(&delete_files, &mut statuses);
            analysis.finish_delete_impact(&statuses);
            analysis.finish();
            let result = analysis.into_result();

            assert_eq!(
                1, result.max_candidate_files_with_applicable_equality_deletes,
                "{case}"
            );
            assert_eq!(
                DeleteImpact::Unknown,
                result.max_equality_delete_impact,
                "{case}"
            );
            assert_eq!(MaxConfidence::Unknown, result.max_confidence, "{case}");
        }
    }

    #[test]
    fn table_max_equality_delete_one_unaffected_candidate_prevents_unknown_or_lowered_confidence() {
        let schema = long_test_schema(true, None);
        let unknown_delete = equality_delete_file(Some(2), 0, Struct::empty());
        let applicable_delete = equality_delete_file(Some(2), 7, partition_value(1));

        let cases = [
            ("one unknown, one unaffected", vec![unknown_delete], None),
            (
                "one affected, one unaffected",
                vec![applicable_delete],
                Some(1),
            ),
        ];

        for (case, delete_files, candidate_sequence) in cases {
            let mut analysis = table_max_analysis_with_two_candidates(&schema);
            analysis.max_candidates[0].sequence_number = candidate_sequence;
            analysis.max_candidates[0].partition_spec_id = 7;
            analysis.max_candidates[0].partition = partition_value(1);
            analysis.max_candidates[1].sequence_number = Some(3);
            analysis.max_candidates[1].partition_spec_id = 7;
            analysis.max_candidates[1].partition = partition_value(2);
            let mut statuses =
                vec![CandidateDeleteStatus::default(); analysis.max_candidates.len()];

            analysis.analyze_equality_delete_files(&delete_files, &mut statuses);
            analysis.finish_delete_impact(&statuses);
            analysis.finish();
            let result = analysis.into_result();

            assert_ne!(
                DeleteImpact::Unknown,
                result.max_equality_delete_impact,
                "{case}"
            );
            assert_ne!(
                DeleteImpact::AllCandidatesPossiblyAffected,
                result.max_equality_delete_impact,
                "{case}"
            );
            assert_eq!(MaxConfidence::High, result.max_confidence, "{case}");
        }
    }

    #[tokio::test]
    async fn table_max_referenced_position_delete_touch_lowers_without_file_path_read() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let mut analysis = table_max_analysis_with_single_candidate(&schema);
        let delete_files = [position_delete_file(
            Some(1),
            DataFileFormat::Puffin,
            Some("s3://warehouse/table/data/max.parquet".to_string()),
        )];

        analysis.analyze_delete_files(&table, &delete_files).await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(1, result.position_delete_files_with_referenced_data_file);
        assert_eq!(0, result.position_delete_files_requiring_file_path_reads);
        assert_eq!(
            DeleteImpact::AllCandidatesTouched,
            result.max_position_delete_impact
        );
        assert_eq!(MaxConfidence::Lowered, result.max_confidence);
    }

    #[tokio::test]
    async fn table_max_position_delete_unknown_applicability_is_not_overwritten_by_later_touch() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let unknown_delete = position_delete_file(None, DataFileFormat::Parquet, None);
        let touching_delete = position_delete_file(
            Some(1),
            DataFileFormat::Puffin,
            Some("s3://warehouse/table/data/max.parquet".to_string()),
        );

        for (case, delete_files) in [
            (
                "unknown before touch",
                vec![unknown_delete.clone(), touching_delete.clone()],
            ),
            ("unknown after touch", vec![touching_delete, unknown_delete]),
        ] {
            let mut analysis = table_max_analysis_with_single_candidate(&schema);

            analysis.analyze_delete_files(&table, &delete_files).await;
            analysis.finish();
            let result = analysis.into_result();

            assert_eq!(
                1, result.position_delete_files_with_unknown_applicability,
                "{case}"
            );
            assert_eq!(
                1, result.max_candidate_files_touched_by_position_deletes,
                "{case}"
            );
            assert_eq!(
                DeleteImpact::Unknown,
                result.max_position_delete_impact,
                "{case}"
            );
            assert_eq!(MaxConfidence::Unknown, result.max_confidence, "{case}");
        }
    }

    #[tokio::test]
    async fn table_max_unreadable_position_delete_unknown_is_not_overwritten_by_later_touch() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let unreadable_delete = position_delete_file(Some(1), DataFileFormat::Parquet, None)
            .with_path("memory://delete/missing-before-touch.parquet");
        let touching_delete = position_delete_file(
            Some(1),
            DataFileFormat::Puffin,
            Some("s3://warehouse/table/data/max.parquet".to_string()),
        );
        let mut analysis = table_max_analysis_with_single_candidate(&schema);

        analysis
            .analyze_delete_files(&table, &[unreadable_delete, touching_delete])
            .await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(1, result.position_delete_files_requiring_file_path_reads);
        assert_eq!(0, result.position_delete_files_read_for_file_path);
        assert_eq!(ReadCompleteness::Incomplete, result.read_completeness);
        assert_eq!(1, result.max_candidate_files_touched_by_position_deletes);
        assert_eq!(DeleteImpact::Unknown, result.max_position_delete_impact);
        assert_eq!(MaxConfidence::Unknown, result.max_confidence);
    }

    #[tokio::test]
    async fn table_max_unsupported_position_delete_unknown_is_not_overwritten_by_later_touch() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let unsupported_delete = position_delete_file(Some(1), DataFileFormat::Orc, None);
        let touching_delete = position_delete_file(
            Some(1),
            DataFileFormat::Puffin,
            Some("s3://warehouse/table/data/max.parquet".to_string()),
        );
        let mut analysis = table_max_analysis_with_single_candidate(&schema);

        analysis
            .analyze_delete_files(&table, &[unsupported_delete, touching_delete])
            .await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(1, result.unsupported_position_delete_files);
        assert_eq!(1, result.max_candidate_files_touched_by_position_deletes);
        assert_eq!(DeleteImpact::Unknown, result.max_position_delete_impact);
        assert_eq!(MaxConfidence::Unknown, result.max_confidence);
    }

    #[tokio::test]
    async fn table_max_position_delete_one_untouched_candidate_prevents_unknown_or_lowered_confidence()
     {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let unknown_delete = position_delete_file(Some(2), DataFileFormat::Parquet, None);
        let touching_delete = position_delete_file(
            Some(2),
            DataFileFormat::Puffin,
            Some("s3://warehouse/table/data/first.parquet".to_string()),
        );
        let cases = [
            ("one unknown, one untouched", vec![unknown_delete], None),
            ("one touched, one untouched", vec![touching_delete], Some(1)),
        ];

        for (case, delete_files, candidate_sequence) in cases {
            let mut analysis = table_max_analysis_with_two_candidates(&schema);
            analysis.max_candidates[0].path = "s3://warehouse/table/data/first.parquet".to_string();
            analysis.max_candidates[0].sequence_number = candidate_sequence;
            analysis.max_candidates[1].path =
                "s3://warehouse/table/data/second.parquet".to_string();
            analysis.max_candidates[1].sequence_number = Some(3);

            analysis.analyze_delete_files(&table, &delete_files).await;
            analysis.finish();
            let result = analysis.into_result();

            assert_ne!(
                DeleteImpact::Unknown,
                result.max_position_delete_impact,
                "{case}"
            );
            assert_ne!(
                DeleteImpact::AllCandidatesTouched,
                result.max_position_delete_impact,
                "{case}"
            );
            assert_eq!(MaxConfidence::High, result.max_confidence, "{case}");
        }
    }

    #[tokio::test]
    async fn table_max_referenced_position_delete_mismatch_keeps_confidence_high() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let mut analysis = table_max_analysis_with_single_candidate(&schema);
        let delete_files = [position_delete_file(
            Some(1),
            DataFileFormat::Puffin,
            Some("s3://warehouse/table/data/other.parquet".to_string()),
        )];

        analysis.analyze_delete_files(&table, &delete_files).await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(
            1,
            result.position_delete_files_not_applicable_by_referenced_data_file
        );
        assert_eq!(0, result.position_delete_files_requiring_file_path_reads);
        assert_eq!(DeleteImpact::Unaffected, result.max_position_delete_impact);
        assert_eq!(MaxConfidence::High, result.max_confidence);
    }

    #[tokio::test]
    async fn table_max_reads_applicable_unreferenced_parquet_delete_when_candidate_absent() {
        let schema = long_test_schema(true, None);
        let file_io = FileIO::new_with_memory();
        let table = test_table(file_io.clone());
        let delete_path = "memory://delete/positions.parquet";
        write_position_delete_parquet(
            &file_io,
            delete_path,
            &["s3://warehouse/table/data/other.parquet"],
        )
        .await;
        let mut analysis = table_max_analysis_with_single_candidate(&schema);
        let delete_files =
            [position_delete_file(Some(1), DataFileFormat::Parquet, None).with_path(delete_path)];

        analysis.analyze_delete_files(&table, &delete_files).await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(1, result.position_delete_files_applicable_to_max_candidates);
        assert_eq!(1, result.position_delete_files_requiring_file_path_reads);
        assert_eq!(1, result.position_delete_files_read_for_file_path);
        assert_eq!(0, result.max_candidate_files_touched_by_position_deletes);
        assert_eq!(MaxConfidence::High, result.max_confidence);
    }

    #[tokio::test]
    async fn table_max_reads_applicable_unreferenced_parquet_delete_and_lowers_when_candidate_present()
     {
        let schema = long_test_schema(true, None);
        let file_io = FileIO::new_with_memory();
        let table = test_table(file_io.clone());
        let delete_path = "memory://delete/positions-touching.parquet";
        write_position_delete_parquet(
            &file_io,
            delete_path,
            &["s3://warehouse/table/data/max.parquet"],
        )
        .await;
        let mut analysis = table_max_analysis_with_single_candidate(&schema);
        let delete_files =
            [position_delete_file(Some(1), DataFileFormat::Parquet, None).with_path(delete_path)];

        analysis.analyze_delete_files(&table, &delete_files).await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(1, result.position_delete_files_read_for_file_path);
        assert_eq!(1, result.max_candidate_files_touched_by_position_deletes);
        assert_eq!(
            DeleteImpact::AllCandidatesTouched,
            result.max_position_delete_impact
        );
        assert_eq!(MaxConfidence::Lowered, result.max_confidence);
    }

    #[tokio::test]
    async fn table_max_unreadable_applicable_position_delete_makes_confidence_unknown() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let mut analysis = table_max_analysis_with_single_candidate(&schema);
        let delete_files = [position_delete_file(Some(1), DataFileFormat::Parquet, None)
            .with_path("memory://delete/missing.parquet")];

        analysis.analyze_delete_files(&table, &delete_files).await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(1, result.position_delete_files_requiring_file_path_reads);
        assert_eq!(0, result.position_delete_files_read_for_file_path);
        assert_eq!(ReadCompleteness::Incomplete, result.read_completeness);
        assert_eq!(MaxConfidence::Unknown, result.max_confidence);
    }

    #[tokio::test]
    async fn table_max_unreferenced_non_parquet_position_delete_makes_confidence_unknown() {
        let schema = long_test_schema(true, None);
        let table = test_table(FileIO::new_with_memory());
        let mut analysis = table_max_analysis_with_single_candidate(&schema);
        let delete_files = [position_delete_file(Some(1), DataFileFormat::Orc, None)];

        analysis.analyze_delete_files(&table, &delete_files).await;
        analysis.finish();
        let result = analysis.into_result();

        assert_eq!(1, result.unsupported_position_delete_files);
        assert_eq!(ReadCompleteness::Incomplete, result.read_completeness);
        assert_eq!(DeleteImpact::Unknown, result.max_position_delete_impact);
        assert_eq!(MaxConfidence::Unknown, result.max_confidence);
    }

    #[tokio::test]
    async fn reads_file_path_column_from_position_delete_parquet() {
        let file_io = FileIO::new_with_memory();
        let table = test_table(file_io.clone());
        let delete_path = "memory://delete/file-paths.parquet";
        write_position_delete_parquet(
            &file_io,
            delete_path,
            &[
                "s3://warehouse/table/data/first.parquet",
                "s3://warehouse/table/data/second.parquet",
            ],
        )
        .await;

        let paths = read_parquet_position_delete_file_paths(&table, delete_path)
            .await
            .expect("file_path values should read");

        assert_eq!(2, paths.len());
        assert!(paths.contains("s3://warehouse/table/data/first.parquet"));
        assert!(paths.contains("s3://warehouse/table/data/second.parquet"));
    }

    fn partition_test_schema() -> Schema {
        Schema::builder()
            .with_fields([
                NestedField::required(1, "org_id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "day", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "level", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("valid schema")
    }

    fn manifest_file(
        manifest_path: &'static str,
        added_files_count: Option<u32>,
        existing_files_count: Option<u32>,
    ) -> ManifestFile {
        ManifestFile {
            manifest_path: manifest_path.to_string(),
            manifest_length: 1024,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 42,
            added_files_count,
            existing_files_count,
            deleted_files_count: Some(0),
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    fn data_manifest_entry(path: &'static str, upper_bound: i64) -> ManifestEntry {
        test_data_entry(path, 1, Some(Datum::long(upper_bound)))
    }

    fn long_test_schema(required: bool, initial_default: Option<Literal>) -> Arc<Schema> {
        let mut field = NestedField::new(
            1,
            "event_id",
            Type::Primitive(PrimitiveType::Long),
            required,
        );
        if let Some(initial_default) = initial_default {
            field = field.with_initial_default(initial_default);
        }

        Arc::new(
            Schema::builder()
                .with_fields([field.into()])
                .build()
                .expect("valid schema"),
        )
    }

    fn initial_default_schema(
        primitive_type: PrimitiveType,
        initial_default: Literal,
        required: bool,
    ) -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_fields([NestedField::new(
                    1,
                    "event_id",
                    Type::Primitive(primitive_type),
                    required,
                )
                .with_initial_default(initial_default)
                .into()])
                .build()
                .expect("valid schema"),
        )
    }

    fn other_test_schema() -> Schema {
        Schema::builder()
            .with_fields([
                NestedField::required(2, "other", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("valid schema")
    }

    fn table_max_analysis(schema: &Arc<Schema>) -> CurrentTableMaxAnalysis {
        max_analysis_for_column(schema, "event_id")
    }

    fn max_analysis_for_column(schema: &Arc<Schema>, column: &str) -> CurrentTableMaxAnalysis {
        let resolution = resolve_current_column_path(schema, column).expect("column resolves");

        CurrentTableMaxAnalysis::new(
            42,
            OffsetDateTime::from_unix_timestamp(1_777_999_300).expect("valid timestamp"),
            "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            column.to_string(),
            resolution,
            CurrentMetricsMode {
                evidence: "Iceberg default".to_string(),
                value: "truncate(16)".to_string(),
            },
        )
    }

    fn table_max_analysis_with_single_candidate(schema: &Arc<Schema>) -> CurrentTableMaxAnalysis {
        let mut analysis = table_max_analysis(schema);
        let manifest = test_manifest(
            schema.clone(),
            vec![test_data_entry(
                "s3://warehouse/table/data/max.parquet",
                1,
                Some(Datum::long(10)),
            )],
        );
        analysis.analyze_data_manifest(&manifest);
        analysis
    }

    fn table_max_analysis_with_two_candidates(schema: &Arc<Schema>) -> CurrentTableMaxAnalysis {
        let mut analysis = table_max_analysis(schema);
        let manifest = test_manifest(
            schema.clone(),
            vec![
                test_data_entry(
                    "s3://warehouse/table/data/first.parquet",
                    1,
                    Some(Datum::long(10)),
                ),
                test_data_entry(
                    "s3://warehouse/table/data/second.parquet",
                    1,
                    Some(Datum::long(10)),
                ),
            ],
        );
        analysis.analyze_data_manifest(&manifest);
        analysis
    }

    fn test_manifest(schema: Arc<Schema>, entries: Vec<ManifestEntry>) -> Manifest {
        Manifest::new(
            ManifestMetadata {
                schema_id: 0,
                schema: schema.clone(),
                partition_spec: PartitionSpec::builder(schema)
                    .build()
                    .expect("valid partition spec"),
                format_version: FormatVersion::V2,
                content: ManifestContentType::Data,
            },
            entries,
        )
    }

    fn test_delete_manifest(schema: Arc<Schema>, entries: Vec<ManifestEntry>) -> Manifest {
        Manifest::new(
            ManifestMetadata {
                schema_id: 0,
                schema: schema.clone(),
                partition_spec: PartitionSpec::builder(schema)
                    .build()
                    .expect("valid partition spec"),
                format_version: FormatVersion::V2,
                content: ManifestContentType::Deletes,
            },
            entries,
        )
    }

    fn test_data_entry(
        path: &str,
        sequence_number: i64,
        upper_bound: Option<Datum>,
    ) -> ManifestEntry {
        test_data_entry_with_record_count(path, sequence_number, 1, upper_bound)
    }

    fn test_data_entry_with_record_count(
        path: &str,
        sequence_number: i64,
        record_count: u64,
        upper_bound: Option<Datum>,
    ) -> ManifestEntry {
        let upper_bounds =
            upper_bound.map_or_else(HashMap::new, |bound| HashMap::from([(1, bound)]));

        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .sequence_number(sequence_number)
            .data_file(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(path.to_string())
                    .file_format(DataFileFormat::Parquet)
                    .record_count(record_count)
                    .file_size_in_bytes(100)
                    .value_counts(HashMap::from([(1, 1)]))
                    .null_value_counts(HashMap::from([(1, 0)]))
                    .upper_bounds(upper_bounds)
                    .build()
                    .expect("valid data file"),
            )
            .build()
    }

    fn test_delete_entry(content_type: DataContentType, record_count: u64) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .sequence_number(2)
            .data_file(
                DataFileBuilder::default()
                    .content(content_type)
                    .file_path("s3://warehouse/table/delete/zero-record.parquet".to_string())
                    .file_format(DataFileFormat::Parquet)
                    .record_count(record_count)
                    .file_size_in_bytes(100)
                    .build()
                    .expect("valid delete file"),
            )
            .build()
    }

    fn partition_value(value: i64) -> Struct {
        Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Long(value)))])
    }

    fn finish_without_deletes(analysis: &mut CurrentTableMaxAnalysis) {
        analysis.finish_delete_impact(&vec![
            CandidateDeleteStatus::default();
            analysis.max_candidates.len()
        ]);
        analysis.finish();
    }

    fn equality_delete_file(
        sequence_number: Option<i64>,
        partition_spec_id: i32,
        partition: Struct,
    ) -> DeleteFileInfo {
        DeleteFileInfo {
            content_type: DataContentType::EqualityDeletes,
            path: "s3://warehouse/table/delete/equality.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            sequence_number,
            partition_spec_id,
            partition_spec_is_unpartitioned: partition_spec_id == 0 && partition == Struct::empty(),
            partition,
            referenced_data_file: None,
        }
    }

    fn position_delete_file(
        sequence_number: Option<i64>,
        file_format: DataFileFormat,
        referenced_data_file: Option<String>,
    ) -> DeleteFileInfo {
        DeleteFileInfo {
            content_type: DataContentType::PositionDeletes,
            path: "s3://warehouse/table/delete/positions.parquet".to_string(),
            file_format,
            sequence_number,
            partition_spec_id: 0,
            partition_spec_is_unpartitioned: true,
            partition: Struct::empty(),
            referenced_data_file,
        }
    }

    impl DeleteFileInfo {
        fn with_path(mut self, path: &str) -> Self {
            self.path = path.to_string();
            self
        }
    }

    fn test_table(file_io: FileIO) -> Table {
        let schema = Schema::builder()
            .with_fields([NestedField::required(
                1,
                "event_id",
                Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .expect("valid schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("valid table metadata builder")
        .build()
        .expect("valid table metadata")
        .metadata;

        Table::builder()
            .file_io(file_io)
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["ns", "table"]).expect("valid table ident"))
            .readonly(true)
            .build()
            .expect("valid test table")
    }

    async fn write_position_delete_parquet(file_io: &FileIO, path: &str, file_paths: &[&str]) {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));
        let positions = 0..i64::try_from(file_paths.len()).expect("test paths length fits in i64");
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(file_paths.to_vec())),
                Arc::new(Int64Array::from_iter_values(positions)),
            ],
        )
        .expect("valid position delete batch");
        let mut writer =
            ArrowWriter::try_new(Vec::new(), arrow_schema, None).expect("valid parquet writer");
        writer.write(&batch).expect("write parquet batch");
        let bytes = writer.into_inner().expect("finish parquet bytes");

        file_io
            .new_output(path)
            .expect("position delete output")
            .write(bytes.into())
            .await
            .expect("write position delete parquet");
    }
}
