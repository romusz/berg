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
use std::borrow::Borrow;
use std::collections::HashSet;

use crate::engine::{
    CurrentDataFileSizeStats, CurrentManifestFileDetail, CurrentManifestFileList,
    CurrentTablePartitionStats, CurrentTablePartitions, CurrentTableStats, DataFileSizeBucketStats,
    DataFileSizeDistribution, ManifestColumnMetadataSummary, ManifestFileListEntry,
    ManifestPartitionMetadataSummary,
};
use crate::spec::{ManifestFile, NestedFieldRef, PartitionSpec, Schema, Type};
use time::OffsetDateTime;

/// Semantic document AST shared by frontends.
///
/// This is intentionally close to a document model rather than a report model:
/// frontends can render it as GitHub-flavored Markdown, terminal widgets, HTML,
/// or another medium without recomputing Iceberg-derived semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    /// Top-level document title.
    pub title: Cell,
    /// Ordered document blocks.
    pub blocks: Vec<Block>,
}

/// Block-level semantic content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    /// Paragraph-like inline content.
    Paragraph(Cell),
    /// Ordered key/value properties.
    Properties(Vec<Property>),
    /// Tabular content.
    Table(Table),
    /// Nested section. Markdown renderers should increase heading depth for
    /// each nested section.
    Section(Section),
    /// Ordered or unordered list.
    List(List),
    /// Fenced code block.
    FencedCode(FencedCode),
    /// Horizontal rule / thematic break.
    ThematicBreak,
}

/// Nested section with its own ordered blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    /// Section heading.
    pub title: Cell,
    /// Section body blocks.
    pub blocks: Vec<Block>,
}

/// Ordered or unordered list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct List {
    /// List marker style.
    pub kind: ListKind,
    /// Ordered list items.
    pub items: Vec<ListItem>,
}

/// List marker style.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListKind {
    /// Bullet list.
    Unordered,
    /// Numbered list.
    Ordered {
        /// First rendered number.
        start: usize,
    },
}

/// One list item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListItem {
    /// Item body blocks.
    pub blocks: Vec<Block>,
}

/// Semantic key/value property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Property {
    /// Property label.
    pub label: String,
    /// Property value.
    pub value: Cell,
}

/// Semantic table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    /// Ordered column labels.
    pub columns: Vec<Cell>,
    /// Ordered rows.
    pub rows: Vec<Row>,
}

/// Semantic table row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// Ordered row cells.
    pub cells: Vec<Cell>,
}

/// Inline content container used by titles, paragraphs, properties, lists, and tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    /// Ordered inline values.
    pub values: Vec<DocumentValue>,
}

impl Cell {
    /// Build a cell from inline values.
    #[must_use]
    pub fn new(values: Vec<DocumentValue>) -> Self {
        Self { values }
    }

    /// Build a plain-text cell.
    #[must_use]
    pub fn text(value: impl Into<String>) -> Self {
        Self::new(vec![DocumentValue::Text(value.into())])
    }

    /// Build a code-like cell.
    #[must_use]
    pub fn code(value: impl Into<String>) -> Self {
        Self::new(vec![DocumentValue::Code(value.into())])
    }

    /// Build a cell containing a single semantic value.
    #[must_use]
    pub fn value(value: DocumentValue) -> Self {
        Self::new(vec![value])
    }
}

/// Fenced code block content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencedCode {
    /// Optional language tag.
    pub language: Option<String>,
    /// Code body.
    pub code: String,
}

/// Semantic inline value that each frontend renders in its own medium.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentValue {
    /// Plain text.
    Text(String),
    /// Code-like text, such as field paths, type names, or identifiers.
    Code(String),
    /// URI or URL value.
    Uri(String),
    /// Instant in time.
    Timestamp(OffsetDateTime),
    /// Instant in local time.
    LocalTimestamp(OffsetDateTime),
    /// Numeric value.
    Number(i64),
    /// Unsigned numeric value.
    Unsigned(u64),
    /// Byte size value.
    Bytes(u64),
    /// Percentage stored as thousandths of one percent.
    PercentageMillis(u64),
    /// Non-negative count.
    Count(usize),
    /// Boolean value.
    Bool(bool),
    /// Emphasized inline values.
    Emphasis(Vec<DocumentValue>),
    /// Strongly emphasized inline values.
    Strong(Vec<DocumentValue>),
    /// Link with inline label and target URI.
    Link {
        /// Link label.
        label: Vec<DocumentValue>,
        /// Link target.
        target: String,
    },
    /// Image with alt text and source URI.
    Image {
        /// Image alt text.
        alt: String,
        /// Image source.
        source: String,
    },
    /// Hard line break.
    LineBreak,
}

/// Build a semantic document view from an Iceberg schema.
#[must_use]
pub fn schema_document(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    schema: impl Borrow<Schema>,
) -> Document {
    let schema = schema.borrow();
    let identifier_ids = schema.identifier_field_ids().collect::<HashSet<_>>();
    let mut identifier_fields = Vec::new();
    let mut field_rows = Vec::new();

    flatten_fields(
        schema.as_struct().fields(),
        None,
        &identifier_ids,
        &mut identifier_fields,
        &mut field_rows,
    );

    let table_ident = table_ident.into();

    Document {
        title: Cell::new(vec![
            DocumentValue::Text("Schema: ".to_string()),
            DocumentValue::Code(table_ident),
        ]),
        blocks: vec![
            Block::Properties(vec![
                Property {
                    label: "Source endpoint".to_string(),
                    value: Cell::value(DocumentValue::Uri(source_endpoint.into())),
                },
                Property {
                    label: "Retrieved at".to_string(),
                    value: Cell::value(DocumentValue::Timestamp(retrieved_at)),
                },
                Property {
                    label: "Schema ID".to_string(),
                    value: Cell::value(DocumentValue::Number(i64::from(schema.schema_id()))),
                },
                Property {
                    label: "Identifier fields".to_string(),
                    value: separated_code_cell(identifier_fields),
                },
                Property {
                    label: "Top-level field count".to_string(),
                    value: Cell::value(DocumentValue::Count(schema.as_struct().fields().len())),
                },
                Property {
                    label: "Total field count including nested fields".to_string(),
                    value: Cell::value(DocumentValue::Count(field_rows.len())),
                },
            ]),
            Block::Section(Section {
                title: Cell::text("Fields"),
                blocks: vec![Block::Table(Table {
                    columns: vec![
                        Cell::text("Path"),
                        Cell::text("Type"),
                        Cell::text("Required"),
                        Cell::text("Field ID"),
                    ],
                    rows: field_rows,
                })],
            }),
        ],
    }
}

/// Build a semantic document view from current Iceberg table statistics.
#[must_use]
pub fn table_stats_document(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    stats: &CurrentTableStats,
) -> Document {
    let table_ident = table_ident.into();
    let total_metadata_size = stats.metadata_json_size_bytes
        + stats.manifest_list_size_bytes
        + stats.manifest_files_size_bytes;

    Document {
        title: Cell::new(vec![
            DocumentValue::Text("Table Stats: ".to_string()),
            DocumentValue::Code(table_ident),
        ]),
        blocks: vec![
            Block::Properties(table_stats_header_properties(
                source_endpoint.into(),
                retrieved_at,
                stats,
            )),
            Block::Section(Section {
                title: Cell::text("Table Files"),
                blocks: vec![Block::Properties(table_file_properties(stats))],
            }),
            Block::Section(Section {
                title: Cell::text("Metadata Files"),
                blocks: vec![Block::Properties(metadata_file_properties(
                    stats,
                    total_metadata_size,
                ))],
            }),
        ],
    }
}

/// Build a semantic document view from current Iceberg data file size statistics.
#[must_use]
pub fn data_file_size_stats_document(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    stats: &CurrentDataFileSizeStats,
) -> Document {
    let table_ident = table_ident.into();

    Document {
        title: Cell::new(vec![
            DocumentValue::Text("Data File Size Stats: ".to_string()),
            DocumentValue::Code(table_ident),
        ]),
        blocks: vec![
            Block::Properties(data_file_size_stats_header_properties(
                source_endpoint.into(),
                retrieved_at,
                stats,
            )),
            Block::Section(Section {
                title: Cell::text("Data File Sizes"),
                blocks: vec![
                    Block::Properties(data_file_size_properties(stats)),
                    Block::Section(Section {
                        title: Cell::text("Distribution"),
                        blocks: vec![Block::Table(data_file_size_distribution_table(
                            stats.distribution.as_ref(),
                        ))],
                    }),
                    Block::Section(Section {
                        title: Cell::text("Buckets"),
                        blocks: vec![Block::Table(data_file_size_bucket_table(&stats.buckets))],
                    }),
                ],
            }),
        ],
    }
}

/// Build a semantic document view from current snapshot manifest files.
#[must_use]
pub fn manifest_file_list_document(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    manifest_files: &CurrentManifestFileList,
) -> Document {
    let table_ident = table_ident.into();

    Document {
        title: Cell::new(vec![
            DocumentValue::Text("Manifest Files: ".to_string()),
            DocumentValue::Code(table_ident),
        ]),
        blocks: vec![
            Block::Properties(manifest_file_list_header_properties(
                source_endpoint.into(),
                retrieved_at,
                manifest_files,
            )),
            Block::Section(Section {
                title: Cell::text("Manifest Files"),
                blocks: manifest_file_list_blocks(&manifest_files.files),
            }),
        ],
    }
}

/// Build a semantic document view from one selected current snapshot manifest file.
#[must_use]
pub fn manifest_file_detail_document(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    detail: &CurrentManifestFileDetail,
) -> Document {
    let table_ident = table_ident.into();

    Document {
        title: Cell::new(vec![
            DocumentValue::Text("Manifest File: ".to_string()),
            DocumentValue::Code(table_ident),
            DocumentValue::Text(" ".to_string()),
            DocumentValue::Code(detail.manifest_file_id.clone()),
        ]),
        blocks: vec![
            Block::Properties(manifest_file_detail_header_properties(
                source_endpoint.into(),
                retrieved_at,
                detail,
            )),
            Block::Section(Section {
                title: Cell::text("Manifest File"),
                blocks: manifest_file_detail_blocks(&detail.manifest_file),
            }),
            Block::Section(Section {
                title: Cell::text("Partition Metadata"),
                blocks: partition_metadata_blocks(&detail.partition_metadata),
            }),
            Block::Section(Section {
                title: Cell::text("Column Metadata"),
                blocks: column_metadata_blocks(&detail.column_metadata),
            }),
        ],
    }
}

/// Build a semantic document view from the current partition spec and partition statistics.
#[must_use]
pub fn table_partitions_document(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    stats: &CurrentTablePartitions,
) -> Document {
    let table_ident = table_ident.into();

    Document {
        title: Cell::new(vec![
            DocumentValue::Text("Table Partitions: ".to_string()),
            DocumentValue::Code(table_ident),
        ]),
        blocks: vec![
            Block::Properties(table_partitions_header_properties(
                source_endpoint.into(),
                retrieved_at,
                stats,
            )),
            Block::Section(Section {
                title: Cell::text("Current Partition Spec"),
                blocks: vec![
                    Block::Properties(current_partition_spec_properties(stats)),
                    Block::Table(partition_spec_table(
                        &stats.current_schema,
                        &stats.partition_spec,
                    )),
                ],
            }),
            Block::Section(Section {
                title: Cell::text("Partitions"),
                blocks: vec![
                    Block::Properties(partition_summary_properties(stats)),
                    Block::Paragraph(Cell::text(
                        "Bucket columns contain data file counts, not bytes or percentages.",
                    )),
                    Block::Table(partition_stats_table(stats)),
                ],
            }),
        ],
    }
}

fn manifest_file_list_header_properties(
    source_endpoint: String,
    retrieved_at: OffsetDateTime,
    manifest_files: &CurrentManifestFileList,
) -> Vec<Property> {
    vec![
        Property {
            label: "Source endpoint".to_string(),
            value: Cell::value(DocumentValue::Uri(source_endpoint)),
        },
        Property {
            label: "Retrieved at".to_string(),
            value: utc_and_local_timestamp_cell(retrieved_at),
        },
        Property {
            label: "Snapshot ID".to_string(),
            value: Cell::value(DocumentValue::Number(manifest_files.snapshot_id)),
        },
        Property {
            label: "Updated at".to_string(),
            value: utc_and_local_timestamp_cell(manifest_files.snapshot_updated_at),
        },
        Property {
            label: "Manifest list".to_string(),
            value: Cell::value(DocumentValue::Uri(
                manifest_files.manifest_list_path.clone(),
            )),
        },
        Property {
            label: "Manifest files".to_string(),
            value: Cell::value(DocumentValue::Count(manifest_files.files.len())),
        },
    ]
}

fn manifest_file_detail_header_properties(
    source_endpoint: String,
    retrieved_at: OffsetDateTime,
    detail: &CurrentManifestFileDetail,
) -> Vec<Property> {
    let mut properties = vec![
        Property {
            label: "Source endpoint".to_string(),
            value: Cell::value(DocumentValue::Uri(source_endpoint)),
        },
        Property {
            label: "Retrieved at".to_string(),
            value: utc_and_local_timestamp_cell(retrieved_at),
        },
        Property {
            label: "Snapshot ID".to_string(),
            value: Cell::value(DocumentValue::Number(detail.snapshot_id)),
        },
        Property {
            label: "Updated at".to_string(),
            value: utc_and_local_timestamp_cell(detail.snapshot_updated_at),
        },
        Property {
            label: "Manifest list".to_string(),
            value: Cell::value(DocumentValue::Uri(detail.manifest_list_path.clone())),
        },
        Property {
            label: "Manifest files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(detail.manifest_file_count)),
        },
    ];

    properties.push(Property {
        label: "Manifest file ID".to_string(),
        value: Cell::code(detail.manifest_file_id.clone()),
    });

    properties
}

fn manifest_file_list_blocks(files: &[ManifestFileListEntry]) -> Vec<Block> {
    if files.is_empty() {
        return vec![Block::Paragraph(Cell::text("No manifest files found."))];
    }

    vec![Block::Table(Table {
        columns: vec![
            Cell::text("ID"),
            Cell::text("Name"),
            Cell::text("Content"),
            Cell::text("Size"),
            Cell::text("Partition spec ID"),
            Cell::text("Added files"),
            Cell::text("Existing files"),
            Cell::text("Deleted files"),
        ],
        rows: files.iter().map(manifest_file_list_row).collect(),
    })]
}

fn manifest_file_list_row(file: &ManifestFileListEntry) -> Row {
    Row {
        cells: vec![
            Cell::code(file.id.clone()),
            Cell::code(file.name.clone()),
            Cell::code(file.content.to_string()),
            Cell::value(DocumentValue::Bytes(file.size_bytes)),
            Cell::value(DocumentValue::Number(i64::from(file.partition_spec_id))),
            optional_u32_cell(file.added_files_count),
            optional_u32_cell(file.existing_files_count),
            optional_u32_cell(file.deleted_files_count),
        ],
    }
}

fn manifest_file_detail_blocks(manifest_file: &ManifestFile) -> Vec<Block> {
    vec![Block::Properties(manifest_file_properties(manifest_file))]
}

fn manifest_file_properties(manifest_file: &ManifestFile) -> Vec<Property> {
    vec![
        Property {
            label: "Path".to_string(),
            value: Cell::value(DocumentValue::Uri(manifest_file.manifest_path.clone())),
        },
        Property {
            label: "Content".to_string(),
            value: Cell::code(manifest_file.content.to_string()),
        },
        Property {
            label: "Length".to_string(),
            value: manifest_length_cell(manifest_file.manifest_length),
        },
        Property {
            label: "Partition spec ID".to_string(),
            value: Cell::value(DocumentValue::Number(i64::from(
                manifest_file.partition_spec_id,
            ))),
        },
        Property {
            label: "Sequence number".to_string(),
            value: Cell::value(DocumentValue::Number(manifest_file.sequence_number)),
        },
        Property {
            label: "Min sequence number".to_string(),
            value: Cell::value(DocumentValue::Number(manifest_file.min_sequence_number)),
        },
        Property {
            label: "Added snapshot ID".to_string(),
            value: Cell::value(DocumentValue::Number(manifest_file.added_snapshot_id)),
        },
        Property {
            label: "Added files".to_string(),
            value: optional_u32_cell(manifest_file.added_files_count),
        },
        Property {
            label: "Existing files".to_string(),
            value: optional_u32_cell(manifest_file.existing_files_count),
        },
        Property {
            label: "Deleted files".to_string(),
            value: optional_u32_cell(manifest_file.deleted_files_count),
        },
        Property {
            label: "Added rows".to_string(),
            value: optional_u64_cell(manifest_file.added_rows_count),
        },
        Property {
            label: "Existing rows".to_string(),
            value: optional_u64_cell(manifest_file.existing_rows_count),
        },
        Property {
            label: "Deleted rows".to_string(),
            value: optional_u64_cell(manifest_file.deleted_rows_count),
        },
        Property {
            label: "Partition summaries".to_string(),
            value: optional_usize_cell(manifest_file.partitions.as_ref().map(Vec::len)),
        },
        Property {
            label: "Key metadata".to_string(),
            value: optional_usize_cell(manifest_file.key_metadata.as_ref().map(Vec::len)),
        },
        Property {
            label: "First row ID".to_string(),
            value: optional_u64_cell(manifest_file.first_row_id),
        },
    ]
}

fn partition_metadata_blocks(metadata: &[ManifestPartitionMetadataSummary]) -> Vec<Block> {
    if metadata.is_empty() {
        return vec![Block::Paragraph(Cell::text("No partition metadata found."))];
    }

    vec![Block::Table(Table {
        columns: vec![
            Cell::text("Partition field"),
            Cell::text("Field ID"),
            Cell::text("Metadata"),
        ],
        rows: metadata.iter().map(partition_metadata_row).collect(),
    })]
}

fn partition_metadata_row(metadata: &ManifestPartitionMetadataSummary) -> Row {
    Row {
        cells: vec![
            Cell::code(metadata.field_name.clone()),
            metadata.field_id.map_or_else(
                || Cell::text("unknown"),
                |id| Cell::value(DocumentValue::Number(i64::from(id))),
            ),
            separated_code_cell(partition_metadata_names(metadata)),
        ],
    }
}

fn partition_metadata_names(metadata: &ManifestPartitionMetadataSummary) -> Vec<String> {
    let mut names = vec!["contains_null".to_string()];

    if metadata.has_contains_nan {
        names.push("contains_nan".to_string());
    }

    if metadata.has_lower_bound {
        names.push("lower_bound".to_string());
    }

    if metadata.has_upper_bound {
        names.push("upper_bound".to_string());
    }

    names
}

fn column_metadata_blocks(metadata: &[ManifestColumnMetadataSummary]) -> Vec<Block> {
    if metadata.is_empty() {
        return vec![Block::Paragraph(Cell::text("No column metadata found."))];
    }

    vec![Block::Table(Table {
        columns: vec![
            Cell::text("Column"),
            Cell::text("Field ID"),
            Cell::text("Metadata"),
        ],
        rows: metadata.iter().map(column_metadata_row).collect(),
    })]
}

fn column_metadata_row(metadata: &ManifestColumnMetadataSummary) -> Row {
    Row {
        cells: vec![
            Cell::code(metadata.column_name.clone()),
            Cell::value(DocumentValue::Number(i64::from(metadata.field_id))),
            separated_code_cell(metadata.metadata_fields.clone()),
        ],
    }
}

fn manifest_length_cell(length: i64) -> Cell {
    match u64::try_from(length) {
        Ok(length) => Cell::value(DocumentValue::Bytes(length)),
        Err(_) => Cell::text(format!("invalid: {length}")),
    }
}

fn table_partitions_header_properties(
    source_endpoint: String,
    retrieved_at: OffsetDateTime,
    stats: &CurrentTablePartitions,
) -> Vec<Property> {
    vec![
        Property {
            label: "Source endpoint".to_string(),
            value: Cell::value(DocumentValue::Uri(source_endpoint)),
        },
        Property {
            label: "Retrieved at".to_string(),
            value: utc_and_local_timestamp_cell(retrieved_at),
        },
        Property {
            label: "Snapshot ID".to_string(),
            value: Cell::value(DocumentValue::Number(stats.snapshot_id)),
        },
        Property {
            label: "Updated at".to_string(),
            value: utc_and_local_timestamp_cell(stats.snapshot_updated_at),
        },
        Property {
            label: "Metadata".to_string(),
            value: Cell::value(DocumentValue::Uri(stats.metadata_json_path.clone())),
        },
        Property {
            label: "Manifest list".to_string(),
            value: Cell::value(DocumentValue::Uri(stats.manifest_list_path.clone())),
        },
    ]
}

fn current_partition_spec_properties(stats: &CurrentTablePartitions) -> Vec<Property> {
    vec![
        Property {
            label: "Default spec ID".to_string(),
            value: Cell::value(DocumentValue::Number(i64::from(
                stats.partition_spec.spec_id(),
            ))),
        },
        Property {
            label: "Partitioned".to_string(),
            value: Cell::value(DocumentValue::Bool(
                !stats.partition_spec.is_unpartitioned(),
            )),
        },
        Property {
            label: "Fields".to_string(),
            value: Cell::value(DocumentValue::Count(stats.partition_spec.fields().len())),
        },
    ]
}

fn partition_summary_properties(stats: &CurrentTablePartitions) -> Vec<Property> {
    vec![
        Property {
            label: "Partitions".to_string(),
            value: Cell::value(DocumentValue::Count(stats.partitions.len())),
        },
        Property {
            label: "Data files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(stats.data_file_count)),
        },
        Property {
            label: "Total data file size".to_string(),
            value: Cell::value(DocumentValue::Bytes(stats.total_data_file_size_bytes)),
        },
        Property {
            label: "Target file size".to_string(),
            value: Cell::value(DocumentValue::Bytes(stats.target_file_size_bytes)),
        },
    ]
}

fn partition_spec_table(schema: &Schema, partition_spec: &PartitionSpec) -> Table {
    Table {
        columns: vec![
            Cell::text("Name"),
            Cell::text("Source field"),
            Cell::text("Source type"),
            Cell::text("Transform"),
            Cell::text("Source ID"),
            Cell::text("Field ID"),
        ],
        rows: partition_spec
            .fields()
            .iter()
            .map(|field| Row {
                cells: vec![
                    Cell::code(field.name.clone()),
                    Cell::code(source_field_name(schema, field.source_id)),
                    Cell::code(source_field_type(schema, field.source_id)),
                    Cell::code(field.transform.to_string()),
                    Cell::value(DocumentValue::Number(i64::from(field.source_id))),
                    Cell::value(DocumentValue::Number(i64::from(field.field_id))),
                ],
            })
            .collect(),
    }
}

fn source_field_name(schema: &Schema, source_id: i32) -> String {
    schema
        .name_by_field_id(source_id)
        .map_or_else(|| format!("<unknown:{source_id}>"), ToString::to_string)
}

fn source_field_type(schema: &Schema, source_id: i32) -> String {
    schema.field_by_id(source_id).map_or_else(
        || "unknown".to_string(),
        |field| type_summary(&field.field_type),
    )
}

fn partition_stats_table(stats: &CurrentTablePartitions) -> Table {
    let mut columns = vec![
        Cell::text("Spec ID"),
        Cell::text("Partition"),
        Cell::text("Files"),
        Cell::text("Size"),
    ];
    columns.extend(
        stats
            .bucket_labels
            .iter()
            .map(|label| Cell::text(partition_bucket_file_count_column(label))),
    );

    Table {
        columns,
        rows: stats
            .partitions
            .iter()
            .map(|partition| partition_stats_row(partition, stats.bucket_labels.len()))
            .collect(),
    }
}

fn partition_stats_row(partition: &CurrentTablePartitionStats, bucket_count: usize) -> Row {
    let mut cells = vec![
        Cell::value(DocumentValue::Number(i64::from(
            partition.partition_spec_id,
        ))),
        Cell::code(partition.partition.clone()),
        Cell::value(DocumentValue::Unsigned(partition.file_count)),
        Cell::value(DocumentValue::Bytes(partition.total_size_bytes)),
    ];

    cells.extend(
        partition
            .buckets
            .iter()
            .take(bucket_count)
            .map(|bucket| Cell::value(DocumentValue::Unsigned(bucket.file_count))),
    );
    cells.resize_with(4 + bucket_count, || Cell::value(DocumentValue::Unsigned(0)));

    Row { cells }
}

fn partition_bucket_file_count_column(label: &str) -> String {
    label.strip_suffix(" target").unwrap_or(label).to_string()
}

fn data_file_size_stats_header_properties(
    source_endpoint: String,
    retrieved_at: OffsetDateTime,
    stats: &CurrentDataFileSizeStats,
) -> Vec<Property> {
    vec![
        Property {
            label: "Source endpoint".to_string(),
            value: Cell::value(DocumentValue::Uri(source_endpoint)),
        },
        Property {
            label: "Retrieved at".to_string(),
            value: utc_and_local_timestamp_cell(retrieved_at),
        },
        Property {
            label: "Snapshot ID".to_string(),
            value: Cell::value(DocumentValue::Number(stats.snapshot_id)),
        },
        Property {
            label: "Updated at".to_string(),
            value: utc_and_local_timestamp_cell(stats.snapshot_updated_at),
        },
        Property {
            label: "Manifest list".to_string(),
            value: Cell::value(DocumentValue::Uri(stats.manifest_list_path.clone())),
        },
    ]
}

fn data_file_size_properties(stats: &CurrentDataFileSizeStats) -> Vec<Property> {
    vec![
        Property {
            label: "Total data file size".to_string(),
            value: Cell::value(DocumentValue::Bytes(stats.total_data_file_size_bytes)),
        },
        Property {
            label: "Data files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(stats.data_file_count)),
        },
        Property {
            label: "Target file size".to_string(),
            value: Cell::value(DocumentValue::Bytes(stats.target_file_size_bytes)),
        },
        Property {
            label: "Average data file size".to_string(),
            value: optional_bytes_cell(stats.avg_data_file_size_bytes),
        },
    ]
}

fn data_file_size_distribution_table(distribution: Option<&DataFileSizeDistribution>) -> Table {
    Table {
        columns: vec![Cell::text("Statistic"), Cell::text("Size")],
        rows: vec![
            data_file_size_distribution_row(
                "min",
                distribution.map(|distribution| distribution.min),
            ),
            data_file_size_distribution_row(
                "p25",
                distribution.map(|distribution| distribution.p25),
            ),
            data_file_size_distribution_row(
                "p50",
                distribution.map(|distribution| distribution.p50),
            ),
            data_file_size_distribution_row(
                "p75",
                distribution.map(|distribution| distribution.p75),
            ),
            data_file_size_distribution_row(
                "p95",
                distribution.map(|distribution| distribution.p95),
            ),
            data_file_size_distribution_row(
                "max",
                distribution.map(|distribution| distribution.max),
            ),
        ],
    }
}

fn data_file_size_distribution_row(label: &str, size_bytes: Option<u64>) -> Row {
    Row {
        cells: vec![Cell::text(label), optional_bytes_cell(size_bytes)],
    }
}

fn data_file_size_bucket_table(buckets: &[DataFileSizeBucketStats]) -> Table {
    Table {
        columns: vec![
            Cell::text("Bucket"),
            Cell::text("Files"),
            Cell::text("Size"),
            Cell::text("Files %"),
            Cell::text("Size %"),
        ],
        rows: buckets.iter().map(data_file_size_bucket_row).collect(),
    }
}

fn data_file_size_bucket_row(bucket: &DataFileSizeBucketStats) -> Row {
    Row {
        cells: vec![
            Cell::text(bucket.label.clone()),
            Cell::value(DocumentValue::Unsigned(bucket.file_count)),
            Cell::value(DocumentValue::Bytes(bucket.total_size_bytes)),
            Cell::value(DocumentValue::PercentageMillis(
                bucket.file_percentage_millis,
            )),
            Cell::value(DocumentValue::PercentageMillis(
                bucket.size_percentage_millis,
            )),
        ],
    }
}

fn optional_bytes_cell(size_bytes: Option<u64>) -> Cell {
    size_bytes.map_or_else(
        || Cell::text("n/a"),
        |size| Cell::value(DocumentValue::Bytes(size)),
    )
}

fn optional_u32_cell(value: Option<u32>) -> Cell {
    value.map_or_else(
        || Cell::text("unknown"),
        |value| Cell::value(DocumentValue::Unsigned(u64::from(value))),
    )
}

fn optional_u64_cell(value: Option<u64>) -> Cell {
    value.map_or_else(
        || Cell::text("unknown"),
        |value| Cell::value(DocumentValue::Unsigned(value)),
    )
}

fn optional_usize_cell(value: Option<usize>) -> Cell {
    value.map_or_else(
        || Cell::text("unknown"),
        |value| Cell::value(DocumentValue::Count(value)),
    )
}

fn table_stats_header_properties(
    source_endpoint: String,
    retrieved_at: OffsetDateTime,
    stats: &CurrentTableStats,
) -> Vec<Property> {
    vec![
        Property {
            label: "Source endpoint".to_string(),
            value: Cell::value(DocumentValue::Uri(source_endpoint)),
        },
        Property {
            label: "Retrieved at".to_string(),
            value: utc_and_local_timestamp_cell(retrieved_at),
        },
        Property {
            label: "Snapshot ID".to_string(),
            value: Cell::value(DocumentValue::Number(stats.snapshot_id)),
        },
        Property {
            label: "Updated at".to_string(),
            value: utc_and_local_timestamp_cell(stats.snapshot_updated_at),
        },
        Property {
            label: "Metadata".to_string(),
            value: Cell::value(DocumentValue::Uri(stats.metadata_json_path.clone())),
        },
        Property {
            label: "Manifest list".to_string(),
            value: Cell::value(DocumentValue::Uri(stats.manifest_list_path.clone())),
        },
    ]
}

fn table_file_properties(stats: &CurrentTableStats) -> Vec<Property> {
    vec![
        Property {
            label: "Records".to_string(),
            value: Cell::value(DocumentValue::Unsigned(stats.record_count)),
        },
        Property {
            label: "Total table size".to_string(),
            value: Cell::value(DocumentValue::Bytes(stats.total_table_file_size_bytes)),
        },
        Property {
            label: "Data files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(stats.data_file_count)),
        },
        Property {
            label: "Position delete files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(stats.position_delete_file_count)),
        },
        Property {
            label: "Position delete records".to_string(),
            value: Cell::value(DocumentValue::Unsigned(stats.position_delete_record_count)),
        },
        Property {
            label: "Equality delete files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(stats.equality_delete_file_count)),
        },
        Property {
            label: "Equality delete records".to_string(),
            value: Cell::value(DocumentValue::Unsigned(stats.equality_delete_record_count)),
        },
    ]
}

fn metadata_file_properties(stats: &CurrentTableStats, total_metadata_size: u64) -> Vec<Property> {
    let mut properties = vec![Property {
        label: "Metadata JSON size".to_string(),
        value: metadata_json_size_cell(stats),
    }];

    if stats.metadata_json_compressed {
        properties.push(Property {
            label: "Metadata JSON size".to_string(),
            value: metadata_json_uncompressed_size_cell(stats),
        });
    }

    properties.extend([
        Property {
            label: "Manifest list size".to_string(),
            value: Cell::value(DocumentValue::Bytes(stats.manifest_list_size_bytes)),
        },
        Property {
            label: "Manifest files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(stats.manifest_file_count)),
        },
        Property {
            label: "Manifest files size".to_string(),
            value: Cell::value(DocumentValue::Bytes(stats.manifest_files_size_bytes)),
        },
        Property {
            label: "Total metadata files".to_string(),
            value: Cell::value(DocumentValue::Bytes(total_metadata_size)),
        },
        Property {
            label: "Metadata overhead".to_string(),
            value: metadata_overhead_cell(total_metadata_size, stats.total_table_file_size_bytes),
        },
    ]);

    properties
}

fn utc_and_local_timestamp_cell(timestamp: OffsetDateTime) -> Cell {
    Cell::new(vec![
        DocumentValue::Timestamp(timestamp),
        DocumentValue::Text(", ".to_string()),
        DocumentValue::LocalTimestamp(timestamp),
    ])
}

fn metadata_json_size_cell(stats: &CurrentTableStats) -> Cell {
    Cell::new(vec![
        DocumentValue::Bytes(stats.metadata_json_size_bytes),
        DocumentValue::Text(if stats.metadata_json_compressed {
            ", compressed".to_string()
        } else {
            ", uncompressed".to_string()
        }),
    ])
}

fn metadata_json_uncompressed_size_cell(stats: &CurrentTableStats) -> Cell {
    Cell::new(vec![
        DocumentValue::Bytes(stats.metadata_json_uncompressed_size_bytes),
        DocumentValue::Text(", uncompressed".to_string()),
    ])
}

fn metadata_overhead_cell(total_metadata_size: u64, total_table_file_size: u64) -> Cell {
    let Some(percentage_millis) = percentage_millis(total_metadata_size, total_table_file_size)
    else {
        return Cell::text("n/a");
    };

    Cell::new(vec![
        DocumentValue::PercentageMillis(percentage_millis),
        DocumentValue::Text(" of table file size".to_string()),
    ])
}

fn percentage_millis(numerator: u64, denominator: u64) -> Option<u64> {
    if denominator == 0 {
        return None;
    }

    let numerator = u128::from(numerator);
    let denominator = u128::from(denominator);
    let rounded = (numerator * 100_000 + denominator / 2) / denominator;

    match u64::try_from(rounded) {
        Ok(value) => Some(value),
        Err(_) => Some(u64::MAX),
    }
}

fn separated_code_cell(values: impl IntoIterator<Item = String>) -> Cell {
    let mut values = values.into_iter();
    let Some(first) = values.next() else {
        return Cell::text("none");
    };

    let mut cell_values = vec![DocumentValue::Code(first)];
    for value in values {
        cell_values.push(DocumentValue::Text(", ".to_string()));
        cell_values.push(DocumentValue::Code(value));
    }

    Cell::new(cell_values)
}

fn flatten_fields(
    fields: &[NestedFieldRef],
    parent_path: Option<&str>,
    identifier_ids: &HashSet<i32>,
    identifier_fields: &mut Vec<String>,
    rows: &mut Vec<Row>,
) {
    for field in fields {
        let path = match parent_path {
            Some(parent_path) => format!("{parent_path}.{}", field.name),
            None => field.name.clone(),
        };

        flatten_field(field, &path, identifier_ids, identifier_fields, rows);
    }
}

fn flatten_field(
    field: &NestedFieldRef,
    path: &str,
    identifier_ids: &HashSet<i32>,
    identifier_fields: &mut Vec<String>,
    rows: &mut Vec<Row>,
) {
    if identifier_ids.contains(&field.id) {
        identifier_fields.push(path.to_string());
    }

    rows.push(Row {
        cells: vec![
            Cell::code(path.to_string()),
            Cell::code(type_summary(&field.field_type)),
            Cell::value(DocumentValue::Bool(field.required)),
            Cell::value(DocumentValue::Number(i64::from(field.id))),
        ],
    });

    flatten_nested_type(
        &field.field_type,
        path,
        identifier_ids,
        identifier_fields,
        rows,
    );
}

fn flatten_nested_type(
    field_type: &Type,
    path: &str,
    identifier_ids: &HashSet<i32>,
    identifier_fields: &mut Vec<String>,
    rows: &mut Vec<Row>,
) {
    match field_type {
        Type::Struct(struct_type) => flatten_fields(
            struct_type.fields(),
            Some(path),
            identifier_ids,
            identifier_fields,
            rows,
        ),
        Type::List(list_type) => {
            let element_path = list_element_path(path);
            flatten_field(
                &list_type.element_field,
                &element_path,
                identifier_ids,
                identifier_fields,
                rows,
            );
        }
        Type::Map(map_type) => {
            let key_path = map_key_path(path);
            flatten_field(
                &map_type.key_field,
                &key_path,
                identifier_ids,
                identifier_fields,
                rows,
            );
            let value_path = map_value_path(path, &map_type.value_field.field_type);
            flatten_field(
                &map_type.value_field,
                &value_path,
                identifier_ids,
                identifier_fields,
                rows,
            );
        }
        Type::Primitive(_) => {}
    }
}

fn list_element_path(path: &str) -> String {
    format!("{path}[]")
}

fn map_key_path(path: &str) -> String {
    format!("{path}{{}}.key")
}

fn map_value_path(path: &str, value_type: &Type) -> String {
    match value_type {
        Type::Struct(_) | Type::List(_) | Type::Map(_) => format!("{path}{{}}"),
        Type::Primitive(_) => format!("{path}{{}}.value"),
    }
}

fn type_summary(field_type: &Type) -> String {
    match field_type {
        Type::Primitive(primitive) => primitive.to_string(),
        Type::Struct(_) => "struct".to_string(),
        Type::List(list) => format!("list<{}>", type_summary(&list.element_field.field_type)),
        Type::Map(map) => format!(
            "map<{}, {}>",
            type_summary(&map.key_field.field_type),
            type_summary(&map.value_field.field_type)
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::engine::{
        CurrentDataFileSizeStats, CurrentManifestFileDetail, CurrentManifestFileList,
        CurrentTablePartitionStats, CurrentTablePartitions, CurrentTableStats,
        DataFileSizeBucketStats, DataFileSizeDistribution, ManifestColumnMetadataSummary,
        ManifestFileListEntry, ManifestPartitionMetadataSummary,
    };
    use crate::spec::{
        ListType, ManifestContentType, ManifestFile, MapType, NestedField, NestedFieldRef,
        PartitionSpec, PrimitiveType, Schema, StructType, Transform, Type,
    };
    use time::OffsetDateTime;

    use super::{
        Block, Cell, DocumentValue, data_file_size_stats_document, manifest_file_detail_document,
        manifest_file_list_document, schema_document, table_partitions_document,
        table_stats_document,
    };

    fn nested_schema() -> Schema {
        Schema::builder()
            .with_schema_id(3)
            .with_identifier_field_ids([1])
            .with_fields([
                org_id_field(),
                metadata_field(),
                containers_field(),
                properties_field(),
                events_field(),
            ])
            .build()
            .expect("valid schema")
    }

    fn org_id_field() -> NestedFieldRef {
        NestedField::required(1, "org_id", Type::Primitive(PrimitiveType::Long)).into()
    }

    fn metadata_field() -> NestedFieldRef {
        NestedField::optional(
            2,
            "metadata",
            Type::Struct(StructType::new(vec![
                NestedField::optional(
                    3,
                    "labels",
                    map_type(4, Type::Primitive(PrimitiveType::String)),
                )
                .into(),
            ])),
        )
        .into()
    }

    fn containers_field() -> NestedFieldRef {
        NestedField::optional(
            6,
            "containers",
            Type::List(ListType::new(
                NestedField::list_element(
                    7,
                    Type::Struct(StructType::new(vec![string_field(8, "name")])),
                    false,
                )
                .into(),
            )),
        )
        .into()
    }

    fn properties_field() -> NestedFieldRef {
        NestedField::optional(
            9,
            "properties",
            map_type(
                10,
                Type::Struct(StructType::new(vec![string_field(12, "value")])),
            ),
        )
        .into()
    }

    fn events_field() -> NestedFieldRef {
        NestedField::optional(
            13,
            "events",
            Type::List(ListType::new(
                NestedField::list_element(
                    14,
                    map_type(
                        15,
                        Type::Struct(StructType::new(vec![string_field(17, "kind")])),
                    ),
                    false,
                )
                .into(),
            )),
        )
        .into()
    }

    fn map_type(key_field_id: i32, value_type: Type) -> Type {
        Type::Map(MapType::new(
            NestedField::map_key_element(key_field_id, Type::Primitive(PrimitiveType::String))
                .into(),
            NestedField::map_value_element(key_field_id + 1, value_type, false).into(),
        ))
    }

    fn string_field(id: i32, name: &'static str) -> NestedFieldRef {
        NestedField::optional(id, name, Type::Primitive(PrimitiveType::String)).into()
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "document shape assertions are intentionally explicit"
    )]
    fn builds_current_schema_document() {
        let schema = nested_schema();

        let document = schema_document(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            schema,
        );

        assert_eq!(
            Cell::new(vec![
                DocumentValue::Text("Schema: ".to_string()),
                DocumentValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string())
            ]),
            document.title
        );

        let Block::Properties(properties) = &document.blocks[0] else {
            panic!("first block should be properties");
        };

        assert_eq!("Identifier fields", properties[3].label);
        assert_eq!(
            Cell::new(vec![DocumentValue::Code("org_id".to_string())]),
            properties[3].value
        );
        assert_eq!("Top-level field count", properties[4].label);
        assert_eq!(Cell::value(DocumentValue::Count(5)), properties[4].value);
        assert_eq!(
            "Total field count including nested fields",
            properties[5].label
        );
        assert_eq!(Cell::value(DocumentValue::Count(17)), properties[5].value);

        let Block::Section(section) = &document.blocks[1] else {
            panic!("second block should be a section");
        };

        assert_eq!(Cell::text("Fields"), section.title);

        let Block::Table(table) = &section.blocks[0] else {
            panic!("fields section should contain a table");
        };

        assert_eq!(
            vec![
                Cell::text("Path"),
                Cell::text("Type"),
                Cell::text("Required"),
                Cell::text("Field ID")
            ],
            table.columns
        );
        assert_eq!(
            vec![
                Cell::code("metadata.labels"),
                Cell::code("map<string, string>"),
                Cell::value(DocumentValue::Bool(false)),
                Cell::value(DocumentValue::Number(3)),
            ],
            table.rows[2].cells
        );
        assert_eq!(
            vec![
                Cell::code("metadata.labels{}.key"),
                Cell::code("string"),
                Cell::value(DocumentValue::Bool(true)),
                Cell::value(DocumentValue::Number(4)),
            ],
            table.rows[3].cells
        );
        assert_eq!(
            vec![
                Cell::code("metadata.labels{}.value"),
                Cell::code("string"),
                Cell::value(DocumentValue::Bool(false)),
                Cell::value(DocumentValue::Number(5)),
            ],
            table.rows[4].cells
        );
        assert_eq!(
            vec![
                Cell::code("containers[]"),
                Cell::code("struct"),
                Cell::value(DocumentValue::Bool(false)),
                Cell::value(DocumentValue::Number(7)),
            ],
            table.rows[6].cells
        );
        assert_eq!(
            vec![
                Cell::code("containers[].name"),
                Cell::code("string"),
                Cell::value(DocumentValue::Bool(false)),
                Cell::value(DocumentValue::Number(8)),
            ],
            table.rows[7].cells
        );
        assert_eq!(
            vec![
                Cell::code("properties{}.value"),
                Cell::code("string"),
                Cell::value(DocumentValue::Bool(false)),
                Cell::value(DocumentValue::Number(12)),
            ],
            table.rows[11].cells
        );
        assert_eq!(
            vec![
                Cell::code("events[]{}.kind"),
                Cell::code("string"),
                Cell::value(DocumentValue::Bool(false)),
                Cell::value(DocumentValue::Number(17)),
            ],
            table.rows[16].cells
        );
    }

    #[test]
    fn builds_current_table_stats_document() {
        let stats = CurrentTableStats {
            snapshot_id: 42,
            snapshot_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            metadata_json_path: "s3://warehouse/table/metadata/00042.gz.metadata.json".to_string(),
            metadata_json_compressed: true,
            manifest_list_path: "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            total_table_file_size_bytes: 700,
            data_file_count: 3,
            position_delete_file_count: 1,
            position_delete_record_count: 50,
            equality_delete_file_count: 2,
            equality_delete_record_count: 25,
            record_count: 900,
            manifest_file_count: 4,
            manifest_list_size_bytes: 100,
            manifest_files_size_bytes: 200,
            metadata_json_size_bytes: 300,
            metadata_json_uncompressed_size_bytes: 900,
        };

        let document = table_stats_document(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &stats,
        );

        assert_eq!(
            Cell::new(vec![
                DocumentValue::Text("Table Stats: ".to_string()),
                DocumentValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string())
            ]),
            document.title
        );

        let Block::Properties(properties) = &document.blocks[0] else {
            panic!("first block should be properties");
        };
        assert_eq!("Updated at", properties[3].label);
        assert_eq!("Metadata", properties[4].label);

        let Block::Section(table_files) = &document.blocks[1] else {
            panic!("second block should be table files section");
        };
        let Block::Properties(table_file_properties) = &table_files.blocks[0] else {
            panic!("table files section should contain properties");
        };

        assert_eq!("Records", table_file_properties[0].label);
        assert_eq!(
            Cell::value(DocumentValue::Unsigned(900)),
            table_file_properties[0].value
        );
        assert_eq!("Position delete files", table_file_properties[3].label);
        assert_eq!("Position delete records", table_file_properties[4].label);
        assert_eq!(
            Cell::value(DocumentValue::Unsigned(50)),
            table_file_properties[4].value
        );
        assert_eq!("Equality delete files", table_file_properties[5].label);
        assert_eq!("Equality delete records", table_file_properties[6].label);
        assert_eq!(
            Cell::value(DocumentValue::Unsigned(25)),
            table_file_properties[6].value
        );

        let Block::Section(metadata_files) = &document.blocks[2] else {
            panic!("third block should be metadata files section");
        };
        let Block::Properties(metadata_file_properties) = &metadata_files.blocks[0] else {
            panic!("metadata files section should contain properties");
        };

        assert_eq!("Metadata JSON size", metadata_file_properties[0].label);
        assert_eq!(
            Cell::new(vec![
                DocumentValue::Bytes(300),
                DocumentValue::Text(", compressed".to_string())
            ]),
            metadata_file_properties[0].value
        );
        assert_eq!("Metadata JSON size", metadata_file_properties[1].label);
        assert_eq!(
            Cell::new(vec![
                DocumentValue::Bytes(900),
                DocumentValue::Text(", uncompressed".to_string())
            ]),
            metadata_file_properties[1].value
        );
        assert_eq!("Total metadata files", metadata_file_properties[5].label);
        assert_eq!(
            Cell::value(DocumentValue::Bytes(600)),
            metadata_file_properties[5].value
        );
        assert_eq!("Metadata overhead", metadata_file_properties[6].label);
        assert_eq!(
            Cell::new(vec![
                DocumentValue::PercentageMillis(85_714),
                DocumentValue::Text(" of table file size".to_string())
            ]),
            metadata_file_properties[6].value
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "document shape assertions are intentionally explicit"
    )]
    fn builds_data_file_size_stats_document() {
        let stats = CurrentDataFileSizeStats {
            snapshot_id: 42,
            snapshot_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            manifest_list_path: "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            target_file_size_bytes: 512,
            total_data_file_size_bytes: 1_200,
            data_file_count: 5,
            avg_data_file_size_bytes: Some(300),
            distribution: Some(DataFileSizeDistribution {
                min: 100,
                p25: 200,
                p50: 300,
                p75: 400,
                p95: 480,
                max: 500,
            }),
            buckets: vec![DataFileSizeBucketStats {
                label: "75-125% target".to_string(),
                file_count: 2,
                total_size_bytes: 600,
                file_percentage_millis: 40_000,
                size_percentage_millis: 50_000,
            }],
        };

        let document = data_file_size_stats_document(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &stats,
        );

        assert_eq!(
            Cell::new(vec![
                DocumentValue::Text("Data File Size Stats: ".to_string()),
                DocumentValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string())
            ]),
            document.title
        );

        let Block::Properties(properties) = &document.blocks[0] else {
            panic!("first block should be properties");
        };
        assert_eq!("Snapshot ID", properties[2].label);
        assert_eq!("Manifest list", properties[4].label);

        let Block::Section(data_file_sizes) = &document.blocks[1] else {
            panic!("second block should be data file sizes section");
        };
        let Block::Properties(size_properties) = &data_file_sizes.blocks[0] else {
            panic!("data file sizes section should contain properties");
        };
        assert_eq!("Total data file size", size_properties[0].label);
        assert_eq!(
            Cell::value(DocumentValue::Bytes(1_200)),
            size_properties[0].value
        );
        assert_eq!("Data files", size_properties[1].label);
        assert_eq!(
            Cell::value(DocumentValue::Unsigned(5)),
            size_properties[1].value
        );
        assert_eq!("Target file size", size_properties[2].label);
        assert_eq!(
            Cell::value(DocumentValue::Bytes(512)),
            size_properties[2].value
        );
        assert_eq!("Average data file size", size_properties[3].label);
        assert_eq!(
            Cell::value(DocumentValue::Bytes(300)),
            size_properties[3].value
        );

        let Block::Section(distribution) = &data_file_sizes.blocks[1] else {
            panic!("data file sizes section should contain distribution section");
        };
        let Block::Table(distribution_table) = &distribution.blocks[0] else {
            panic!("distribution section should contain a table");
        };
        assert_eq!(
            vec![Cell::text("Statistic"), Cell::text("Size")],
            distribution_table.columns
        );
        assert_eq!(
            vec![Cell::text("p50"), Cell::value(DocumentValue::Bytes(300))],
            distribution_table.rows[2].cells
        );

        let Block::Section(buckets) = &data_file_sizes.blocks[2] else {
            panic!("data file sizes section should contain buckets section");
        };
        let Block::Table(bucket_table) = &buckets.blocks[0] else {
            panic!("buckets section should contain a table");
        };
        assert_eq!(Cell::text("Buckets"), buckets.title);
        assert_eq!(
            vec![
                Cell::text("Bucket"),
                Cell::text("Files"),
                Cell::text("Size"),
                Cell::text("Files %"),
                Cell::text("Size %")
            ],
            bucket_table.columns
        );
        assert_eq!(
            vec![
                Cell::text("75-125% target"),
                Cell::value(DocumentValue::Unsigned(2)),
                Cell::value(DocumentValue::Bytes(600)),
                Cell::value(DocumentValue::PercentageMillis(40_000)),
                Cell::value(DocumentValue::PercentageMillis(50_000)),
            ],
            bucket_table.rows[0].cells
        );
    }

    #[test]
    fn builds_manifest_file_list_document() {
        let manifest_files = CurrentManifestFileList {
            snapshot_id: 42,
            snapshot_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            manifest_list_path: "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            files: vec![ManifestFileListEntry {
                id: "m1".to_string(),
                name: "manifest-1.avro".to_string(),
                path: "s3://warehouse/table/metadata/manifest-1.avro".to_string(),
                content: ManifestContentType::Data,
                size_bytes: 2048,
                partition_spec_id: 7,
                added_files_count: Some(2),
                existing_files_count: Some(5),
                deleted_files_count: Some(1),
            }],
        };

        let document = manifest_file_list_document(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &manifest_files,
        );

        assert_eq!(
            Cell::new(vec![
                DocumentValue::Text("Manifest Files: ".to_string()),
                DocumentValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string())
            ]),
            document.title
        );

        let Block::Properties(properties) = &document.blocks[0] else {
            panic!("first block should be properties");
        };
        assert_eq!("Manifest files", properties[5].label);
        assert_eq!(Cell::value(DocumentValue::Count(1)), properties[5].value);

        let Block::Section(files_section) = &document.blocks[1] else {
            panic!("second block should be files section");
        };
        let Block::Table(files_table) = &files_section.blocks[0] else {
            panic!("files section should contain a table");
        };
        assert_eq!(Cell::text("Manifest Files"), files_section.title);
        assert_eq!(
            vec![
                Cell::text("ID"),
                Cell::text("Name"),
                Cell::text("Content"),
                Cell::text("Size"),
                Cell::text("Partition spec ID"),
                Cell::text("Added files"),
                Cell::text("Existing files"),
                Cell::text("Deleted files")
            ],
            files_table.columns
        );
        assert_eq!(
            vec![
                Cell::code("m1"),
                Cell::code("manifest-1.avro"),
                Cell::code("data"),
                Cell::value(DocumentValue::Bytes(2048)),
                Cell::value(DocumentValue::Number(7)),
                Cell::value(DocumentValue::Unsigned(2)),
                Cell::value(DocumentValue::Unsigned(5)),
                Cell::value(DocumentValue::Unsigned(1)),
            ],
            files_table.rows[0].cells
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "document shape assertions are intentionally explicit"
    )]
    fn builds_manifest_file_detail_document() {
        let detail = CurrentManifestFileDetail {
            snapshot_id: 42,
            snapshot_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            manifest_list_path: "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            manifest_file_count: 3,
            manifest_file_id: "m1".to_string(),
            manifest_file: ManifestFile {
                manifest_path: "s3://warehouse/table/metadata/manifest-1.avro".to_string(),
                manifest_length: 2048,
                partition_spec_id: 7,
                content: ManifestContentType::Data,
                sequence_number: 11,
                min_sequence_number: 9,
                added_snapshot_id: 42,
                added_files_count: Some(2),
                existing_files_count: Some(5),
                deleted_files_count: Some(1),
                added_rows_count: Some(200),
                existing_rows_count: Some(500),
                deleted_rows_count: Some(100),
                partitions: Some(Vec::new()),
                key_metadata: Some(vec![1, 2, 3]),
                first_row_id: Some(10_000),
            },
            partition_metadata: vec![
                ManifestPartitionMetadataSummary {
                    field_name: "org_id".to_string(),
                    field_id: Some(1000),
                    has_contains_nan: true,
                    has_lower_bound: true,
                    has_upper_bound: true,
                },
                ManifestPartitionMetadataSummary {
                    field_name: "day_bucket".to_string(),
                    field_id: Some(1001),
                    has_contains_nan: false,
                    has_lower_bound: true,
                    has_upper_bound: false,
                },
            ],
            column_metadata: vec![
                ManifestColumnMetadataSummary {
                    column_name: "org_id".to_string(),
                    field_id: 1,
                    metadata_fields: vec![
                        "column_sizes".to_string(),
                        "value_counts".to_string(),
                        "null_value_counts".to_string(),
                        "lower_bounds".to_string(),
                        "upper_bounds".to_string(),
                    ],
                },
                ManifestColumnMetadataSummary {
                    column_name: "metadata.labels".to_string(),
                    field_id: 3,
                    metadata_fields: vec!["column_sizes".to_string()],
                },
            ],
        };

        let document = manifest_file_detail_document(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &detail,
        );

        assert_eq!(
            Cell::new(vec![
                DocumentValue::Text("Manifest File: ".to_string()),
                DocumentValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string()),
                DocumentValue::Text(" ".to_string()),
                DocumentValue::Code("m1".to_string())
            ]),
            document.title
        );

        let Block::Properties(properties) = &document.blocks[0] else {
            panic!("first block should be properties");
        };
        assert_eq!("Snapshot ID", properties[2].label);
        assert_eq!("Manifest list", properties[4].label);
        assert_eq!("Manifest files", properties[5].label);
        assert_eq!(Cell::value(DocumentValue::Unsigned(3)), properties[5].value);
        assert_eq!("Manifest file ID", properties[6].label);
        assert_eq!(Cell::code("m1"), properties[6].value);

        let Block::Section(detail_section) = &document.blocks[1] else {
            panic!("second block should be manifest file section");
        };
        let Block::Properties(detail_properties) = &detail_section.blocks[0] else {
            panic!("manifest file section should contain properties");
        };
        assert_eq!(Cell::text("Manifest File"), detail_section.title);
        assert_eq!("Path", detail_properties[0].label);
        assert_eq!(
            Cell::value(DocumentValue::Uri(
                "s3://warehouse/table/metadata/manifest-1.avro".to_string()
            )),
            detail_properties[0].value
        );
        assert_eq!("Content", detail_properties[1].label);
        assert_eq!(Cell::code("data"), detail_properties[1].value);
        assert_eq!("Length", detail_properties[2].label);
        assert_eq!(
            Cell::value(DocumentValue::Bytes(2048)),
            detail_properties[2].value
        );
        assert_eq!("First row ID", detail_properties[15].label);
        assert_eq!(
            Cell::value(DocumentValue::Unsigned(10_000)),
            detail_properties[15].value
        );

        let Block::Section(partition_metadata_section) = &document.blocks[2] else {
            panic!("third block should be partition metadata section");
        };
        let Block::Table(partition_metadata_table) = &partition_metadata_section.blocks[0] else {
            panic!("partition metadata section should contain a table");
        };
        assert_eq!(
            Cell::text("Partition Metadata"),
            partition_metadata_section.title
        );
        assert_eq!(
            vec![
                Cell::text("Partition field"),
                Cell::text("Field ID"),
                Cell::text("Metadata")
            ],
            partition_metadata_table.columns
        );
        assert_eq!(
            vec![
                Cell::code("org_id"),
                Cell::value(DocumentValue::Number(1000)),
                Cell::new(vec![
                    DocumentValue::Code("contains_null".to_string()),
                    DocumentValue::Text(", ".to_string()),
                    DocumentValue::Code("contains_nan".to_string()),
                    DocumentValue::Text(", ".to_string()),
                    DocumentValue::Code("lower_bound".to_string()),
                    DocumentValue::Text(", ".to_string()),
                    DocumentValue::Code("upper_bound".to_string()),
                ]),
            ],
            partition_metadata_table.rows[0].cells
        );
        assert_eq!(
            vec![
                Cell::code("day_bucket"),
                Cell::value(DocumentValue::Number(1001)),
                Cell::new(vec![
                    DocumentValue::Code("contains_null".to_string()),
                    DocumentValue::Text(", ".to_string()),
                    DocumentValue::Code("lower_bound".to_string()),
                ]),
            ],
            partition_metadata_table.rows[1].cells
        );

        let Block::Section(column_metadata_section) = &document.blocks[3] else {
            panic!("fourth block should be column metadata section");
        };
        let Block::Table(column_metadata_table) = &column_metadata_section.blocks[0] else {
            panic!("column metadata section should contain a table");
        };
        assert_eq!(Cell::text("Column Metadata"), column_metadata_section.title);
        assert_eq!(
            vec![
                Cell::text("Column"),
                Cell::text("Field ID"),
                Cell::text("Metadata")
            ],
            column_metadata_table.columns
        );
        assert_eq!(
            vec![
                Cell::code("org_id"),
                Cell::value(DocumentValue::Number(1)),
                Cell::new(vec![
                    DocumentValue::Code("column_sizes".to_string()),
                    DocumentValue::Text(", ".to_string()),
                    DocumentValue::Code("value_counts".to_string()),
                    DocumentValue::Text(", ".to_string()),
                    DocumentValue::Code("null_value_counts".to_string()),
                    DocumentValue::Text(", ".to_string()),
                    DocumentValue::Code("lower_bounds".to_string()),
                    DocumentValue::Text(", ".to_string()),
                    DocumentValue::Code("upper_bounds".to_string()),
                ]),
            ],
            column_metadata_table.rows[0].cells
        );
        assert_eq!(
            vec![
                Cell::code("metadata.labels"),
                Cell::value(DocumentValue::Number(3)),
                Cell::new(vec![DocumentValue::Code("column_sizes".to_string())]),
            ],
            column_metadata_table.rows[1].cells
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "document shape assertions are intentionally explicit"
    )]
    fn builds_table_partitions_document() {
        let schema = Arc::new(nested_schema());
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(7)
                .add_partition_field("org_id", "org_id", Transform::Identity)
                .expect("valid partition field")
                .build()
                .expect("valid partition spec"),
        );
        let stats = CurrentTablePartitions {
            snapshot_id: 42,
            snapshot_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            metadata_json_path: "s3://warehouse/table/metadata/00042.metadata.json".to_string(),
            manifest_list_path: "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            current_schema: schema,
            partition_spec,
            target_file_size_bytes: 512,
            total_data_file_size_bytes: 900,
            data_file_count: 3,
            bucket_labels: vec!["< 16 MiB".to_string(), "75-125% target".to_string()],
            partitions: vec![CurrentTablePartitionStats {
                partition_spec_id: 7,
                partition: "org_id=123".to_string(),
                file_count: 3,
                total_size_bytes: 900,
                buckets: vec![
                    DataFileSizeBucketStats {
                        label: "< 16 MiB".to_string(),
                        file_count: 1,
                        total_size_bytes: 100,
                        file_percentage_millis: 33_333,
                        size_percentage_millis: 11_111,
                    },
                    DataFileSizeBucketStats {
                        label: "75-125% target".to_string(),
                        file_count: 2,
                        total_size_bytes: 800,
                        file_percentage_millis: 66_667,
                        size_percentage_millis: 88_889,
                    },
                ],
            }],
        };

        let document = table_partitions_document(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &stats,
        );

        assert_eq!(
            Cell::new(vec![
                DocumentValue::Text("Table Partitions: ".to_string()),
                DocumentValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string())
            ]),
            document.title
        );

        let Block::Properties(properties) = &document.blocks[0] else {
            panic!("first block should be properties");
        };
        assert_eq!("Snapshot ID", properties[2].label);
        assert_eq!("Metadata", properties[4].label);
        assert_eq!("Manifest list", properties[5].label);

        let Block::Section(spec_section) = &document.blocks[1] else {
            panic!("second block should be current partition spec section");
        };
        let Block::Properties(spec_properties) = &spec_section.blocks[0] else {
            panic!("partition spec section should contain properties");
        };
        assert_eq!("Default spec ID", spec_properties[0].label);
        assert_eq!(
            Cell::value(DocumentValue::Number(7)),
            spec_properties[0].value
        );

        let Block::Table(spec_table) = &spec_section.blocks[1] else {
            panic!("partition spec section should contain a table");
        };
        assert_eq!(
            vec![
                Cell::text("Name"),
                Cell::text("Source field"),
                Cell::text("Source type"),
                Cell::text("Transform"),
                Cell::text("Source ID"),
                Cell::text("Field ID")
            ],
            spec_table.columns
        );
        assert_eq!(
            vec![
                Cell::code("org_id"),
                Cell::code("org_id"),
                Cell::code("long"),
                Cell::code("identity"),
                Cell::value(DocumentValue::Number(1)),
                Cell::value(DocumentValue::Number(1000)),
            ],
            spec_table.rows[0].cells
        );

        let Block::Section(partitions_section) = &document.blocks[2] else {
            panic!("third block should be partitions section");
        };
        let Block::Properties(partition_properties) = &partitions_section.blocks[0] else {
            panic!("partitions section should contain properties");
        };
        assert_eq!("Partitions", partition_properties[0].label);
        assert_eq!(
            Cell::value(DocumentValue::Count(1)),
            partition_properties[0].value
        );

        let Block::Table(partitions_table) = &partitions_section.blocks[2] else {
            panic!("partitions section should contain a table");
        };
        assert_eq!(
            vec![
                Cell::text("Spec ID"),
                Cell::text("Partition"),
                Cell::text("Files"),
                Cell::text("Size"),
                Cell::text("< 16 MiB"),
                Cell::text("75-125%"),
            ],
            partitions_table.columns
        );
        assert_eq!(
            vec![
                Cell::value(DocumentValue::Number(7)),
                Cell::code("org_id=123"),
                Cell::value(DocumentValue::Unsigned(3)),
                Cell::value(DocumentValue::Bytes(900)),
                Cell::value(DocumentValue::Unsigned(1)),
                Cell::value(DocumentValue::Unsigned(2)),
            ],
            partitions_table.rows[0].cells
        );
    }
}
