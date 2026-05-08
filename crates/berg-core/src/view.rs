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

use crate::engine::CurrentTableStats;
use crate::spec::{NestedFieldRef, Schema, Type};
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
    vec![
        Property {
            label: "Metadata JSON size".to_string(),
            value: metadata_json_size_cell(stats),
        },
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
    ]
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

        if identifier_ids.contains(&field.id) {
            identifier_fields.push(path.clone());
        }

        rows.push(Row {
            cells: vec![
                Cell::code(path.clone()),
                Cell::code(type_summary(&field.field_type)),
                Cell::value(DocumentValue::Bool(field.required)),
                Cell::value(DocumentValue::Number(i64::from(field.id))),
            ],
        });

        flatten_nested_type(
            &field.field_type,
            &path,
            identifier_ids,
            identifier_fields,
            rows,
        );
    }
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
        Type::List(list_type) => flatten_nested_type(
            &list_type.element_field.field_type,
            &format!("{path}[]"),
            identifier_ids,
            identifier_fields,
            rows,
        ),
        Type::Map(map_type) => flatten_nested_type(
            &map_type.value_field.field_type,
            &format!("{path}{{}}"),
            identifier_ids,
            identifier_fields,
            rows,
        ),
        Type::Primitive(_) => {}
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
    use crate::engine::CurrentTableStats;
    use crate::spec::{
        ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
    };
    use time::OffsetDateTime;

    use super::{Block, Cell, DocumentValue, schema_document, table_stats_document};

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
        assert_eq!(Cell::value(DocumentValue::Count(9)), properties[5].value);

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
                Cell::code("containers[].name"),
                Cell::code("string"),
                Cell::value(DocumentValue::Bool(false)),
                Cell::value(DocumentValue::Number(8)),
            ],
            table.rows[4].cells
        );
        assert_eq!(
            vec![
                Cell::code("properties{}.value"),
                Cell::code("string"),
                Cell::value(DocumentValue::Bool(false)),
                Cell::value(DocumentValue::Number(12)),
            ],
            table.rows[6].cells
        );
        assert_eq!(
            vec![
                Cell::code("events[]{}.kind"),
                Cell::code("string"),
                Cell::value(DocumentValue::Bool(false)),
                Cell::value(DocumentValue::Number(17)),
            ],
            table.rows[8].cells
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
        assert_eq!("Total metadata files", metadata_file_properties[4].label);
        assert_eq!(
            Cell::value(DocumentValue::Bytes(600)),
            metadata_file_properties[4].value
        );
        assert_eq!("Metadata overhead", metadata_file_properties[5].label);
        assert_eq!(
            Cell::new(vec![
                DocumentValue::PercentageMillis(85_714),
                DocumentValue::Text(" of table file size".to_string())
            ]),
            metadata_file_properties[5].value
        );
    }
}
