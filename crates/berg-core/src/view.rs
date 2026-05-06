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
use std::collections::HashSet;

use crate::spec::{NestedFieldRef, Schema, SchemaRef, Type};
use time::OffsetDateTime;

/// Presentation-neutral report document shared by frontends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportDocument {
    /// Report title.
    pub title: ReportTitle,
    /// Ordered report metadata properties.
    pub properties: Vec<ReportProperty>,
    /// Ordered report tables.
    pub tables: Vec<ReportTable>,
}

/// Presentation-neutral report title.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportTitle {
    /// Title label, such as `Schema`.
    pub label: &'static str,
    /// Subject of the title.
    pub subject: ReportValue,
}

/// Presentation-neutral report metadata property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportProperty {
    /// Property label.
    pub label: &'static str,
    /// Property value.
    pub value: ReportValue,
}

/// Presentation-neutral report table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportTable {
    /// Table title.
    pub title: &'static str,
    /// Ordered column labels.
    pub columns: Vec<&'static str>,
    /// Ordered rows.
    pub rows: Vec<ReportRow>,
}

/// Presentation-neutral report row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportRow {
    /// Ordered row cells.
    pub cells: Vec<ReportValue>,
}

/// Semantic report value that each frontend renders in its own medium.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportValue {
    /// Plain text.
    Text(String),
    /// Code-like text, such as field paths, type names, or identifiers.
    Code(String),
    /// URI or URL value.
    Uri(String),
    /// Instant in time.
    Timestamp(OffsetDateTime),
    /// Iceberg schema ID.
    SchemaId(i32),
    /// Iceberg field ID.
    FieldId(i32),
    /// Numeric value.
    Number(i64),
    /// Non-negative count.
    Count(usize),
    /// Boolean value.
    Bool(bool),
    /// Requiredness/nullability value.
    Required(bool),
    /// Ordered list of code-like values.
    CodeList(Vec<String>),
    /// Ordered list of identifiers.
    IdentifierList(Vec<String>),
}

/// Report-friendly view of an Iceberg schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaReport {
    /// Fully qualified table identifier displayed in the title.
    pub table_ident: String,
    /// REST endpoint used to fetch the table metadata.
    pub source_endpoint: String,
    /// Retrieval timestamp.
    pub retrieved_at: OffsetDateTime,
    /// Iceberg schema ID.
    pub schema_id: i32,
    /// Identifier field names in schema order.
    pub identifier_fields: Vec<String>,
    /// Number of top-level fields.
    pub top_level_field_count: usize,
    /// Number of flattened rows including nested struct/list-struct fields.
    pub total_field_count: usize,
    /// Flattened field rows.
    pub fields: Vec<FieldRow>,
}

/// One flattened schema field row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldRow {
    /// Dot-separated field path. List element structs use `[]`.
    pub path: String,
    /// Iceberg type rendered compactly for schema reports.
    pub field_type: String,
    /// Whether the field is required.
    pub required: bool,
    /// Iceberg field ID.
    pub field_id: i32,
}

/// Build a schema report from an Iceberg schema.
#[must_use]
pub fn schema_report(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    schema: &SchemaRef,
) -> SchemaReport {
    schema_report_from_schema(table_ident, source_endpoint, retrieved_at, schema.as_ref())
}

/// Build a schema report from an Iceberg schema.
#[must_use]
pub fn schema_report_from_schema(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    schema: &Schema,
) -> SchemaReport {
    let identifier_ids = schema.identifier_field_ids().collect::<HashSet<_>>();
    let mut fields = Vec::new();

    flatten_fields(schema.as_struct().fields(), None, &mut fields);

    let identifier_fields = fields
        .iter()
        .filter(|field| identifier_ids.contains(&field.field_id))
        .map(|field| field.path.clone())
        .collect::<Vec<_>>();

    SchemaReport {
        table_ident: table_ident.into(),
        source_endpoint: source_endpoint.into(),
        retrieved_at,
        schema_id: schema.schema_id(),
        identifier_fields,
        top_level_field_count: schema.as_struct().fields().len(),
        total_field_count: fields.len(),
        fields,
    }
}

/// Build a presentation-neutral report document for a schema report.
#[must_use]
pub fn current_schema_report_document(report: &SchemaReport) -> ReportDocument {
    ReportDocument {
        title: ReportTitle {
            label: "Schema",
            subject: ReportValue::Code(report.table_ident.clone()),
        },
        properties: vec![
            ReportProperty {
                label: "Source endpoint",
                value: ReportValue::Uri(report.source_endpoint.clone()),
            },
            ReportProperty {
                label: "Retrieved at",
                value: ReportValue::Timestamp(report.retrieved_at),
            },
            ReportProperty {
                label: "Schema ID",
                value: ReportValue::SchemaId(report.schema_id),
            },
            ReportProperty {
                label: "Identifier fields",
                value: ReportValue::IdentifierList(report.identifier_fields.clone()),
            },
            ReportProperty {
                label: "Top-level field count",
                value: ReportValue::Count(report.top_level_field_count),
            },
            ReportProperty {
                label: "Total field count including nested fields",
                value: ReportValue::Count(report.total_field_count),
            },
        ],
        tables: vec![ReportTable {
            title: "Fields",
            columns: vec!["Path", "Type", "Required", "Field ID"],
            rows: report
                .fields
                .iter()
                .map(|field| ReportRow {
                    cells: vec![
                        ReportValue::Code(field.path.clone()),
                        ReportValue::Code(field.field_type.clone()),
                        ReportValue::Required(field.required),
                        ReportValue::FieldId(field.field_id),
                    ],
                })
                .collect(),
        }],
    }
}

fn flatten_fields(fields: &[NestedFieldRef], parent_path: Option<&str>, rows: &mut Vec<FieldRow>) {
    for field in fields {
        let path = match parent_path {
            Some(parent_path) => format!("{parent_path}.{}", field.name),
            None => field.name.clone(),
        };

        rows.push(FieldRow {
            path: path.clone(),
            field_type: type_summary(&field.field_type),
            required: field.required,
            field_id: field.id,
        });

        match field.field_type.as_ref() {
            Type::Struct(struct_type) => flatten_fields(struct_type.fields(), Some(&path), rows),
            Type::List(list_type) => {
                if let Type::Struct(struct_type) = list_type.element_field.field_type.as_ref() {
                    flatten_fields(struct_type.fields(), Some(&format!("{path}[]")), rows);
                }
            }
            Type::Primitive(_) | Type::Map(_) => {}
        }
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
    use crate::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};
    use time::OffsetDateTime;

    use super::{ReportValue, current_schema_report_document, schema_report_from_schema};

    #[test]
    fn builds_current_schema_report_document() {
        let schema = Schema::builder()
            .with_schema_id(3)
            .with_identifier_field_ids([1])
            .with_fields([
                NestedField::required(1, "org_id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(
                    2,
                    "metadata",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(
                            3,
                            "labels",
                            Type::Map(MapType::new(
                                NestedField::map_key_element(
                                    4,
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                                NestedField::map_value_element(
                                    5,
                                    Type::Primitive(PrimitiveType::String),
                                    false,
                                )
                                .into(),
                            )),
                        )
                        .into(),
                    ])),
                )
                .into(),
                NestedField::optional(
                    6,
                    "containers",
                    Type::List(ListType::new(
                        NestedField::list_element(
                            7,
                            Type::Struct(StructType::new(vec![
                                NestedField::optional(
                                    8,
                                    "name",
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                            ])),
                            false,
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .expect("valid schema");

        let report = schema_report_from_schema(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &schema,
        );

        assert_eq!(3, report.schema_id);
        assert_eq!(vec!["org_id"], report.identifier_fields);
        assert_eq!(3, report.top_level_field_count);
        assert_eq!(5, report.total_field_count);

        let document = current_schema_report_document(&report);

        assert_eq!("Schema", document.title.label);
        assert_eq!(
            ReportValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string()),
            document.title.subject
        );
        assert_eq!("Identifier fields", document.properties[3].label);
        assert_eq!(
            ReportValue::IdentifierList(vec!["org_id".to_string()]),
            document.properties[3].value
        );
        assert_eq!("Fields", document.tables[0].title);
        assert_eq!(
            vec!["Path", "Type", "Required", "Field ID"],
            document.tables[0].columns
        );
        assert_eq!(
            vec![
                ReportValue::Code("metadata.labels".to_string()),
                ReportValue::Code("map<string, string>".to_string()),
                ReportValue::Required(false),
                ReportValue::FieldId(3),
            ],
            document.tables[0].rows[2].cells
        );
        assert_eq!(
            vec![
                ReportValue::Code("containers[].name".to_string()),
                ReportValue::Code("string".to_string()),
                ReportValue::Required(false),
                ReportValue::FieldId(8),
            ],
            document.tables[0].rows[4].cells
        );
    }
}
