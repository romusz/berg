//! Berg/Iceberg-specific report builders.
//!
//! Reports turn Iceberg/domain data produced by [`crate::engine`] into the
//! generic, presentation-neutral [`crate::document`] model. Final output is a
//! renderer/frontend concern.
//!
//! ## Module vocabulary
//!
//! - **document**: generic presentation-neutral model.
//! - **report**: Berg/Iceberg-specific builders that create documents.
//! - **render**: pure conversion from model to output format.
//! - **view**: final UI representation, especially TUI widgets/screens.
//!
//! This module should not contain final presentation details such as Markdown,
//! ANSI escapes, ratatui widgets, or HTML. Those belong in renderers/frontends.
//!
use std::borrow::Borrow;
use std::collections::HashSet;

use crate::document::{
    ApplicabilityStatus, Block, Cell, CompatibilityStatus, CompletenessStatus, ConfidenceStatus,
    DeltaDirection, Document, DocumentValue, List, ListItem, ListKind, PrecisionStatus, Presence,
    Property, Row, Section, Status, SupportStatus, Table, UnknownValueKind,
};
use crate::engine::{
    BoundPrecision, CurrentDataFileSizeStats, CurrentManifestFileDetail, CurrentManifestFileList,
    CurrentTableMax, CurrentTablePartitionDistribution, CurrentTablePartitionStats,
    CurrentTablePartitions, CurrentTableProperties, CurrentTableStats, DataFileSizeBucketStats,
    DataFileSizeDistribution, DeleteAnalysisCompleteness, DeleteImpact,
    ManifestColumnMetadataSummary, ManifestFileListEntry, ManifestPartitionMetadataSummary,
    MaxConfidence, ReadCompleteness, TablePropertyEntry, TableSnapshotList, TableSnapshotListEntry,
    TypeCompatibility,
};
use crate::spec::{ManifestFile, NestedFieldRef, PartitionSpec, Schema, Type};
use time::OffsetDateTime;

/// Build a document from an Iceberg schema report.
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

/// Build a document from current Iceberg table statistics.
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
                title: Cell::text("Partitioning"),
                blocks: vec![
                    Block::Properties(table_stats_partitioning_properties(stats)),
                    Block::Table(partition_spec_table(
                        &stats.current_schema,
                        &stats.partition_spec,
                    )),
                ],
            }),
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

/// Build a document from current Iceberg table properties.
#[must_use]
pub fn table_properties_document(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    properties: &CurrentTableProperties,
) -> Document {
    let table_ident = table_ident.into();

    Document {
        title: Cell::new(vec![
            DocumentValue::Text("Table Properties: ".to_string()),
            DocumentValue::Code(table_ident),
        ]),
        blocks: vec![
            Block::Properties(table_properties_header_properties(
                source_endpoint.into(),
                retrieved_at,
                properties,
            )),
            Block::Section(Section {
                title: Cell::text("Properties"),
                blocks: table_property_blocks(&properties.properties),
            }),
        ],
    }
}

/// Build a document from snapshots retained in the current table metadata.
#[must_use]
pub fn table_snapshot_list_document(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    snapshots: &TableSnapshotList,
) -> Document {
    let table_ident = table_ident.into();

    Document {
        title: Cell::new(vec![
            DocumentValue::Text("Table Snapshots: ".to_string()),
            DocumentValue::Code(table_ident),
        ]),
        blocks: vec![
            Block::Properties(table_snapshot_list_header_properties(
                source_endpoint.into(),
                retrieved_at,
                snapshots,
            )),
            Block::Section(Section {
                title: Cell::text("Snapshots"),
                blocks: table_snapshot_list_blocks(snapshots),
            }),
        ],
    }
}

/// Build a document from current Iceberg data file size statistics.
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

/// Build a document from a metadata-derived current snapshot max result.
#[must_use]
pub fn table_data_max_document(
    table_ident: impl Into<String>,
    source_endpoint: impl Into<String>,
    retrieved_at: OffsetDateTime,
    max: &CurrentTableMax,
) -> Document {
    let table_ident = table_ident.into();
    let mut blocks = if max.unsupported_reason.is_some() {
        vec![Block::Properties(table_data_max_unsupported_properties(
            max,
        ))]
    } else {
        vec![table_data_max_result_block(max)]
    };

    blocks.extend([
        Block::Section(Section {
            title: Cell::text("Scope"),
            blocks: vec![Block::Properties(table_data_max_scope_properties(
                source_endpoint.into(),
                retrieved_at,
                max,
            ))],
        }),
        Block::Section(Section {
            title: Cell::text("Data File Metadata"),
            blocks: vec![Block::Properties(table_data_max_data_file_properties(max))],
        }),
        Block::Section(Section {
            title: Cell::text("Equality Deletes"),
            blocks: vec![Block::Properties(
                table_data_max_equality_delete_properties(max),
            )],
        }),
        Block::Section(Section {
            title: Cell::text("Position Deletes"),
            blocks: vec![Block::Properties(
                table_data_max_position_delete_properties(max),
            )],
        }),
        Block::Section(Section {
            title: Cell::text("Completeness And Precision"),
            blocks: vec![Block::Properties(table_data_max_completeness_properties(
                max,
            ))],
        }),
    ]);

    if !max.caveats.is_empty() {
        blocks.push(Block::Section(Section {
            title: Cell::text("Caveats"),
            blocks: vec![Block::List(List {
                kind: ListKind::Unordered,
                items: max
                    .caveats
                    .iter()
                    .map(|caveat| ListItem {
                        blocks: vec![Block::Paragraph(Cell::text(caveat.clone()))],
                    })
                    .collect(),
            })],
        }));
    }

    Document {
        title: Cell::new(vec![
            DocumentValue::Text("Table Data Max: ".to_string()),
            DocumentValue::Code(table_ident),
        ]),
        blocks,
    }
}

/// Build a document from current snapshot manifest files.
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

/// Build a document from one selected current snapshot manifest file.
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

/// Build a document from the current partition spec and partition statistics.
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
                    Block::Properties(current_partition_spec_properties(&stats.partition_spec)),
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
                    Block::Section(Section {
                        title: Cell::text("Partition distribution"),
                        blocks: vec![Block::Table(partition_distribution_table(
                            stats.partition_distribution.as_ref(),
                        ))],
                    }),
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

fn table_data_max_result_block(max: &CurrentTableMax) -> Block {
    Block::List(List {
        kind: ListKind::Unordered,
        items: vec![
            ListItem {
                blocks: vec![Block::Paragraph(label_value_cell(
                    "Metadata max",
                    max.metadata_max
                        .as_ref()
                        .map_or_else(unavailable_status_cell, Cell::code),
                ))],
            },
            ListItem {
                blocks: vec![Block::Paragraph(label_value_cell(
                    "Max confidence",
                    status_cell(max_confidence_status(max.max_confidence)),
                ))],
            },
            ListItem {
                blocks: vec![Block::Paragraph(label_value_cell(
                    "Max precision",
                    status_cell(bound_precision_status(max.max_precision)),
                ))],
            },
            ListItem {
                blocks: vec![
                    Block::Paragraph(Cell::text("Max confidence reasons:")),
                    Block::List(List {
                        kind: ListKind::Unordered,
                        items: max
                            .max_confidence_reasons
                            .iter()
                            .map(|reason| ListItem {
                                blocks: vec![Block::Paragraph(Cell::text(reason.clone()))],
                            })
                            .collect(),
                    }),
                ],
            },
        ],
    })
}

fn label_value_cell(label: &str, value: Cell) -> Cell {
    let mut values = vec![DocumentValue::Text(format!("{label}: "))];
    values.extend(value.values);
    Cell::new(values)
}

fn table_data_max_unsupported_properties(max: &CurrentTableMax) -> Vec<Property> {
    vec![
        Property {
            label: "Result".to_string(),
            value: status_cell(Status::Support(SupportStatus::Unsupported)),
        },
        Property {
            label: "Reason".to_string(),
            value: Cell::text(max.unsupported_reason.clone().unwrap_or_default()),
        },
    ]
}

fn table_data_max_scope_properties(
    source_endpoint: String,
    retrieved_at: OffsetDateTime,
    max: &CurrentTableMax,
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
            value: Cell::value(DocumentValue::Number(max.snapshot_id)),
        },
        Property {
            label: "Snapshot updated at".to_string(),
            value: utc_and_local_timestamp_cell(max.snapshot_updated_at),
        },
        Property {
            label: "Manifest list".to_string(),
            value: Cell::value(DocumentValue::Uri(max.manifest_list_path.clone())),
        },
        Property {
            label: "Result scope".to_string(),
            value: Cell::text(
                "metadata-derived current snapshot max; table data files were not scanned",
            ),
        },
        Property {
            label: "Column".to_string(),
            value: Cell::code(max.column.clone()),
        },
        Property {
            label: "Field path".to_string(),
            value: Cell::code(max.field_path.clone()),
        },
        Property {
            label: "Field ID".to_string(),
            value: Cell::value(DocumentValue::Number(i64::from(max.field_id))),
        },
        Property {
            label: "Field type".to_string(),
            value: Cell::code(max.field_type.clone()),
        },
        Property {
            label: "Schema scope".to_string(),
            value: Cell::text("current schema"),
        },
    ]
}

fn table_data_max_data_file_properties(max: &CurrentTableMax) -> Vec<Property> {
    vec![
        Property {
            label: "Data file metadata entries scanned".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.data_file_metadata_entries_scanned,
            )),
        },
        Property {
            label: "Zero-record data file metadata entries".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.zero_record_data_file_metadata_entries,
            )),
        },
        Property {
            label: "Data files where field is absent from file schema".to_string(),
            value: Cell::value(DocumentValue::Unsigned(max.data_files_field_absent)),
        },
        Property {
            label: "Data files using initial-default".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.data_files_using_initial_default,
            )),
        },
        Property {
            label: "Data files with only null/NaN column values".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.data_files_with_no_non_null_values,
            )),
        },
        Property {
            label: "Data files with NaN values".to_string(),
            value: Cell::value(DocumentValue::Unsigned(max.data_files_with_nan_values)),
        },
        Property {
            label: "Data files without upper bound".to_string(),
            value: Cell::value(DocumentValue::Unsigned(max.data_files_without_upper_bound)),
        },
        Property {
            label: "NaN upper bounds".to_string(),
            value: Cell::value(DocumentValue::Unsigned(max.nan_upper_bounds)),
        },
        Property {
            label: "Upper-bound decode failures".to_string(),
            value: Cell::value(DocumentValue::Unsigned(max.upper_bound_decode_failures)),
        },
        Property {
            label: "Manifest decode failures".to_string(),
            value: Cell::value(DocumentValue::Unsigned(max.manifest_decode_failures)),
        },
    ]
}

fn table_data_max_equality_delete_properties(max: &CurrentTableMax) -> Vec<Property> {
    vec![
        Property {
            label: "Equality delete files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(max.equality_delete_files)),
        },
        Property {
            label: "Zero-record delete files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(max.zero_record_delete_files)),
        },
        Property {
            label: "Max candidate files with applicable equality deletes".to_string(),
            value: ratio_count_cell(
                max.max_candidate_files_with_applicable_equality_deletes,
                max.max_candidate_file_count,
            ),
        },
        Property {
            label: "Max equality-delete impact".to_string(),
            value: status_cell(delete_impact_status(max.max_equality_delete_impact)),
        },
    ]
}

#[expect(
    clippy::too_many_lines,
    reason = "position delete diagnostics intentionally stay together"
)]
fn table_data_max_position_delete_properties(max: &CurrentTableMax) -> Vec<Property> {
    vec![
        Property {
            label: "Position delete files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(max.position_delete_files)),
        },
        Property {
            label: "Max candidate data sequence number range".to_string(),
            value: optional_i64_range_cell(
                max.max_candidate_data_sequence_number_min,
                max.max_candidate_data_sequence_number_max,
            ),
        },
        Property {
            label: "Max candidate files without sequence number".to_string(),
            value: Cell::value(DocumentValue::Count(
                max.max_candidate_files_without_sequence_number,
            )),
        },
        Property {
            label: "Position delete sequence number range".to_string(),
            value: optional_i64_range_cell(
                max.position_delete_sequence_number_min,
                max.position_delete_sequence_number_max,
            ),
        },
        Property {
            label: "Position delete files without sequence number".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.position_delete_files_without_sequence_number,
            )),
        },
        Property {
            label: "Position delete files with referenced_data_file".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.position_delete_files_with_referenced_data_file,
            )),
        },
        Property {
            label: "Position delete files not applicable by sequence number".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.position_delete_files_not_applicable_by_sequence,
            )),
        },
        Property {
            label: "Position delete files not applicable by partition".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.position_delete_files_not_applicable_by_partition,
            )),
        },
        Property {
            label: "Position delete files not applicable by referenced_data_file".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.position_delete_files_not_applicable_by_referenced_data_file,
            )),
        },
        Property {
            label: "Position delete files with unknown max-candidate applicability".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.position_delete_files_with_unknown_applicability,
            )),
        },
        Property {
            label: "Position delete files applicable to max candidates".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.position_delete_files_applicable_to_max_candidates,
            )),
        },
        Property {
            label: "Position delete files requiring file_path reads".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.position_delete_files_requiring_file_path_reads,
            )),
        },
        Property {
            label: "Position delete files read for file_path".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.position_delete_files_read_for_file_path,
            )),
        },
        Property {
            label: "Unsupported position delete files".to_string(),
            value: Cell::value(DocumentValue::Unsigned(
                max.unsupported_position_delete_files,
            )),
        },
        Property {
            label: "Max candidate files touched by position deletes".to_string(),
            value: ratio_count_cell(
                max.max_candidate_files_touched_by_position_deletes,
                max.max_candidate_file_count,
            ),
        },
        Property {
            label: "Max position-delete impact".to_string(),
            value: status_cell(delete_impact_status(max.max_position_delete_impact)),
        },
        Property {
            label: "Max position-delete analysis".to_string(),
            value: status_cell(delete_analysis_status(max.max_position_delete_analysis)),
        },
    ]
}

fn table_data_max_completeness_properties(max: &CurrentTableMax) -> Vec<Property> {
    let mut properties = vec![
        Property {
            label: "Read completeness".to_string(),
            value: status_cell(read_completeness_status(max.read_completeness)),
        },
        Property {
            label: "Type compatibility".to_string(),
            value: status_cell(type_compatibility_status(max.type_compatibility)),
        },
    ];

    if let Some(detail) = &max.type_compatibility_detail {
        properties.push(Property {
            label: "Type compatibility detail".to_string(),
            value: Cell::text(detail.clone()),
        });
    }

    properties.extend([
        Property {
            label: "Metrics mode evidence".to_string(),
            value: Cell::text(max.metrics_mode_evidence.clone()),
        },
        Property {
            label: "Current/default metrics mode for column".to_string(),
            value: Cell::code(max.current_metrics_mode.clone()),
        },
    ]);

    if let Some(detail) = &max.precision_detail {
        properties.push(Property {
            label: "Precision detail".to_string(),
            value: Cell::text(detail.clone()),
        });
    }

    properties
}

fn max_confidence_status(confidence: MaxConfidence) -> Status {
    Status::Confidence(match confidence {
        MaxConfidence::High => ConfidenceStatus::High,
        MaxConfidence::Partial => ConfidenceStatus::Partial,
        MaxConfidence::Lowered => ConfidenceStatus::Lowered,
        MaxConfidence::Unknown => ConfidenceStatus::Unknown,
        MaxConfidence::Unavailable => ConfidenceStatus::Unavailable,
    })
}

fn bound_precision_status(precision: BoundPrecision) -> Status {
    Status::Precision(match precision {
        BoundPrecision::Exact => PrecisionStatus::Exact,
        BoundPrecision::ProbablyExact => PrecisionStatus::ProbablyExact,
        BoundPrecision::PossiblyTruncated => PrecisionStatus::PossiblyTruncated,
        BoundPrecision::Unknown => PrecisionStatus::Unknown,
        BoundPrecision::Unavailable => PrecisionStatus::Unavailable,
    })
}

fn delete_impact_status(impact: DeleteImpact) -> Status {
    Status::Applicability(match impact {
        DeleteImpact::Unaffected | DeleteImpact::NotApplicable => ApplicabilityStatus::DoesNotApply,
        DeleteImpact::PartiallyAffected => ApplicabilityStatus::PartiallyApplies,
        DeleteImpact::AllCandidatesTouched => ApplicabilityStatus::Applies,
        DeleteImpact::AllCandidatesPossiblyAffected | DeleteImpact::Unknown => {
            ApplicabilityStatus::Unknown
        }
    })
}

fn delete_analysis_status(analysis: DeleteAnalysisCompleteness) -> Status {
    Status::Completeness(match analysis {
        DeleteAnalysisCompleteness::Complete => CompletenessStatus::Complete,
        DeleteAnalysisCompleteness::Incomplete => CompletenessStatus::Incomplete,
        DeleteAnalysisCompleteness::NotApplicable => CompletenessStatus::NotApplicable,
    })
}

fn read_completeness_status(completeness: ReadCompleteness) -> Status {
    Status::Completeness(match completeness {
        ReadCompleteness::Complete => CompletenessStatus::Complete,
        ReadCompleteness::Incomplete => CompletenessStatus::Incomplete,
    })
}

fn type_compatibility_status(compatibility: TypeCompatibility) -> Status {
    Status::Compatibility(match compatibility {
        TypeCompatibility::Exact => CompatibilityStatus::Compatible,
        TypeCompatibility::SafelyPromoted => CompatibilityStatus::SafelyPromoted,
        TypeCompatibility::Incompatible => CompatibilityStatus::Incompatible,
        TypeCompatibility::Unknown => CompatibilityStatus::Unknown,
    })
}

fn ratio_count_cell(numerator: usize, denominator: usize) -> Cell {
    Cell::new(vec![
        DocumentValue::Count(numerator),
        DocumentValue::Text(" / ".to_string()),
        DocumentValue::Count(denominator),
    ])
}

fn status_cell(status: Status) -> Cell {
    Cell::value(DocumentValue::Status(status))
}

fn unavailable_status_cell() -> Cell {
    status_cell(Status::Confidence(ConfidenceStatus::Unavailable))
}

fn missing_value_cell() -> Cell {
    Cell::value(DocumentValue::MissingValue)
}

fn unknown_numeric_cell() -> Cell {
    Cell::value(DocumentValue::UnknownValue {
        kind: UnknownValueKind::Numeric,
    })
}

fn unknown_generic_cell() -> Cell {
    Cell::value(DocumentValue::UnknownValue {
        kind: UnknownValueKind::Generic,
    })
}

fn optional_i64_range_cell(min: Option<i64>, max: Option<i64>) -> Cell {
    match (min, max) {
        (Some(min), Some(max)) if min == max => Cell::value(DocumentValue::Number(min)),
        (Some(min), Some(max)) => Cell::new(vec![
            DocumentValue::Number(min),
            DocumentValue::Text("..".to_string()),
            DocumentValue::Number(max),
        ]),
        _ => missing_value_cell(),
    }
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
            metadata.field_id.map_or_else(unknown_numeric_cell, |id| {
                Cell::value(DocumentValue::Number(i64::from(id)))
            }),
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

    let metadata_fields = column_metadata_table_fields(metadata);
    let mut columns = vec![Cell::text("Column"), Cell::text("Field ID")];
    columns.extend(metadata_fields.iter().cloned().map(Cell::text));

    vec![Block::Table(Table {
        columns,
        rows: metadata
            .iter()
            .map(|metadata| column_metadata_row(metadata, &metadata_fields))
            .collect(),
    })]
}

const DEFAULT_COLUMN_METADATA_FIELDS: [&str; 6] = [
    "column_sizes",
    "value_counts",
    "null_value_counts",
    "nan_value_counts",
    "lower_bounds",
    "upper_bounds",
];

fn column_metadata_table_fields(metadata: &[ManifestColumnMetadataSummary]) -> Vec<String> {
    let mut fields = DEFAULT_COLUMN_METADATA_FIELDS
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();

    for summary in metadata {
        for field in &summary.metadata_fields {
            if !fields.iter().any(|existing| existing == field) {
                fields.push(field.clone());
            }
        }
    }

    fields
}

fn column_metadata_row(metadata: &ManifestColumnMetadataSummary, fields: &[String]) -> Row {
    let mut cells = vec![
        Cell::code(metadata.column_name.clone()),
        Cell::value(DocumentValue::Number(i64::from(metadata.field_id))),
    ];
    cells.extend(
        fields
            .iter()
            .map(|field| column_metadata_presence_cell(metadata, field)),
    );

    Row { cells }
}

fn column_metadata_presence_cell(metadata: &ManifestColumnMetadataSummary, field: &str) -> Cell {
    let presence = if metadata
        .metadata_fields
        .iter()
        .any(|metadata_field| metadata_field == field)
    {
        Presence::Present
    } else {
        Presence::Absent
    };

    Cell::value(DocumentValue::Presence(presence))
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

fn table_stats_partitioning_properties(stats: &CurrentTableStats) -> Vec<Property> {
    vec![
        Property {
            label: "Partitioned".to_string(),
            value: Cell::value(DocumentValue::Bool(
                !stats.partition_spec.is_unpartitioned(),
            )),
        },
        Property {
            label: "Partitions".to_string(),
            value: Cell::value(DocumentValue::Count(stats.partition_count)),
        },
        Property {
            label: "Fields".to_string(),
            value: Cell::value(DocumentValue::Count(stats.partition_spec.fields().len())),
        },
        Property {
            label: "Files per partition".to_string(),
            value: Cell::code(files_per_partition(stats)),
        },
        Property {
            label: "Default spec ID".to_string(),
            value: Cell::value(DocumentValue::Number(i64::from(
                stats.partition_spec.spec_id(),
            ))),
        },
    ]
}

fn files_per_partition(stats: &CurrentTableStats) -> String {
    files_per_partition_value(stats.data_file_count, stats.partition_count)
        .unwrap_or_else(|| "0.00".to_string())
}

fn current_partition_spec_properties(partition_spec: &PartitionSpec) -> Vec<Property> {
    vec![
        Property {
            label: "Default spec ID".to_string(),
            value: Cell::value(DocumentValue::Number(i64::from(partition_spec.spec_id()))),
        },
        Property {
            label: "Partitioned".to_string(),
            value: Cell::value(DocumentValue::Bool(!partition_spec.is_unpartitioned())),
        },
        Property {
            label: "Fields".to_string(),
            value: Cell::value(DocumentValue::Count(partition_spec.fields().len())),
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
            label: "Files per partition".to_string(),
            value: files_per_partition_cell(stats.data_file_count, stats.partitions.len()),
        },
        Property {
            label: "Data per partition".to_string(),
            value: optional_bytes_cell(bytes_per_partition(
                stats.total_data_file_size_bytes,
                stats.partitions.len(),
            )),
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

fn files_per_partition_cell(data_file_count: u64, partition_count: usize) -> Cell {
    files_per_partition_value(data_file_count, partition_count)
        .map_or_else(missing_value_cell, Cell::code)
}

fn files_per_partition_value(data_file_count: u64, partition_count: usize) -> Option<String> {
    if partition_count == 0 {
        return None;
    }

    let partitions = u128::try_from(partition_count).expect("usize fits in u128");
    let hundredths = (u128::from(data_file_count) * 100 + partitions / 2) / partitions;

    Some(format!("{}.{:02}", hundredths / 100, hundredths % 100))
}

fn bytes_per_partition(total_size_bytes: u64, partition_count: usize) -> Option<u64> {
    if partition_count == 0 {
        return None;
    }

    let partitions = u128::try_from(partition_count).expect("usize fits in u128");
    let average = (u128::from(total_size_bytes) + partitions / 2) / partitions;

    Some(u64::try_from(average).expect("average partition size fits in u64"))
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
                    source_field_type_cell(schema, field.source_id),
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

fn source_field_type_cell(schema: &Schema, source_id: i32) -> Cell {
    schema
        .field_by_id(source_id)
        .map_or_else(unknown_generic_cell, |field| {
            Cell::code(type_summary(&field.field_type))
        })
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

fn partition_distribution_table(distribution: Option<&CurrentTablePartitionDistribution>) -> Table {
    Table {
        columns: vec![
            Cell::text("Percentile"),
            Cell::text("Files"),
            Cell::text("Binary size"),
        ],
        rows: vec![
            partition_distribution_row(
                "min",
                distribution.map(|distribution| distribution.files.min),
                distribution.map(|distribution| distribution.total_size_bytes.min),
            ),
            partition_distribution_row(
                "p25",
                distribution.map(|distribution| distribution.files.p25),
                distribution.map(|distribution| distribution.total_size_bytes.p25),
            ),
            partition_distribution_row(
                "p50",
                distribution.map(|distribution| distribution.files.p50),
                distribution.map(|distribution| distribution.total_size_bytes.p50),
            ),
            partition_distribution_row(
                "p75",
                distribution.map(|distribution| distribution.files.p75),
                distribution.map(|distribution| distribution.total_size_bytes.p75),
            ),
            partition_distribution_row(
                "p90",
                distribution.map(|distribution| distribution.files.p90),
                distribution.map(|distribution| distribution.total_size_bytes.p90),
            ),
            partition_distribution_row(
                "p95",
                distribution.map(|distribution| distribution.files.p95),
                distribution.map(|distribution| distribution.total_size_bytes.p95),
            ),
            partition_distribution_row(
                "p99",
                distribution.map(|distribution| distribution.files.p99),
                distribution.map(|distribution| distribution.total_size_bytes.p99),
            ),
            partition_distribution_row(
                "max",
                distribution.map(|distribution| distribution.files.max),
                distribution.map(|distribution| distribution.total_size_bytes.max),
            ),
        ],
    }
}

fn partition_distribution_row(
    label: &str,
    file_count: Option<u64>,
    size_bytes: Option<u64>,
) -> Row {
    let file_count = file_count.map_or_else(missing_value_cell, |count| {
        Cell::value(DocumentValue::Unsigned(count))
    });
    let size = size_bytes.map_or_else(missing_value_cell, |size| {
        Cell::value(DocumentValue::Bytes(size))
    });
    Row {
        cells: vec![Cell::text(label), file_count, size],
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
    size_bytes.map_or_else(missing_value_cell, |size| {
        Cell::value(DocumentValue::Bytes(size))
    })
}

fn optional_u32_cell(value: Option<u32>) -> Cell {
    value.map_or_else(unknown_numeric_cell, |value| {
        Cell::value(DocumentValue::Unsigned(u64::from(value)))
    })
}

fn optional_u64_cell(value: Option<u64>) -> Cell {
    value.map_or_else(unknown_numeric_cell, |value| {
        Cell::value(DocumentValue::Unsigned(value))
    })
}

fn optional_usize_cell(value: Option<usize>) -> Cell {
    value.map_or_else(unknown_numeric_cell, |value| {
        Cell::value(DocumentValue::Count(value))
    })
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
            label: "Updated at".to_string(),
            value: utc_and_local_timestamp_cell(stats.snapshot_updated_at),
        },
        Property {
            label: "Snapshot ID".to_string(),
            value: Cell::value(DocumentValue::Number(stats.snapshot_id)),
        },
        Property {
            label: "Snapshot count".to_string(),
            value: Cell::value(DocumentValue::Count(stats.retained_snapshot_count)),
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

fn table_properties_header_properties(
    source_endpoint: String,
    retrieved_at: OffsetDateTime,
    properties: &CurrentTableProperties,
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
            label: "Metadata".to_string(),
            value: Cell::value(DocumentValue::Uri(properties.metadata_json_path.clone())),
        },
        Property {
            label: "Last updated at".to_string(),
            value: utc_and_local_timestamp_cell(properties.last_updated_at),
        },
        Property {
            label: "Format version".to_string(),
            value: Cell::code(properties.format_version.to_string()),
        },
        Property {
            label: "Table UUID".to_string(),
            value: Cell::code(properties.table_uuid.clone()),
        },
        Property {
            label: "Location".to_string(),
            value: Cell::value(DocumentValue::Uri(properties.location.clone())),
        },
        Property {
            label: "Current snapshot ID".to_string(),
            value: optional_snapshot_id_cell(properties.current_snapshot_id),
        },
        Property {
            label: "Current schema ID".to_string(),
            value: Cell::value(DocumentValue::Number(i64::from(
                properties.current_schema_id,
            ))),
        },
        Property {
            label: "Default partition spec ID".to_string(),
            value: Cell::value(DocumentValue::Number(i64::from(
                properties.default_partition_spec_id,
            ))),
        },
        Property {
            label: "Default sort order ID".to_string(),
            value: Cell::value(DocumentValue::Number(properties.default_sort_order_id)),
        },
        Property {
            label: "Properties".to_string(),
            value: Cell::value(DocumentValue::Count(properties.properties.len())),
        },
    ]
}

fn table_snapshot_list_header_properties(
    source_endpoint: String,
    retrieved_at: OffsetDateTime,
    snapshots: &TableSnapshotList,
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
            label: "Metadata".to_string(),
            value: Cell::value(DocumentValue::Uri(snapshots.metadata_json_path.clone())),
        },
        Property {
            label: "Last updated at".to_string(),
            value: utc_and_local_timestamp_cell(snapshots.last_updated_at),
        },
        Property {
            label: "Current snapshot ID".to_string(),
            value: optional_i64_or_missing_cell(snapshots.current_snapshot_id),
        },
        Property {
            label: "Snapshots in metadata JSON".to_string(),
            value: Cell::value(DocumentValue::Count(snapshots.snapshots.len())),
        },
        Property {
            label: "Snapshot log entries".to_string(),
            value: Cell::value(DocumentValue::Count(snapshots.snapshot_log_entry_count)),
        },
    ]
}

fn table_snapshot_list_blocks(snapshots: &TableSnapshotList) -> Vec<Block> {
    if snapshots.snapshots.is_empty() {
        return vec![Block::Paragraph(Cell::text("No snapshots found."))];
    }

    let record_widths = summary_metric_widths(snapshots.snapshots.iter().map(|snapshot| {
        (
            snapshot.added_records,
            snapshot.deleted_records,
            snapshot.total_records,
        )
    }));
    let data_file_widths = summary_metric_widths(snapshots.snapshots.iter().map(|snapshot| {
        (
            snapshot.added_data_files,
            snapshot.deleted_data_files,
            snapshot.total_data_files,
        )
    }));
    let delete_file_widths = summary_metric_widths(snapshots.snapshots.iter().map(|snapshot| {
        (
            snapshot.added_delete_files,
            snapshot.removed_delete_files,
            snapshot.total_delete_files,
        )
    }));

    vec![Block::Table(Table {
        columns: vec![
            Cell::text("Committed at"),
            Cell::text("Snapshot ID"),
            Cell::text("Operation"),
            Cell::text("Records"),
            Cell::text("Data Files"),
            Cell::text("Delete Files"),
            Cell::text("Size"),
            Cell::text("Δ Partitions"),
        ],
        rows: snapshots
            .snapshots
            .iter()
            .map(|snapshot| {
                table_snapshot_list_row(
                    snapshot,
                    snapshots.current_snapshot_id == Some(snapshot.snapshot_id),
                    record_widths,
                    data_file_widths,
                    delete_file_widths,
                )
            })
            .collect(),
    })]
}

fn table_snapshot_list_row(
    snapshot: &TableSnapshotListEntry,
    is_current: bool,
    record_widths: SummaryMetricWidths,
    data_file_widths: SummaryMetricWidths,
    delete_file_widths: SummaryMetricWidths,
) -> Row {
    Row {
        cells: vec![
            Cell::value(DocumentValue::Timestamp(snapshot.committed_at)),
            snapshot_id_cell(snapshot.snapshot_id, is_current),
            Cell::code(snapshot.operation.clone()),
            snapshot_summary_metric_cell(
                snapshot.added_records,
                snapshot.deleted_records,
                snapshot.total_records,
                record_widths,
            ),
            snapshot_summary_metric_cell(
                snapshot.added_data_files,
                snapshot.deleted_data_files,
                snapshot.total_data_files,
                data_file_widths,
            ),
            snapshot_summary_metric_cell(
                snapshot.added_delete_files,
                snapshot.removed_delete_files,
                snapshot.total_delete_files,
                delete_file_widths,
            ),
            optional_bytes_or_missing_cell(snapshot.total_file_size_bytes),
            optional_u64_or_missing_cell(snapshot.changed_partition_count),
        ],
    }
}

fn snapshot_id_cell(snapshot_id: i64, is_current: bool) -> Cell {
    let mut values = vec![DocumentValue::Code(snapshot_id.to_string())];

    if is_current {
        values.push(DocumentValue::Text(" ✓".to_string()));
    }

    Cell::new(values)
}

#[derive(Debug, Clone, Copy)]
struct SummaryMetricWidths {
    added: usize,
    removed: usize,
    total: usize,
}

fn summary_metric_widths(
    metrics: impl IntoIterator<Item = (Option<u64>, Option<u64>, Option<u64>)>,
) -> SummaryMetricWidths {
    metrics
        .into_iter()
        .map(|(added, removed, total)| SummaryMetricWidths {
            added: summary_delta_value_len(added, DeltaDirection::Positive),
            removed: summary_delta_value_len(removed, DeltaDirection::Negative),
            total: optional_summary_value(total).len(),
        })
        .fold(
            SummaryMetricWidths {
                added: 0,
                removed: 0,
                total: 0,
            },
            |left, right| SummaryMetricWidths {
                added: left.added.max(right.added),
                removed: left.removed.max(right.removed),
                total: left.total.max(right.total),
            },
        )
}

fn snapshot_summary_metric_cell(
    added: Option<u64>,
    removed: Option<u64>,
    total: Option<u64>,
    widths: SummaryMetricWidths,
) -> Cell {
    let mut values = Vec::new();

    push_summary_delta_value(&mut values, added, DeltaDirection::Positive, widths.added);
    push_text(&mut values, " ");
    push_summary_delta_value(
        &mut values,
        removed,
        DeltaDirection::Negative,
        widths.removed,
    );
    push_text(&mut values, " ");
    push_optional_summary_value(&mut values, total, widths.total);

    Cell::new(values)
}

fn summary_delta_value_len(value: Option<u64>, direction: DeltaDirection) -> usize {
    summary_delta_value(value, direction).len()
}

fn push_summary_delta_value(
    values: &mut Vec<DocumentValue>,
    value: Option<u64>,
    direction: DeltaDirection,
    width: usize,
) {
    values.push(DocumentValue::Delta { direction, value });
    push_padding(
        values,
        width.saturating_sub(summary_delta_value_len(value, direction)),
    );
}

fn push_optional_summary_value(values: &mut Vec<DocumentValue>, value: Option<u64>, width: usize) {
    match value {
        Some(value) => push_right_padded_code(values, grouped_u64(value), width),
        None => push_right_padded_missing_value(values, width),
    }
}

fn push_right_padded_code(values: &mut Vec<DocumentValue>, value: String, width: usize) {
    push_padding(values, width.saturating_sub(value.len()));
    values.push(DocumentValue::Code(value));
}

fn push_right_padded_missing_value(values: &mut Vec<DocumentValue>, width: usize) {
    push_padding(values, width.saturating_sub(1));
    values.push(DocumentValue::MissingValue);
}

fn push_padding(values: &mut Vec<DocumentValue>, width: usize) {
    if width > 0 {
        push_text(values, " ".repeat(width));
    }
}

fn push_text(values: &mut Vec<DocumentValue>, text: impl Into<String>) {
    let text = text.into();
    if text.is_empty() {
        return;
    }

    if let Some(DocumentValue::Text(existing)) = values.last_mut() {
        existing.push_str(&text);
    } else {
        values.push(DocumentValue::Text(text));
    }
}

fn summary_delta_value(value: Option<u64>, direction: DeltaDirection) -> String {
    let sign = match direction {
        DeltaDirection::Positive => '+',
        DeltaDirection::Negative => '-',
    };

    format!("{sign}{}", grouped_u64(value.unwrap_or(0)))
}

fn optional_summary_value(value: Option<u64>) -> String {
    value.map_or_else(|| "?".to_string(), grouped_u64)
}

fn grouped_u64(value: u64) -> String {
    let digits = value.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, digit) in digits.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            formatted.push(',');
        }

        formatted.push(digit);
    }

    formatted.chars().rev().collect()
}

fn optional_i64_or_missing_cell(value: Option<i64>) -> Cell {
    value.map_or_else(
        || Cell::value(DocumentValue::MissingValue),
        |value| Cell::value(DocumentValue::Number(value)),
    )
}

fn optional_u64_or_missing_cell(value: Option<u64>) -> Cell {
    Cell::value(optional_u64_or_missing_value(value))
}

fn optional_u64_or_missing_value(value: Option<u64>) -> DocumentValue {
    value.map_or(DocumentValue::MissingValue, DocumentValue::Unsigned)
}

fn optional_bytes_or_missing_cell(value: Option<u64>) -> Cell {
    value.map_or_else(
        || Cell::value(DocumentValue::MissingValue),
        |value| Cell::value(DocumentValue::Bytes(value)),
    )
}

fn table_property_blocks(properties: &[TablePropertyEntry]) -> Vec<Block> {
    if properties.is_empty() {
        return vec![Block::Paragraph(Cell::text("No table properties found."))];
    }

    let mut properties = properties.iter().collect::<Vec<_>>();
    properties.sort_unstable_by(|left, right| left.key.cmp(&right.key));

    vec![Block::Table(Table {
        columns: vec![Cell::text("Key"), Cell::text("Value")],
        rows: properties.into_iter().map(table_property_row).collect(),
    })]
}

fn table_property_row(property: &TablePropertyEntry) -> Row {
    Row {
        cells: vec![
            Cell::code(property.key.clone()),
            Cell::code(property.value.clone()),
        ],
    }
}

fn optional_snapshot_id_cell(snapshot_id: Option<i64>) -> Cell {
    snapshot_id.map_or_else(missing_value_cell, |id| {
        Cell::value(DocumentValue::Number(id))
    })
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
        return missing_value_cell();
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
        BoundPrecision, CurrentDataFileSizeStats, CurrentManifestFileDetail,
        CurrentManifestFileList, CurrentTableMax, CurrentTablePartitionDistribution,
        CurrentTablePartitionStats, CurrentTablePartitions, CurrentTableProperties,
        CurrentTableStats, DataFileSizeBucketStats, DataFileSizeDistribution,
        DeleteAnalysisCompleteness, DeleteImpact, ManifestColumnMetadataSummary,
        ManifestFileListEntry, ManifestPartitionMetadataSummary, MaxConfidence,
        PartitionMetricDistribution, ReadCompleteness, TablePropertyEntry, TableSnapshotList,
        TableSnapshotListEntry, TypeCompatibility,
    };
    use crate::spec::{
        FormatVersion, ListType, ManifestContentType, ManifestFile, MapType, NestedField,
        NestedFieldRef, PartitionSpec, PrimitiveType, Schema, StructType, Transform, Type,
    };
    use time::OffsetDateTime;

    use super::{
        Block, Cell, DeltaDirection, DocumentValue, PrecisionStatus, Presence, Status,
        data_file_size_stats_document, manifest_file_detail_document, manifest_file_list_document,
        schema_document, table_data_max_document, table_partitions_document,
        table_properties_document, table_snapshot_list_document, table_stats_document,
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
    #[expect(
        clippy::too_many_lines,
        reason = "document shape assertions are intentionally explicit"
    )]
    fn builds_current_table_stats_document() {
        let schema = Arc::new(nested_schema());
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(3)
                .add_partition_field("org_id", "org_id", Transform::Identity)
                .expect("valid partition field")
                .build()
                .expect("valid partition spec"),
        );
        let stats = CurrentTableStats {
            snapshot_id: 42,
            snapshot_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            retained_snapshot_count: 6,
            metadata_json_path: "s3://warehouse/table/metadata/00042.gz.metadata.json".to_string(),
            metadata_json_compressed: true,
            manifest_list_path: "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            current_schema: schema,
            partition_spec,
            total_table_file_size_bytes: 700,
            data_file_count: 461,
            position_delete_file_count: 1,
            position_delete_record_count: 50,
            equality_delete_file_count: 2,
            equality_delete_record_count: 25,
            record_count: 900,
            partition_count: 8,
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
        assert_eq!("Updated at", properties[2].label);
        assert_eq!("Snapshot ID", properties[3].label);
        assert_eq!("Snapshot count", properties[4].label);
        assert_eq!(Cell::value(DocumentValue::Count(6)), properties[4].value);
        assert_eq!("Metadata", properties[5].label);

        let Block::Section(partitioning) = &document.blocks[1] else {
            panic!("second block should be partitioning section");
        };
        let Block::Properties(partitioning_properties) = &partitioning.blocks[0] else {
            panic!("partitioning section should contain properties");
        };
        assert_eq!("Partitioned", partitioning_properties[0].label);
        assert_eq!(
            Cell::value(DocumentValue::Bool(true)),
            partitioning_properties[0].value
        );
        assert_eq!("Partitions", partitioning_properties[1].label);
        assert_eq!(
            Cell::value(DocumentValue::Count(8)),
            partitioning_properties[1].value
        );
        assert_eq!("Fields", partitioning_properties[2].label);
        assert_eq!(
            Cell::value(DocumentValue::Count(1)),
            partitioning_properties[2].value
        );
        assert_eq!("Files per partition", partitioning_properties[3].label);
        assert_eq!(Cell::code("57.63"), partitioning_properties[3].value);
        assert_eq!("Default spec ID", partitioning_properties[4].label);
        assert_eq!(
            Cell::value(DocumentValue::Number(3)),
            partitioning_properties[4].value
        );
        let Block::Table(spec_table) = &partitioning.blocks[1] else {
            panic!("partitioning section should contain a spec table");
        };
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

        let Block::Section(table_files) = &document.blocks[2] else {
            panic!("third block should be table files section");
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

        let Block::Section(metadata_files) = &document.blocks[3] else {
            panic!("fourth block should be metadata files section");
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
    fn builds_current_table_properties_document() {
        let properties = CurrentTableProperties {
            metadata_json_path: "s3://warehouse/table/metadata/00042.gz.metadata.json".to_string(),
            last_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            format_version: FormatVersion::V2,
            table_uuid: "68f2b482-2a8f-4db4-8cfd-3ce78b11f1ed".to_string(),
            location: "s3://warehouse/table".to_string(),
            current_snapshot_id: Some(42),
            current_schema_id: 7,
            default_partition_spec_id: 3,
            default_sort_order_id: 0,
            properties: vec![
                TablePropertyEntry {
                    key: "write.target-file-size-bytes".to_string(),
                    value: "536870912".to_string(),
                },
                TablePropertyEntry {
                    key: "commit.retry.num-retries".to_string(),
                    value: "10".to_string(),
                },
            ],
        };

        let document = table_properties_document(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &properties,
        );

        assert_eq!(
            Cell::new(vec![
                DocumentValue::Text("Table Properties: ".to_string()),
                DocumentValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string())
            ]),
            document.title
        );

        let Block::Properties(header_properties) = &document.blocks[0] else {
            panic!("first block should be properties");
        };
        assert_eq!("Source endpoint", header_properties[0].label);
        assert_eq!("Metadata", header_properties[2].label);
        assert_eq!("Last updated at", header_properties[3].label);
        assert_eq!("Format version", header_properties[4].label);
        assert_eq!(Cell::code("v2"), header_properties[4].value);
        assert_eq!("Table UUID", header_properties[5].label);
        assert_eq!("Current snapshot ID", header_properties[7].label);
        assert_eq!(
            Cell::value(DocumentValue::Number(42)),
            header_properties[7].value
        );
        assert_eq!("Properties", header_properties[11].label);
        assert_eq!(
            Cell::value(DocumentValue::Count(2)),
            header_properties[11].value
        );

        let Block::Section(section) = &document.blocks[1] else {
            panic!("second block should be properties section");
        };
        assert_eq!(Cell::text("Properties"), section.title);

        let Block::Table(table) = &section.blocks[0] else {
            panic!("properties section should contain a table");
        };
        assert_eq!(vec![Cell::text("Key"), Cell::text("Value")], table.columns);
        assert_eq!(
            vec![Cell::code("commit.retry.num-retries"), Cell::code("10")],
            table.rows[0].cells
        );
        assert_eq!(
            vec![
                Cell::code("write.target-file-size-bytes"),
                Cell::code("536870912")
            ],
            table.rows[1].cells
        );
    }

    #[test]
    fn builds_current_table_properties_document_without_properties_or_snapshot() {
        let properties = CurrentTableProperties {
            metadata_json_path: "s3://warehouse/table/metadata/00042.gz.metadata.json".to_string(),
            last_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            format_version: FormatVersion::V2,
            table_uuid: "68f2b482-2a8f-4db4-8cfd-3ce78b11f1ed".to_string(),
            location: "s3://warehouse/table".to_string(),
            current_snapshot_id: None,
            current_schema_id: 7,
            default_partition_spec_id: 3,
            default_sort_order_id: 0,
            properties: Vec::new(),
        };

        let document = table_properties_document(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &properties,
        );

        let Block::Properties(header_properties) = &document.blocks[0] else {
            panic!("first block should be properties");
        };
        assert_eq!("Current snapshot ID", header_properties[7].label);
        assert_eq!(
            Cell::value(DocumentValue::MissingValue),
            header_properties[7].value
        );
        assert_eq!("Properties", header_properties[11].label);
        assert_eq!(
            Cell::value(DocumentValue::Count(0)),
            header_properties[11].value
        );

        let Block::Section(section) = &document.blocks[1] else {
            panic!("second block should be properties section");
        };
        assert_eq!(Cell::text("Properties"), section.title);
        assert_eq!(
            Block::Paragraph(Cell::text("No table properties found.")),
            section.blocks[0]
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "document shape assertions are intentionally explicit"
    )]
    fn builds_table_snapshot_list_document() {
        let snapshots = TableSnapshotList {
            metadata_json_path: "s3://warehouse/table/metadata/00042.gz.metadata.json".to_string(),
            last_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            current_snapshot_id: Some(42),
            snapshot_log_entry_count: 3,
            snapshots: vec![
                TableSnapshotListEntry {
                    snapshot_id: 42,
                    committed_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                        .expect("valid timestamp"),
                    operation: "append".to_string(),
                    added_records: Some(100),
                    deleted_records: Some(0),
                    total_records: Some(900),
                    added_data_files: Some(4),
                    deleted_data_files: Some(0),
                    total_data_files: Some(12),
                    added_delete_files: Some(0),
                    removed_delete_files: Some(0),
                    total_delete_files: Some(1),
                    total_file_size_bytes: Some(2048),
                    changed_partition_count: Some(2),
                },
                TableSnapshotListEntry {
                    snapshot_id: 41,
                    committed_at: OffsetDateTime::from_unix_timestamp(1_777_999_200)
                        .expect("valid timestamp"),
                    operation: "replace".to_string(),
                    added_records: None,
                    deleted_records: None,
                    total_records: Some(800),
                    added_data_files: Some(8),
                    deleted_data_files: Some(6),
                    total_data_files: Some(8),
                    added_delete_files: None,
                    removed_delete_files: None,
                    total_delete_files: None,
                    total_file_size_bytes: None,
                    changed_partition_count: None,
                },
            ],
        };

        let document = table_snapshot_list_document(
            "lakehouse.redapl_v3.k8s_pod_blue",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/k8s_pod_blue",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &snapshots,
        );

        assert_eq!(
            Cell::new(vec![
                DocumentValue::Text("Table Snapshots: ".to_string()),
                DocumentValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string())
            ]),
            document.title
        );

        let Block::Properties(header_properties) = &document.blocks[0] else {
            panic!("first block should be properties");
        };
        assert_eq!("Current snapshot ID", header_properties[4].label);
        assert_eq!(
            Cell::value(DocumentValue::Number(42)),
            header_properties[4].value
        );
        assert_eq!("Snapshots in metadata JSON", header_properties[5].label);
        assert_eq!(
            Cell::value(DocumentValue::Count(2)),
            header_properties[5].value
        );

        let Block::Section(section) = &document.blocks[1] else {
            panic!("second block should be snapshots section");
        };
        let Block::Table(table) = &section.blocks[0] else {
            panic!("snapshots section should contain a table");
        };
        assert_eq!(
            vec![
                Cell::text("Committed at"),
                Cell::text("Snapshot ID"),
                Cell::text("Operation"),
                Cell::text("Records"),
                Cell::text("Data Files"),
                Cell::text("Delete Files"),
                Cell::text("Size"),
                Cell::text("Δ Partitions"),
            ],
            table.columns
        );
        assert_eq!(
            Cell::new(vec![
                DocumentValue::Code("42".to_string()),
                DocumentValue::Text(" ✓".to_string())
            ]),
            table.rows[0].cells[1]
        );
        assert_eq!(
            Cell::new(vec![
                DocumentValue::Delta {
                    direction: DeltaDirection::Positive,
                    value: Some(100),
                },
                DocumentValue::Text(" ".to_string()),
                DocumentValue::Delta {
                    direction: DeltaDirection::Negative,
                    value: Some(0),
                },
                DocumentValue::Text(" ".to_string()),
                DocumentValue::Code("900".to_string()),
            ]),
            table.rows[0].cells[3]
        );
        assert_eq!(
            Cell::value(DocumentValue::MissingValue),
            table.rows[1].cells[6]
        );
        assert_eq!(
            Cell::new(vec![
                DocumentValue::Delta {
                    direction: DeltaDirection::Positive,
                    value: None,
                },
                DocumentValue::Text("   ".to_string()),
                DocumentValue::Delta {
                    direction: DeltaDirection::Negative,
                    value: None,
                },
                DocumentValue::Text(" ".to_string()),
                DocumentValue::Code("800".to_string()),
            ]),
            table.rows[1].cells[3]
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
    #[expect(
        clippy::too_many_lines,
        reason = "document shape assertions are intentionally explicit"
    )]
    fn builds_table_data_max_document_without_min_side_fields() {
        let max = CurrentTableMax {
            snapshot_id: 42,
            snapshot_updated_at: OffsetDateTime::from_unix_timestamp(1_777_999_300)
                .expect("valid timestamp"),
            manifest_list_path: "s3://warehouse/table/metadata/snap-42.avro".to_string(),
            column: "event_id".to_string(),
            field_path: "event_id".to_string(),
            field_id: 1,
            field_type: "long".to_string(),
            unsupported_reason: None,
            metadata_max: Some("999".to_string()),
            max_confidence: MaxConfidence::High,
            max_confidence_reasons: vec!["complete upper-bound coverage".to_string()],
            max_precision: BoundPrecision::Exact,
            data_file_metadata_entries_scanned: 2,
            zero_record_data_file_metadata_entries: 0,
            data_files_field_absent: 0,
            data_files_using_initial_default: 0,
            data_files_with_no_non_null_values: 0,
            data_files_with_nan_values: 0,
            data_files_without_upper_bound: 0,
            nan_upper_bounds: 0,
            upper_bound_decode_failures: 0,
            manifest_decode_failures: 0,
            equality_delete_files: 0,
            zero_record_delete_files: 0,
            max_candidate_files_with_applicable_equality_deletes: 0,
            max_candidate_file_count: 1,
            max_candidate_data_sequence_number_min: Some(7),
            max_candidate_data_sequence_number_max: Some(7),
            max_candidate_files_without_sequence_number: 0,
            max_equality_delete_impact: DeleteImpact::Unaffected,
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
            max_candidate_files_touched_by_position_deletes: 0,
            max_position_delete_impact: DeleteImpact::Unaffected,
            max_position_delete_analysis: DeleteAnalysisCompleteness::Complete,
            read_completeness: ReadCompleteness::Complete,
            type_compatibility: TypeCompatibility::Exact,
            type_compatibility_detail: None,
            metrics_mode_evidence:
                "Iceberg default; per-file historical metrics mode is not available".to_string(),
            current_metrics_mode: "truncate(16)".to_string(),
            precision_detail: Some(
                "metrics truncation does not apply to this field type".to_string(),
            ),
            caveats: Vec::new(),
        };

        let document = table_data_max_document(
            "lakehouse.redapl_v3.events",
            "https://example.test/v1/lakehouse/namespaces/redapl_v3/tables/events",
            OffsetDateTime::from_unix_timestamp(1_777_999_315).expect("valid timestamp"),
            &max,
        );

        assert_eq!(
            Cell::new(vec![
                DocumentValue::Text("Table Data Max: ".to_string()),
                DocumentValue::Code("lakehouse.redapl_v3.events".to_string())
            ]),
            document.title
        );

        let Block::List(result_items) = &document.blocks[0] else {
            panic!("first block should contain max result items");
        };
        assert_eq!(4, result_items.items.len());
        assert_eq!(
            Block::Paragraph(Cell::new(vec![
                DocumentValue::Text("Metadata max: ".to_string()),
                DocumentValue::Code("999".to_string())
            ])),
            result_items.items[0].blocks[0]
        );
        assert_eq!(
            Block::Paragraph(Cell::new(vec![
                DocumentValue::Text("Max precision: ".to_string()),
                DocumentValue::Status(Status::Precision(PrecisionStatus::Exact))
            ])),
            result_items.items[2].blocks[0]
        );
        let Block::List(reason_items) = &result_items.items[3].blocks[1] else {
            panic!("max confidence reasons should be nested list items");
        };
        assert_eq!(1, reason_items.items.len());

        let labels = document_property_labels(&document);
        assert!(
            labels
                .iter()
                .any(|label| label == "Data files without upper bound")
        );
        let null_only_label = "Data files with only null/NaN column values";
        assert!(labels.iter().any(|label| label == null_only_label));
        assert!(
            labels
                .iter()
                .any(|label| label == "Max candidate data sequence number range")
        );
        assert!(
            labels
                .iter()
                .any(|label| label == "Position delete sequence number range")
        );
        assert!(
            labels.iter().any(|label| {
                label == "Position delete files not applicable by sequence number"
            })
        );
        assert!(
            labels
                .iter()
                .any(|label| label == "Position delete files applicable to max candidates")
        );
        assert!(!labels.iter().any(|label| label.contains("Metadata min")));
        assert!(!labels.iter().any(|label| label.contains("Min confidence")));
        assert!(!labels.iter().any(|label| label.contains("lower bound")));
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

    fn document_property_labels(document: &super::Document) -> Vec<String> {
        let mut labels = Vec::new();
        collect_property_labels(&document.blocks, &mut labels);
        labels
    }

    fn collect_property_labels(blocks: &[Block], labels: &mut Vec<String>) {
        for block in blocks {
            match block {
                Block::Properties(properties) => {
                    labels.extend(properties.iter().map(|property| property.label.clone()));
                }
                Block::Section(section) => collect_property_labels(&section.blocks, labels),
                Block::Paragraph(_)
                | Block::Table(_)
                | Block::List(_)
                | Block::FencedCode(_)
                | Block::ThematicBreak => {}
            }
        }
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
                Cell::text("column_sizes"),
                Cell::text("value_counts"),
                Cell::text("null_value_counts"),
                Cell::text("nan_value_counts"),
                Cell::text("lower_bounds"),
                Cell::text("upper_bounds"),
            ],
            column_metadata_table.columns
        );
        assert_eq!(
            vec![
                Cell::code("org_id"),
                Cell::value(DocumentValue::Number(1)),
                Cell::value(DocumentValue::Presence(Presence::Present)),
                Cell::value(DocumentValue::Presence(Presence::Present)),
                Cell::value(DocumentValue::Presence(Presence::Present)),
                Cell::value(DocumentValue::Presence(Presence::Absent)),
                Cell::value(DocumentValue::Presence(Presence::Present)),
                Cell::value(DocumentValue::Presence(Presence::Present)),
            ],
            column_metadata_table.rows[0].cells
        );
        assert_eq!(
            vec![
                Cell::code("metadata.labels"),
                Cell::value(DocumentValue::Number(3)),
                Cell::value(DocumentValue::Presence(Presence::Present)),
                Cell::value(DocumentValue::Presence(Presence::Absent)),
                Cell::value(DocumentValue::Presence(Presence::Absent)),
                Cell::value(DocumentValue::Presence(Presence::Absent)),
                Cell::value(DocumentValue::Presence(Presence::Absent)),
                Cell::value(DocumentValue::Presence(Presence::Absent)),
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
            partition_distribution: Some(CurrentTablePartitionDistribution {
                files: PartitionMetricDistribution {
                    min: 5,
                    p25: 10,
                    p50: 20,
                    p75: 30,
                    p90: 40,
                    p95: 50,
                    p99: 60,
                    max: 70,
                },
                total_size_bytes: PartitionMetricDistribution {
                    min: 1_024,
                    p25: 2_048,
                    p50: 3_072,
                    p75: 4_096,
                    p90: 5_120,
                    p95: 6_144,
                    p99: 7_168,
                    max: 8_192,
                },
            }),
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
        assert_eq!("Files per partition", partition_properties[2].label);
        assert_eq!(Cell::code("3.00"), partition_properties[2].value);
        assert_eq!("Data per partition", partition_properties[3].label);
        assert_eq!(
            Cell::value(DocumentValue::Bytes(900)),
            partition_properties[3].value
        );

        let Block::Section(partition_distribution_section) = &partitions_section.blocks[1] else {
            panic!("partitions section should contain partition distribution section");
        };
        assert_eq!(
            Cell::text("Partition distribution"),
            partition_distribution_section.title
        );
        let Block::Table(partition_distribution_table) = &partition_distribution_section.blocks[0]
        else {
            panic!("partition distribution section should contain a table");
        };
        assert_eq!(
            vec![
                Cell::text("Percentile"),
                Cell::text("Files"),
                Cell::text("Binary size")
            ],
            partition_distribution_table.columns
        );
        assert_eq!(
            vec![
                vec![
                    Cell::text("min"),
                    Cell::value(DocumentValue::Unsigned(5)),
                    Cell::value(DocumentValue::Bytes(1_024))
                ],
                vec![
                    Cell::text("p25"),
                    Cell::value(DocumentValue::Unsigned(10)),
                    Cell::value(DocumentValue::Bytes(2_048))
                ],
                vec![
                    Cell::text("p50"),
                    Cell::value(DocumentValue::Unsigned(20)),
                    Cell::value(DocumentValue::Bytes(3_072))
                ],
                vec![
                    Cell::text("p75"),
                    Cell::value(DocumentValue::Unsigned(30)),
                    Cell::value(DocumentValue::Bytes(4_096))
                ],
                vec![
                    Cell::text("p90"),
                    Cell::value(DocumentValue::Unsigned(40)),
                    Cell::value(DocumentValue::Bytes(5_120))
                ],
                vec![
                    Cell::text("p95"),
                    Cell::value(DocumentValue::Unsigned(50)),
                    Cell::value(DocumentValue::Bytes(6_144))
                ],
                vec![
                    Cell::text("p99"),
                    Cell::value(DocumentValue::Unsigned(60)),
                    Cell::value(DocumentValue::Bytes(7_168))
                ],
                vec![
                    Cell::text("max"),
                    Cell::value(DocumentValue::Unsigned(70)),
                    Cell::value(DocumentValue::Bytes(8_192))
                ],
            ],
            partition_distribution_table
                .rows
                .iter()
                .map(|row| row.cells.clone())
                .collect::<Vec<_>>()
        );

        let Block::Table(partitions_table) = &partitions_section.blocks[3] else {
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
