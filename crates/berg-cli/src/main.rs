use std::collections::HashMap;
use std::env;
use std::fmt::Write;

use anyhow::{Context, anyhow};
use berg_core::engine::{
    QualifiedTableIdent, RestCatalogConfig, load_current_schema, parse_catalog_property,
};
use berg_core::view::{ReportDocument, ReportValue, current_schema_report_document, schema_report};
use clap::{Args, Parser, Subcommand};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Debug, Parser)]
#[command(name = "berg", version, about = "Command-line interface for Berg.")]
struct Cli {
    #[command(flatten)]
    catalog: CatalogArgs,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Args)]
struct CatalogArgs {
    /// Iceberg REST catalog base URI. Overrides `BERG_CATALOG_URI`.
    #[arg(long = "catalog-uri", value_name = "URI", global = true)]
    uri: Option<String>,

    /// REST catalog prefix. Defaults to the catalog segment in the table ID.
    /// Overrides `BERG_CATALOG_PREFIX`.
    #[arg(long = "catalog-prefix", value_name = "PREFIX", global = true)]
    prefix: Option<String>,

    /// REST catalog warehouse. Overrides `BERG_CATALOG_WAREHOUSE`.
    #[arg(long = "catalog-warehouse", value_name = "WAREHOUSE", global = true)]
    warehouse: Option<String>,

    /// REST catalog bearer token. Overrides `BERG_CATALOG_TOKEN`.
    #[arg(long = "catalog-token", value_name = "TOKEN", global = true)]
    token: Option<String>,

    /// REST catalog OAuth credential. Overrides `BERG_CATALOG_CREDENTIAL`.
    #[arg(long = "catalog-credential", value_name = "CREDENTIAL", global = true)]
    credential: Option<String>,

    /// Additional REST catalog header as name=value. Can be repeated.
    #[arg(long = "catalog-header", value_name = "NAME=VALUE", global = true)]
    headers: Vec<String>,

    /// Additional REST catalog property as key=value. Can be repeated.
    #[arg(long = "catalog-property", value_name = "KEY=VALUE", global = true)]
    properties: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Inspect Iceberg table schemas.
    Schema(SchemaArgs),
}

#[derive(Debug, Args)]
struct SchemaArgs {
    #[command(subcommand)]
    command: SchemaCommands,
}

#[derive(Debug, Subcommand)]
enum SchemaCommands {
    /// Show the current schema for a fully-qualified table ID.
    Current(CurrentSchemaArgs),
}

#[derive(Debug, Args)]
struct CurrentSchemaArgs {
    /// Fully-qualified table ID: catalog.namespace.table.
    table: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let catalog_args = cli.catalog;

    match cli.command {
        Some(Commands::Schema(schema_args)) => match schema_args.command {
            SchemaCommands::Current(args) => print_current_schema(args, catalog_args).await?,
        },
        None => println!("{}", berg_core::welcome_message("berg")?),
    }

    Ok(())
}

async fn print_current_schema(
    args: CurrentSchemaArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let catalog_uri = first_configured_value(catalog_args.uri, "BERG_CATALOG_URI")?
        .ok_or(berg_core::BergError::MissingCatalogUri)?;
    let catalog_prefix = first_configured_value(catalog_args.prefix, "BERG_CATALOG_PREFIX")?
        .unwrap_or_else(|| table.catalog().to_string());
    let catalog_warehouse =
        first_configured_value(catalog_args.warehouse, "BERG_CATALOG_WAREHOUSE")?;
    let catalog_token = first_configured_value(catalog_args.token, "BERG_CATALOG_TOKEN")?;
    let catalog_credential =
        first_configured_value(catalog_args.credential, "BERG_CATALOG_CREDENTIAL")?;
    let catalog_properties = catalog_properties(
        catalog_args.properties,
        catalog_args.headers,
        catalog_token,
        catalog_credential,
    )?;
    let config = RestCatalogConfig::new(
        catalog_uri,
        catalog_prefix,
        catalog_warehouse,
        catalog_properties,
    )?;

    let schema = load_current_schema(&config, table.table())
        .await
        .with_context(|| {
            format!(
                "failed to load current schema for `{}`",
                table.display_name()
            )
        })?;
    let report = schema_report(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        &schema,
    );

    print!(
        "{}",
        render_report_document_markdown(&current_schema_report_document(&report))
    );

    Ok(())
}

fn first_configured_value(
    explicit_value: Option<String>,
    env_var_name: &str,
) -> anyhow::Result<Option<String>> {
    explicit_value.map_or_else(
        || match env::var(env_var_name) {
            Ok(value) => Ok(Some(value)),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(err) => Err(anyhow!(err)).with_context(|| format!("failed to read {env_var_name}")),
        },
        |value| Ok(Some(value)),
    )
}

fn catalog_properties(
    explicit_properties: Vec<String>,
    explicit_headers: Vec<String>,
    catalog_token: Option<String>,
    catalog_credential: Option<String>,
) -> anyhow::Result<HashMap<String, String>> {
    let mut properties = HashMap::new();

    if let Some(env_properties) = first_configured_value(None, "BERG_CATALOG_PROPERTIES")? {
        for property in env_properties
            .split(',')
            .filter(|property| !property.trim().is_empty())
        {
            let (key, value) = parse_catalog_property(property)?;
            properties.insert(key, value);
        }
    }

    if let Some(env_headers) = first_configured_value(None, "BERG_CATALOG_HEADERS")? {
        for header in env_headers
            .split(',')
            .filter(|header| !header.trim().is_empty())
        {
            let (key, value) = parse_header_property(header)?;
            properties.insert(key, value);
        }
    }

    if let Some(token) = catalog_token {
        properties.insert("token".to_string(), token);
    }

    if let Some(credential) = catalog_credential {
        properties.insert("credential".to_string(), credential);
    }

    for property in explicit_properties {
        let (key, value) = parse_catalog_property(&property)?;
        properties.insert(key, value);
    }

    for header in explicit_headers {
        let (key, value) = parse_header_property(&header)?;
        properties.insert(key, value);
    }

    Ok(properties)
}

fn parse_header_property(value: &str) -> anyhow::Result<(String, String)> {
    let (key, value) = parse_catalog_property(value)?;

    Ok((format!("header.{key}"), value))
}

fn render_report_document_markdown(document: &ReportDocument) -> String {
    let mut markdown = String::new();

    writeln!(
        markdown,
        "# {}: {}",
        document.title.label,
        render_report_value_markdown(&document.title.subject)
    )
    .expect("write to string");
    writeln!(markdown).expect("write to string");

    for property in &document.properties {
        writeln!(
            markdown,
            "- {}: {}",
            property.label,
            render_report_value_markdown(&property.value)
        )
        .expect("write to string");
    }

    for table in &document.tables {
        writeln!(markdown).expect("write to string");
        writeln!(markdown, "## {}", table.title).expect("write to string");
        writeln!(markdown).expect("write to string");
        writeln!(markdown, "| {} |", table.columns.join(" | ")).expect("write to string");
        writeln!(
            markdown,
            "| {} |",
            vec!["---"; table.columns.len()].join(" | ")
        )
        .expect("write to string");

        for row in &table.rows {
            let cells = row
                .cells
                .iter()
                .map(|cell| escape_markdown_table_cell(&render_report_value_markdown(cell)))
                .collect::<Vec<_>>();
            writeln!(markdown, "| {} |", cells.join(" | ")).expect("write to string");
        }
    }

    markdown
}

fn render_report_value_markdown(value: &ReportValue) -> String {
    match value {
        ReportValue::Text(value) => value.clone(),
        ReportValue::Code(value) => format!("`{value}`"),
        ReportValue::Uri(value) => render_uri_markdown(value),
        ReportValue::Timestamp(value) => format!("`{}`", render_timestamp_utc(*value)),
        ReportValue::SchemaId(value) | ReportValue::FieldId(value) => format!("`{value}`"),
        ReportValue::Number(value) => format!("`{value}`"),
        ReportValue::Count(value) => format!("`{value}`"),
        ReportValue::Bool(value) | ReportValue::Required(value) => {
            if *value {
                "yes".to_string()
            } else {
                "no".to_string()
            }
        }
        ReportValue::CodeList(values) | ReportValue::IdentifierList(values) => {
            if values.is_empty() {
                "none".to_string()
            } else {
                values
                    .iter()
                    .map(|value| format!("`{value}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        }
    }
}

fn render_uri_markdown(value: &str) -> String {
    format!("`{value}`")
}

fn escape_markdown_table_cell(value: &str) -> String {
    value.replace('|', r"\|")
}

fn render_timestamp_utc(timestamp: OffsetDateTime) -> String {
    let rfc3339 = timestamp
        .format(&Rfc3339)
        .expect("timestamp should format as RFC 3339");
    let (date, time) = rfc3339
        .split_once('T')
        .expect("RFC 3339 timestamp should contain date/time separator");
    let time = time.trim_end_matches('Z');

    format!("{date} {} UTC", time.split('.').next().unwrap_or(time))
}

#[cfg(test)]
mod tests {
    use berg_core::view::{ReportDocument, ReportProperty, ReportRow, ReportTable, ReportTitle};

    use super::{ReportValue, render_report_document_markdown};

    #[test]
    fn renders_report_document_as_markdown() {
        let document = ReportDocument {
            title: ReportTitle {
                label: "Schema",
                subject: ReportValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string()),
            },
            properties: vec![
                ReportProperty {
                    label: "Source endpoint",
                    value: ReportValue::Uri("https://example.test/catalog".to_string()),
                },
                ReportProperty {
                    label: "Schema ID",
                    value: ReportValue::SchemaId(3),
                },
                ReportProperty {
                    label: "Identifier fields",
                    value: ReportValue::IdentifierList(vec![
                        "org_id".to_string(),
                        "_key".to_string(),
                    ]),
                },
            ],
            tables: vec![ReportTable {
                title: "Fields",
                columns: vec!["Path", "Type", "Required", "Field ID"],
                rows: vec![ReportRow {
                    cells: vec![
                        ReportValue::Code("metadata.labels".to_string()),
                        ReportValue::Code("map<string, string>".to_string()),
                        ReportValue::Required(false),
                        ReportValue::FieldId(36),
                    ],
                }],
            }],
        };

        let markdown = render_report_document_markdown(&document);

        assert!(markdown.contains("# Schema: `lakehouse.redapl_v3.k8s_pod_blue`"));
        assert!(markdown.contains("- Source endpoint: `https://example.test/catalog`"));
        assert!(markdown.contains("- Schema ID: `3`"));
        assert!(markdown.contains("- Identifier fields: `org_id`, `_key`"));
        assert!(markdown.contains("| `metadata.labels` | `map<string, string>` | no | `36` |"));
    }
}
