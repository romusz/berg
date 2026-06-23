use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fmt::Write;

use anyhow::{Context, anyhow};
use berg_core::document::{
    ApplicabilityStatus, Block, Cell, CompatibilityStatus, CompletenessStatus, ConfidenceStatus,
    DeltaDirection, Document, DocumentValue, List, ListKind, PrecisionStatus, Presence, Row,
    Status, SupportStatus, Table, UnknownValueKind,
};
use berg_core::engine::{
    CurrentSchemaInfo, QualifiedTableIdent, RestCatalogConfig, load_current_data_file_size_stats,
    load_current_manifest_file_detail, load_current_manifest_file_list, load_current_schema,
    load_current_schema_info, load_current_table_max, load_current_table_partitions,
    load_current_table_properties, load_current_table_stats, load_table_snapshot_list,
    parse_catalog_property,
};
use berg_core::report::{
    data_file_size_stats_document, manifest_file_detail_document, manifest_file_list_document,
    schema_document, table_data_max_document, table_partitions_document, table_properties_document,
    table_snapshot_list_document, table_stats_document,
};
use berg_core::spec;
use clap::error::ErrorKind;
use clap::{ArgAction, Args, Command, CommandFactory, Parser, Subcommand, ValueEnum};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

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
    #[arg(
        long = "catalog-uri",
        value_name = "URI",
        global = true,
        help_heading = "Global catalog options"
    )]
    uri: Option<String>,

    /// REST catalog prefix. Defaults to the catalog segment in the table ID.
    /// Overrides `BERG_CATALOG_PREFIX`.
    #[arg(
        long = "catalog-prefix",
        value_name = "PREFIX",
        global = true,
        help_heading = "Global catalog options"
    )]
    prefix: Option<String>,

    /// REST catalog warehouse. Overrides `BERG_CATALOG_WAREHOUSE`.
    #[arg(
        long = "catalog-warehouse",
        value_name = "WAREHOUSE",
        global = true,
        help_heading = "Global catalog options"
    )]
    warehouse: Option<String>,

    /// REST catalog bearer token. Overrides `BERG_CATALOG_TOKEN`.
    #[arg(
        long = "catalog-token",
        value_name = "TOKEN",
        global = true,
        help_heading = "Global catalog options"
    )]
    token: Option<String>,

    /// REST catalog OAuth credential. Overrides `BERG_CATALOG_CREDENTIAL`.
    #[arg(
        long = "catalog-credential",
        value_name = "CREDENTIAL",
        global = true,
        help_heading = "Global catalog options"
    )]
    credential: Option<String>,

    /// Additional REST catalog header as name=value. Can be repeated.
    #[arg(
        long = "catalog-header",
        value_name = "NAME=VALUE",
        global = true,
        help_heading = "Global catalog options"
    )]
    headers: Vec<String>,

    /// Additional REST catalog property as key=value. Can be repeated.
    #[arg(
        long = "catalog-property",
        value_name = "KEY=VALUE",
        global = true,
        help_heading = "Global catalog options"
    )]
    properties: Vec<String>,

    /// AWS profile used to read S3 table metadata/data files. Overrides `BERG_S3_PROFILE`.
    #[arg(
        long = "s3-profile",
        value_name = "PROFILE",
        global = true,
        help_heading = "Global storage options"
    )]
    s3_profile: Option<String>,

    /// aws-vault profile used to read S3 table metadata/data files. Overrides `BERG_AWS_VAULT_PROFILE`.
    #[arg(
        long = "aws-vault-profile",
        value_name = "PROFILE",
        global = true,
        help_heading = "Global storage options"
    )]
    aws_vault_profile: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Inspect tables.
    Table(TableArgs),
    /// Print the full command tree.
    #[command(name = "commands")]
    CommandTree(CommandTreeArgs),
}

#[derive(Debug, Args)]
#[command(disable_help_flag = true)]
struct CommandTreeArgs {
    /// Print the full command tree.
    #[arg(short = 'h', long = "help")]
    _help: bool,
}

#[derive(Debug, Args)]
struct TableArgs {
    #[command(subcommand)]
    command: TableCommands,
}

#[derive(Debug, Subcommand)]
enum TableCommands {
    /// Inspect table data.
    Data(TableDataArgs),
    /// Inspect table manifests.
    Manifest(TableManifestArgs),
    /// Inspect table partitions.
    Partitions(TablePartitionsArgs),
    /// Inspect table properties.
    Properties(TablePropertiesArgs),
    /// Inspect table schemas.
    Schema(TableSchemaArgs),
    /// Inspect table snapshots.
    Snapshots(TableSnapshotsArgs),
    /// Inspect table statistics.
    Stats(TableStatsArgs),
}

#[derive(Debug, Args)]
struct TableSchemaArgs {
    #[command(subcommand)]
    command: TableSchemaCommands,
}

#[derive(Debug, Subcommand)]
enum TableSchemaCommands {
    /// Compare the current schema across catalog endpoints.
    Compare(CompareSchemaArgs),
    /// Show the current schema.
    Current(CurrentSchemaArgs),
}

#[derive(Debug, Args)]
#[command(
    override_usage = "berg table schema compare --catalog-uri-template <URI> [--show-schema] <table-id> <endpoint-labels>"
)]
struct CompareSchemaArgs {
    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    /// Comma-separated endpoint labels to compare.
    #[arg(value_name = "endpoint-labels")]
    endpoint_labels: String,

    /// REST catalog URI template. Use `{label}` or `{endpoint}` for each endpoint label.
    #[arg(long = "catalog-uri-template", value_name = "URI")]
    catalog_uri_template: String,

    /// Print the baseline current schema when all endpoints match.
    #[arg(long)]
    show_schema: bool,
}

#[derive(Debug, Args)]
struct CurrentSchemaArgs {
    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    #[command(flatten)]
    output: DocumentOutputArgs,
}

#[derive(Debug, Args)]
struct TableDataArgs {
    #[command(subcommand)]
    command: TableDataCommands,
}

#[derive(Debug, Subcommand)]
enum TableDataCommands {
    /// Inspect data files.
    Files(TableDataFilesArgs),
    /// Compute metadata-derived max values.
    Max(TableDataMaxArgs),
}

#[derive(Debug, Args)]
struct TableDataMaxArgs {
    #[command(subcommand)]
    command: TableDataMaxCommands,
}

#[derive(Debug, Subcommand)]
enum TableDataMaxCommands {
    /// Show metadata-derived max for a current snapshot column.
    Current(CurrentTableDataMaxArgs),
}

#[derive(Debug, Args)]
struct CurrentTableDataMaxArgs {
    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    /// Exact current-schema field path.
    #[arg(value_name = "column-name")]
    column: String,

    #[command(flatten)]
    output: DocumentOutputArgs,
}

#[derive(Debug, Args)]
struct TableDataFilesArgs {
    #[command(subcommand)]
    command: TableDataFilesCommands,
}

#[derive(Debug, Subcommand)]
enum TableDataFilesCommands {
    /// Show data file size statistics for the current snapshot.
    Stats(DataFileSizeStatsArgs),
}

#[derive(Debug, Args)]
struct TableManifestArgs {
    #[command(subcommand)]
    command: TableManifestCommands,
}

#[derive(Debug, Subcommand)]
enum TableManifestCommands {
    /// Inspect manifest files.
    Files(TableManifestFilesArgs),
}

#[derive(Debug, Args)]
struct TableManifestFilesArgs {
    #[command(subcommand)]
    command: TableManifestFilesCommands,
}

#[derive(Debug, Subcommand)]
enum TableManifestFilesCommands {
    /// List manifest files for the current snapshot.
    List(ManifestFileListArgs),
    /// Inspect one manifest file from the current snapshot.
    Inspect(ManifestFileDetailArgs),
}

#[derive(Debug, Args)]
struct TablePartitionsArgs {
    #[command(subcommand)]
    command: TablePartitionsCommands,
}

#[derive(Debug, Subcommand)]
enum TablePartitionsCommands {
    /// Show the current partition spec and current snapshot partitions.
    Current(CurrentTablePartitionsArgs),
}

#[derive(Debug, Args)]
struct CurrentTablePartitionsArgs {
    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    #[command(flatten)]
    output: DocumentOutputArgs,
}

#[derive(Debug, Args)]
struct TablePropertiesArgs {
    #[command(subcommand)]
    command: TablePropertiesCommands,
}

#[derive(Debug, Subcommand)]
enum TablePropertiesCommands {
    /// Show properties from the current table metadata.
    Current(CurrentTablePropertiesArgs),
}

#[derive(Debug, Args)]
struct CurrentTablePropertiesArgs {
    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    #[command(flatten)]
    output: DocumentOutputArgs,
}

#[derive(Debug, Args)]
struct TableStatsArgs {
    #[command(subcommand)]
    command: TableStatsCommands,
}

#[derive(Debug, Args)]
struct TableSnapshotsArgs {
    #[command(subcommand)]
    command: TableSnapshotsCommands,
}

#[derive(Debug, Subcommand)]
enum TableSnapshotsCommands {
    /// List snapshots retained in the current table metadata.
    List(TableSnapshotListArgs),
}

#[derive(Debug, Args)]
struct TableSnapshotListArgs {
    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    #[command(flatten)]
    output: DocumentOutputArgs,
}

#[derive(Debug, Subcommand)]
enum TableStatsCommands {
    /// Show statistics for the current snapshot.
    Current(CurrentTableStatsArgs),
}

#[derive(Debug, Args)]
struct CurrentTableStatsArgs {
    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    #[command(flatten)]
    output: DocumentOutputArgs,
}

#[derive(Debug, Args)]
struct DataFileSizeStatsArgs {
    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    #[command(flatten)]
    output: DocumentOutputArgs,
}

#[derive(Debug, Args)]
struct ManifestFileListArgs {
    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    #[command(flatten)]
    output: DocumentOutputArgs,
}

#[derive(Debug, Args)]
struct DocumentOutputArgs {
    /// Output format for document-producing commands.
    #[arg(
        long,
        value_enum,
        default_value = "markdown",
        help_heading = "Output options"
    )]
    format: DocumentFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DocumentFormat {
    /// Render as GitHub-flavored Markdown.
    Markdown,
    /// Render the semantic document AST using Rust debug formatting.
    Ast,
    /// Render as JSON. Reserved for future implementation.
    Json,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args_os().collect::<Vec<_>>();
    let cli = match Cli::try_parse_from(args.clone()) {
        Ok(cli) => cli,
        Err(err) => {
            if should_show_incomplete_command_help(err.kind())
                && let Some(help) = incomplete_command_help(&args[1..])
            {
                print!("{help}");
                return Ok(());
            }

            err.exit();
        }
    };
    let catalog_args = cli.catalog;

    match cli.command {
        Some(Commands::Table(table_args)) => match table_args.command {
            TableCommands::Data(data_args) => match data_args.command {
                TableDataCommands::Files(files_args) => match files_args.command {
                    TableDataFilesCommands::Stats(args) => {
                        print_data_file_size_stats(args, catalog_args).await?;
                    }
                },
                TableDataCommands::Max(max_args) => match max_args.command {
                    TableDataMaxCommands::Current(args) => {
                        print_current_table_data_max(args, catalog_args).await?;
                    }
                },
            },
            TableCommands::Manifest(manifest_args) => match manifest_args.command {
                TableManifestCommands::Files(files_args) => {
                    print_manifest_files(files_args, catalog_args).await?;
                }
            },
            TableCommands::Partitions(partitions_args) => match partitions_args.command {
                TablePartitionsCommands::Current(args) => {
                    print_current_table_partitions(args, catalog_args).await?;
                }
            },
            TableCommands::Properties(properties_args) => match properties_args.command {
                TablePropertiesCommands::Current(args) => {
                    print_current_table_properties(args, catalog_args).await?;
                }
            },
            TableCommands::Schema(schema_args) => match schema_args.command {
                TableSchemaCommands::Compare(args) => {
                    if !print_compare_schema(args, catalog_args).await? {
                        std::process::exit(1);
                    }
                }
                TableSchemaCommands::Current(args) => {
                    print_current_schema(args, catalog_args).await?;
                }
            },
            TableCommands::Snapshots(snapshots_args) => match snapshots_args.command {
                TableSnapshotsCommands::List(args) => {
                    print_table_snapshot_list(args, catalog_args).await?;
                }
            },
            TableCommands::Stats(stats_args) => match stats_args.command {
                TableStatsCommands::Current(args) => {
                    print_current_table_stats(args, catalog_args).await?;
                }
            },
        },
        Some(Commands::CommandTree(_)) => print_command_tree(),
        None => println!("{}", berg_core::welcome_message("berg")?),
    }

    Ok(())
}

fn print_command_tree() {
    print!("{}", command_tree());
}

fn should_show_incomplete_command_help(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::InvalidSubcommand | ErrorKind::MissingSubcommand
    )
}

fn incomplete_command_help(args: &[OsString]) -> Option<String> {
    let command = Cli::command();
    let path = deepest_valid_subcommand_path(&command, args);
    let mut command = Cli::command();
    let command = command_path_mut(&mut command, &path)?;

    command
        .has_subcommands()
        .then(|| command_help(&path))
        .flatten()
}

fn command_help(path: &[String]) -> Option<String> {
    let mut args = Vec::with_capacity(path.len() + 2);
    args.push(OsString::from("berg"));
    args.extend(path.iter().map(OsString::from));
    args.push(OsString::from("--help"));

    let err = Cli::command()
        .try_get_matches_from(args)
        .expect_err("help exits");
    (err.kind() == ErrorKind::DisplayHelp).then(|| err.render().to_string())
}

fn deepest_valid_subcommand_path(command: &Command, args: &[OsString]) -> Vec<String> {
    let root_command = command;
    let mut command = command;
    let mut path = Vec::new();
    let mut index = 0;

    while index < args.len() {
        let arg = &args[index];

        if let Some(skip_next) = option_value_to_skip(command, root_command, arg) {
            index += 1 + usize::from(skip_next);
            continue;
        }

        if let Some(subcommand) = command.find_subcommand(arg) {
            path.push(subcommand.get_name().to_string());
            command = subcommand;
            index += 1;
            continue;
        }

        if command.has_subcommands() {
            break;
        }

        index += 1;
    }

    path
}

fn option_value_to_skip(command: &Command, root_command: &Command, arg: &OsString) -> Option<bool> {
    let arg = arg.to_str()?;

    if !arg.starts_with('-') || arg == "-" {
        return None;
    }

    if arg == "--" {
        return Some(false);
    }

    let Some(long_option) = arg.strip_prefix("--") else {
        return Some(false);
    };
    let (long_option, inline_value) = long_option
        .split_once('=')
        .map_or((long_option, false), |(long_option, _)| (long_option, true));

    Some(
        !inline_value
            && option_takes_value(command, long_option).or_else(|| {
                (std::ptr::addr_eq(command, root_command))
                    .then_some(false)
                    .or_else(|| option_takes_value(root_command, long_option))
            })?,
    )
}

fn option_takes_value(command: &Command, long_option: &str) -> Option<bool> {
    command
        .get_arguments()
        .find(|argument| argument.get_long() == Some(long_option))
        .map(|argument| matches!(argument.get_action(), ArgAction::Set | ArgAction::Append))
}

fn command_path_mut<'a>(command: &'a mut Command, path: &[String]) -> Option<&'a mut Command> {
    let mut command = command;

    for name in path {
        command = command.find_subcommand_mut(name)?;
    }

    Some(command)
}

fn command_tree() -> String {
    let command = Cli::command();
    let mut output = String::new();

    writeln!(
        output,
        "{} - {}",
        command.get_name(),
        command_description(&command)
    )
    .expect("write to string");
    render_subcommand_tree(&command, "", &mut output);

    output
}

fn render_subcommand_tree(command: &clap::Command, prefix: &str, output: &mut String) {
    let subcommands = command.get_subcommands().collect::<Vec<_>>();

    for (index, subcommand) in subcommands.iter().enumerate() {
        let is_last = index == subcommands.len() - 1;
        let branch = if is_last { "└── " } else { "├── " };

        writeln!(
            output,
            "{prefix}{branch}{} - {}",
            subcommand.get_name(),
            command_description(subcommand)
        )
        .expect("write to string");

        let next_prefix = format!("{prefix}{}", if is_last { "    " } else { "│   " });
        render_subcommand_tree(subcommand, &next_prefix, output);
    }
}

fn command_description(command: &clap::Command) -> String {
    command
        .get_about()
        .map_or_else(String::new, ToString::to_string)
}

async fn print_current_schema(
    args: CurrentSchemaArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let config = rest_catalog_config(catalog_args, &table)?;

    let schema = load_current_schema(&config, table.table())
        .await
        .with_context(|| {
            format!(
                "failed to load current schema for `{}`",
                table.display_name()
            )
        })?;
    let document = schema_document(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        schema,
    );

    print!("{}", render_document(&document, args.output.format)?);

    Ok(())
}

#[derive(Debug)]
struct EndpointSchema {
    label: String,
    endpoint: String,
    info: CurrentSchemaInfo,
}

#[derive(Debug)]
struct SchemaCompareConfig {
    catalog_uri_template: String,
    catalog_prefix: Option<String>,
    catalog_warehouse: Option<String>,
    catalog_properties: HashMap<String, String>,
    s3_profile: Option<String>,
    aws_vault_profile: Option<String>,
}

async fn print_compare_schema(
    args: CompareSchemaArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<bool> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let endpoint_labels = parse_endpoint_labels(&args.endpoint_labels)?;
    let compare_config = schema_compare_config(&args, catalog_args)?;
    let mut results = Vec::with_capacity(endpoint_labels.len());

    for endpoint_label in &endpoint_labels {
        results.push(
            load_endpoint_schema(endpoint_label, &table, &compare_config)
                .await
                .with_context(|| {
                    format!(
                        "failed to load current schema for `{}` from endpoint `{endpoint_label}`",
                        table.display_name()
                    )
                })?,
        );
    }

    let baseline = &results[0];
    let mut mismatches = Vec::new();
    for result in &results[1..] {
        if !schema_equal(baseline.info.schema.as_ref(), result.info.schema.as_ref()) {
            mismatches.push(result);
        }
    }

    let mut output = String::new();
    write_schema_compare_header(&mut output, &table, &endpoint_labels);
    write_schema_compare_summary(&mut output, &results);

    if !mismatches.is_empty() {
        write_schema_compare_result(&mut output, false, baseline.label.as_str(), results.len());
        write_schema_diffs(&mut output, baseline, &mismatches)?;
        print!("{output}");

        return Ok(false);
    }

    write_schema_compare_result(&mut output, true, baseline.label.as_str(), results.len());

    if args.show_schema {
        write_baseline_schema_section(&mut output, &table, baseline);
    }

    print!("{output}");

    Ok(true)
}

fn write_schema_compare_header(
    output: &mut String,
    table: &QualifiedTableIdent,
    endpoint_labels: &[String],
) {
    writeln!(output, "# Schema Compare: `{}`", table.display_name()).expect("write to string");
    writeln!(output).expect("write to string");
    writeln!(output, "- Table: `{}`", table.display_name()).expect("write to string");
    writeln!(
        output,
        "- Compared endpoints: `{}`",
        endpoint_labels.join("`, `")
    )
    .expect("write to string");
    writeln!(output).expect("write to string");
}

fn write_schema_compare_result(
    output: &mut String,
    schemas_match: bool,
    baseline_label: &str,
    endpoint_count: usize,
) {
    writeln!(output, "## Result").expect("write to string");
    writeln!(output).expect("write to string");

    if schemas_match {
        writeln!(output, "Schemas match across `{endpoint_count}` endpoints.")
            .expect("write to string");
    } else {
        writeln!(output, "Schemas differ from baseline `{baseline_label}`.")
            .expect("write to string");
    }

    writeln!(output).expect("write to string");
}

fn write_baseline_schema_section(
    output: &mut String,
    table: &QualifiedTableIdent,
    baseline: &EndpointSchema,
) {
    writeln!(output, "## Baseline Schema: `{}`", baseline.label).expect("write to string");
    writeln!(output).expect("write to string");

    let document = schema_document(
        table.display_name(),
        baseline.endpoint.clone(),
        OffsetDateTime::now_utc(),
        baseline.info.schema.clone(),
    );
    render_blocks_markdown(&document.blocks, 3, output);
}

async fn load_endpoint_schema(
    endpoint_label: &str,
    table: &QualifiedTableIdent,
    compare_config: &SchemaCompareConfig,
) -> anyhow::Result<EndpointSchema> {
    let config = endpoint_rest_catalog_config(endpoint_label, table, compare_config)?;
    let endpoint = config.table_endpoint(table.table());
    let info = load_current_schema_info(&config, table.table()).await?;

    Ok(EndpointSchema {
        label: endpoint_label.to_string(),
        endpoint,
        info,
    })
}

fn schema_compare_config(
    args: &CompareSchemaArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<SchemaCompareConfig> {
    let catalog_uri_template = args.catalog_uri_template.clone();
    let catalog_prefix = first_configured_value(catalog_args.prefix, "BERG_CATALOG_PREFIX")?;
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
    let s3_profile = first_configured_value(catalog_args.s3_profile, "BERG_S3_PROFILE")?;
    let aws_vault_profile =
        first_configured_value(catalog_args.aws_vault_profile, "BERG_AWS_VAULT_PROFILE")?;

    Ok(SchemaCompareConfig {
        catalog_uri_template,
        catalog_prefix,
        catalog_warehouse,
        catalog_properties,
        s3_profile,
        aws_vault_profile,
    })
}

fn endpoint_rest_catalog_config(
    endpoint_label: &str,
    table: &QualifiedTableIdent,
    compare_config: &SchemaCompareConfig,
) -> anyhow::Result<RestCatalogConfig> {
    let prefix = compare_config
        .catalog_prefix
        .as_deref()
        .unwrap_or_else(|| table.catalog());
    let config = RestCatalogConfig::new(
        render_endpoint_template(&compare_config.catalog_uri_template, endpoint_label),
        prefix,
        compare_config.catalog_warehouse.clone(),
        compare_config.catalog_properties.clone(),
    )?;

    Ok(config
        .with_s3_profile(compare_config.s3_profile.clone())
        .with_aws_vault_profile(compare_config.aws_vault_profile.clone()))
}

fn render_endpoint_template(template: &str, endpoint_label: &str) -> String {
    template
        .replace("{endpoint}", endpoint_label)
        .replace("{label}", endpoint_label)
}

fn parse_endpoint_labels(value: &str) -> anyhow::Result<Vec<String>> {
    let endpoint_labels = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if endpoint_labels.len() < 2 {
        anyhow::bail!("provide at least two endpoint labels");
    }

    Ok(endpoint_labels)
}

fn write_schema_compare_summary(output: &mut String, results: &[EndpointSchema]) {
    writeln!(output, "## Endpoint Summary").expect("write to string");
    writeln!(output).expect("write to string");
    writeln!(
        output,
        "| Endpoint | Schema ID | Fields | Metadata location |"
    )
    .expect("write to string");
    writeln!(output, "| --- | ---: | ---: | --- |").expect("write to string");

    for result in results {
        writeln!(
            output,
            "| `{}` | `{}` | `{}` | `{}` |",
            escape_markdown_table_cell(&result.label),
            result.info.current_schema_id,
            result.info.schema.as_struct().fields().len(),
            escape_markdown_table_cell(&result.info.metadata_json_path)
        )
        .expect("write to string");
    }

    writeln!(output).expect("write to string");
}

fn write_schema_diffs(
    output: &mut String,
    baseline: &EndpointSchema,
    others: &[&EndpointSchema],
) -> anyhow::Result<()> {
    let baseline_text = canonical_schema_text(baseline.info.schema.as_ref())?;

    writeln!(output, "## Schema Diffs").expect("write to string");

    for other in others {
        writeln!(output).expect("write to string");
        writeln!(output, "### `{}` vs `{}`", baseline.label, other.label).expect("write to string");
        writeln!(output).expect("write to string");
        writeln!(output, "```diff").expect("write to string");
        output.push_str(&unified_diff(
            &baseline.label,
            &other.label,
            &baseline_text,
            &canonical_schema_text(other.info.schema.as_ref())?,
        ));
        writeln!(output, "```").expect("write to string");
    }

    writeln!(output).expect("write to string");

    Ok(())
}

fn schema_equal(left: &spec::Schema, right: &spec::Schema) -> bool {
    left == right
}

fn canonical_schema_text(schema: &spec::Schema) -> anyhow::Result<Vec<String>> {
    Ok(serde_json::to_string_pretty(schema)?
        .lines()
        .map(ToString::to_string)
        .collect())
}

fn unified_diff(left_label: &str, right_label: &str, left: &[String], right: &[String]) -> String {
    const CONTEXT_LINES: usize = 3;

    let mut prefix_len = 0;
    while prefix_len < left.len()
        && prefix_len < right.len()
        && left[prefix_len] == right[prefix_len]
    {
        prefix_len += 1;
    }

    if prefix_len == left.len() && prefix_len == right.len() {
        return String::new();
    }

    let mut suffix_len = 0;
    while suffix_len < left.len() - prefix_len
        && suffix_len < right.len() - prefix_len
        && left[left.len() - 1 - suffix_len] == right[right.len() - 1 - suffix_len]
    {
        suffix_len += 1;
    }

    let left_change_end = left.len() - suffix_len;
    let right_change_end = right.len() - suffix_len;
    let left_hunk_start = prefix_len.saturating_sub(CONTEXT_LINES);
    let right_hunk_start = prefix_len.saturating_sub(CONTEXT_LINES);
    let left_hunk_end = left.len().min(left_change_end + CONTEXT_LINES);
    let right_hunk_end = right.len().min(right_change_end + CONTEXT_LINES);
    let mut output = String::new();

    writeln!(output, "--- {left_label}").expect("write to string");
    writeln!(output, "+++ {right_label}").expect("write to string");
    writeln!(
        output,
        "@@ -{},{} +{},{} @@",
        hunk_range_start(left_hunk_start, left_hunk_end - left_hunk_start),
        left_hunk_end - left_hunk_start,
        hunk_range_start(right_hunk_start, right_hunk_end - right_hunk_start),
        right_hunk_end - right_hunk_start
    )
    .expect("write to string");

    for line in &left[left_hunk_start..prefix_len] {
        writeln!(output, " {line}").expect("write to string");
    }
    for line in &left[prefix_len..left_change_end] {
        writeln!(output, "-{line}").expect("write to string");
    }
    for line in &right[prefix_len..right_change_end] {
        writeln!(output, "+{line}").expect("write to string");
    }
    for line in &left[left_change_end..left_hunk_end] {
        writeln!(output, " {line}").expect("write to string");
    }

    output
}

fn hunk_range_start(start_index: usize, count: usize) -> usize {
    if count == 0 {
        start_index
    } else {
        start_index + 1
    }
}

async fn print_current_table_partitions(
    args: CurrentTablePartitionsArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let config = rest_catalog_config(catalog_args, &table)?;

    let stats = load_current_table_partitions(&config, table.table())
        .await
        .with_context(|| {
            format!(
                "failed to load current table partitions for `{}`",
                table.display_name()
            )
        })?;
    let document = table_partitions_document(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        &stats,
    );

    print!("{}", render_document(&document, args.output.format)?);

    Ok(())
}

async fn print_current_table_properties(
    args: CurrentTablePropertiesArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let config = rest_catalog_config(catalog_args, &table)?;

    let properties = load_current_table_properties(&config, table.table())
        .await
        .with_context(|| {
            format!(
                "failed to load current table properties for `{}`",
                table.display_name()
            )
        })?;
    let document = table_properties_document(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        &properties,
    );

    print!("{}", render_document(&document, args.output.format)?);

    Ok(())
}

async fn print_table_snapshot_list(
    args: TableSnapshotListArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let config = rest_catalog_config(catalog_args, &table)?;

    let snapshots = load_table_snapshot_list(&config, table.table())
        .await
        .with_context(|| {
            format!(
                "failed to load table snapshots for `{}`",
                table.display_name()
            )
        })?;
    let document = table_snapshot_list_document(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        &snapshots,
    );

    print!("{}", render_document(&document, args.output.format)?);

    Ok(())
}

async fn print_current_table_stats(
    args: CurrentTableStatsArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let config = rest_catalog_config(catalog_args, &table)?;

    let stats = load_current_table_stats(&config, table.table())
        .await
        .with_context(|| {
            format!(
                "failed to load current table statistics for `{}`",
                table.display_name()
            )
        })?;
    let document = table_stats_document(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        &stats,
    );

    print!("{}", render_document(&document, args.output.format)?);

    Ok(())
}

async fn print_data_file_size_stats(
    args: DataFileSizeStatsArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let config = rest_catalog_config(catalog_args, &table)?;

    let stats = load_current_data_file_size_stats(&config, table.table())
        .await
        .with_context(|| {
            format!(
                "failed to load current data file size statistics for `{}`",
                table.display_name()
            )
        })?;
    let document = data_file_size_stats_document(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        &stats,
    );

    print!("{}", render_document(&document, args.output.format)?);

    Ok(())
}

async fn print_current_table_data_max(
    args: CurrentTableDataMaxArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let config = rest_catalog_config(catalog_args, &table)?;

    let max = load_current_table_max(&config, table.table(), &args.column)
        .await
        .with_context(|| {
            format!(
                "failed to load metadata-derived max for `{}` column `{}`",
                table.display_name(),
                args.column
            )
        })?;
    let document = table_data_max_document(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        &max,
    );

    print!("{}", render_document(&document, args.output.format)?);

    Ok(())
}

async fn print_manifest_files(
    args: TableManifestFilesArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    match args.command {
        TableManifestFilesCommands::List(args) => {
            print_manifest_file_list(args, catalog_args).await
        }
        TableManifestFilesCommands::Inspect(args) => {
            print_manifest_file_detail(args, catalog_args).await
        }
    }
}

async fn print_manifest_file_list(
    args: ManifestFileListArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let config = rest_catalog_config(catalog_args, &table)?;

    let manifest_files = load_current_manifest_file_list(&config, table.table())
        .await
        .with_context(|| {
            format!(
                "failed to load current manifest files for `{}`",
                table.display_name()
            )
        })?;
    let document = manifest_file_list_document(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        &manifest_files,
    );

    print!("{}", render_document(&document, args.output.format)?);

    Ok(())
}

#[derive(Debug, Args)]
struct ManifestFileDetailArgs {
    /// Manifest ID from `table manifest files list`.
    #[arg(value_name = "manifest-id")]
    manifest_file_id: String,

    /// Table ID: catalog.namespace.table.
    #[arg(value_name = "table-id")]
    table: String,

    #[command(flatten)]
    output: DocumentOutputArgs,
}

async fn print_manifest_file_detail(
    args: ManifestFileDetailArgs,
    catalog_args: CatalogArgs,
) -> anyhow::Result<()> {
    let table = QualifiedTableIdent::parse(&args.table)?;
    let config = rest_catalog_config(catalog_args, &table)?;

    let detail = load_current_manifest_file_detail(&config, table.table(), &args.manifest_file_id)
        .await
        .with_context(|| {
            format!(
                "failed to load current manifest file `{}` for `{}`",
                args.manifest_file_id,
                table.display_name()
            )
        })?;
    let document = manifest_file_detail_document(
        table.display_name(),
        config.table_endpoint(table.table()),
        OffsetDateTime::now_utc(),
        &detail,
    );

    print!("{}", render_document(&document, args.output.format)?);

    Ok(())
}

fn rest_catalog_config(
    catalog_args: CatalogArgs,
    table: &QualifiedTableIdent,
) -> anyhow::Result<RestCatalogConfig> {
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
    let s3_profile = first_configured_value(catalog_args.s3_profile, "BERG_S3_PROFILE")?;
    let aws_vault_profile =
        first_configured_value(catalog_args.aws_vault_profile, "BERG_AWS_VAULT_PROFILE")?;

    let config = RestCatalogConfig::new(
        catalog_uri,
        catalog_prefix,
        catalog_warehouse,
        catalog_properties,
    )?;

    Ok(config
        .with_s3_profile(s3_profile)
        .with_aws_vault_profile(aws_vault_profile))
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

fn render_document(document: &Document, format: DocumentFormat) -> anyhow::Result<String> {
    match format {
        DocumentFormat::Markdown => Ok(render_document_markdown(document)),
        DocumentFormat::Ast => Ok(format!("{document:#?}\n")),
        DocumentFormat::Json => Err(anyhow!("JSON document rendering is not implemented yet")),
    }
}

fn render_document_markdown(document: &Document) -> String {
    let mut markdown = String::new();

    writeln!(markdown, "# {}", render_cell_markdown(&document.title)).expect("write to string");
    writeln!(markdown).expect("write to string");

    render_blocks_markdown(&document.blocks, 2, &mut markdown);

    markdown
}

fn render_blocks_markdown(blocks: &[Block], heading_level: usize, markdown: &mut String) {
    for block in blocks {
        match block {
            Block::Paragraph(cell) => {
                writeln!(markdown, "{}", render_cell_markdown(cell)).expect("write to string");
            }
            Block::Properties(properties) => {
                for property in properties {
                    writeln!(
                        markdown,
                        "- {}: {}",
                        property.label,
                        render_cell_markdown(&property.value)
                    )
                    .expect("write to string");
                }
            }
            Block::Table(table) => render_table_markdown(table, markdown),
            Block::Section(section) => {
                writeln!(markdown).expect("write to string");
                writeln!(
                    markdown,
                    "{} {}",
                    "#".repeat(heading_level.min(6)),
                    render_cell_markdown(&section.title)
                )
                .expect("write to string");
                writeln!(markdown).expect("write to string");
                render_blocks_markdown(&section.blocks, heading_level + 1, markdown);
            }
            Block::List(list) => render_list_markdown(list, heading_level, markdown),
            Block::FencedCode(code) => {
                writeln!(
                    markdown,
                    "```{}",
                    code.language.as_deref().unwrap_or_default()
                )
                .expect("write to string");
                writeln!(markdown, "{}", code.code).expect("write to string");
                writeln!(markdown, "```").expect("write to string");
            }
            Block::ThematicBreak => {
                writeln!(markdown, "---").expect("write to string");
            }
        }

        writeln!(markdown).expect("write to string");
    }
}

fn render_table_markdown(table: &Table, markdown: &mut String) {
    let format = markdown_table_format(table);
    let columns = markdown_table_columns(table);
    let headers = columns
        .iter()
        .map(|column| escape_markdown_table_cell(&render_table_header_markdown(table, *column)))
        .collect::<Vec<_>>();
    writeln!(markdown, "| {} |", headers.join(" | ")).expect("write to string");

    let separators = columns
        .iter()
        .map(|column| markdown_table_column_separator(table, *column))
        .collect::<Vec<_>>();
    writeln!(markdown, "| {} |", separators.join(" | ")).expect("write to string");

    for row in &table.rows {
        let cells = columns
            .iter()
            .map(|column| {
                escape_markdown_table_cell(&render_table_cell_markdown(row, *column, &format))
            })
            .collect::<Vec<_>>();
        writeln!(markdown, "| {} |", cells.join(" | ")).expect("write to string");
    }
}

#[derive(Debug, Clone)]
struct MarkdownTableFormat {
    change_summary_widths: Vec<Option<ChangeSummaryWidths>>,
}

fn markdown_table_format(table: &Table) -> MarkdownTableFormat {
    MarkdownTableFormat {
        change_summary_widths: (0..table.columns.len())
            .map(|column| table_change_summary_widths(table, column))
            .collect(),
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ChangeSummaryWidths {
    positive: usize,
    negative: usize,
    total: usize,
}

fn table_change_summary_widths(table: &Table, column_index: usize) -> Option<ChangeSummaryWidths> {
    let mut widths = ChangeSummaryWidths::default();
    let mut found = false;

    for row in &table.rows {
        let Some((positive, negative, total)) = row
            .cells
            .get(column_index)
            .and_then(change_summary_cell_values)
        else {
            continue;
        };

        found = true;
        let cell_widths = change_summary_widths(positive, negative, total);
        widths.positive = widths.positive.max(cell_widths.positive);
        widths.negative = widths.negative.max(cell_widths.negative);
        widths.total = widths.total.max(cell_widths.total);
    }

    found.then_some(widths)
}

#[derive(Debug, Clone, Copy)]
enum MarkdownTableColumn {
    Source(usize),
    Bytes(usize),
    BinarySize(usize),
}

fn markdown_table_columns(table: &Table) -> Vec<MarkdownTableColumn> {
    let mut columns = Vec::new();

    for index in 0..table.columns.len() {
        if is_bytes_table_column(table, index) {
            // Keep exact bytes visible for copy/paste and audits. Binary sizes are easier
            // to scan, but they must not replace the canonical byte count.
            if is_binary_size_table_column(table, index) {
                columns.push(MarkdownTableColumn::BinarySize(index));
            } else {
                columns.push(MarkdownTableColumn::Bytes(index));
                columns.push(MarkdownTableColumn::BinarySize(index));
            }
        } else {
            columns.push(MarkdownTableColumn::Source(index));
        }
    }

    columns
}

fn render_table_header_markdown(table: &Table, column: MarkdownTableColumn) -> String {
    match column {
        MarkdownTableColumn::Source(index) => table
            .columns
            .get(index)
            .map_or_else(String::new, render_cell_markdown),
        MarkdownTableColumn::Bytes(index) => render_bytes_table_header_markdown(table, index),
        MarkdownTableColumn::BinarySize(index) => {
            render_binary_size_table_header_markdown(table, index)
        }
    }
}

fn render_bytes_table_header_markdown(table: &Table, column_index: usize) -> String {
    let label = table
        .columns
        .get(column_index)
        .map_or_else(String::new, render_cell_markdown);

    if label == "Size" {
        "Bytes".to_string()
    } else {
        format!("{label} (bytes)")
    }
}

fn render_binary_size_table_header_markdown(table: &Table, column_index: usize) -> String {
    let label = table
        .columns
        .get(column_index)
        .map_or_else(String::new, render_cell_markdown);

    if label == "Size" || label == "Binary size" {
        "Binary size".to_string()
    } else {
        format!("{label} (binary)")
    }
}

fn render_table_cell_markdown(
    row: &Row,
    column: MarkdownTableColumn,
    format: &MarkdownTableFormat,
) -> String {
    match column {
        MarkdownTableColumn::Source(index) => {
            row.cells.get(index).map_or_else(String::new, |cell| {
                render_source_table_cell_markdown(
                    cell,
                    format.change_summary_widths.get(index).copied().flatten(),
                )
            })
        }
        MarkdownTableColumn::Bytes(index) => row
            .cells
            .get(index)
            .map_or_else(String::new, render_bytes_table_cell_markdown),
        MarkdownTableColumn::BinarySize(index) => row
            .cells
            .get(index)
            .map_or_else(String::new, render_binary_size_table_cell_markdown),
    }
}

fn render_source_table_cell_markdown(
    cell: &Cell,
    summary_widths: Option<ChangeSummaryWidths>,
) -> String {
    match cell.values.as_slice() {
        [
            DocumentValue::ChangeSummary {
                positive,
                negative,
                total,
            },
        ] => render_change_summary_markdown(
            *positive,
            *negative,
            *total,
            summary_widths.unwrap_or_else(|| change_summary_widths(*positive, *negative, *total)),
        ),
        _ => render_cell_markdown(cell),
    }
}

fn render_bytes_table_cell_markdown(cell: &Cell) -> String {
    match cell.values.as_slice() {
        [DocumentValue::Bytes(value)] => format!("`{}`", format_u64(*value)),
        _ => render_cell_markdown(cell),
    }
}

fn render_binary_size_table_cell_markdown(cell: &Cell) -> String {
    match cell.values.as_slice() {
        [DocumentValue::Bytes(value)] => render_binary_size_markdown(*value),
        _ => render_cell_markdown(cell),
    }
}

fn markdown_table_column_separator(table: &Table, column: MarkdownTableColumn) -> &'static str {
    if is_center_aligned_markdown_table_column(table, column) {
        ":---:"
    } else if is_right_aligned_markdown_table_column(table, column) {
        "---:"
    } else {
        "---"
    }
}

fn is_center_aligned_markdown_table_column(table: &Table, column: MarkdownTableColumn) -> bool {
    match column {
        MarkdownTableColumn::Source(index) => is_center_aligned_table_column(table, index),
        MarkdownTableColumn::Bytes(_) | MarkdownTableColumn::BinarySize(_) => false,
    }
}

fn is_right_aligned_markdown_table_column(table: &Table, column: MarkdownTableColumn) -> bool {
    match column {
        MarkdownTableColumn::Source(index) => is_right_aligned_table_column(table, index),
        MarkdownTableColumn::Bytes(_) | MarkdownTableColumn::BinarySize(_) => true,
    }
}

fn is_center_aligned_table_column(table: &Table, column_index: usize) -> bool {
    column_index >= 2 && is_manifest_column_metadata_table(table)
}

fn is_manifest_column_metadata_table(table: &Table) -> bool {
    matches!(
        (table.columns.first(), table.columns.get(1)),
        (Some(column), Some(field_id))
            if text_cell_value(column) == Some("Column")
                && text_cell_value(field_id) == Some("Field ID")
    ) && table.columns.len() > 2
        && !table.rows.is_empty()
        && table
            .rows
            .iter()
            .all(|row| row.cells.iter().skip(2).all(is_presence_table_cell))
}

fn is_presence_table_cell(cell: &Cell) -> bool {
    matches!(cell.values.as_slice(), [DocumentValue::Presence(_)])
}

fn text_cell_value(cell: &Cell) -> Option<&str> {
    match cell.values.as_slice() {
        [DocumentValue::Text(value)] => Some(value),
        _ => None,
    }
}

fn is_right_aligned_table_column(table: &Table, column_index: usize) -> bool {
    !table.rows.is_empty()
        && table.rows.iter().all(|row| {
            row.cells
                .get(column_index)
                .is_some_and(is_numeric_table_cell)
        })
}

fn is_numeric_table_cell(cell: &Cell) -> bool {
    matches!(
        cell.values.as_slice(),
        [DocumentValue::Number(_)
            | DocumentValue::Unsigned(_)
            | DocumentValue::Bytes(_)
            | DocumentValue::Delta { .. }
            | DocumentValue::ChangeSummary { .. }
            | DocumentValue::PercentageMillis(_)
            | DocumentValue::Count(_)
            | DocumentValue::MissingValue
            | DocumentValue::UnknownValue {
                kind: UnknownValueKind::Numeric,
            },]
    )
}

fn is_bytes_table_column(table: &Table, column_index: usize) -> bool {
    !table.rows.is_empty()
        && table.rows.iter().all(|row| {
            row.cells
                .get(column_index)
                .is_some_and(is_bytes_or_missing_table_cell)
        })
        && (table
            .rows
            .iter()
            .any(|row| row.cells.get(column_index).is_some_and(is_bytes_table_cell))
            || is_bytes_table_column_label(table, column_index))
}

fn is_bytes_table_cell(cell: &Cell) -> bool {
    matches!(cell.values.as_slice(), [DocumentValue::Bytes(_)])
}

fn is_bytes_or_missing_table_cell(cell: &Cell) -> bool {
    matches!(
        cell.values.as_slice(),
        [DocumentValue::Bytes(_) | DocumentValue::MissingValue]
    )
}

fn is_bytes_table_column_label(table: &Table, column_index: usize) -> bool {
    table.columns.get(column_index).is_some_and(|column| {
        matches!(
            render_cell_markdown(column).as_str(),
            "Size" | "Total size" | "Binary size"
        )
    })
}

fn is_binary_size_table_column(table: &Table, column_index: usize) -> bool {
    table
        .columns
        .get(column_index)
        .is_some_and(|column| render_cell_markdown(column) == "Binary size")
}

fn render_list_markdown(list: &List, heading_level: usize, markdown: &mut String) {
    for (index, item) in list.items.iter().enumerate() {
        let marker = match list.kind {
            ListKind::Unordered => "- ".to_string(),
            ListKind::Ordered { start } => format!("{}. ", start + index),
        };

        let Some((first_block, remaining_blocks)) = item.blocks.split_first() else {
            writeln!(markdown, "{}", marker.trim_end()).expect("write to string");
            continue;
        };

        if let Block::Paragraph(cell) = first_block {
            writeln!(markdown, "{marker}{}", render_cell_markdown(cell)).expect("write to string");
            render_indented_blocks_markdown(
                remaining_blocks,
                heading_level,
                marker.len(),
                markdown,
            );
        } else {
            writeln!(markdown, "{}", marker.trim_end()).expect("write to string");
            render_indented_blocks_markdown(&item.blocks, heading_level, marker.len(), markdown);
        }
    }
}

fn render_indented_blocks_markdown(
    blocks: &[Block],
    heading_level: usize,
    spaces: usize,
    markdown: &mut String,
) {
    if blocks.is_empty() {
        return;
    }

    let mut nested_markdown = String::new();
    render_blocks_markdown(blocks, heading_level, &mut nested_markdown);
    let indentation = " ".repeat(spaces);

    for line in nested_markdown.lines() {
        if line.is_empty() {
            writeln!(markdown).expect("write to string");
        } else {
            writeln!(markdown, "{indentation}{line}").expect("write to string");
        }
    }
}

fn render_cell_markdown(cell: &Cell) -> String {
    cell.values
        .iter()
        .map(render_document_value_markdown)
        .collect::<String>()
}

fn render_document_value_markdown(value: &DocumentValue) -> String {
    match value {
        DocumentValue::Text(value) => value.clone(),
        DocumentValue::Code(value) => format!("`{value}`"),
        DocumentValue::Uri(value) => render_uri_markdown(value),
        DocumentValue::Timestamp(value) => format!("`{}`", render_timestamp_utc(*value)),
        DocumentValue::LocalTimestamp(value) => format!("`{}`", render_timestamp_local(*value)),
        DocumentValue::Number(value) => format!("`{value}`"),
        DocumentValue::Unsigned(value) => format!("`{}`", format_u64(*value)),
        DocumentValue::Bytes(value) => render_bytes_markdown(*value),
        DocumentValue::Delta { direction, value } => render_delta_markdown(*direction, *value),
        DocumentValue::ChangeSummary {
            positive,
            negative,
            total,
        } => render_change_summary_markdown(
            *positive,
            *negative,
            *total,
            change_summary_widths(*positive, *negative, *total),
        ),
        DocumentValue::MissingValue => "?".to_string(),
        DocumentValue::UnknownValue { .. } => "unknown".to_string(),
        DocumentValue::Status(status) => format!("`{}`", render_status_label(*status)),
        DocumentValue::Presence(presence) => render_presence_markdown(*presence).to_string(),
        DocumentValue::PercentageMillis(value) => render_percentage_millis_markdown(*value),
        DocumentValue::Count(value) => format!("`{}`", format_usize(*value)),
        DocumentValue::Bool(value) => {
            if *value {
                "yes".to_string()
            } else {
                "no".to_string()
            }
        }
        DocumentValue::Emphasis(values) => {
            format!("*{}*", render_document_values_markdown(values))
        }
        DocumentValue::Strong(values) => {
            format!("**{}**", render_document_values_markdown(values))
        }
        DocumentValue::Link { label, target } => {
            format!("[{}]({target})", render_document_values_markdown(label))
        }
        DocumentValue::Image { alt, source } => format!("![{alt}]({source})"),
        DocumentValue::LineBreak => "  \n".to_string(),
    }
}

fn render_document_values_markdown(values: &[DocumentValue]) -> String {
    values
        .iter()
        .map(render_document_value_markdown)
        .collect::<String>()
}

fn render_uri_markdown(value: &str) -> String {
    format!("`{value}`")
}

fn render_bytes_markdown(value: u64) -> String {
    let bytes = format_u64(value);
    let Some((scaled, unit)) = binary_size(value) else {
        return format!("`{bytes}` bytes");
    };

    format!("`{bytes}` bytes (`{scaled} {unit}`)")
}

fn render_binary_size_markdown(value: u64) -> String {
    let Some((scaled, unit)) = binary_size(value) else {
        return format!("`{}` bytes", format_u64(value));
    };

    format!("`{scaled} {unit}`")
}

fn render_delta_markdown(direction: DeltaDirection, value: Option<u64>) -> String {
    let sign = match direction {
        DeltaDirection::Positive => '+',
        DeltaDirection::Negative => '-',
    };

    format!("`{sign}{}`", format_u64(value.unwrap_or(0)))
}

fn change_summary_cell_values(cell: &Cell) -> Option<(Option<u64>, Option<u64>, Option<u64>)> {
    match cell.values.as_slice() {
        [
            DocumentValue::ChangeSummary {
                positive,
                negative,
                total,
            },
        ] => Some((*positive, *negative, *total)),
        _ => None,
    }
}

fn change_summary_widths(
    positive: Option<u64>,
    negative: Option<u64>,
    total: Option<u64>,
) -> ChangeSummaryWidths {
    ChangeSummaryWidths {
        positive: change_summary_signed_value('+', positive).len(),
        negative: change_summary_signed_value('-', negative).len(),
        total: change_summary_total_value(total).len(),
    }
}

fn render_change_summary_markdown(
    positive: Option<u64>,
    negative: Option<u64>,
    total: Option<u64>,
    widths: ChangeSummaryWidths,
) -> String {
    let positive = change_summary_signed_value('+', positive);
    let negative = change_summary_signed_value('-', negative);
    let total_value = change_summary_total_value(total);
    let total = total.map_or_else(|| "?".to_string(), |_| format!("`{total_value}`"));
    let positive_padding = " ".repeat(widths.positive.saturating_sub(positive.len()));
    let negative_padding = " ".repeat(widths.negative.saturating_sub(negative.len()));
    let total_padding = " ".repeat(widths.total.saturating_sub(total_value.len()));

    format!("`{positive}`{positive_padding} `{negative}`{negative_padding} {total_padding}{total}")
}

fn change_summary_signed_value(sign: char, value: Option<u64>) -> String {
    format!("{sign}{}", format_u64(value.unwrap_or(0)))
}

fn change_summary_total_value(value: Option<u64>) -> String {
    value.map_or_else(|| "?".to_string(), format_u64)
}

fn render_status_label(status: Status) -> &'static str {
    match status {
        Status::Confidence(status) => render_confidence_status_label(status),
        Status::Precision(status) => render_precision_status_label(status),
        Status::Applicability(status) => render_applicability_status_label(status),
        Status::Completeness(status) => render_completeness_status_label(status),
        Status::Compatibility(status) => render_compatibility_status_label(status),
        Status::Support(status) => render_support_status_label(status),
    }
}

fn render_confidence_status_label(status: ConfidenceStatus) -> &'static str {
    match status {
        ConfidenceStatus::High => "high",
        ConfidenceStatus::Partial => "partial",
        ConfidenceStatus::Lowered => "lowered",
        ConfidenceStatus::Unknown => "unknown",
        ConfidenceStatus::Unavailable => "unavailable",
    }
}

fn render_precision_status_label(status: PrecisionStatus) -> &'static str {
    match status {
        PrecisionStatus::Exact => "exact",
        PrecisionStatus::ProbablyExact => "probably exact",
        PrecisionStatus::PossiblyTruncated => "possibly truncated",
        PrecisionStatus::Unknown => "unknown",
        PrecisionStatus::Unavailable => "unavailable",
    }
}

fn render_applicability_status_label(status: ApplicabilityStatus) -> &'static str {
    match status {
        ApplicabilityStatus::Applies => "applies",
        ApplicabilityStatus::PartiallyApplies => "partially applies",
        ApplicabilityStatus::DoesNotApply => "does not apply",
        ApplicabilityStatus::Unknown => "unknown",
    }
}

fn render_completeness_status_label(status: CompletenessStatus) -> &'static str {
    match status {
        CompletenessStatus::Complete => "complete",
        CompletenessStatus::Incomplete => "incomplete",
        CompletenessStatus::NotApplicable => "not applicable",
    }
}

fn render_compatibility_status_label(status: CompatibilityStatus) -> &'static str {
    match status {
        CompatibilityStatus::Compatible => "compatible",
        CompatibilityStatus::SafelyPromoted => "safely promoted",
        CompatibilityStatus::Incompatible => "incompatible",
        CompatibilityStatus::Unknown => "unknown",
    }
}

fn render_support_status_label(status: SupportStatus) -> &'static str {
    match status {
        SupportStatus::Supported => "supported",
        SupportStatus::Unsupported => "unsupported",
    }
}

fn render_presence_markdown(presence: Presence) -> &'static str {
    match presence {
        Presence::Present => "✓",
        Presence::Absent => "",
        Presence::NotApplicable => "n/a",
    }
}

fn binary_size(value: u64) -> Option<(String, &'static str)> {
    if value < 1024 {
        return None;
    }

    let mut divisor = 1024_u128;
    let mut unit_index = 0;
    let units = ["KiB", "MiB", "GiB", "TiB", "PiB"];

    while u128::from(value) >= divisor * 1024 && unit_index < units.len() - 1 {
        divisor *= 1024;
        unit_index += 1;
    }

    let scaled_millis = (u128::from(value) * 1000 + divisor / 2) / divisor;
    let scaled = format!("{}.{:03}", scaled_millis / 1000, scaled_millis % 1000);

    Some((scaled, units[unit_index]))
}

fn render_percentage_millis_markdown(value: u64) -> String {
    format!("`{}.{:03}%`", value / 1000, value % 1000)
}

fn format_usize(value: usize) -> String {
    format_u64(value as u64)
}

fn format_u64(value: u64) -> String {
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
    let time = trim_fractional_seconds(time.trim_end_matches('Z'));

    format!("{date} {time} UTC")
}

fn render_timestamp_local(timestamp: OffsetDateTime) -> String {
    let timestamp =
        UtcOffset::current_local_offset().map_or(timestamp, |offset| timestamp.to_offset(offset));
    let rfc3339 = timestamp
        .format(&Rfc3339)
        .expect("timestamp should format as RFC 3339");
    let (date, time) = rfc3339
        .split_once('T')
        .expect("RFC 3339 timestamp should contain date/time separator");
    let time = trim_fractional_seconds(time);

    format!("{date} {}", separate_utc_offset(&time))
}

fn trim_fractional_seconds(time: &str) -> String {
    let Some(dot_index) = time.find('.') else {
        return time.to_string();
    };
    let suffix_index = time[dot_index..]
        .find(['Z', '+', '-'])
        .map_or(time.len(), |index| dot_index + index);

    format!("{}{}", &time[..dot_index], &time[suffix_index..])
}

fn separate_utc_offset(time: &str) -> String {
    if let Some(time) = time.strip_suffix('Z') {
        return format!("{time} UTC");
    }

    let offset_index = time
        .char_indices()
        .skip("00:00:00".len())
        .find_map(|(index, character)| matches!(character, '+' | '-').then_some(index));

    offset_index.map_or_else(
        || time.to_string(),
        |index| format!("{} {}", &time[..index], &time[index..]),
    )
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::Arc;

    use berg_core::document::{
        Block, Document, List, ListItem, ListKind, Property, Row, Section, Table,
    };
    use berg_core::engine::CurrentSchemaInfo;
    use berg_core::spec::Schema;

    use super::{
        ApplicabilityStatus, Cell, ConfidenceStatus, DeltaDirection, DocumentFormat, DocumentValue,
        EndpointSchema, Presence, Status, SupportStatus, UnknownValueKind, command_tree,
        incomplete_command_help, parse_endpoint_labels, render_document, render_document_markdown,
        render_endpoint_template, unified_diff, write_schema_compare_header,
        write_schema_compare_result, write_schema_compare_summary,
    };

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "document shape assertions are intentionally explicit"
    )]
    fn renders_document_as_markdown() {
        let document = Document {
            title: Cell::new(vec![
                DocumentValue::Text("Schema: ".to_string()),
                DocumentValue::Code("warehouse.analytics.events".to_string()),
            ]),
            blocks: vec![
                Block::Properties(vec![
                    Property {
                        label: "Source endpoint".to_string(),
                        value: Cell::value(DocumentValue::Uri(
                            "https://example.test/catalog".to_string(),
                        )),
                    },
                    Property {
                        label: "Schema ID".to_string(),
                        value: Cell::value(DocumentValue::Number(3)),
                    },
                    Property {
                        label: "Data files".to_string(),
                        value: Cell::value(DocumentValue::Unsigned(7)),
                    },
                    Property {
                        label: "Total size".to_string(),
                        value: Cell::value(DocumentValue::Bytes(2048)),
                    },
                    Property {
                        label: "Identifier fields".to_string(),
                        value: Cell::new(vec![
                            DocumentValue::Code("org_id".to_string()),
                            DocumentValue::Text(", ".to_string()),
                            DocumentValue::Code("_key".to_string()),
                        ]),
                    },
                ]),
                Block::Section(Section {
                    title: Cell::text("Fields"),
                    blocks: vec![
                        Block::Table(Table {
                            columns: vec![
                                Cell::text("Path"),
                                Cell::text("Type"),
                                Cell::text("Required"),
                                Cell::text("Field ID"),
                                Cell::text("Size"),
                            ],
                            rows: vec![Row {
                                cells: vec![
                                    Cell::code("metadata.labels"),
                                    Cell::code("map<string, string>"),
                                    Cell::value(DocumentValue::Bool(false)),
                                    Cell::value(DocumentValue::Number(36)),
                                    Cell::value(DocumentValue::Bytes(2048)),
                                ],
                            }],
                        }),
                        Block::Section(Section {
                            title: Cell::text("Nested"),
                            blocks: vec![Block::Paragraph(Cell::text("details"))],
                        }),
                    ],
                }),
                Block::List(List {
                    kind: ListKind::Ordered { start: 1 },
                    items: vec![
                        ListItem {
                            blocks: vec![Block::Paragraph(Cell::text("Load table metadata"))],
                        },
                        ListItem {
                            blocks: vec![
                                Block::Paragraph(Cell::text("Derive schema view")),
                                Block::List(List {
                                    kind: ListKind::Unordered,
                                    items: vec![ListItem {
                                        blocks: vec![Block::Paragraph(Cell::text(
                                            "Flatten nested fields",
                                        ))],
                                    }],
                                }),
                            ],
                        },
                    ],
                }),
            ],
        };

        let markdown = render_document_markdown(&document);

        assert!(markdown.contains("# Schema: `warehouse.analytics.events`"));
        assert!(markdown.contains("- Source endpoint: `https://example.test/catalog`"));
        assert!(markdown.contains("- Schema ID: `3`"));
        assert!(markdown.contains("- Data files: `7`"));
        assert!(markdown.contains("- Total size: `2,048` bytes (`2.000 KiB`)"));
        assert!(markdown.contains("- Identifier fields: `org_id`, `_key`"));
        assert!(markdown.contains("## Fields"));
        assert!(markdown.contains("### Nested"));
        assert!(markdown.contains("| Path | Type | Required | Field ID | Bytes | Binary size |"));
        assert!(markdown.contains("| --- | --- | --- | ---: | ---: | ---: |"));
        assert!(markdown.contains(
            "| `metadata.labels` | `map<string, string>` | no | `36` | `2,048` | `2.000 KiB` |"
        ));
        assert!(markdown.contains("1. Load table metadata"));
        assert!(markdown.contains("2. Derive schema view"));
        assert!(markdown.contains("   - Flatten nested fields"));
    }

    #[test]
    fn renders_bytes_table_cells_as_exact_and_binary_columns() {
        const ONE_PIB: u64 = 1024_u64.pow(5);

        let document = Document {
            title: Cell::text("Sizes"),
            blocks: vec![Block::Table(Table {
                columns: vec![Cell::text("File"), Cell::text("Size")],
                rows: vec![
                    Row {
                        cells: vec![
                            Cell::text("a.parquet"),
                            Cell::value(DocumentValue::Bytes(2048)),
                        ],
                    },
                    Row {
                        cells: vec![
                            Cell::text("huge.parquet"),
                            Cell::value(DocumentValue::Bytes(ONE_PIB)),
                        ],
                    },
                ],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(markdown.contains("| File | Bytes | Binary size |"));
        assert!(markdown.contains("| --- | ---: | ---: |"));
        assert!(markdown.contains("| a.parquet | `2,048` | `2.000 KiB` |"));
        assert!(markdown.contains("| huge.parquet | `1,125,899,906,842,624` | `1.000 PiB` |"));
    }

    #[test]
    fn renders_binary_size_table_cells_without_bytes_column() {
        let document = Document {
            title: Cell::text("Partition distribution"),
            blocks: vec![Block::Table(Table {
                columns: vec![Cell::text("Percentile"), Cell::text("Binary size")],
                rows: vec![Row {
                    cells: vec![Cell::text("p50"), Cell::value(DocumentValue::Bytes(2048))],
                }],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(markdown.contains("| Percentile | Binary size |"));
        assert!(markdown.contains("| --- | ---: |"));
        assert!(markdown.contains("| p50 | `2.000 KiB` |"));
        assert!(!markdown.contains("| Percentile | Bytes | Binary size |"));
    }

    #[test]
    fn renders_missing_values_as_question_marks() {
        let document = Document {
            title: Cell::text("Snapshots"),
            blocks: vec![Block::Table(Table {
                columns: vec![Cell::text("Value")],
                rows: vec![Row {
                    cells: vec![Cell::value(DocumentValue::MissingValue)],
                }],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(markdown.contains("| ? |"));
    }

    #[test]
    fn renders_unknown_values_as_unknown_with_typed_alignment() {
        let document = Document {
            title: Cell::text("Unknowns"),
            blocks: vec![Block::Table(Table {
                columns: vec![Cell::text("Generic"), Cell::text("Numeric")],
                rows: vec![Row {
                    cells: vec![
                        Cell::value(DocumentValue::UnknownValue {
                            kind: UnknownValueKind::Generic,
                        }),
                        Cell::value(DocumentValue::UnknownValue {
                            kind: UnknownValueKind::Numeric,
                        }),
                    ],
                }],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(markdown.contains("| Generic | Numeric |"));
        assert!(markdown.contains("| --- | ---: |"));
        assert!(markdown.contains("| unknown | unknown |"));
    }

    #[test]
    fn renders_status_values_as_code_labels() {
        let document = Document {
            title: Cell::text("Statuses"),
            blocks: vec![Block::Table(Table {
                columns: vec![
                    Cell::text("Confidence"),
                    Cell::text("Applicability"),
                    Cell::text("Support"),
                ],
                rows: vec![Row {
                    cells: vec![
                        Cell::value(DocumentValue::Status(Status::Confidence(
                            ConfidenceStatus::Lowered,
                        ))),
                        Cell::value(DocumentValue::Status(Status::Applicability(
                            ApplicabilityStatus::DoesNotApply,
                        ))),
                        Cell::value(DocumentValue::Status(Status::Support(
                            SupportStatus::Unsupported,
                        ))),
                    ],
                }],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(markdown.contains("| `lowered` | `does not apply` | `unsupported` |"));
    }

    #[test]
    fn renders_delta_values_with_signs() {
        let document = Document {
            title: Cell::text("Delta values"),
            blocks: vec![Block::Table(Table {
                columns: vec![
                    Cell::text("Added"),
                    Cell::text("Removed"),
                    Cell::text("Missing"),
                ],
                rows: vec![Row {
                    cells: vec![
                        Cell::value(DocumentValue::Delta {
                            direction: DeltaDirection::Positive,
                            value: Some(0),
                        }),
                        Cell::value(DocumentValue::Delta {
                            direction: DeltaDirection::Negative,
                            value: Some(12),
                        }),
                        Cell::value(DocumentValue::Delta {
                            direction: DeltaDirection::Positive,
                            value: None,
                        }),
                    ],
                }],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(markdown.contains("| `+0` | `-12` | `+0` |"));
    }

    #[test]
    fn renders_change_summary_values_with_column_alignment() {
        let document = Document {
            title: Cell::text("Change summaries"),
            blocks: vec![Block::Table(Table {
                columns: vec![Cell::text("Records")],
                rows: vec![
                    Row {
                        cells: vec![Cell::value(DocumentValue::ChangeSummary {
                            positive: Some(100),
                            negative: Some(0),
                            total: Some(900),
                        })],
                    },
                    Row {
                        cells: vec![Cell::value(DocumentValue::ChangeSummary {
                            positive: None,
                            negative: None,
                            total: Some(800),
                        })],
                    },
                    Row {
                        cells: vec![Cell::value(DocumentValue::ChangeSummary {
                            positive: Some(10),
                            negative: Some(1),
                            total: None,
                        })],
                    },
                ],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(markdown.contains("| `+100` `-0` `900` |"));
        assert!(markdown.contains("| `+0`   `-0` `800` |"));
        assert!(markdown.contains("| `+10`  `-1`   ? |"));
    }

    #[test]
    fn centers_manifest_metadata_columns_in_markdown() {
        let document = Document {
            title: Cell::text("Manifest File"),
            blocks: vec![Block::Table(Table {
                columns: vec![
                    Cell::text("Column"),
                    Cell::text("Field ID"),
                    Cell::text("column_sizes"),
                    Cell::text("value_counts"),
                    Cell::text("null_value_counts"),
                    Cell::text("nan_value_counts"),
                    Cell::text("lower_bounds"),
                    Cell::text("upper_bounds"),
                ],
                rows: vec![Row {
                    cells: vec![
                        Cell::code("org_id"),
                        Cell::value(DocumentValue::Number(1)),
                        Cell::value(DocumentValue::Presence(Presence::Present)),
                        Cell::value(DocumentValue::Presence(Presence::Present)),
                        Cell::value(DocumentValue::Presence(Presence::Absent)),
                        Cell::value(DocumentValue::Presence(Presence::NotApplicable)),
                        Cell::value(DocumentValue::Presence(Presence::Present)),
                        Cell::value(DocumentValue::Presence(Presence::Absent)),
                    ],
                }],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(
            markdown.contains("| --- | ---: | :---: | :---: | :---: | :---: | :---: | :---: |")
        );
        assert!(markdown.contains("| `org_id` | `1` | ✓ | ✓ |  | n/a | ✓ |  |"));
    }

    #[test]
    fn renders_code_cells_in_markdown_tables() {
        let document = Document {
            title: Cell::text("Properties"),
            blocks: vec![Block::Table(Table {
                columns: vec![Cell::text("Key"), Cell::text("Value")],
                rows: vec![Row {
                    cells: vec![
                        Cell::code("write.target-file-size-bytes"),
                        Cell::code("536870912"),
                    ],
                }],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(markdown.contains("| Key | Value |"));
        assert!(markdown.contains("| `write.target-file-size-bytes` | `536870912` |"));
    }

    #[test]
    fn renders_document_as_debug_ast() {
        let document = Document {
            title: Cell::text("Schema"),
            blocks: vec![Block::Paragraph(Cell::text("details"))],
        };

        let ast = render_document(&document, DocumentFormat::Ast).expect("AST should render");

        assert!(ast.contains("Document {"));
        assert!(ast.contains("Paragraph("));
    }

    #[test]
    fn rejects_unimplemented_json_format() {
        let document = Document {
            title: Cell::text("Schema"),
            blocks: Vec::new(),
        };

        let err = render_document(&document, DocumentFormat::Json).expect_err("JSON is deferred");

        assert!(err.to_string().contains("not implemented"));
    }

    #[test]
    fn renders_full_command_tree() {
        let tree = command_tree();

        assert!(tree.contains("berg - Command-line interface for Berg."));
        assert!(tree.contains("├── table - Inspect tables"));
        assert!(tree.contains("│   ├── data - Inspect table data"));
        assert!(tree.contains("│   │   ├── files - Inspect data files"));
        assert!(tree.contains("stats - Show data file size statistics for the current snapshot"));
        assert!(tree.contains("│   │   └── max - Compute metadata-derived max values"));
        assert!(tree.contains(
            "│   │       └── current - Show metadata-derived max for a current snapshot column"
        ));
        assert!(tree.contains("│   ├── manifest - Inspect table manifests"));
        assert!(tree.contains("│   │   └── files - Inspect manifest files"));
        assert!(
            tree.contains("│   │       ├── list - List manifest files for the current snapshot")
        );
        assert!(tree.contains(
            "│   │       └── inspect - Inspect one manifest file from the current snapshot"
        ));
        assert!(tree.contains("│   ├── partitions - Inspect table partitions"));
        assert!(tree.contains("│   ├── properties - Inspect table properties"));
        assert!(
            tree.contains("│   │   └── current - Show properties from the current table metadata")
        );
        assert!(tree.contains("│   ├── schema - Inspect table schemas"));
        assert!(
            tree.contains(
                "│   │   ├── compare - Compare the current schema across catalog endpoints"
            )
        );
        assert!(tree.contains("│   │   └── current - Show the current schema"));
        assert!(tree.contains("│   ├── snapshots - Inspect table snapshots"));
        assert!(
            tree.contains(
                "│   │   └── list - List snapshots retained in the current table metadata"
            )
        );
        assert!(tree.contains("│   └── stats - Inspect table statistics"));
        assert!(tree.contains("└── commands - Print the full command tree"));
    }

    #[test]
    fn parses_schema_compare_endpoint_labels() {
        let endpoint_labels =
            parse_endpoint_labels("east, west,central").expect("valid endpoint labels");

        assert_eq!(endpoint_labels, vec!["east", "west", "central"]);
    }

    #[test]
    fn rejects_schema_compare_with_less_than_two_endpoint_labels() {
        let err = parse_endpoint_labels("east").expect_err("requires at least two endpoint labels");

        assert!(err.to_string().contains("at least two"));
    }

    #[test]
    fn renders_endpoint_template() {
        let rendered =
            render_endpoint_template("https://catalog-{label}.example.com/{endpoint}", "east");

        assert_eq!(rendered, "https://catalog-east.example.com/east");
    }

    #[test]
    fn renders_schema_compare_summary_as_markdown() {
        let table =
            super::QualifiedTableIdent::parse("warehouse.analytics.events").expect("valid table");
        let endpoint_labels = vec!["east".to_string(), "west".to_string()];
        let schema = Arc::new(Schema::builder().with_schema_id(7).build().expect("schema"));
        let results = [EndpointSchema {
            label: "east".to_string(),
            endpoint:
                "https://catalog-east.example.com/v1/warehouse/namespaces/analytics/tables/events"
                    .to_string(),
            info: CurrentSchemaInfo {
                metadata_json_path: "s3://bucket/metadata.json".to_string(),
                table_location: "s3://bucket/table".to_string(),
                current_schema_id: 7,
                schema,
            },
        }];
        let mut markdown = String::new();

        write_schema_compare_header(&mut markdown, &table, &endpoint_labels);
        write_schema_compare_summary(&mut markdown, &results);
        write_schema_compare_result(&mut markdown, true, "east", 2);

        assert!(markdown.contains("# Schema Compare: `warehouse.analytics.events`"));
        assert!(markdown.contains("- Compared endpoints: `east`, `west`"));
        assert!(markdown.contains("## Endpoint Summary"));
        assert!(markdown.contains("| Endpoint | Schema ID | Fields | Metadata location |"));
        assert!(markdown.contains("| `east` | `7` | `0` | `s3://bucket/metadata.json` |"));
        assert!(markdown.contains("## Result"));
        assert!(markdown.contains("Schemas match across `2` endpoints."));
    }

    #[test]
    fn renders_unified_schema_diff() {
        let left = ["{".to_string(), "  \"a\": 1".to_string(), "}".to_string()];
        let right = ["{".to_string(), "  \"a\": 2".to_string(), "}".to_string()];
        let diff = unified_diff("dc1", "dc2", &left, &right);

        assert!(diff.contains("--- dc1"));
        assert!(diff.contains("+++ dc2"));
        assert!(diff.contains("-  \"a\": 1"));
        assert!(diff.contains("+  \"a\": 2"));
    }

    #[test]
    fn renders_parent_help_for_incomplete_manifest_command() {
        let help = incomplete_command_help(&args([
            "table",
            "manifest",
            "warehouse.analytics.events",
            "--catalog-uri=https://example.test",
        ]))
        .expect("manifest help");

        assert!(help.contains("Usage: berg table manifest [OPTIONS] <COMMAND>"));
        assert!(help.contains("Commands:"));
        assert!(help.contains("files  Inspect manifest files"));
    }

    #[test]
    fn renders_parent_help_for_incomplete_manifest_files_command() {
        let help = incomplete_command_help(&args([
            "table",
            "manifest",
            "files",
            "warehouse.analytics.events",
            "--aws-vault-profile",
            "example-profile",
        ]))
        .expect("manifest files help");

        assert!(help.contains("Usage: berg table manifest files [OPTIONS] <COMMAND>"));
        assert!(help.contains("list     List manifest files for the current snapshot"));
        assert!(help.contains("inspect  Inspect one manifest file from the current snapshot"));
    }

    #[test]
    fn renders_parent_help_for_incomplete_data_files_command() {
        let help = incomplete_command_help(&args([
            "table",
            "data",
            "files",
            "warehouse.analytics.events",
            "--catalog-uri=https://example.test",
        ]))
        .expect("data files help");

        assert!(help.contains("Usage: berg table data files [OPTIONS] <COMMAND>"));
        assert!(help.contains("stats  Show data file size statistics for the current snapshot"));
    }

    #[test]
    fn renders_parent_help_for_incomplete_data_max_command() {
        let help = incomplete_command_help(&args([
            "table",
            "data",
            "max",
            "warehouse.analytics.events",
            "event_id",
            "--catalog-uri=https://example.test",
        ]))
        .expect("data max help");

        assert!(help.contains("Usage: berg table data max [OPTIONS] <COMMAND>"));
        assert!(help.contains("current  Show metadata-derived max for a current snapshot column"));
    }

    #[test]
    fn renders_parent_help_for_incomplete_table_command() {
        let help = incomplete_command_help(&args([
            "table",
            "warehouse.analytics.events",
            "--catalog-uri=https://example.test",
        ]))
        .expect("table help");

        assert!(help.contains("Usage: berg table [OPTIONS] <COMMAND>"));
        assert!(help.contains("data        Inspect table data"));
        assert!(help.contains("manifest    Inspect table manifests"));
    }

    fn args<const N: usize>(values: [&str; N]) -> Vec<OsString> {
        values.into_iter().map(OsString::from).collect()
    }
}
