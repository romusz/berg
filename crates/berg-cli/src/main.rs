use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fmt::Write;

use anyhow::{Context, anyhow};
use berg_core::engine::{
    QualifiedTableIdent, RestCatalogConfig, load_current_data_file_size_stats,
    load_current_manifest_file_detail, load_current_manifest_file_list, load_current_schema,
    load_current_table_max, load_current_table_partitions, load_current_table_properties,
    load_current_table_stats, parse_catalog_property,
};
use berg_core::view::{
    Block, Cell, Document, DocumentValue, List, ListKind, data_file_size_stats_document,
    manifest_file_detail_document, manifest_file_list_document, schema_document,
    table_data_max_document, table_partitions_document, table_properties_document,
    table_stats_document,
};
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
    /// Show the current schema.
    Current(CurrentSchemaArgs),
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
                TableSchemaCommands::Current(args) => {
                    print_current_schema(args, catalog_args).await?;
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

#[derive(Debug, Clone, Copy)]
enum MarkdownTableColumn {
    Source(usize),
    Bytes(usize),
    BinarySize(usize),
}

fn render_table_markdown(table: &berg_core::view::Table, markdown: &mut String) {
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
            .map(|column| escape_markdown_table_cell(&render_table_cell_markdown(row, *column)))
            .collect::<Vec<_>>();
        writeln!(markdown, "| {} |", cells.join(" | ")).expect("write to string");
    }
}

fn markdown_table_columns(table: &berg_core::view::Table) -> Vec<MarkdownTableColumn> {
    let mut columns = Vec::new();

    for index in 0..table.columns.len() {
        if is_bytes_table_column(table, index) {
            columns.push(MarkdownTableColumn::Bytes(index));
            columns.push(MarkdownTableColumn::BinarySize(index));
        } else {
            columns.push(MarkdownTableColumn::Source(index));
        }
    }

    columns
}

fn render_table_header_markdown(
    table: &berg_core::view::Table,
    column: MarkdownTableColumn,
) -> String {
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

fn render_bytes_table_header_markdown(
    table: &berg_core::view::Table,
    column_index: usize,
) -> String {
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

fn render_binary_size_table_header_markdown(
    table: &berg_core::view::Table,
    column_index: usize,
) -> String {
    let label = table
        .columns
        .get(column_index)
        .map_or_else(String::new, render_cell_markdown);

    if label == "Size" {
        "Binary size".to_string()
    } else {
        format!("{label} (binary)")
    }
}

fn render_table_cell_markdown(row: &berg_core::view::Row, column: MarkdownTableColumn) -> String {
    match column {
        MarkdownTableColumn::Source(index) => row
            .cells
            .get(index)
            .map_or_else(String::new, render_cell_markdown),
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

fn markdown_table_column_separator(
    table: &berg_core::view::Table,
    column: MarkdownTableColumn,
) -> &'static str {
    if is_center_aligned_markdown_table_column(table, column) {
        ":---:"
    } else if is_right_aligned_markdown_table_column(table, column) {
        "---:"
    } else {
        "---"
    }
}

fn is_center_aligned_markdown_table_column(
    table: &berg_core::view::Table,
    column: MarkdownTableColumn,
) -> bool {
    match column {
        MarkdownTableColumn::Source(index) => is_center_aligned_table_column(table, index),
        MarkdownTableColumn::Bytes(_) | MarkdownTableColumn::BinarySize(_) => false,
    }
}

fn is_right_aligned_markdown_table_column(
    table: &berg_core::view::Table,
    column: MarkdownTableColumn,
) -> bool {
    match column {
        MarkdownTableColumn::Source(index) => is_right_aligned_table_column(table, index),
        MarkdownTableColumn::Bytes(_) | MarkdownTableColumn::BinarySize(_) => true,
    }
}

fn is_center_aligned_table_column(table: &berg_core::view::Table, column_index: usize) -> bool {
    column_index >= 2 && is_manifest_column_metadata_table(table)
}

fn is_manifest_column_metadata_table(table: &berg_core::view::Table) -> bool {
    matches!(
        (table.columns.first(), table.columns.get(1)),
        (Some(column), Some(field_id))
            if text_cell_value(column) == Some("Column")
                && text_cell_value(field_id) == Some("Field ID")
    ) && table.columns.len() > 2
        && !table.rows.is_empty()
        && table.rows.iter().all(|row| {
            row.cells
                .iter()
                .skip(2)
                .all(|cell| matches!(text_cell_value(cell), Some("" | "✓")))
        })
}

fn text_cell_value(cell: &Cell) -> Option<&str> {
    match cell.values.as_slice() {
        [DocumentValue::Text(value)] => Some(value),
        _ => None,
    }
}

fn is_right_aligned_table_column(table: &berg_core::view::Table, column_index: usize) -> bool {
    !table.rows.is_empty()
        && table.rows.iter().all(|row| {
            row.cells
                .get(column_index)
                .is_some_and(is_numeric_table_cell)
        })
}

fn is_numeric_table_cell(cell: &Cell) -> bool {
    match cell.values.as_slice() {
        [
            DocumentValue::Number(_)
            | DocumentValue::Unsigned(_)
            | DocumentValue::Bytes(_)
            | DocumentValue::PercentageMillis(_)
            | DocumentValue::Count(_),
        ] => true,
        [DocumentValue::Text(value)] if value == "n/a" => true,
        _ => false,
    }
}

fn is_bytes_table_column(table: &berg_core::view::Table, column_index: usize) -> bool {
    !table.rows.is_empty()
        && table.rows.iter().all(|row| {
            row.cells
                .get(column_index)
                .is_some_and(is_bytes_or_na_table_cell)
        })
}

fn is_bytes_or_na_table_cell(cell: &Cell) -> bool {
    match cell.values.as_slice() {
        [DocumentValue::Bytes(_)] => true,
        [DocumentValue::Text(value)] if value == "n/a" => true,
        _ => false,
    }
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

fn binary_size(value: u64) -> Option<(String, &'static str)> {
    if value < 1024 {
        return None;
    }

    let mut divisor = 1024_u128;
    let mut unit_index = 0;
    let units = ["KiB", "MiB", "GiB", "TiB"];

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

    use berg_core::view::{
        Block, Document, List, ListItem, ListKind, Property, Row, Section, Table,
    };

    use super::{
        Cell, DocumentFormat, DocumentValue, command_tree, incomplete_command_help,
        render_document, render_document_markdown,
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
                DocumentValue::Code("lakehouse.redapl_v3.k8s_pod_blue".to_string()),
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

        assert!(markdown.contains("# Schema: `lakehouse.redapl_v3.k8s_pod_blue`"));
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
                        Cell::text("✓"),
                        Cell::text("✓"),
                        Cell::text(""),
                        Cell::text(""),
                        Cell::text("✓"),
                        Cell::text(""),
                    ],
                }],
            })],
        };

        let markdown = render_document_markdown(&document);

        assert!(
            markdown.contains("| --- | ---: | :---: | :---: | :---: | :---: | :---: | :---: |")
        );
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
        assert!(tree.contains("│   │   └── current - Show the current schema"));
        assert!(tree.contains("│   └── stats - Inspect table statistics"));
        assert!(tree.contains("└── commands - Print the full command tree"));
    }

    #[test]
    fn renders_parent_help_for_incomplete_manifest_command() {
        let help = incomplete_command_help(&args([
            "table",
            "manifest",
            "lakehouse.redapl_v3.k8s_pod_blue",
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
            "lakehouse.redapl_v3.k8s_pod_blue",
            "--aws-vault-profile",
            "sso-prod-engineering",
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
            "lakehouse.redapl_v3.k8s_pod_blue",
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
            "lakehouse.redapl_v3.k8s_pod_blue",
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
            "lakehouse.redapl_v3.k8s_pod_blue",
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
