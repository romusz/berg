# Berg

[![CI](https://github.com/romusz/berg/actions/workflows/ci.yml/badge.svg)](https://github.com/romusz/berg/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Berg inspects Apache Iceberg tables from your terminal.

It connects to an Apache Iceberg REST catalog, reads table metadata and manifest
data, and renders reports as GitHub-flavored Markdown. The CLI is the primary
interface today. `berg-tui` exists as an early terminal UI scaffold and is not yet
feature-complete.

## Features

- Inspect the current schema, table properties, snapshot statistics, and partition summary.
- List manifest files for the current snapshot and inspect one manifest file in detail.
- Summarize data file sizes for the current snapshot.
- Compute metadata-derived maximum values for current-schema columns, including confidence notes when delete files or missing metrics affect the answer.
- Read Iceberg metadata and data files through supported Apache Iceberg Rust storage backends, including S3 via OpenDAL.
- Render reports as Markdown by default, with a debug AST format for development.

## Status

Berg is early-stage software. The CLI is usable for inspecting REST-catalog-backed
tables, but command names and report formats may still change before a stable
release. JSON output is reserved in the CLI but is not implemented yet.

## Installation

Berg requires Rust `1.92` or newer.

Install the CLI from GitHub:

```sh
cargo install --git https://github.com/romusz/berg berg-cli
```

Install the TUI scaffold:

```sh
cargo install --git https://github.com/romusz/berg berg-tui
```

Build release binaries from a checkout:

```sh
cargo build --workspace --release --locked
```

If you use [`mise`](https://mise.jdx.dev/), this repository also includes an
install task that builds both binaries and copies them to `~/bin`:

```sh
mise run install
```

## Quick Start

Print the command tree:

```sh
berg commands
```

Inspect a table schema through an Iceberg REST catalog:

```sh
berg --catalog-uri http://localhost:8181 table schema current warehouse.db.table
```

Inspect current snapshot statistics:

```sh
berg --catalog-uri http://localhost:8181 table stats current warehouse.db.table
```

Summarize data file sizes:

```sh
berg --catalog-uri http://localhost:8181 table data files stats warehouse.db.table
```

Compute a metadata-derived maximum value:

```sh
berg --catalog-uri http://localhost:8181 table data max current warehouse.db.table column_name
```

Use `--help` on any command for its arguments:

```sh
berg table manifest files inspect --help
```

## Table Identifiers

Berg expects table IDs in this form:

```text
catalog.namespace.table
```

The first segment is treated as the REST catalog prefix unless you provide
`--catalog-prefix` or `BERG_CATALOG_PREFIX`. The remaining segments are the
Iceberg namespace and table name.

## Configuration

Every catalog and storage setting can be passed as a flag or through an
environment variable.

| Purpose | Flag | Environment variable |
| --- | --- | --- |
| REST catalog URI | `--catalog-uri` | `BERG_CATALOG_URI` |
| REST catalog prefix | `--catalog-prefix` | `BERG_CATALOG_PREFIX` |
| REST catalog warehouse | `--catalog-warehouse` | `BERG_CATALOG_WAREHOUSE` |
| REST catalog bearer token | `--catalog-token` | `BERG_CATALOG_TOKEN` |
| REST catalog OAuth credential | `--catalog-credential` | `BERG_CATALOG_CREDENTIAL` |
| Additional catalog property | `--catalog-property KEY=VALUE` | `BERG_CATALOG_PROPERTIES` |
| Additional REST header | `--catalog-header NAME=VALUE` | `BERG_CATALOG_HEADERS` |
| AWS profile for S3 reads | `--s3-profile` | `BERG_S3_PROFILE` |
| aws-vault profile for S3 reads | `--aws-vault-profile` | `BERG_AWS_VAULT_PROFILE` |

`--catalog-property` and `--catalog-header` can be repeated. The environment
variables `BERG_CATALOG_PROPERTIES` and `BERG_CATALOG_HEADERS` accept
comma-separated `KEY=VALUE` entries.

Example with environment variables:

```sh
export BERG_CATALOG_URI=http://localhost:8181
export BERG_CATALOG_WAREHOUSE=s3://warehouse
export BERG_S3_PROFILE=dev

berg table properties current warehouse.db.table
```

Example with a bearer token:

```sh
berg \
  --catalog-uri https://catalog.example.com \
  --catalog-token "$ICEBERG_TOKEN" \
  table partitions current warehouse.db.table
```

## Output Formats

Document-producing commands support `--format`:

| Format | Status | Notes |
| --- | --- | --- |
| `markdown` | Default | GitHub-flavored Markdown reports. |
| `ast` | Supported | Debug rendering of Berg's semantic document model. |
| `json` | Reserved | Not implemented yet. |

## Command Overview

```text
berg
├── table data files stats
├── table data max current
├── table manifest files list
├── table manifest files inspect
├── table partitions current
├── table properties current
├── table schema current
├── table stats current
└── commands
```

Run `berg commands` to print the full command tree generated by the current CLI.

## Development

The workspace contains three crates:

- `berg-cli`, which builds the `berg` command-line interface.
- `berg-tui`, which builds the experimental `berg-tui` terminal interface.
- `berg-core`, which contains shared Iceberg inspection and report-building code.

Run the local verification commands:

```sh
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

CI runs these checks on every pull request and push to `main`. Tests run on both
the declared MSRV (`1.92`) and stable Rust. Formatting and clippy run on stable
Rust.

`--locked` rejects builds when `Cargo.lock` is out of date. After editing
`Cargo.toml`, run the same Cargo command without `--locked` once to refresh the
lockfile, then commit `Cargo.toml` and `Cargo.lock` together.

Run the CLI from source:

```sh
cargo run -p berg-cli -- --help
```

Run the TUI from source in an interactive terminal:

```sh
cargo run -p berg-tui
```

## Contributing

Issues and pull requests are welcome. Before opening a pull request, run the
local verification commands from the [Development](#development) section.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Berg, as defined in the Apache-2.0 license, shall be dual
licensed as below, without any additional terms or conditions.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
