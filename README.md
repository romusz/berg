# Berg

Berg is a Rust workspace that builds two executables:

- `berg` (from the `berg-cli` crate): command-line interface
- `berg-tui`: terminal user interface

Shared application code lives in `berg-core`.

## Development

This project tracks the latest stable Rust release. The declared MSRV is `1.92` (driven by `iceberg-rust`); CI verifies both MSRV and stable on every change.

These commands mirror CI exactly:

```sh
cargo build --workspace --all-targets --locked
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

`--locked` rejects builds when `Cargo.lock` is out of date. After editing `Cargo.toml`, run the same command without `--locked` once to refresh the lockfile, then commit both files together.

Run the CLI:

```sh
cargo run -p berg-cli
```

Run the TUI:

```sh
cargo run -p berg-tui
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
