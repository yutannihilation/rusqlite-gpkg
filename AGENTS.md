# Repository Guidelines

## Project Structure & Module Organization
- `src/lib.rs` exposes the library entry point and shared functions (e.g., `read_gpkg`).
- `src/gpkg.rs` implements core GeoPackage parsing/reading logic.
- `src/ogc_sql.rs` holds GeoPackage SQL schema constants.
- `src/types.rs` defines shared GeoPackage types.
- `src/bin/read_gpkg.rs` is a small CLI binary for reading a sample GeoPackage.
- `src/test/test.gpkg` is sample data used by the binary.

## Build, Test, and Development Commands
- `cargo build`: compile the library and binaries.
- `cargo check`: type-check quickly without producing artifacts.
- `cargo test`: run unit tests (currently minimal; add as features grow).
- `cargo run --bin read_gpkg`: run the sample reader against `src/test/test.gpkg`.

## Coding Style & Naming Conventions
- Rust 2024 edition is used (see `Cargo.toml`).
- Follow standard Rust style: 4-space indentation, `snake_case` for functions/modules, `UpperCamelCase` for types.
- Prefer small, focused modules and keep SQL constants grouped in `src/ogc_sql.rs`.
- Use `rustfmt` defaults for formatting (`cargo fmt`) when modifying Rust files.

## Testing Guidelines
- Use `cargo test` for unit tests; place tests in `src/lib.rs` under `#[cfg(test)]` or in `tests/` if integration tests are added later.
- Name tests descriptively (e.g., `reads_points_table`).
- If you add new behavior, include a regression test or update the sample data workflow.

## Commit & Pull Request Guidelines
- Git history is minimal and does not establish a strict convention. Use concise, imperative messages (e.g., "Add gpkg schema helpers").
- PRs should describe the change, include the rationale, and list any new commands or test steps.
- If you update sample data, call it out explicitly and note any required regeneration steps.

## Configuration & Data Notes
- The crate uses `rusqlite` with the `bundled` feature; keep this in mind for build times and binary size.
- Avoid editing `src/test/test.gpkg` unless the change is intentional and documented in the PR.
