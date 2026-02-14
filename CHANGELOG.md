# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

### Added

- Add `Gpkg::open_with_writer()` for wasm targets. This uses the crate's Hybrid VFS implementation under the hood and is intended for browser workflows that provide a writer such as an OPFS-backed file handle.
  Example:
  ```rust
  use rusqlite_gpkg::Gpkg;
  // `opfs_file` is a writer wrapper around `FileSystemSyncAccessHandle`.
  let gpkg = Gpkg::open_with_writer("demo.sqlite", opfs_file)?;
  # Ok::<(), rusqlite_gpkg::GpkgError>(())
  ```
- Define finer-grained `GpkgError` enum variants so callers can handle failure modes more precisely.
