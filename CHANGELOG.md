# Changelog

<!-- next-header -->
## [Unreleased] (ReleaseDate)

### Added

- Add `ArrowGpkgWriter` for writing Arrow `RecordBatch`es into a GeoPackage layer (feature = `arrow`). Note that this feature is incomplete; when the writer auto-registers a new SRS entry, the `definition` column in `gpkg_spatial_ref_sys` is set to `"undefined"` because we lack a WKT1 source.

## [v0.0.6] (2026-04-03)

### Added

- Add `ColumnType::Blob` for raw binary data columns. Previously, `BLOB` columns were incorrectly mapped to `ColumnType::Geometry`; they are now correctly distinguished (#22).

## [v0.0.5] (2026-04-02)

### Added

- Support `DATE` and `DATETIME` column types as defined in the GeoPackage specification. Values are stored as ISO 8601 TEXT (`YYYY-MM-DD` for DATE, `YYYY-MM-DDTHH:MM:SS.SSSZ` for DATETIME) and represented as `Value::Text` at the value level.

## [v0.0.4] (2026-02-14)

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

<!-- next-url -->
[Unreleased]: https://github.com/yutannihilation/rusqlite-gpkg/compare/v0.0.6...HEAD
[v0.0.6]: https://github.com/yutannihilation/rusqlite-gpkg/compare/v0.0.5...v0.0.6
[v0.0.5]: https://github.com/yutannihilation/rusqlite-gpkg/compare/v0.0.4...v0.0.5
[v0.0.4]: https://github.com/yutannihilation/rusqlite-gpkg/compare/v0.0.3...v0.0.4
