# Changelog

<!-- next-header -->
## [Unreleased] (ReleaseDate)

### Added

- Support non-spatial attribute tables (`data_type = 'attributes'` in `gpkg_contents`, GeoPackage spec Section 2.4). New types: `GpkgAttributeTable`, `GpkgAttributeRow`. New `Gpkg` methods: `create_attribute_table()`, `get_attribute_table()`, `delete_attribute_table()`, `list_attribute_tables()` (#34).
- Add `ArrowGpkgAttributeReader` and `ArrowGpkgAttributeWriter` for Arrow integration with attribute tables (feature = `arrow`) (#34).

### Changed

- `list_layers()` now returns only feature layers (`data_type = 'features'`). Previously it returned all `gpkg_contents` rows. Use the new `list_attribute_tables()` for attribute tables (#34).
- `get_layer()` and `delete_layer()` now return a clear error when called on an attribute table or unsupported data type (e.g., tiles) (#34).

### Fixed

- Set `PRAGMA application_id` to `0x47504B47` ("GPKG") when creating a new GeoPackage, as required by the spec (#28).
- Set `PRAGMA user_version` to `10400` (spec version 1.4.0) when creating a new GeoPackage (#28).
- Register RTree spatial indexes in `gpkg_extensions` so other readers can discover them (#29).
- `create_layer()` now checks all `gpkg_contents` entries for name collisions, not just feature layers (#34).

## [v0.0.7] (2026-04-05)

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
[Unreleased]: https://github.com/yutannihilation/rusqlite-gpkg/compare/v0.0.7...HEAD
[v0.0.7]: https://github.com/yutannihilation/rusqlite-gpkg/compare/v0.0.6...v0.0.7
[v0.0.6]: https://github.com/yutannihilation/rusqlite-gpkg/compare/v0.0.5...v0.0.6
[v0.0.5]: https://github.com/yutannihilation/rusqlite-gpkg/compare/v0.0.4...v0.0.5
[v0.0.4]: https://github.com/yutannihilation/rusqlite-gpkg/compare/v0.0.3...v0.0.4
