//! GeoPackage reader/writer built on top of rusqlite.
//!
//! ## Overview
//!
//! The crate supports two GeoPackage content types:
//!
//! **Feature layers** (spatial data with geometry):
//! - [`Gpkg`] -- the GeoPackage connection.
//! - [`GpkgLayer`] -- a single feature layer (table with geometry column).
//! - [`GpkgFeature`] -- a single feature (row with geometry + properties).
//!
//! **Attribute tables** (non-spatial tabular data, no geometry):
//! - [`GpkgAttributeTable`] -- a single attribute table.
//! - [`GpkgAttributeRow`] -- a single row (properties only, no geometry).
//!
//! [`Value`] represents a single property value in both cases.
//!
//! Apache Arrow support is available behind the `arrow` feature flag.
//!
//! `Gpkg` is the entry point and supports several open modes:
//! `Gpkg::open_read_only(path)`, `Gpkg::open(path)`, and `Gpkg::open_in_memory()`.
//!
//! `GpkgLayer::features()` loads all features into memory. For large datasets,
//! use `features_batch(batch_size)` to stream in chunks.
//!
//! `GpkgLayer::insert` and `GpkgLayer::update` accept any geometry that implements
//! `geo_traits::GeometryTrait<T = f64>` (for example `geo_types::Point`).
//!
//! ## Browser usage
//!
//! On wasm targets, you can open with a custom writer (for example an OPFS-backed
//! writer wrapper) via `open_with_writer()`:
//!
//! ```no_run
//! # #[cfg(target_family = "wasm")]
//! use rusqlite_gpkg::Gpkg;
//! # #[cfg(target_family = "wasm")]
//! # fn open_from_opfs<W: std::io::Write + 'static>(opfs_writer: W) -> Result<(), rusqlite_gpkg::GpkgError> {
//! let _gpkg = Gpkg::open_with_writer("demo.sqlite", opfs_writer)?;
//! # Ok(())
//! # }
//! ```
//!
//! If you prefer a storage-agnostic flow, use `to_bytes()` / `from_bytes()`:
//!
//! ```no_run
//! use rusqlite_gpkg::Gpkg;
//! let gpkg = Gpkg::open_in_memory()?;
//! let bytes = gpkg.to_bytes()?;
//! let _restored = Gpkg::from_bytes(&bytes)?;
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! ## Gpkg
//!
//! `Gpkg` represents the GeoPackage connection and is the entry point for
//! opening databases, listing layers, and creating new layers.
//!
//! - `list_layers()` / `get_layer(name)` / `create_layer(...)` for feature layers.
//! - `list_attribute_tables()` / `get_attribute_table(name)` / `create_attribute_table(...)` for attribute tables.
//!
//! ```no_run
//! use rusqlite_gpkg::Gpkg;
//! let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
//! let layer = gpkg.get_layer("points")?;
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! ## GpkgLayer
//!
//! `GpkgLayer` models a single feature table. It exposes schema information
//! (geometry column, property columns) and provides read/write operations.
//!
//! ```no_run
//! use geo_types::Point;
//! use rusqlite_gpkg::{Gpkg, params};
//! let layer = Gpkg::open("data.gpkg")?.get_layer("points")?;
//! layer.insert(Point::new(1.0, 2.0), params!["alpha", 7_i64])?;
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! ## GpkgFeature
//!
//! `GpkgFeature` represents one row. You can read the primary key, geometry, and
//! property values from it.
//!
//! ```no_run
//! use rusqlite_gpkg::Gpkg;
//! let features = Gpkg::open_read_only("data.gpkg")?
//!     .get_layer("points")?
//!     .features()?;
//! let feature = features.first().expect("feature");
//! let _geom = feature.geometry()?;
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! ## Value
//!
//! `Value` is the crate's owned dynamic value for feature properties, mirroring
//! SQLite's dynamic typing. Convert with `try_into()` or match directly.
//!
//! ```no_run
//! use rusqlite_gpkg::Gpkg;
//! let features = Gpkg::open_read_only("data.gpkg")?
//!     .get_layer("points")?
//!     .features()?;
//! let feature = features.first().expect("feature");
//! let name: String = feature
//!     .property("name")
//!     .ok_or_else(|| rusqlite_gpkg::GpkgError::MissingProperty {
//!         property: "name".to_string(),
//!     })?
//!     .try_into()?;
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! ## Attribute tables
//!
//! Attribute tables hold non-spatial data (no geometry column). They follow
//! the GeoPackage spec Section 2.4 (`data_type = 'attributes'` in `gpkg_contents`).
//!
//! ```no_run
//! use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg, params};
//! let gpkg = Gpkg::open_in_memory()?;
//! let columns = vec![
//!     ColumnSpec { name: "name".to_string(), column_type: ColumnType::Varchar },
//!     ColumnSpec { name: "value".to_string(), column_type: ColumnType::Integer },
//! ];
//! let table = gpkg.create_attribute_table("observations", &columns)?;
//! table.insert(params!["alpha", 7_i64])?;
//!
//! let rows = table.rows()?;
//! let name: String = rows[0].property("name").unwrap().try_into()?;
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! ## Arrow (feature = "arrow")
//!
//! The Arrow reader yields `RecordBatch`es for a layer. It borrows the `Gpkg`
//! because it holds a prepared statement internally.
//!
//! ```no_run
//! # #[cfg(feature = "arrow")]
//! use rusqlite_gpkg::{ArrowGpkgReader, Gpkg};
//! # #[cfg(feature = "arrow")]
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
//!     let mut reader = ArrowGpkgReader::new(&gpkg, "points", 256)?;
//!     let _batch = reader.next().transpose()?;
//!     Ok(())
//! }
//! # #[cfg(not(feature = "arrow"))]
//! fn main() {}
//! ```
mod error;
mod gpkg;
mod sql_functions;

#[cfg(feature = "arrow")]
mod arrow;

mod conversions;
mod ogc_sql;
mod types;
#[cfg(target_family = "wasm")]
#[cfg_attr(docsrs, doc(cfg(target_family = "wasm")))]
pub mod vfs;

#[cfg(feature = "arrow")]
pub use arrow::attribute_reader::ArrowGpkgAttributeReader;
#[cfg(feature = "arrow")]
pub use arrow::attribute_writer::ArrowGpkgAttributeWriter;
#[cfg(feature = "arrow")]
pub use arrow::reader::ArrowGpkgReader;
#[cfg(feature = "arrow")]
pub use arrow::writer::ArrowGpkgWriter;

pub use error::{GpkgError, Result};
pub use gpkg::{
    Gpkg, GpkgAttributeRow, GpkgAttributeTable, GpkgFeature, GpkgFeatureBatchIterator, GpkgLayer,
};
pub use sql_functions::register_spatial_functions;
pub use types::{ColumnSpec, ColumnType, GpkgLayerMetadata, Value};

// Re-export types used in public fields to keep the public API stable.
pub use wkb::reader::{Dimension, GeometryType};

#[cfg(target_family = "wasm")]
#[cfg_attr(docsrs, doc(cfg(target_family = "wasm")))]
pub use vfs::{HybridVfsBuilder, HybridVfsHandle};
