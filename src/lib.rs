//! GeoPackage reader/writer built on top of rusqlite.
//!
//! ## Overview
//!
//! - `Gpkg` represents the GeoPackage connection.
//! - `GpkgLayer` represents a single layer (feature table).
//! - `GpkgFeature` represents a single feature (row).
//! - `Value` represents a single property value.
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
//! ## Browser usage (to_bytes / from_bytes)
//!
//! In browser environments, file access is often unavailable. Use `to_bytes()` to
//! serialize an in-memory GeoPackage and `from_bytes()` to restore it later.
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
pub use arrow::reader::ArrowGpkgReader;

pub use error::{GpkgError, Result};
pub use gpkg::{Gpkg, GpkgFeature, GpkgFeatureBatchIterator, GpkgLayer};
pub use sql_functions::register_spatial_functions;
pub use types::{ColumnSpec, ColumnType, GpkgLayerMetadata, Value};

// Re-export types used in public fields to keep the public API stable.
pub use wkb::reader::{Dimension, GeometryType};

#[cfg(target_family = "wasm")]
#[cfg_attr(docsrs, doc(cfg(target_family = "wasm")))]
pub use vfs::{HybridVfsBuilder, HybridVfsHandle};
