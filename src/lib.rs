//! GeoPackage reader/writer built on top of rusqlite.
//!
//! ## Overview
//!
//! - `Gpkg` represents the whole data of GeoPackage data.
//! - `GpkgLayer` represents a single layer in the data.
//! - `GpkgFeature` represents a single feature in the layer.
//! - `Value` represents a single property value related to the feature.
//!
//! `Gpkg` is the entry point and supports several open modes:
//!
//! - `Gpkg::open_read_only(path)`: open an existing file without write access.
//! - `Gpkg::open(path)`: open a new or existing file for read/write.
//! - `Gpkg::open_in_memory()`: create a transient in-memory GeoPackage.
//!
//! You access a `GpkgLayer` via `Gpkg::get_layer(name)` for existing layers
//! or `Gpkg::create_layer(...)` for a new layer.
//!
//! `GpkgLayer::insert` and `GpkgLayer::update` accept any geometry that implements
//! `geo_traits::GeometryTrait<T = f64>` (for example `geo_types::Point` or `wkt::Wkt`).
//!
//! ## Short usage
//!
//! ```no_run
//! use rusqlite_gpkg::Gpkg;
//!
//! let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
//! let layers = gpkg.list_layers()?;
//! let layer = gpkg.get_layer(&layers[0])?;
//! let feature = layer.features()?.next().expect("feature");
//! let _id = feature.id();
//! let _geom = feature.geometry()?;
//! let _name: String = feature
//!     .property("name")
//!     .ok_or("missing name")?
//!     .try_into()?;
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! ## Reader
//!
//! ```no_run
//! use rusqlite_gpkg::{Gpkg, Value};
//! use wkt::to_wkt::write_geometry;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let gpkg = Gpkg::open("data.gpkg")?;
//!     for layer_name in gpkg.list_layers()? {
//!         let layer = gpkg.get_layer(&layer_name)?;
//!         for feature in layer.features()? {
//!             let geom: wkb::reader::Wkb<'_> = feature.geometry()?;
//!
//!             // Use wkt to show the context of the geometry
//!             let mut wkt = String::new();
//!             write_geometry(&mut wkt, &geom)?;
//!             println!("{layer_name}: {wkt}");
//!
//!             for column in &layer.property_columns {
//!                 let value = feature.property(&column.name).unwrap_or(Value::Null);
//!                 println!("  {} = {:?}", column.name, value);
//!             }
//!         }
//!     }
//!     Ok(())
//! }
//! ```
//!
//! ## Writer
//!
//! ```no_run
//! use geo_types::Point;
//! use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg, params};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let gpkg = Gpkg::open("data.gpkg")?;
//!
//!     let columns = vec![
//!         ColumnSpec {
//!             name: "name".to_string(),
//!             column_type: ColumnType::Varchar,
//!         },
//!         ColumnSpec {
//!             name: "value".to_string(),
//!             column_type: ColumnType::Integer,
//!         },
//!     ];
//!
//!     let layer = gpkg.create_layer(
//!         "points",
//!         "geom",
//!         wkb::reader::GeometryType::Point,
//!         wkb::reader::Dimension::Xy,
//!         4326,
//!         &columns,
//!     )?;
//!
//!     
//!     layer.insert(
//!         Point::new(1.0, 2.0),    // geometry: You can pass whatever object that implements GeometryTrait
//!         params!["alpha", 7_i64]  // other properties: Use params! macro to create &[&dyn ToSQL]
//!     )?;
//!
//!     Ok(())
//! }
//! ```
mod error;
mod gpkg;
mod sql_functions;

mod conversions;
mod ogc_sql;
mod types;

pub use error::{GpkgError, Result};
pub use gpkg::{Gpkg, GpkgFeature, GpkgFeatureIterator, GpkgLayer};
pub use sql_functions::register_spatial_functions;
pub use types::{ColumnSpec, ColumnType, Value};

// Re-export types used in public fields to keep the public API stable.
pub use rusqlite::params;
pub use wkb::reader::{Dimension, GeometryType};
