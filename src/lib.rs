//! Read and write GeoPackage data with a small, rusqlite-backed API.
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
//!         let layer = gpkg.open_layer(&layer_name)?;
//!         for feature in layer.features()? {
//!             let geom: wkb::reader::Wkb<'_> = feature.geometry()?;
//!
//!             // Use wkt to show the context of the geometry
//!             let mut wkt = String::new();
//!             write_geometry(&mut wkt, &geom)?;
//!             println!("{layer_name}: {wkt}");
//!
//!             for (idx, column) in layer.property_columns().iter().enumerate() {
//!                 let value: Value = feature.property(idx)?;
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
//! use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let gpkg = Gpkg::new("data.gpkg")?;
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
//!     let layer = gpkg.new_layer(
//!         "points",
//!         "geom".to_string(),
//!         wkb::reader::GeometryType::Point,
//!         wkb::reader::Dimension::Xy,
//!         4326,
//!         &columns,
//!     )?;
//!
//!     // You can pass whatever object that implements GeometryTrait
//!     layer.insert(Point::new(1.0, 2.0), ("alpha", 7_i64))?;
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
pub use types::{ColumnSpec, ColumnType, RusqliteValues};

// Re-export types used in public fields to keep the public API stable.
pub use rusqlite::types::Value;
pub use wkb::reader::{Dimension, GeometryType};
