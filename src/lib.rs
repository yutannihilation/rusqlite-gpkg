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
//! `GpkgLayer::features()` always allocates a `Vec<GpkgFeature>` for the whole
//! layer. For large datasets, use `features_batch(batch_size)` to iterate in
//! chunks and limit peak memory.
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
//! let features = layer.features()?;
//! let feature = features.first().expect("feature");
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
//! If you want to process features in batches:
//!
//! ```no_run
//! use rusqlite_gpkg::Gpkg;
//!
//! let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
//! let layer = gpkg.get_layer("points")?;
//! for batch in layer.features_batch(100)? {
//!     let features = batch?;
//!     for feature in features {
//!         let _id = feature.id();
//!         let _geom = feature.geometry()?;
//!     }
//! }
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! `Value` is the crate's owned dynamic value used for feature properties. It
//! mirrors SQLite's dynamic typing (null, integer, real, text, blob) and is
//! returned by `GpkgFeature::property` as `Option<Value>`. Convert using
//! `try_into()` or match directly:
//!
//! ```no_run
//! # use rusqlite_gpkg::Gpkg;
//! # let gpkg = Gpkg::open("data.gpkg")?;
//! # let layer = gpkg.get_layer("points")?;
//! # let features = layer.features()?;
//! # let feature = features.first().expect("feature");
//! let name: String = feature.property("name").ok_or("missing name")?.try_into()?;
//! let active: bool = feature.property("active").ok_or("missing active")?.try_into()?;
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! The conversion above returns an error if the value is `NULL`. If you want to
//! handle `NULL`, convert to `Option<T>`; `NULL` becomes `None` and non-null
//! values become `Some(T)`:
//!
//! ```no_run
//! use rusqlite_gpkg::Value;
//!
//! let value = Value::Null;
//! let maybe_i64: Option<i64> = value.try_into()?;
//! assert_eq!(maybe_i64, None);
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! ## Writer
//!
//! ```no_run
//! use geo_types::Point;
//! use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg, Value, params};
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
//!         Point::new(1.0, 2.0),  // geometry: You can pass whatever object that implements GeometryTrait
//!         params!["alpha", 7_i64], // other properties: pass references to Value
//!     )?;
//!
//! // You might notice the `params!` macro in the example above. It is useful when
//! // you want to pass a fixed list of values.
//! //
//! // `params!` accepts `Option<T>` and converts `None` to SQL `NULL`. Because
//! // `None` has no inherent type, you may need to annotate it:
//! //
//! // ```
//! // layer.insert(
//! //     Point::new(0.0, 0.0),
//! //     params![Some(1.0_f64), Option::<i64>::None],
//! // )?;
//! // ```
//! //
//! // When programmatically constructing parameters, build an iterator of `&Value`
//! // from owned values:
//! //
//! // ```no_run
//! // use rusqlite_gpkg::Value;
//! //
//! // fn convert_to_value(input: &str) -> Value {
//! //     Value::from(input)
//! // }
//! //
//! // let raw = vec!["alpha", "beta"];
//! // let values: Vec<Value> = raw.iter().map(|v| convert_to_value(v)).collect();
//! // layer.insert(Point::new(1.0, 2.0), values.iter())?;
//! // ```
//!
//!     Ok(())
//! }
//! ```
mod error;
mod gpkg;
mod sql_functions;

#[cfg(feature = "arrow")]
mod arrow;

mod conversions;
mod ogc_sql;
mod types;

#[cfg(feature = "arrow")]
pub use arrow::reader::ArrowGpkgReader;

pub use error::{GpkgError, Result};
pub use gpkg::{Gpkg, GpkgFeature, GpkgFeatureBatchIterator, GpkgLayer};
pub use sql_functions::register_spatial_functions;
pub use types::{ColumnSpec, ColumnType, GpkgLayerMetadata, Value};

// Re-export types used in public fields to keep the public API stable.
pub use wkb::reader::{Dimension, GeometryType};
