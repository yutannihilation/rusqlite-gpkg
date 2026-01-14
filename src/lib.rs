//! Read and explore GeoPackage data with a small, rusqlite-backed API.
//!
//! ```no_run
//! use rusqlite_gpkg::Gpkg;
//! use wkt::to_wkt::write_geometry;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let gpkg = Gpkg::open("data.gpkg")?;
//!     for layer_name in gpkg.list_layers()? {
//!         let layer = gpkg.layer(&layer_name)?;
//!         for feature in layer.features()? {
//!             let geom = feature.geometry()?;
//!             let mut wkt = String::new();
//!             write_geometry(&mut wkt, &geom)?;
//!             println!("{layer_name}: {wkt}");
//!         }
//!     }
//!     Ok(())
//! }
//! ```
mod gpkg;

mod ogc_sql;
mod types;

pub use gpkg::{Gpkg, GpkgFeature, GpkgFeatureIterator, GpkgLayer};

// Re-export types used in public fields to keep the public API stable.
pub use rusqlite::types::Value;
pub use wkb::reader::{Dimension, GeometryType};
