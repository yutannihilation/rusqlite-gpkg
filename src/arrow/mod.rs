//! Arrow integration for reading GeoPackage layers as `RecordBatch`es.
//!
//! This module is available behind the `arrow` feature and exposes
//! [`ArrowGpkgReader`], an iterator that yields Arrow `RecordBatch`es of features.
//!
//! ## Example
//!
//! ```no_run
//! use rusqlite_gpkg::{ArrowGpkgReader, Gpkg};
//!
//! let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
//! let mut reader = ArrowGpkgReader::new(&gpkg, "points", 1024)?;
//! while let Some(batch) = reader.next() {
//!     let batch = batch?;
//!     println!("rows = {}", batch.num_rows());
//! }
//! # Ok::<(), rusqlite_gpkg::GpkgError>(())
//! ```
//!
//! The reader borrows the `Gpkg` because it holds a prepared `rusqlite::Statement`,
//! so the `Gpkg` must outlive the reader.
//!
//! ## Geometry handling
//!
//! Geometry columns are stored as WKB in the record batch. You can access the raw
//! bytes using `geoarrow_array`:
//!
//! ```no_run
//! use geoarrow_array::array::WkbArray;
//! use geoarrow_array::GeoArrowArrayAccessor;
//! use rusqlite_gpkg::{ArrowGpkgReader, Gpkg};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
//!     let mut reader = ArrowGpkgReader::new(&gpkg, "points", 256)?;
//!     if let Some(batch) = reader.next() {
//!         let batch = batch?;
//!         let geom_index = batch.num_columns() - 1;
//!         let schema = batch.schema();
//!         let geom_field = schema.field(geom_index).as_ref();
//!         let geom_array =
//!             WkbArray::try_from((batch.column(geom_index).as_ref(), geom_field))?;
//!
//!         let wkb = geom_array.value(0)?;
//!         let _bytes: &[u8] = wkb.buf();
//!     }
//!     Ok(())
//! }
//! ```

pub mod reader;
