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

pub mod reader;
