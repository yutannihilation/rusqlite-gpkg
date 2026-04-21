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

pub mod attribute_reader;
pub mod attribute_writer;
pub mod reader;
pub mod writer;

use geoarrow_array::builder::WkbBuilder;

pub(crate) fn crs_from_srs_id(srs_id: u32) -> geoarrow_schema::Crs {
    match epsg_utils::epsg_to_projjson(srs_id as i32) {
        Ok(projjson_str) => {
            let value: serde_json::Value =
                serde_json::from_str(projjson_str).expect("PROJJSON from epsg-utils must be valid");
            geoarrow_schema::Crs::from_projjson(value)
        }
        Err(_) => geoarrow_schema::Crs::from_srid(srs_id.to_string()),
    }
}

pub(crate) fn wkb_geometry_field(field_name: &str, srs_id: u32) -> arrow_schema::Field {
    let geoarrow_metadata = geoarrow_schema::Metadata::new(crs_from_srs_id(srs_id), None);
    geoarrow_schema::GeoArrowType::Wkb(geoarrow_schema::WkbType::new(geoarrow_metadata.into()))
        .to_field(field_name, true)
}

pub(crate) fn wkb_geometry_builder(srs_id: u32, batch_size: usize) -> WkbBuilder<i32> {
    let geoarrow_metadata = geoarrow_schema::Metadata::new(crs_from_srs_id(srs_id), None);
    WkbBuilder::with_capacity(
        geoarrow_schema::WkbType::new(geoarrow_metadata.into()),
        geoarrow_array::capacity::WkbCapacity::new(21 * batch_size, batch_size),
    )
}
