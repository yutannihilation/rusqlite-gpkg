//! GeoPackage reader/writer surface backed by rusqlite.
//!
//! This module currently focuses on reading layers and features from a GeoPackage,
//! while keeping the API shape flexible for future write support.

mod feature;
mod gpkg;
mod layer;

pub use feature::{GpkgFeature, GpkgFeatureIterator};
pub use gpkg::Gpkg;
pub use layer::GpkgLayer;

pub(crate) use feature::{gpkg_geometry_to_wkb, wkb_to_gpkg_geometry};
