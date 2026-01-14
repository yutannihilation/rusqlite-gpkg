mod ogc_sql;
mod gpkg;
mod types;

pub use gpkg::Gpkg;
// Re-export types used in public fields to keep the public API stable.
pub use wkb::reader::{Dimension, GeometryType};
