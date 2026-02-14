mod io;

use geo_types::Point;
use io::OpfsFile;
use rusqlite_gpkg::{ColumnSpec, ColumnType, Dimension, GeometryType, Gpkg, params};
use wasm_bindgen::prelude::*;

/// Generate a demo GeoPackage and write it to an OPFS sync access handle.
///
/// Flow:
/// 1) Wrap OPFS handle as `std::io::Write` (`OpfsFile`).
/// 2) Call `Gpkg::open_with_writer` (register-once + writer replacement).
/// 4) Insert pseudo-random points.
/// 5) Return inserted count so JS can display progress.
#[wasm_bindgen]
pub fn generate_gpkg_to_opfs(
    output_file: web_sys::FileSystemSyncAccessHandle,
    point_count: u32,
) -> Result<u32, JsValue> {
    let writer = OpfsFile::new(output_file).map_err(|e| JsValue::from_str(&e))?;
    // The filename must end with `.sqlite` so HybridVfs routes writes to the writer.
    let gpkg = Gpkg::open_with_writer("demo.sqlite", writer)
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    let columns = vec![ColumnSpec {
        name: "value".to_string(),
        column_type: ColumnType::Integer,
    }];

    let layer = gpkg
        .create_layer(
            "points",
            "geom",
            GeometryType::Point,
            Dimension::Xy,
            4326,
            &columns,
        )
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    // Deterministic PRNG for reproducible demo output without extra deps.
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..point_count {
        state ^= state << 7;
        state ^= state >> 9;
        let lon = -180.0 + (state as f64 / u64::MAX as f64) * 360.0;

        state ^= state << 7;
        state ^= state >> 9;
        let lat = -85.0 + (state as f64 / u64::MAX as f64) * 170.0;

        layer
            .insert(Point::new(lon, lat), params![i as i64])
            .map_err(|e| JsValue::from_str(&format!("{e}")))?;
    }

    drop(layer);
    drop(gpkg);

    Ok(point_count)
}
