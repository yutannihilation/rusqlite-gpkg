mod io;

use geo_types::Point;
use io::OpfsFile;
use rusqlite_gpkg::{ColumnSpec, ColumnType, Dimension, GeometryType, Gpkg, params};
use std::io::Write;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn generate_gpkg_to_opfs(
    output_file: web_sys::FileSystemSyncAccessHandle,
    point_count: u32,
) -> Result<u32, JsValue> {
    let gpkg = Gpkg::open_in_memory().map_err(|e| JsValue::from_str(&format!("{e}")))?;

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

    // Small fast deterministic PRNG so we don't need an extra dependency.
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

    let bytes = gpkg
        .to_bytes()
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    let file = OpfsFile::new(output_file).map_err(|e| JsValue::from_str(&e))?;
    let mut writer = std::io::BufWriter::new(file);
    writer
        .write_all(&bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    writer
        .flush()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(point_count)
}
