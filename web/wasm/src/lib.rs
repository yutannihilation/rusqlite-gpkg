mod io;

use geo_types::Point;
use io::OpfsFile;
use rusqlite_gpkg::{ColumnSpec, ColumnType, Dimension, GeometryType, Gpkg, params};
use std::io::Write;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn generate_gpkg_to_opfs(
    output_file: web_sys::FileSystemSyncAccessHandle,
) -> Result<(), JsValue> {
    let gpkg = Gpkg::open_in_memory().map_err(|e| JsValue::from_str(&format!("{e}")))?;

    let columns = vec![
        ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        },
        ColumnSpec {
            name: "value".to_string(),
            column_type: ColumnType::Integer,
        },
    ];

    let layer = gpkg
        .create_layer(
            "points",
            "geom".to_string(),
            GeometryType::Point,
            Dimension::Xy,
            4326,
            &columns,
        )
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    layer
        .insert(Point::new(139.767, 35.681), params!["Tokyo Station", 1_i64])
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

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

    Ok(())
}
