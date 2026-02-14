mod io;

use geo_types::Point;
use io::OpfsFile;
use rusqlite_gpkg::{
    ColumnSpec, ColumnType, Dimension, GeometryType, Gpkg, HybridVfsBuilder, params,
};
use std::cell::Cell;
use wasm_bindgen::prelude::*;

// In a long-lived worker, ensure each registration gets a unique VFS name.
thread_local! {
    static NEXT_VFS_ID: Cell<u32> = const { Cell::new(0) };
}

/// Generate a demo GeoPackage and write it to an OPFS sync access handle.
///
/// Flow:
/// 1) Wrap OPFS handle as `std::io::Write` (`OpfsFile`).
/// 2) Register `HybridVfsBuilder` with that writer.
/// 3) Open sqlite through that VFS (`Gpkg::open_with_vfs`).
/// 4) Insert pseudo-random points.
/// 5) Return inserted count so JS can display progress.
#[wasm_bindgen]
pub fn generate_gpkg_to_opfs(
    output_file: web_sys::FileSystemSyncAccessHandle,
    point_count: u32,
) -> Result<u32, JsValue> {
    let vfs_name = NEXT_VFS_ID.with(|id| {
        let next = id.get().wrapping_add(1);
        id.set(next);
        format!("hybrid-opfs-{next}")
    });
    let writer = OpfsFile::new(output_file).map_err(|e| JsValue::from_str(&e))?;
    HybridVfsBuilder::new(writer)
        .register(&vfs_name, false)
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    // The filename must end with `.sqlite` so HybridVfs routes writes to `MainFile`.
    let gpkg = Gpkg::open_with_vfs("demo.sqlite", &vfs_name)
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
