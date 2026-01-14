mod gpkg;
mod ogc_sql;
mod types;

// The data chunk size. This can be obtained via libduckdb_sys::duckdb_vector_size(),
// but use a fixed value here.
pub(crate) const VECTOR_SIZE: usize = 2048;

use crate::gpkg::{gpkg_geometry_to_wkb, Gpkg};
use crate::types::ColumnType;

pub fn read_gpkg(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let gpkg = Gpkg::new(path, None)?;
    let sources = gpkg.list_data_sources()?;

    for source in sources {
        println!("layer: {}", source.layer_name);
        let mut conn = source.gpkg.conn.lock().unwrap();
        let mut offset: u32 = 0;

        loop {
            let fetched = conn.fetch_rows(&source.sql, offset, |row, row_idx| {
                let mut values = Vec::with_capacity(source.column_specs.len());
                for (idx, spec) in source.column_specs.iter().enumerate() {
                    let value = match spec.column_type {
                        ColumnType::Integer => row.get::<_, i64>(idx)?.to_string(),
                        ColumnType::Double => row.get::<_, f64>(idx)?.to_string(),
                        ColumnType::Varchar => row.get::<_, String>(idx)?,
                        ColumnType::Boolean => row.get::<_, bool>(idx)?.to_string(),
                        ColumnType::Geometry => {
                            let bytes = row.get::<_, Vec<u8>>(idx)?;
                            let wkb = gpkg_geometry_to_wkb(&bytes);
                            format!("wkb({} bytes)", wkb.len())
                        }
                    };
                    values.push(format!("{}={}", spec.name, value));
                }
                println!("  row {}: {}", (offset as usize) + row_idx, values.join(", "));
                Ok(())
            })?;

            if fetched < VECTOR_SIZE {
                break;
            }
            offset = offset.saturating_add(fetched as u32);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
}
