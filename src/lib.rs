mod gpkg;
mod ogc_sql;
mod types;

// The data chunk size. This can be obtained via libduckdb_sys::duckdb_vector_size(),
// but use a fixed value here.
pub(crate) const VECTOR_SIZE: usize = 2048;

use rusqlite::Connection;

pub fn read_gpkg(path: &str) -> rusqlite::Result<()> {
    let conn = Connection::open(path)?;
    let mut stmt = conn.prepare("SELECT * FROM points")?;
    let mut rows = stmt.query(())?;

    while let Some(row) = rows.next()? {
        let val = row.get::<_, Vec<u8>>(1)?;
        let flags = val[3];

        println!("{val:?}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
}
