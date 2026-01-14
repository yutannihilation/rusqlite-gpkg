mod ogc_sql;

use rusqlite::Connection;

pub fn read_gpkg(path: &str) -> rusqlite::Result<()> {
    let conn = Connection::open(path)?;
    let mut stmt = conn.prepare("SELECT * FROM points")?;
    let mut rows = stmt.query(())?;

    while let Some(row) = rows.next()? {
        let val = row.get::<_, Vec<u8>>(1)?;
        println!("{val:?}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
}
