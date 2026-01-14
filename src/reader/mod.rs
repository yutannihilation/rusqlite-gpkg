use crate::VECTOR_SIZE;
use crate::types::{ColumnSpec, ColumnType};

use rusqlite::{Connection, OpenFlags, Result, Row};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use wkb::error::WkbResult;
use wkb::reader::Wkb;

pub struct Gpkg {
    conn: rusqlite::Connection,
}

impl Gpkg {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        Ok(Self { conn })
    }

    pub fn list_layers(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT table_name FROM gpkg_contents")?;
        let layers = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(layers)
    }

    pub fn layer(&self, layer_name: &str) -> Result<GpkgLayer> {
        todo!()
    }

    fn get_column_specs(
        &self,
        layer_name: &str,
    ) -> Result<Vec<ColumnSpec>, Box<dyn std::error::Error>> {
        let query = format!(
            "SELECT name, type FROM pragma_table_info('{layer_name}') WHERE name != 'fid'",
        );
        let mut stmt = self.conn.prepare(&query)?;

        let column_specs = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let column_type_str: String = row.get(1)?;

            // cf. https://www.geopackage.org/spec140/index.html#_sqlite_container
            let column_type = match column_type_str.to_uppercase().as_str() {
                "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" => ColumnType::Integer,
                "DOUBLE" | "FLOAT" | "REAL" => ColumnType::Double,
                "TEXT" => ColumnType::Varchar,
                "BOOLEAN" => ColumnType::Boolean,
                // cf. https://www.geopackage.org/spec140/index.html#geometry_types
                "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT"
                | "MULTILINESTRING" | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => ColumnType::Geometry,
                _ => {
                    return Err(rusqlite::Error::InvalidColumnType(
                        1,
                        format!("Unexpected type {}", column_type_str),
                        rusqlite::types::Type::Text,
                    ));
                }
            };

            Ok(ColumnSpec { name, column_type })
        })?;

        let result: Result<Vec<ColumnSpec>, rusqlite::Error> = column_specs.collect();
        Ok(result?)
    }

    fn get_geometry_column_and_srs_id(
        &self,
        layer_name: &str,
    ) -> Result<
        (String, wkb::reader::GeometryType, wkb::reader::Dimension),
        Box<dyn std::error::Error>,
    > {
        let mut stmt = self.conn.prepare(
            "
SELECT column_name, geometry_type_name, z, m, srs_id
FROM gpkg_geometry_columns
WHERE table_name = ?
",
        )?;

        let result = stmt.query_one([layer_name], |row| {
            let geometry_column: String = row.get(0)?;
            let geometry_type_str: String = row.get(1)?;
            let z: i8 = row.get(2)?;
            let m: i8 = row.get(3)?;

            let geometry_type = match geometry_type_str.as_str() {
                "GEOMETRY" => Some(wkb::reader::GeometryType::GeometryCollection),
                "POINT" => Some(wkb::reader::GeometryType::Point),
                "LINESTRING" => Some(wkb::reader::GeometryType::LineString),
                "POLYGON" => Some(wkb::reader::GeometryType::Polygon),
                "MULTIPOINT" => Some(wkb::reader::GeometryType::MultiPoint),
                "MULTILINESTRING" => Some(wkb::reader::GeometryType::MultiLineString),
                "MULTIPOLYGON" => Some(wkb::reader::GeometryType::MultiLineString),
                "GEOMETRYCOLLECTION" => Some(wkb::reader::GeometryType::GeometryCollection),
                // TODO: want to return the geometry_type name to show in the error message
                _ => None,
            };

            // Note: the spec says z and m are
            //
            //   0: z/m values prohibited
            //   1: z/m values mandatory
            //   2: z/m values optional
            //
            // but I don't know how 2 can be handled
            let geometry_dimension = match (z, m) {
                (0, 0) => Some(wkb::reader::Dimension::Xy),
                (1, 0) => Some(wkb::reader::Dimension::Xyz),
                (0, 1) => Some(wkb::reader::Dimension::Xym),
                (1, 1) => Some(wkb::reader::Dimension::Xyzm),
                // TODO: these two cases should be distinguished, but we can only return rusqlite's Error here.
                // Are there any nicer way?
                (2, _) | (_, 2) => None,
                _ => None,
            };

            Ok((geometry_column, geometry_type, geometry_dimension))
        })?;

        let geometry_type = match result.1 {
            Some(geometry_type) => geometry_type,
            None => return Err("Unsupported geometry".into()),
        };

        let geometry_dimension = match result.2 {
            Some(geometry_dimension) => geometry_dimension,
            None => return Err("Invalid or mixed dimension".into()),
        };

        Ok((result.0, geometry_type, geometry_dimension))
    }
}

pub struct GpkgLayer<'a> {
    conn: &'a Gpkg,
    layer_name: String,
    geometry_column: String,
    geometry_type: wkb::reader::GeometryType,
    geometry_dimension: wkb::reader::Dimension,
    srs_id: u32,
    other_columns: Vec<ColumnSpec>,
}

impl<'a> GpkgLayer<'a> {
    fn features(&self) -> Result<GpkgFeatureIterator<'a>> {
        todo!()
    }
}

pub struct GpkgFeature<'a> {
    row: rusqlite::Row<'a>,
    geometry_column: usize,
}

impl<'a> GpkgFeature<'a> {
    fn geometry(&'a self) -> Result<Wkb<'a>> {
        todo!()
    }

    fn property<T>(&self, idx: usize) -> Result<T> {
        todo!()
    }

    fn properties<T>(&self) -> Result<&'a [T]> {
        todo!()
    }
}

pub struct GpkgFeatureIterator<'a> {
    rows: rusqlite::Rows<'a>,
}

impl<'a> Iterator for GpkgFeatureIterator<'a> {
    type Item = GpkgFeature<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rows.next() {
            Ok(_) => todo!(),
            Err(_) => todo!(),
        }
    }
}

// Below are the old source code that should be cleaned up when the above code is complete

#[repr(C)]
pub struct GpkgDataSource {
    pub layer_name: String,
    pub column_specs: Vec<ColumnSpec>,
    pub sql: String,
    pub gpkg: GpkgReaderOld,
}

#[derive(Clone)]
pub struct GpkgReaderOld {
    pub conn: Arc<Mutex<GpkgConnection>>,
    pub path: String,
    pub layers: Vec<String>,
}

pub struct GpkgConnection {
    // TODO: probably, this should contain Statement instaed of Connection.
    // But, it seems it's not possible due to the lifetime requirement.
    pub conn: Connection,
}

impl GpkgConnection {
    fn new(conn: Connection) -> Self {
        Self { conn }
    }

    // Returns the number of rows fetched.
    pub fn fetch_rows<F>(&mut self, sql: &str, offset: u32, mut f: F) -> Result<usize>
    where
        F: FnMut(&Row<'_>, usize) -> Result<()>,
    {
        let mut row_idx: usize = 0;

        let mut stmt = self.conn.prepare_cached(sql)?;
        let result = stmt
            .query_map([offset], |row| {
                let result = f(row, row_idx);
                row_idx += 1;
                result
            })?
            // result needs to be consumed, otherwise, the closure is not executed.
            .collect::<Result<Vec<()>>>()?;

        Ok(result.len())
    }
}

impl GpkgReaderOld {
    pub(crate) fn new<P: AsRef<Path>>(
        path: P,
        layer_name: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open_with_flags(
            path.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_ONLY, // open as read only
        )?;

        let mut stmt = conn.prepare("SELECT table_name FROM gpkg_contents")?;
        let layers = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        drop(stmt);

        let path = path.as_ref().to_string_lossy().to_string();
        if let Some(layer_name) = layer_name {
            let layers = if !layers.contains(&layer_name) {
                eprintln!("[WARN] No such layer '{layer_name}' in {path}",);
                vec![]
            } else {
                vec![layer_name]
            };

            Ok(Self {
                conn: Arc::new(Mutex::new(GpkgConnection::new(conn))),
                path,
                layers,
            })
        } else {
            // If layer is not specified, return all the layers
            Ok(Self {
                conn: Arc::new(Mutex::new(GpkgConnection::new(conn))),
                path,
                layers,
            })
        }
    }

    pub(crate) fn get_column_specs<T: AsRef<str>>(
        &self,
        table_name: T,
    ) -> Result<Vec<ColumnSpec>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().unwrap();

        let query = format!(
            "SELECT name, type FROM pragma_table_info('{}') WHERE name != 'fid'",
            table_name.as_ref()
        );
        let mut stmt = conn.conn.prepare(&query)?;

        let column_specs = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let column_type_str: String = row.get(1)?;

            // cf. https://www.geopackage.org/spec140/index.html#_sqlite_container
            let column_type = match column_type_str.to_uppercase().as_str() {
                "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" => ColumnType::Integer,
                "DOUBLE" | "FLOAT" | "REAL" => ColumnType::Double,
                "TEXT" => ColumnType::Varchar,
                "BOOLEAN" => ColumnType::Boolean,
                // cf. https://www.geopackage.org/spec140/index.html#geometry_types
                "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT"
                | "MULTILINESTRING" | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => ColumnType::Geometry,
                _ => {
                    return Err(rusqlite::Error::InvalidColumnType(
                        1,
                        format!("Unexpected type {}", column_type_str),
                        rusqlite::types::Type::Text,
                    ));
                }
            };

            Ok(ColumnSpec { name, column_type })
        })?;

        let result: Result<Vec<ColumnSpec>, rusqlite::Error> = column_specs.collect();
        Ok(result?)
    }

    pub(crate) fn list_data_sources(
        &self,
    ) -> Result<Vec<GpkgDataSource>, Box<dyn std::error::Error>> {
        let mut sources = Vec::new();

        for layer in &self.layers {
            let column_specs = self.get_column_specs(layer)?;
            let order_column = if self.has_fid(layer)? { "fid" } else { "rowid" };

            let sql = format!(
                r#"SELECT {} FROM "{}" ORDER BY {} LIMIT {VECTOR_SIZE} OFFSET ?"#,
                column_specs
                    .iter()
                    .map(|s| format!(r#""{}""#, s.name))
                    .collect::<Vec<String>>()
                    .join(","),
                layer,
                order_column,
            );

            sources.push(GpkgDataSource {
                layer_name: layer.to_string(),
                column_specs,
                sql,
                gpkg: self.clone(),
            });
        }

        Ok(sources)
    }
}

impl GpkgReaderOld {
    fn has_fid<T: AsRef<str>>(&self, table_name: T) -> Result<bool, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().unwrap();
        let query = format!(
            "SELECT 1 FROM pragma_table_info('{}') WHERE name = 'fid' LIMIT 1",
            table_name.as_ref()
        );
        let mut stmt = conn.conn.prepare(&query)?;
        let mut rows = stmt.query([])?;
        Ok(rows.next()?.is_some())
    }
}

// cf. https://www.geopackage.org/spec140/index.html#gpb_format
pub(crate) fn gpkg_geometry_to_wkb<'a>(b: &'a [u8]) -> WkbResult<Wkb<'a>> {
    let flags = b[3];
    let envelope_size: usize = match flags & 0b00001110 {
        0b00000000 => 0,  // no envelope
        0b00000010 => 32, // envelope is [minx, maxx, miny, maxy], 32 bytes
        0b00000100 => 48, // envelope is [minx, maxx, miny, maxy, minz, maxz], 48 bytes
        0b00000110 => 48, // envelope is [minx, maxx, miny, maxy, minm, maxm], 48 bytes
        0b00001000 => 64, // envelope is [minx, maxx, miny, maxy, minz, maxz, minm, maxm], 64 bytes
        _ => {
            // invalid
            return Wkb::try_new(&[]);
        }
    };
    let offset = 8 + envelope_size;

    Wkb::try_new(&b[offset..])
}

#[cfg(test)]
mod tests {
    use crate::types::ColumnType;

    #[test]
    fn test_get_column_specs() -> Result<(), Box<dyn std::error::Error>> {
        let gpkg = super::GpkgReaderOld::new("./test/data/points.gpkg", None)?;
        let layers = gpkg.get_column_specs("points")?;

        assert_eq!(layers.len(), 3);
        assert_eq!(&layers[0].name, "geom");
        assert_eq!(layers[0].column_type, ColumnType::Geometry);
        assert_eq!(&layers[1].name, "val1");
        assert_eq!(layers[1].column_type, ColumnType::Integer);
        assert_eq!(&layers[2].name, "val2");
        assert_eq!(layers[2].column_type, ColumnType::Varchar);

        Ok(())
    }
}
