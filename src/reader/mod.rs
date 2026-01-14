use crate::VECTOR_SIZE;
use crate::types::{ColumnSpec, ColumnType};

use rusqlite::{
    Connection, OpenFlags, Result, Row,
    types::{FromSql, FromSqlError, Type, Value, ValueRef},
};
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
        let conn = rusqlite::Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
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
        let (geometry_column, geometry_type, geometry_dimension, srs_id) =
            self.get_geometry_column_and_srs_id(layer_name)?;
        let column_specs = self.get_column_specs(layer_name)?;
        let other_columns = column_specs
            .into_iter()
            .filter(|spec| spec.name != geometry_column)
            .collect();

        Ok(GpkgLayer {
            conn: self,
            layer_name: layer_name.to_string(),
            geometry_column,
            geometry_type,
            geometry_dimension,
            srs_id,
            other_columns,
        })
    }

    fn get_column_specs(&self, layer_name: &str) -> Result<Vec<ColumnSpec>> {
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
    ) -> Result<(
        String,
        wkb::reader::GeometryType,
        wkb::reader::Dimension,
        u32,
    )> {
        let mut stmt = self.conn.prepare(
            "
SELECT column_name, geometry_type_name, z, m, srs_id
FROM gpkg_geometry_columns
WHERE table_name = ?
",
        )?;

        stmt.query_one([layer_name], |row| {
            let geometry_column: String = row.get(0)?;
            let geometry_type_str: String = row.get(1)?;
            let z: i8 = row.get(2)?;
            let m: i8 = row.get(3)?;
            let srs_id: u32 = row.get(4)?;

            let geometry_type = match geometry_type_str.as_str() {
                "GEOMETRY" => wkb::reader::GeometryType::GeometryCollection,
                "POINT" => wkb::reader::GeometryType::Point,
                "LINESTRING" => wkb::reader::GeometryType::LineString,
                "POLYGON" => wkb::reader::GeometryType::Polygon,
                "MULTIPOINT" => wkb::reader::GeometryType::MultiPoint,
                "MULTILINESTRING" => wkb::reader::GeometryType::MultiLineString,
                "MULTIPOLYGON" => wkb::reader::GeometryType::MultiPolygon,
                "GEOMETRYCOLLECTION" => wkb::reader::GeometryType::GeometryCollection,
                _ => {
                    return Err(rusqlite::Error::InvalidColumnType(
                        1,
                        "geometry_type_name".to_string(),
                        Type::Text,
                    ));
                }
            };

            // Note: the spec says z and m are
            //
            //   0: z/m values prohibited
            //   1: z/m values mandatory
            //   2: z/m values optional
            //
            // but I don't know how 2 can be handled
            let geometry_dimension = match (z, m) {
                (0, 0) => wkb::reader::Dimension::Xy,
                (1, 0) => wkb::reader::Dimension::Xyz,
                (0, 1) => wkb::reader::Dimension::Xym,
                (1, 1) => wkb::reader::Dimension::Xyzm,
                // TODO: these cases with 2 should be distinguished, but we can only return rusqlite's Error here.
                // Are there any nicer way?
                (2, _) | (_, 2) | _ => {
                    return Err(rusqlite::Error::InvalidColumnType(
                        2,
                        "dimension".to_string(),
                        Type::Integer,
                    ));
                }
            };

            Ok((geometry_column, geometry_type, geometry_dimension, srs_id))
        })
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
    pub fn features(&self) -> Result<GpkgFeatureIterator> {
        let column_specs = self.conn.get_column_specs(&self.layer_name)?;
        let geometry_index = column_specs
            .iter()
            .position(|spec| spec.name == self.geometry_column)
            .ok_or_else(|| rusqlite::Error::InvalidColumnName(self.geometry_column.clone()))?;
        let columns = column_specs
            .iter()
            .map(|spec| format!(r#""{}""#, spec.name))
            .collect::<Vec<String>>()
            .join(",");
        let sql = format!(
            r#"SELECT {} FROM "{}" ORDER BY rowid"#,
            columns, self.layer_name
        );
        let mut stmt = self.conn.conn.prepare(&sql)?;
        let features = stmt
            .query_map([], |row| {
                let mut geometry: Option<Vec<u8>> = None;
                let mut properties = Vec::with_capacity(column_specs.len().saturating_sub(1));

                for (idx, spec) in column_specs.iter().enumerate() {
                    let value_ref = row.get_ref(idx)?;
                    let value = Value::from(value_ref);

                    if idx == geometry_index {
                        match value {
                            Value::Blob(bytes) => geometry = Some(bytes),
                            Value::Null => geometry = None,
                            _ => {
                                return Err(rusqlite::Error::InvalidColumnType(
                                    idx,
                                    spec.name.clone(),
                                    value_ref.data_type(),
                                ));
                            }
                        }
                    } else {
                        properties.push(value);
                    }
                }

                Ok(GpkgFeature {
                    geometry,
                    properties,
                })
            })?
            .collect::<Result<Vec<GpkgFeature>>>()?;

        Ok(GpkgFeatureIterator {
            features: features.into_iter(),
        })
    }
}

pub struct GpkgFeature {
    geometry: Option<Vec<u8>>,
    properties: Vec<Value>,
}

impl GpkgFeature {
    pub fn geometry(&self) -> Result<Wkb<'_>> {
        let bytes = self.geometry.as_ref().ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(0, "geometry".to_string(), Type::Null)
        })?;
        gpkg_geometry_to_wkb(bytes)
            .map_err(|err| rusqlite::Error::FromSqlConversionFailure(0, Type::Blob, Box::new(err)))
    }

    pub fn property<T: FromSql>(&self, idx: usize) -> Result<T> {
        let value = self
            .properties
            .get(idx)
            .ok_or(rusqlite::Error::InvalidColumnIndex(idx))?;
        let value_ref = ValueRef::from(value);
        FromSql::column_result(value_ref).map_err(|err| match err {
            FromSqlError::InvalidType => rusqlite::Error::InvalidColumnType(
                idx,
                format!("column {idx}"),
                value_ref.data_type(),
            ),
            FromSqlError::OutOfRange(i) => rusqlite::Error::IntegralValueOutOfRange(idx, i),
            FromSqlError::Other(err) => {
                rusqlite::Error::FromSqlConversionFailure(idx, value_ref.data_type(), err)
            }
            FromSqlError::InvalidBlobSize { .. } => {
                rusqlite::Error::FromSqlConversionFailure(idx, value_ref.data_type(), Box::new(err))
            }
            _ => unimplemented!(),
        })
    }
}

pub struct GpkgFeatureIterator {
    features: std::vec::IntoIter<GpkgFeature>,
}

impl Iterator for GpkgFeatureIterator {
    type Item = GpkgFeature;

    fn next(&mut self) -> Option<Self::Item> {
        self.features.next()
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
