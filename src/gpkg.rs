//! GeoPackage reader/writer surface backed by rusqlite.
//!
//! This module currently focuses on reading layers and features from a GeoPackage,
//! while keeping the API shape flexible for future write support.

use crate::types::{ColumnSpec, ColumnType};

use rusqlite::{
    OpenFlags, Result,
    types::{FromSql, FromSqlError, Type, Value, ValueRef},
};
use std::path::Path;
use wkb::error::WkbResult;
use wkb::reader::Wkb;

/// GeoPackage connection wrapper for reading (and later writing) layers.
pub struct Gpkg {
    conn: rusqlite::Connection,
    read_only: bool,
}

impl Gpkg {
    /// Open a GeoPackage in read-only mode.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = rusqlite::Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Ok(Self {
            conn,
            read_only: true,
        })
    }

    /// Open a GeoPackage in read-write mode.
    pub fn open_rw<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        Ok(Self {
            conn,
            read_only: false,
        })
    }

    /// Open a GeoPackage in memory
    pub fn open_in_memory() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Ok(Self {
            conn,
            read_only: false,
        })
    }

    /// List the names of the layers.
    pub fn list_layers(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT table_name FROM gpkg_contents")?;
        let layers = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(layers)
    }

    /// Load a layer definition and metadata by name.
    pub fn layer<'a>(&'a self, layer_name: &str) -> Result<GpkgLayer<'a>> {
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

    /// Resolve the table columns (excluding `fid`) and map SQLite types.
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

    /// Resolve the geometry column metadata and SRS information for a layer.
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

/// A GeoPackage layer with geometry metadata and column specs.
pub struct GpkgLayer<'a> {
    conn: &'a Gpkg,
    layer_name: String,
    geometry_column: String,
    pub geometry_type: wkb::reader::GeometryType,
    pub geometry_dimension: wkb::reader::Dimension,
    pub srs_id: u32,
    other_columns: Vec<ColumnSpec>,
}

impl<'a> GpkgLayer<'a> {
    /// Return the geometry column name.
    pub fn geometry_column(&self) -> &str {
        &self.geometry_column
    }

    /// Return the non-geometry columns in order.
    pub fn property_columns(&self) -> &[ColumnSpec] {
        &self.other_columns
    }

    /// Iterate over features in the layer in rowid order.
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

/// A single feature with geometry bytes and owned properties.
pub struct GpkgFeature {
    geometry: Option<Vec<u8>>,
    properties: Vec<Value>,
}

impl GpkgFeature {
    /// Decode the geometry column into WKB.
    pub fn geometry(&self) -> Result<Wkb<'_>> {
        let bytes = self.geometry.as_ref().ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(0, "geometry".to_string(), Type::Null)
        })?;
        gpkg_geometry_to_wkb(bytes)
            .map_err(|err| rusqlite::Error::FromSqlConversionFailure(0, Type::Blob, Box::new(err)))
    }

    /// Read a property by index using rusqlite's `FromSql` conversion.
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

/// Owned iterator over features.
pub struct GpkgFeatureIterator {
    features: std::vec::IntoIter<GpkgFeature>,
}

impl Iterator for GpkgFeatureIterator {
    type Item = GpkgFeature;

    fn next(&mut self) -> Option<Self::Item> {
        self.features.next()
    }
}

/// Strip GeoPackage header and envelope bytes to access raw WKB.
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
    use super::Gpkg;
    use rusqlite::types::Value;
    use wkb::reader::GeometryType;

    fn generated_gpkg_path() -> &'static str {
        "src/test/test_generated.gpkg"
    }

    fn property_index(columns: &[super::ColumnSpec], name: &str) -> Option<usize> {
        columns.iter().position(|col| col.name == name)
    }

    #[test]
    fn reads_generated_layers_and_counts() -> Result<(), Box<dyn std::error::Error>> {
        let gpkg = Gpkg::open(generated_gpkg_path())?;
        let mut layers = gpkg.list_layers()?;
        layers.sort();
        assert_eq!(layers, vec!["lines", "points", "polygons"]);

        let points = gpkg.layer("points")?;
        let lines = gpkg.layer("lines")?;
        let polygons = gpkg.layer("polygons")?;

        assert_eq!(points.features()?.count(), 5);
        assert_eq!(lines.features()?.count(), 3);
        assert_eq!(polygons.features()?.count(), 2);

        Ok(())
    }

    #[test]
    fn reads_geometry_and_properties_from_points() -> Result<(), Box<dyn std::error::Error>> {
        let gpkg = Gpkg::open(generated_gpkg_path())?;
        let layer = gpkg.layer("points")?;
        let columns = layer.property_columns();

        let id_idx = property_index(columns, "id").expect("id column");
        let name_idx = property_index(columns, "name").expect("name column");
        let active_idx = property_index(columns, "active").expect("active column");
        let note_idx = property_index(columns, "note").expect("note column");

        let mut iter = layer.features()?;
        let feature = iter.next().expect("first feature");

        let geom = feature.geometry()?;
        assert_eq!(geom.geometry_type(), GeometryType::Point);

        assert_eq!(feature.property::<i64>(id_idx)?, 1);
        assert_eq!(feature.property::<String>(name_idx)?, "alpha");
        assert_eq!(feature.property::<bool>(active_idx)?, true);

        let note = feature.property::<Value>(note_idx)?;
        assert_eq!(note, Value::Text("first".to_string()));

        Ok(())
    }
}
