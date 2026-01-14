//! GeoPackage reader/writer surface backed by rusqlite.
//!
//! This module currently focuses on reading layers and features from a GeoPackage,
//! while keeping the API shape flexible for future write support.

use crate::error::{GpkgError, Result};
use crate::sql_functions::register_spatial_functions;
use crate::types::{ColumnSpec, ColumnType};

use geo_traits::GeometryTrait;
use rusqlite::{
    OpenFlags, params_from_iter,
    types::{FromSql, FromSqlError, Type, Value, ValueRef},
};
use std::path::Path;
use wkb::reader::Wkb;

/// GeoPackage connection wrapper for reading (and later writing) layers.
pub struct Gpkg {
    conn: rusqlite::Connection,
    read_only: bool,
}

impl Gpkg {
    /// Open a GeoPackage in read-only mode.
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = rusqlite::Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        register_spatial_functions(&conn)?;
        Ok(Self {
            conn,
            read_only: true,
        })
    }

    /// Open a GeoPackage in read-write mode.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        // TODO: validate if the path exists at least. Hopefully, we should check if it's valid as a GeoPackage, but I'm not sure.
        let conn = rusqlite::Connection::open(path)?;
        register_spatial_functions(&conn)?;
        Ok(Self {
            conn,
            read_only: false,
        })
    }

    /// Create a new GeoPackage
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // TODO: raise an error if the file already exists
        let conn = rusqlite::Connection::open(path)?;

        // TODO: initialize database with necessary tables and triggers as a GeoPackage
        register_spatial_functions(&conn)?;

        Ok(Self {
            conn,
            read_only: false,
        })
    }

    /// Create a new GeoPackage in memory
    pub fn new_in_memory() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;

        // TODO: initialize database with necessary tables and triggers as a GeoPackage
        register_spatial_functions(&conn)?;

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
            .collect::<std::result::Result<Vec<String>, _>>()?;
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

    pub fn new_layer<'a>(
        &'a self,
        layer_name: &str,
        geometry_column: String,
        geometry_type: wkb::reader::GeometryType,
        geometry_dimension: wkb::reader::Dimension,
        srs_id: u32,
        other_column_specs: &[ColumnSpec],
    ) -> Result<GpkgLayer<'a>> {
        todo!()
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

        let result: std::result::Result<Vec<ColumnSpec>, rusqlite::Error> = column_specs.collect();
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

        let (geometry_column, geometry_type_str, z, m, srs_id) =
            stmt.query_one([layer_name], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i8>(2)?,
                    row.get::<_, i8>(3)?,
                    row.get::<_, u32>(4)?,
                ))
            })?;

        let geometry_type = match geometry_type_str.as_str() {
            "GEOMETRY" => wkb::reader::GeometryType::GeometryCollection,
            "POINT" => wkb::reader::GeometryType::Point,
            "LINESTRING" => wkb::reader::GeometryType::LineString,
            "POLYGON" => wkb::reader::GeometryType::Polygon,
            "MULTIPOINT" => wkb::reader::GeometryType::MultiPoint,
            "MULTILINESTRING" => wkb::reader::GeometryType::MultiLineString,
            "MULTIPOLYGON" => wkb::reader::GeometryType::MultiPolygon,
            "GEOMETRYCOLLECTION" => wkb::reader::GeometryType::GeometryCollection,
            _ => return Err(GpkgError::UnsupportedGeometryType(geometry_type_str)),
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
            (2, _) | (_, 2) | _ => return Err(GpkgError::InvalidDimension { z, m }),
        };

        Ok((geometry_column, geometry_type, geometry_dimension, srs_id))
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
            .collect::<std::result::Result<Vec<GpkgFeature>, _>>()?;

        Ok(GpkgFeatureIterator {
            features: features.into_iter(),
        })
    }

    /// Remove all rows from the layer.
    pub fn truncate(&self) -> Result<usize> {
        self.ensure_writable()?;
        let sql = format!(r#"DELETE FROM "{}""#, self.layer_name);
        Ok(self.conn.conn.execute(&sql, [])?)
    }

    /// Insert a feature with geometry and ordered property values.
    pub fn insert<G, I>(&self, geometry: G, params: I) -> Result<usize>
    where
        G: GeometryTrait<T = f64>,
        I: IntoIterator<Item = Value>,
    {
        self.ensure_writable()?;

        let mut wkb = Vec::new();
        wkb::writer::write_geometry(&mut wkb, &geometry, &Default::default())?;
        let wkb = Wkb::try_new(&wkb)?;
        let geom = wkb_to_gpkg_geometry(wkb, self.srs_id)?;

        let properties: Vec<Value> = params.into_iter().collect();
        if properties.len() != self.other_columns.len() {
            return Err(GpkgError::InvalidPropertyCount {
                expected: self.other_columns.len(),
                got: properties.len(),
            });
        }

        let mut values = Vec::with_capacity(self.other_columns.len() + 1);
        values.push(Value::Blob(geom));
        values.extend(properties);

        let mut column_names = Vec::with_capacity(self.other_columns.len() + 1);
        column_names.push(self.geometry_column.clone());
        column_names.extend(self.other_columns.iter().map(|col| col.name.clone()));

        let columns = column_names
            .iter()
            .map(|name| format!(r#""{}""#, name))
            .collect::<Vec<String>>()
            .join(",");
        let placeholders = (1..=column_names.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<String>>()
            .join(",");
        let sql = format!(
            r#"INSERT INTO "{}" ({}) VALUES ({})"#,
            self.layer_name, columns, placeholders
        );

        let mut stmt = self.conn.conn.prepare(&sql)?;
        Ok(stmt.execute(params_from_iter(values))?)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.conn.read_only {
            return Err(GpkgError::ReadOnly);
        }
        Ok(())
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
            GpkgError::Sql(rusqlite::Error::InvalidColumnType(
                0,
                "geometry".to_string(),
                Type::Null,
            ))
        })?;
        Ok(gpkg_geometry_to_wkb(bytes)?)
    }

    /// Read a property by index using rusqlite's `FromSql` conversion.
    pub fn property<T: FromSql>(&self, idx: usize) -> Result<T> {
        let value = self
            .properties
            .get(idx)
            .ok_or(GpkgError::Sql(rusqlite::Error::InvalidColumnIndex(idx)))?;
        let value_ref = ValueRef::from(value);
        FromSql::column_result(value_ref).map_err(|err| match err {
            FromSqlError::InvalidType => GpkgError::Sql(rusqlite::Error::InvalidColumnType(
                idx,
                format!("column {idx}"),
                value_ref.data_type(),
            )),
            FromSqlError::OutOfRange(i) => {
                GpkgError::Sql(rusqlite::Error::IntegralValueOutOfRange(idx, i))
            }
            FromSqlError::Other(err) => GpkgError::Sql(rusqlite::Error::FromSqlConversionFailure(
                idx,
                value_ref.data_type(),
                err,
            )),
            FromSqlError::InvalidBlobSize { .. } => {
                GpkgError::Sql(rusqlite::Error::FromSqlConversionFailure(
                    idx,
                    value_ref.data_type(),
                    Box::new(err),
                ))
            }
            _ => GpkgError::Message("unsupported sqlite type conversion".to_string()),
        })
    }

    pub fn new<G, I>(geometry: G, properties: I) -> Result<Self>
    where
        G: GeometryTrait<T = f64>,
        I: IntoIterator<Item = Value>,
    {
        let mut wkb = Vec::new();
        wkb::writer::write_geometry(&mut wkb, &geometry, &Default::default())?;
        Ok(Self {
            geometry: Some(wkb),
            properties: properties.into_iter().collect(),
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
pub(crate) fn gpkg_geometry_to_wkb<'a>(b: &'a [u8]) -> Result<Wkb<'a>> {
    let flags = b[3];
    let envelope_size: usize = match flags & 0b00001110 {
        0b00000000 => 0,  // no envelope
        0b00000010 => 32, // envelope is [minx, maxx, miny, maxy], 32 bytes
        0b00000100 => 48, // envelope is [minx, maxx, miny, maxy, minz, maxz], 48 bytes
        0b00000110 => 48, // envelope is [minx, maxx, miny, maxy, minm, maxm], 48 bytes
        0b00001000 => 64, // envelope is [minx, maxx, miny, maxy, minz, maxz, minm, maxm], 64 bytes
        _ => {
            // invalid
            return Wkb::try_new(&[]).map_err(GpkgError::from);
        }
    };
    let offset = 8 + envelope_size;

    Ok(Wkb::try_new(&b[offset..])?)
}

// cf. https://www.geopackage.org/spec140/index.html#gpb_format
pub(crate) fn wkb_to_gpkg_geometry<'a>(wkb: Wkb<'a>, srs_id: u32) -> Result<Vec<u8>> {
    let mut geom = Vec::with_capacity(wkb.buf().len() + 8);
    geom.extend_from_slice(&[
        0x47u8, // magic
        0x50u8, // magic
        0x00u8, // version
        0x01u8, // flags (little endian SRS ID, no envelope)
    ]);
    geom.extend_from_slice(&srs_id.to_le_bytes());
    geom.extend_from_slice(wkb.buf());

    Ok(geom)
}

#[cfg(test)]
mod tests {
    use super::Gpkg;
    use crate::Result;
    use rusqlite::types::Value;
    use wkb::reader::GeometryType;

    fn generated_gpkg_path() -> &'static str {
        "src/test/test_generated.gpkg"
    }

    fn property_index(columns: &[super::ColumnSpec], name: &str) -> Option<usize> {
        columns.iter().position(|col| col.name == name)
    }

    #[test]
    fn reads_generated_layers_and_counts() -> Result<()> {
        let gpkg = Gpkg::open_read_only(generated_gpkg_path())?;
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
    fn reads_geometry_and_properties_from_points() -> Result<()> {
        let gpkg = Gpkg::open_read_only(generated_gpkg_path())?;
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
