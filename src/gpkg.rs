//! GeoPackage reader/writer surface backed by rusqlite.
//!
//! This module currently focuses on reading layers and features from a GeoPackage,
//! while keeping the API shape flexible for future write support.

use crate::conversions::{
    column_type_from_str, column_type_to_str, dimension_from_zm, dimension_to_zm,
    geometry_type_from_str, geometry_type_to_str,
};
use crate::error::{GpkgError, Result};
use crate::ogc_sql::{
    SQL_INSERT_GPKG_CONTENTS, SQL_INSERT_GPKG_GEOMETRY_COLUMNS, SQL_LIST_LAYERS,
    SQL_SELECT_GEOMETRY_COLUMN_META, execute_rtree_sqls, gpkg_rtree_drop_sql, initialize_gpkg,
    sql_create_table, sql_delete_all, sql_drop_table, sql_insert_feature, sql_select_features,
    sql_table_columns,
};
use crate::sql_functions::register_spatial_functions;
use crate::types::{ColumnSpec, ColumnSpecs};

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
        let path = path.as_ref();
        if !path.exists() {
            return Err(GpkgError::Message(format!(
                "GeoPackage file does not exist: {}",
                path.display()
            )));
        }

        let conn = rusqlite::Connection::open(path)?;
        register_spatial_functions(&conn)?;
        Ok(Self {
            conn,
            read_only: false,
        })
    }

    /// Create a new GeoPackage
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            return Err(GpkgError::Message(format!(
                "GeoPackage file already exists: {}",
                path.display()
            )));
        }

        let conn = rusqlite::Connection::open(path)?;

        initialize_gpkg(&conn)?;
        register_spatial_functions(&conn)?;

        Ok(Self {
            conn,
            read_only: false,
        })
    }

    /// Create a new GeoPackage in memory
    pub fn new_in_memory() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;

        initialize_gpkg(&conn)?;
        register_spatial_functions(&conn)?;

        Ok(Self {
            conn,
            read_only: false,
        })
    }

    /// List the names of the layers.
    pub fn list_layers(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(SQL_LIST_LAYERS)?;
        let layers = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(layers)
    }

    /// Load a layer definition and metadata by name.
    pub fn open_layer<'a>(&'a self, layer_name: &str) -> Result<GpkgLayer<'a>> {
        let (geometry_column, geometry_type, geometry_dimension, srs_id) =
            self.get_geometry_column_and_srs_id(layer_name)?;
        let column_specs = self.get_column_specs(layer_name)?;
        let primary_key_column = column_specs.primary_key.clone();
        let other_columns = column_specs
            .other_columns
            .into_iter()
            .filter(|spec| spec.name != geometry_column)
            .collect();

        Ok(GpkgLayer {
            conn: self,
            layer_name: layer_name.to_string(),
            geometry_column,
            primary_key_column,
            geometry_type,
            geometry_dimension,
            srs_id,
            other_columns,
        })
    }

    // Create a new layer.
    pub fn new_layer<'a>(
        &'a self,
        layer_name: &str,
        geometry_column: String,
        geometry_type: wkb::reader::GeometryType,
        geometry_dimension: wkb::reader::Dimension,
        srs_id: u32,
        other_column_specs: &[ColumnSpec],
    ) -> Result<GpkgLayer<'a>> {
        if self.read_only {
            return Err(GpkgError::ReadOnly);
        }

        if self.list_layers()?.iter().any(|name| name == layer_name) {
            return Err(GpkgError::Message(format!(
                "Layer already exists: {layer_name}"
            )));
        }

        let geometry_type_name = geometry_type_to_str(geometry_type);
        let (z, m) = dimension_to_zm(geometry_dimension);

        let mut column_defs = Vec::with_capacity(other_column_specs.len() + 2);
        column_defs.push("fid INTEGER PRIMARY KEY AUTOINCREMENT".to_string());
        column_defs.push(format!(r#""{}" BLOB"#, geometry_column));
        for spec in other_column_specs {
            let col_type = column_type_to_str(spec.column_type);
            column_defs.push(format!(r#""{}" {col_type}"#, spec.name));
        }

        let create_sql = sql_create_table(layer_name, &column_defs.join(", "));
        self.conn.execute_batch(&create_sql)?;

        self.conn.execute(
            SQL_INSERT_GPKG_CONTENTS,
            rusqlite::params![layer_name, layer_name, srs_id],
        )?;
        self.conn.execute(
            SQL_INSERT_GPKG_GEOMETRY_COLUMNS,
            rusqlite::params![
                layer_name,
                geometry_column,
                geometry_type_name,
                srs_id,
                z,
                m
            ],
        )?;

        execute_rtree_sqls(&self.conn, layer_name, &geometry_column, "fid")?;

        Ok(GpkgLayer {
            conn: self,
            layer_name: layer_name.to_string(),
            geometry_column,
            primary_key_column: "fid".to_string(),
            geometry_type,
            geometry_dimension,
            srs_id,
            other_columns: other_column_specs.to_vec(),
        })
    }

    /// Delete a layer.
    pub fn delete_layer(&self, layer_name: &str) -> Result<()> {
        if self.read_only {
            return Err(GpkgError::ReadOnly);
        }

        let (geometry_column, _, _, _) = self.get_geometry_column_and_srs_id(layer_name)?;

        self.conn
            .execute_batch(&gpkg_rtree_drop_sql(layer_name, &geometry_column))?;

        self.conn.execute_batch(&sql_drop_table(layer_name))?;
        Ok(())
    }

    /// Resolve the table columns and map SQLite types.
    fn get_column_specs(&self, layer_name: &str) -> Result<ColumnSpecs> {
        let query = sql_table_columns(layer_name);
        let mut stmt = self.conn.prepare(&query)?;

        let mut primary_key: Option<String> = None;
        let column_specs = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let column_type_str: String = row.get(1)?;
            let primary_key: i32 = row.get(2)?;
            let primary_key = primary_key != 0;

            // cf. https://www.geopackage.org/spec140/index.html#_sqlite_container
            let column_type = column_type_from_str(&column_type_str).ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    1,
                    format!("Unexpected type {}", column_type_str),
                    rusqlite::types::Type::Text,
                )
            })?;

            Ok((name, column_type, primary_key))
        })?;

        let result: std::result::Result<Vec<(String, crate::types::ColumnType, bool)>, _> =
            column_specs.collect();
        let mut other_columns = Vec::new();
        for (name, column_type, is_primary_key) in result? {
            if is_primary_key {
                if primary_key.is_some() {
                    return Err(GpkgError::Message(format!(
                        "Composite primary keys are not supported yet for layer: {layer_name}"
                    )));
                }
                primary_key = Some(name.clone());
            }
            other_columns.push(ColumnSpec { name, column_type });
        }

        let primary_key = primary_key.ok_or_else(|| {
            GpkgError::Message(format!(
                "No primary key column found for layer: {layer_name}"
            ))
        })?;

        Ok(ColumnSpecs {
            primary_key,
            other_columns,
        })
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
        let mut stmt = self.conn.prepare(SQL_SELECT_GEOMETRY_COLUMN_META)?;

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

        let geometry_type = geometry_type_from_str(&geometry_type_str)?;
        let geometry_dimension = dimension_from_zm(z, m)?;

        Ok((geometry_column, geometry_type, geometry_dimension, srs_id))
    }
}

/// A GeoPackage layer with geometry metadata and column specs.
pub struct GpkgLayer<'a> {
    conn: &'a Gpkg,
    layer_name: String,
    geometry_column: String,
    primary_key_column: String,
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

    /// Return the primary key column name.
    pub fn primary_key_column(&self) -> &str {
        &self.primary_key_column
    }

    /// Return the non-geometry columns in order.
    pub fn property_columns(&self) -> &[ColumnSpec] {
        &self.other_columns
    }

    /// Iterate over features in the layer in rowid order.
    pub fn features(&self) -> Result<GpkgFeatureIterator> {
        let column_specs = self.conn.get_column_specs(&self.layer_name)?;
        let geometry_index = column_specs
            .other_columns
            .iter()
            .position(|spec| spec.name == self.geometry_column)
            .ok_or_else(|| rusqlite::Error::InvalidColumnName(self.geometry_column.clone()))?;
        let primary_index = column_specs
            .other_columns
            .iter()
            .position(|spec| spec.name == self.primary_key_column)
            .ok_or_else(|| rusqlite::Error::InvalidColumnName(self.primary_key_column.clone()))?;
        let columns = column_specs
            .other_columns
            .iter()
            .map(|spec| format!(r#""{}""#, spec.name))
            .collect::<Vec<String>>()
            .join(",");
        let sql = sql_select_features(&self.layer_name, &columns);
        let mut stmt = self.conn.conn.prepare(&sql)?;
        let features = stmt
            .query_map([], |row| {
                let mut id: Option<i64> = None;
                let mut geometry: Option<Vec<u8>> = None;
                let mut properties =
                    Vec::with_capacity(column_specs.other_columns.len().saturating_sub(1));

                for (idx, spec) in column_specs.other_columns.iter().enumerate() {
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
                    } else if idx == primary_index {
                        match &value {
                            Value::Integer(value) => id = Some(*value),
                            _ => {
                                return Err(rusqlite::Error::InvalidColumnType(
                                    idx,
                                    spec.name.clone(),
                                    value_ref.data_type(),
                                ));
                            }
                        }
                        properties.push(value);
                    } else {
                        properties.push(value);
                    }
                }

                let id = id.ok_or_else(|| {
                    rusqlite::Error::InvalidColumnType(
                        primary_index,
                        self.primary_key_column.clone(),
                        Type::Null,
                    )
                })?;

                Ok(GpkgFeature {
                    id,
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
        let sql = sql_delete_all(&self.layer_name);
        Ok(self.conn.conn.execute(&sql, [])?)
    }

    /// Insert a feature with geometry and ordered property values.
    pub fn insert<G, I>(&self, geometry: G, params: I) -> Result<()>
    where
        G: GeometryTrait<T = f64>,
        I: IntoIterator<Item = Value>,
    {
        let (values, column_names) = self.feature_values_and_columns(geometry, params)?;

        let columns = column_names
            .iter()
            .map(|name| format!(r#""{}""#, name))
            .collect::<Vec<String>>()
            .join(",");
        let placeholders = (1..=column_names.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<String>>()
            .join(",");
        let sql = sql_insert_feature(&self.layer_name, &columns, &placeholders);

        let mut stmt = self.conn.conn.prepare(&sql)?;
        stmt.execute(params_from_iter(values))?;

        Ok(())
    }

    /// Update the feature with geometry and ordered property values.
    pub fn update<G, I>(&self, geometry: G, params: I, id: i64) -> Result<()>
    where
        G: GeometryTrait<T = f64>,
        I: IntoIterator<Item = Value>,
    {
        let (mut values, column_names) = self.feature_values_and_columns(geometry, params)?;
        values.push(Value::Integer(id));

        let assignments = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| format!(r#""{}"=?{}"#, name, idx + 1))
            .collect::<Vec<String>>()
            .join(",");
        let id_idx = values.len();
        let sql = format!(
            r#"UPDATE "{}" SET {} WHERE "{}"=?{}"#,
            self.layer_name, assignments, self.primary_key_column, id_idx
        );

        let mut stmt = self.conn.conn.prepare(&sql)?;
        stmt.execute(params_from_iter(values))?;

        Ok(())
    }

    fn feature_values_and_columns<G, I>(
        &self,
        geometry: G,
        params: I,
    ) -> Result<(Vec<Value>, Vec<String>)>
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

        Ok((values, column_names))
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
    id: i64,
    geometry: Option<Vec<u8>>,
    properties: Vec<Value>,
}

impl GpkgFeature {
    /// Return the primary key value.
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Decode the geometry column into WKB.
    pub fn geometry(&self) -> Result<Wkb<'_>> {
        let bytes = self.geometry.as_ref().ok_or_else(|| {
            GpkgError::Sql(rusqlite::Error::InvalidColumnType(
                0,
                "geometry".to_string(),
                Type::Null,
            ))
        })?;
        gpkg_geometry_to_wkb(bytes)
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

    pub fn new<G, I>(id: i64, geometry: G, properties: I) -> Result<Self>
    where
        G: GeometryTrait<T = f64>,
        I: IntoIterator<Item = Value>,
    {
        let mut wkb = Vec::new();
        wkb::writer::write_geometry(&mut wkb, &geometry, &Default::default())?;
        Ok(Self {
            id,
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
            return Err(GpkgError::InvalidGpkgGeometryFlags(flags));
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

        let points = gpkg.open_layer("points")?;
        let lines = gpkg.open_layer("lines")?;
        let polygons = gpkg.open_layer("polygons")?;

        assert_eq!(points.features()?.count(), 5);
        assert_eq!(lines.features()?.count(), 3);
        assert_eq!(polygons.features()?.count(), 2);

        Ok(())
    }

    #[test]
    fn reads_geometry_and_properties_from_points() -> Result<()> {
        let gpkg = Gpkg::open_read_only(generated_gpkg_path())?;
        let layer = gpkg.open_layer("points")?;
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
