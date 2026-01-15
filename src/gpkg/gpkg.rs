use crate::conversions::{
    column_type_from_str, column_type_to_str, dimension_from_zm, dimension_to_zm,
    geometry_type_from_str, geometry_type_to_str,
};
use crate::error::{GpkgError, Result};
use crate::ogc_sql::{
    SQL_INSERT_GPKG_CONTENTS, SQL_INSERT_GPKG_GEOMETRY_COLUMNS, SQL_LIST_LAYERS,
    SQL_SELECT_GEOMETRY_COLUMN_META, execute_rtree_sqls, gpkg_rtree_drop_sql, initialize_gpkg,
    sql_create_table, sql_drop_table, sql_table_columns,
};
use crate::sql_functions::register_spatial_functions;
use crate::types::{ColumnSpec, ColumnSpecs};
use rusqlite::OpenFlags;
use std::path::Path;

use super::layer::GpkgLayer;

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

    pub(crate) fn connection(&self) -> &rusqlite::Connection {
        &self.conn
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Resolve the table columns and map SQLite types.
    pub(crate) fn get_column_specs(&self, layer_name: &str) -> Result<ColumnSpecs> {
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
    pub(crate) fn get_geometry_column_and_srs_id(
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
