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
use std::sync::Arc;

use super::layer::GpkgLayer;

#[derive(Debug)]
/// GeoPackage connection wrapper for reading (and later writing) layers.
pub struct Gpkg {
    conn: rusqlite::Connection,
    read_only: bool,
}

impl Gpkg {
    /// Open a GeoPackage in read-only mode.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = rusqlite::Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        register_spatial_functions(&conn)?;
        Ok(Self {
            conn,
            read_only: true,
        })
    }

    /// Open a GeoPackage in read-write mode.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open("data/example.gpkg")?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
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

    /// Create a new GeoPackage.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::new("data/new.gpkg")?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
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

    /// Create a new GeoPackage in memory.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::new_in_memory()?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn new_in_memory() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;

        initialize_gpkg(&conn)?;
        register_spatial_functions(&conn)?;

        Ok(Self {
            conn,
            read_only: false,
        })
    }

    /// Expert-only: register a spatial reference system in gpkg_spatial_ref_sys.
    ///
    /// GeoPackage layers must reference a valid `srs_id` that already exists in
    /// `gpkg_spatial_ref_sys`. The GeoPackage spec requires a full SRS definition
    /// (notably the WKT `definition` and descriptive metadata). In practice, this
    /// data is often sourced from an external authority like EPSG, but this crate
    /// does not bundle or generate that catalog. As a result, callers must insert
    /// SRS entries themselves before creating layers, which is why this low-level
    /// helper exists.
    ///
    /// This method performs a direct insert with all required columns and does
    /// no validation of the WKT or authority fields. Use only if you understand
    /// the GeoPackage SRS requirements and have authoritative metadata.
    ///
    /// Example: register EPSG:3857 (Web Mercator / Pseudo-Mercator).
    /// ```no_run
    /// # use rusqlite_gpkg::Gpkg;
    /// let gpkg = Gpkg::new_in_memory().expect("new gpkg");
    /// let definition = r#"PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs"],AUTHORITY["EPSG","3857"]]"#;
    /// gpkg.register_srs(
    ///     "WGS 84 / Pseudo-Mercator",
    ///     3857,
    ///     "EPSG",
    ///     3857,
    ///     definition,
    ///     "Web Mercator / Pseudo-Mercator (EPSG:3857)",
    /// ).expect("register srs");
    /// ```
    pub fn register_srs(
        &self,
        srs_name: &str,
        srs_id: i32,
        organization: &str,
        organization_coordsys_id: i32,
        definition: &str,
        description: &str,
    ) -> Result<()> {
        if self.read_only {
            return Err(GpkgError::ReadOnly);
        }

        self.conn.execute(
            "INSERT INTO gpkg_spatial_ref_sys \
            (srs_name, srs_id, organization, organization_coordsys_id, definition, description) \
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                srs_name,
                srs_id,
                organization,
                organization_coordsys_id,
                definition,
                description
            ],
        )?;
        Ok(())
    }

    /// List the names of the layers.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
    /// let layers = gpkg.list_layers()?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn list_layers(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(SQL_LIST_LAYERS)?;
        let layers = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(layers)
    }

    /// Load a layer definition and metadata by name.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
    /// let layer = gpkg.open_layer("points")?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn open_layer<'a>(&'a self, layer_name: &str) -> Result<GpkgLayer<'a>> {
        let (geometry_column, geometry_type, geometry_dimension, srs_id) =
            self.get_geometry_column_and_srs_id(layer_name)?;
        let column_specs = self.get_column_specs(layer_name, &geometry_column)?;
        let primary_key_column = column_specs.primary_key_column.clone();
        let other_columns = column_specs.other_columns;

        let insert_sql = GpkgLayer::build_insert_sql(layer_name, &geometry_column, &other_columns);
        let update_sql = GpkgLayer::build_update_sql(
            layer_name,
            &geometry_column,
            &primary_key_column,
            &other_columns,
        );
        let property_index_by_name =
            Arc::new(GpkgLayer::build_property_index_by_name(&other_columns));

        Ok(GpkgLayer {
            conn: self,
            layer_name: layer_name.to_string(),
            geometry_column,
            primary_key_column,
            geometry_type,
            geometry_dimension,
            srs_id,
            property_columns: other_columns,
            property_index_by_name,
            insert_sql,
            update_sql,
        })
    }

    // Create a new layer.
    ///
    /// Example:
    /// ```no_run
    /// use geo_types::Point;
    /// use rusqlite_gpkg::{ColumnSpec, ColumnType, Gpkg};
    ///
    /// let gpkg = Gpkg::new_in_memory()?;
    /// let columns = vec![ColumnSpec {
    ///     name: "name".to_string(),
    ///     column_type: ColumnType::Varchar,
    /// }];
    /// let layer = gpkg.new_layer(
    ///     "points",
    ///     "geom".to_string(),
    ///     wkb::reader::GeometryType::Point,
    ///     wkb::reader::Dimension::Xy,
    ///     4326,
    ///     &columns,
    /// )?;
    /// layer.insert(Point::new(1.0, 2.0), &[&"alpha"])?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
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

        let srs_exists: i64 = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM gpkg_spatial_ref_sys WHERE srs_id = ?1)",
            rusqlite::params![srs_id],
            |row| row.get(0),
        )?;
        if srs_exists == 0 {
            return Err(GpkgError::Message(format!(
                "srs_id {srs_id} not found in gpkg_spatial_ref_sys"
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

        let insert_sql =
            GpkgLayer::build_insert_sql(layer_name, &geometry_column, other_column_specs);
        let update_sql =
            GpkgLayer::build_update_sql(layer_name, &geometry_column, "fid", other_column_specs);
        let property_index_by_name =
            Arc::new(GpkgLayer::build_property_index_by_name(other_column_specs));

        Ok(GpkgLayer {
            conn: self,
            layer_name: layer_name.to_string(),
            geometry_column,
            primary_key_column: "fid".to_string(),
            geometry_type,
            geometry_dimension,
            srs_id,
            property_columns: other_column_specs.to_vec(),
            property_index_by_name,
            insert_sql,
            update_sql,
        })
    }

    /// Delete a layer.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open("data/example.gpkg")?;
    /// gpkg.delete_layer("points")?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
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

    /// Dump the GeoPackage data to `Vec<u8>`.
    ///
    /// This is intended for environments without filesystem access (for example,
    /// running in a web browser). You can serialize an in-memory GeoPackage and
    /// move the bytes over the wire or store them in browser storage.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::new_in_memory()?;
    /// let bytes = gpkg.to_bytes()?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let data: &[u8] = &self.conn.serialize("main")?;
        Ok(data.to_vec())
    }

    /// Load the GeoPackage data from a dump.
    ///
    /// This is intended for environments without filesystem access (for example,
    /// running in a web browser). Provide the bytes from `Gpkg::to_bytes()` to
    /// recreate an in-memory GeoPackage.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::new_in_memory()?;
    /// let bytes = gpkg.to_bytes()?;
    /// let restored = Gpkg::from_bytes(&bytes)?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn from_bytes<D: AsRef<[u8]>>(data: D) -> Result<Self> {
        let mut conn = rusqlite::Connection::open_in_memory()?;

        let data_ref = data.as_ref();
        let reader = std::io::Cursor::new(data_ref);
        conn.deserialize_read_exact("main", reader, data_ref.len(), false)?;
        Ok(Self {
            conn,
            read_only: false,
        })
    }

    pub(crate) fn connection(&self) -> &rusqlite::Connection {
        &self.conn
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Resolve the table columns and map SQLite types.
    pub(crate) fn get_column_specs(
        &self,
        layer_name: &str,
        geometry_column: &str,
    ) -> Result<ColumnSpecs> {
        let query = sql_table_columns(layer_name);
        let mut stmt = self.conn.prepare(&query)?;

        let mut primary_key_column: Option<String> = None;
        let mut geometry_column_name: Option<String> = None;
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
                if primary_key_column.is_some() {
                    return Err(GpkgError::Message(format!(
                        "Composite primary keys are not supported yet for layer: {layer_name}"
                    )));
                }
                primary_key_column = Some(name.clone());
                continue;
            }
            if name == geometry_column {
                geometry_column_name = Some(name.clone());
            } else {
                other_columns.push(ColumnSpec { name, column_type });
            }
        }

        let primary_key_column = primary_key_column.ok_or_else(|| {
            GpkgError::Message(format!(
                "No primary key column found for layer: {layer_name}"
            ))
        })?;

        let geometry_column = geometry_column_name.ok_or_else(|| {
            GpkgError::Message(format!("No geometry column found for layer: {layer_name}"))
        })?;

        Ok(ColumnSpecs {
            primary_key_column,
            geometry_column,
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

#[cfg(test)]
mod tests {
    use super::Gpkg;
    use crate::error::GpkgError;
    use crate::types::{ColumnSpec, ColumnType};
    use geo_types::Point;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
    use wkb::reader::{Dimension, GeometryType};

    #[test]
    fn new_layer_requires_existing_srs() {
        let gpkg = Gpkg::new_in_memory().expect("new gpkg");
        let columns: Vec<ColumnSpec> = Vec::new();
        let err = gpkg
            .new_layer(
                "missing_srs",
                "geom".to_string(),
                wkb::reader::GeometryType::Point,
                wkb::reader::Dimension::Xy,
                9999,
                &columns,
            )
            .expect_err("missing srs should fail");

        match err {
            GpkgError::Message(message) => {
                assert!(message.contains("srs_id 9999"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
    #[test]
    fn new_fails_if_file_exists() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        path.push(format!("rusqlite_gpkg_exists_{nanos}.gpkg"));

        fs::write(&path, []).expect("create temp file");
        let err = Gpkg::new(&path).expect_err("existing file should fail");
        match err {
            GpkgError::Message(message) => {
                assert!(message.contains("already exists"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn open_fails_if_missing_file() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        path.push(format!("rusqlite_gpkg_missing_{nanos}.gpkg"));

        let err = Gpkg::open(&path).expect_err("missing file should fail");
        match err {
            GpkgError::Message(message) => {
                assert!(message.contains("does not exist"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn delete_layer_rejects_read_only() {
        let gpkg =
            Gpkg::open_read_only("src/test/test_generated.gpkg").expect("open read-only gpkg");
        let err = gpkg
            .delete_layer("points")
            .expect_err("read-only should fail");
        assert!(matches!(err, GpkgError::ReadOnly));
    }

    #[test]
    fn dump_roundtrips_in_memory_gpkg() -> Result<(), GpkgError> {
        let gpkg = Gpkg::new_in_memory()?;

        let columns = vec![
            ColumnSpec {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnSpec {
                name: "value".to_string(),
                column_type: ColumnType::Integer,
            },
        ];
        let layer = gpkg.new_layer(
            "points",
            "geom".to_string(),
            GeometryType::Point,
            Dimension::Xy,
            4326,
            &columns,
        )?;

        let name_a = "alpha".to_string();
        let value_a = 7_i64;
        layer.insert(Point::new(1.0, 2.0), &[&name_a, &value_a])?;
        let name_b = "beta".to_string();
        let value_b = 9_i64;
        layer.insert(Point::new(-3.0, 4.5), &[&name_b, &value_b])?;

        let dump = gpkg.to_bytes()?;
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        path.push(format!("rusqlite_gpkg_dump_{nanos}.gpkg"));
        fs::write(&path, dump).unwrap();

        let reopened = Gpkg::open_read_only(&path)?;
        let layers = reopened.list_layers()?;
        assert_eq!(layers, vec!["points".to_string()]);

        let reopened_layer = reopened.open_layer("points")?;
        let features = reopened_layer.features()?;
        let collected: Vec<_> = features.collect();
        assert_eq!(collected.len(), 2);

        assert_eq!(collected[0].id(), 1);
        assert_eq!(collected[0].property::<String>("name")?, "alpha");
        assert_eq!(collected[0].property::<i64>("value")?, 7);
        assert_eq!(
            collected[0].geometry()?.geometry_type(),
            GeometryType::Point
        );

        assert_eq!(collected[1].id(), 2);
        assert_eq!(collected[1].property::<String>("name")?, "beta");
        assert_eq!(collected[1].property::<i64>("value")?, 9);
        assert_eq!(
            collected[1].geometry()?.geometry_type(),
            GeometryType::Point
        );

        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn dump_roundtrips_in_memory_gpkg_from_bytes() -> Result<(), GpkgError> {
        let gpkg = Gpkg::new_in_memory()?;

        let columns = vec![
            ColumnSpec {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnSpec {
                name: "value".to_string(),
                column_type: ColumnType::Integer,
            },
        ];
        let layer = gpkg.new_layer(
            "points",
            "geom".to_string(),
            GeometryType::Point,
            Dimension::Xy,
            4326,
            &columns,
        )?;

        let name_a = "alpha".to_string();
        let value_a = 7_i64;
        layer.insert(Point::new(1.0, 2.0), &[&name_a, &value_a])?;
        let name_b = "beta".to_string();
        let value_b = 9_i64;
        layer.insert(Point::new(-3.0, 4.5), &[&name_b, &value_b])?;

        let dump = gpkg.to_bytes()?;

        let restored = Gpkg::from_bytes(&dump)?;

        let layers = restored.list_layers()?;
        assert_eq!(layers, vec!["points".to_string()]);

        let restored_layer = restored.open_layer("points")?;
        let features = restored_layer.features()?;
        let collected: Vec<_> = features.collect();
        assert_eq!(collected.len(), 2);

        assert_eq!(collected[0].id(), 1);
        assert_eq!(collected[0].property::<String>("name")?, "alpha");
        assert_eq!(collected[0].property::<i64>("value")?, 7);
        assert_eq!(
            collected[0].geometry()?.geometry_type(),
            GeometryType::Point
        );

        assert_eq!(collected[1].id(), 2);
        assert_eq!(collected[1].property::<String>("name")?, "beta");
        assert_eq!(collected[1].property::<i64>("value")?, 9);
        assert_eq!(
            collected[1].geometry()?.geometry_type(),
            GeometryType::Point
        );

        Ok(())
    }
}
