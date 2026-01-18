// cf. https://www.geopackage.org/spec140/index.html#table_definition_sql

// gpkg_contents: lists all geospatial contents in the package with identifying
// and descriptive metadata for user display and access.
pub(crate) const SQL_GPKG_CONTENTS: &str = "
CREATE TABLE gpkg_contents (
  table_name TEXT NOT NULL PRIMARY KEY,
  data_type TEXT NOT NULL,
  identifier TEXT UNIQUE,
  description TEXT DEFAULT '',
  last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  min_x DOUBLE,
  min_y DOUBLE,
  max_x DOUBLE,
  max_y DOUBLE,
  srs_id INTEGER,
  CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
);
";

// gpkg_extensions: declares which extensions apply to the GeoPackage, a table,
// or a column so clients can detect requirements without scanning user tables.
pub(crate) const SQL_GPKG_EXTENSIONS: &str = "
CREATE TABLE gpkg_extensions (
  table_name TEXT,
  column_name TEXT,
  extension_name TEXT NOT NULL,
  definition TEXT NOT NULL,
  scope TEXT NOT NULL,
  CONSTRAINT ge_tce UNIQUE (table_name, column_name, extension_name)
);
";

pub(crate) const SQL_LIST_LAYERS: &str = "SELECT table_name FROM gpkg_contents";

pub(crate) const SQL_INSERT_GPKG_CONTENTS: &str = "
INSERT INTO gpkg_contents
  (table_name, data_type, identifier, description, srs_id)
VALUES
  (?1, 'features', ?2, '', ?3)
";

pub(crate) const SQL_INSERT_GPKG_GEOMETRY_COLUMNS: &str = "
INSERT INTO gpkg_geometry_columns
  (table_name, column_name, geometry_type_name, srs_id, z, m)
VALUES
  (?1, ?2, ?3, ?4, ?5, ?6)
";

pub(crate) const SQL_SELECT_GEOMETRY_COLUMN_META: &str = "
SELECT column_name, geometry_type_name, z, m, srs_id
FROM gpkg_geometry_columns
WHERE table_name = ?
";

pub(crate) fn sql_create_table(layer_name: &str, column_defs: &str) -> String {
    format!(r#"CREATE TABLE "{}" ({})"#, layer_name, column_defs)
}

pub(crate) fn sql_drop_table(layer_name: &str) -> String {
    format!(r#"DROP TABLE "{layer_name}""#)
}

pub(crate) fn sql_table_columns(layer_name: &str) -> String {
    format!("SELECT name, type, pk FROM pragma_table_info('{layer_name}')")
}

pub(crate) fn sql_select_features<'a, I>(
    layer_name: &'a str,
    geometry_column: &'a str,
    primary_key_column: &'a str,
    other_columns: I,
    limit: Option<u32>,
) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let joined = other_columns
        .into_iter()
        .map(|name| format!(r#""{}""#, name))
        .collect::<Vec<String>>()
        .join(", ");

    let limit_clause = match limit {
        Some(n) => format!("LIMIT {n} OFFSET ?"),
        None => "".to_string(),
    };

    let columns = if joined.is_empty() {
        format!(r#""{geometry_column}", "{primary_key_column}""#,)
    } else {
        format!(r#""{geometry_column}", "{primary_key_column}", {joined}"#,)
    };

    format!(
        r#"SELECT {columns} FROM "{layer_name}" ORDER BY "{primary_key_column}" {limit_clause}"#,
    )
}

pub(crate) fn sql_delete_all(layer_name: &str) -> String {
    format!(r#"DELETE FROM "{}""#, layer_name)
}

pub(crate) fn sql_insert_feature(layer_name: &str, columns: &str, values: &str) -> String {
    format!(
        r#"INSERT INTO "{}" ({}) VALUES ({})"#,
        layer_name, columns, values
    )
}

pub(crate) fn initialize_gpkg(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.execute_batch(SQL_GPKG_SPATIAL_REF_SYS)?;
    register_default_srs_ids(conn)?;
    conn.execute_batch(SQL_GPKG_CONTENTS)?;
    conn.execute_batch(SQL_GPKG_GEOMETRY_COLUMNS)?;
    conn.execute_batch(SQL_GPKG_TILE_MATRIX_SET)?;
    conn.execute_batch(SQL_GPKG_TILE_MATRIX)?;
    conn.execute_batch(SQL_GPKG_EXTENSIONS)?;
    Ok(())
}

// gpkg_geometry_columns: identifies geometry columns and geometry types for
// vector feature user data tables.
pub(crate) const SQL_GPKG_GEOMETRY_COLUMNS: &str = "
CREATE TABLE gpkg_geometry_columns (
  table_name TEXT NOT NULL,
  column_name TEXT NOT NULL,
  geometry_type_name TEXT NOT NULL,
  srs_id INTEGER NOT NULL,
  z TINYINT NOT NULL,
  m TINYINT NOT NULL,
  CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
  CONSTRAINT uk_gc_table_name UNIQUE (table_name),
  CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
  CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id)
);
";

// gpkg_spatial_ref_sys: the SRS catalog referenced by gpkg_contents and
// gpkg_geometry_columns to describe spatial reference systems.
pub(crate) const SQL_GPKG_SPATIAL_REF_SYS: &str = "
CREATE TABLE gpkg_spatial_ref_sys (
  srs_name TEXT NOT NULL,
  srs_id INTEGER PRIMARY KEY,
  organization TEXT NOT NULL,
  organization_coordsys_id INTEGER NOT NULL,
  definition  TEXT NOT NULL,
  description TEXT
);
";

// This is a bit horrible part. gpkg_spatial_ref_sys requires the WKT of the SRS, but we don't have a good source for this.
// Adding 4326 is easy, but what should I do to support other SRS?
fn register_default_srs_ids(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    const EPSG4326_WKT: &str = r#"GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]"#;

    let sql = "INSERT INTO gpkg_spatial_ref_sys \
            (srs_name, srs_id, organization, organization_coordsys_id, definition, description) \
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
    conn.execute(
        sql,
        rusqlite::params!["WGS 84", 4326, "EPSG", 4326, EPSG4326_WKT, "WGS 84"],
    )?;
    conn.execute(
        sql,
        rusqlite::params![
            "Undefined Cartesian SRS",
            -1,
            "NONE",
            -1,
            "undefined",
            "undefined Cartesian coordinate reference system"
        ],
    )?;
    conn.execute(
        sql,
        rusqlite::params![
            "Undefined geographic SRS",
            0,
            "NONE",
            0,
            "undefined",
            "undefined geographic coordinate reference system"
        ],
    )?;
    Ok(())
}

// gpkg_tile_matrix: documents tile pyramid structure per zoom level (tile size,
// matrix size, and pixel sizes) to support non-square tiles and varied intervals.
//
// Note that this is for raster, so not used in this crate
pub(crate) const SQL_GPKG_TILE_MATRIX: &str = "
CREATE TABLE gpkg_tile_matrix(
  table_name TEXT NOT NULL,
  zoom_level INTEGER NOT NULL,
  matrix_width INTEGER NOT NULL,
  matrix_height INTEGER NOT NULL,
  tile_width INTEGER NOT NULL,
  tile_height INTEGER NOT NULL,
  pixel_x_size DOUBLE NOT NULL,
  pixel_y_size DOUBLE NOT NULL,
  CONSTRAINT pk_ttm PRIMARY KEY(table_name, zoom_level),
  CONSTRAINT fk_tmm_table_name FOREIGN KEY(table_name) REFERENCES gpkg_contents(table_name)
);
CREATE TRIGGER 'gpkg_tile_matrix_zoom_level_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: zoom_level cannot be less than 0') WHERE (NEW.zoom_level < 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_zoom_level_update' BEFORE UPDATE of zoom_level ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: zoom_level cannot be less than 0') WHERE (NEW.zoom_level < 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_matrix_width_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: matrix_width cannot be less than 1') WHERE (NEW.matrix_width < 1); END;
CREATE TRIGGER 'gpkg_tile_matrix_matrix_width_update' BEFORE UPDATE OF matrix_width ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: matrix_width cannot be less than 1') WHERE (NEW.matrix_width < 1); END;
CREATE TRIGGER 'gpkg_tile_matrix_matrix_height_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: matrix_height cannot be less than 1') WHERE (NEW.matrix_height < 1); END;
CREATE TRIGGER 'gpkg_tile_matrix_matrix_height_update' BEFORE UPDATE OF matrix_height ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: matrix_height cannot be less than 1') WHERE (NEW.matrix_height < 1); END;
CREATE TRIGGER 'gpkg_tile_matrix_pixel_x_size_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: pixel_x_size must be greater than 0') WHERE NOT (NEW.pixel_x_size > 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_pixel_x_size_update' BEFORE UPDATE OF pixel_x_size ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: pixel_x_size must be greater than 0') WHERE NOT (NEW.pixel_x_size > 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_pixel_y_size_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: pixel_y_size must be greater than 0') WHERE NOT (NEW.pixel_y_size > 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_pixel_y_size_update' BEFORE UPDATE OF pixel_y_size ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: pixel_y_size must be greater than 0') WHERE NOT (NEW.pixel_y_size > 0); END;
";

// gpkg_tile_matrix_set: defines SRS and overall bounds for all tiles in a tile
// pyramid user data table.
//
// Note that this is for raster, so not used in this crate
pub(crate) const SQL_GPKG_TILE_MATRIX_SET: &str = "
CREATE TABLE gpkg_tile_matrix_set (
  table_name TEXT NOT NULL PRIMARY KEY,
  srs_id INTEGER NOT NULL,
  min_x DOUBLE NOT NULL,
  min_y DOUBLE NOT NULL,
  max_x DOUBLE NOT NULL,
  max_y DOUBLE NOT NULL,
  CONSTRAINT fk_gtms_table_name FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
  CONSTRAINT fk_gtms_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id)
);
";

// cf. https://www.geopackage.org/spec140/index.html#extension_rtree
pub(crate) fn gpkg_rtree_create_sql(table: &str, geom_column: &str) -> String {
    format!(
        "CREATE VIRTUAL TABLE rtree_{t}_{c} USING rtree(id, minx, maxx, miny, maxy);",
        t = table,
        c = geom_column,
    )
}

pub(crate) fn gpkg_rtree_drop_sql(table: &str, geom_column: &str) -> String {
    format!(
        "DROP TABLE rtree_{t}_{c} USING rtree(id, minx, maxx, miny, maxy);",
        t = table,
        c = geom_column,
    )
}

pub(crate) fn gpkg_rtree_load_sql(table: &str, geom_column: &str, id_column: &str) -> String {
    format!(
        "INSERT OR REPLACE INTO rtree_{t}_{c}
  SELECT {i}, ST_MinX({c}), ST_MaxX({c}), ST_MinY({c}), ST_MaxY({c})
  FROM {t} WHERE {c} NOT NULL AND NOT ST_IsEmpty({c});",
        t = table,
        c = geom_column,
        i = id_column
    )
}

pub(crate) fn gpkg_rtree_triggers_sql(table: &str, geom_column: &str, id_column: &str) -> String {
    format!(
        "CREATE TRIGGER rtree_{t}_{c}_insert AFTER INSERT ON {t}
  WHEN (new.{c} NOT NULL AND NOT ST_IsEmpty(NEW.{c}))
BEGIN
  INSERT OR REPLACE INTO rtree_{t}_{c} VALUES (
    NEW.{i},
    ST_MinX(NEW.{c}), ST_MaxX(NEW.{c}),
    ST_MinY(NEW.{c}), ST_MaxY(NEW.{c})
  );
END;

CREATE TRIGGER rtree_{t}_{c}_update2 AFTER UPDATE OF {c} ON {t}
  WHEN OLD.{i} = NEW.{i} AND
       (NEW.{c} ISNULL OR ST_IsEmpty(NEW.{c}))
BEGIN
  DELETE FROM rtree_{t}_{c} WHERE id = OLD.{i};
END;

CREATE TRIGGER rtree_{t}_{c}_update4 AFTER UPDATE ON {t}
  WHEN OLD.{i} != NEW.{i} AND
       (NEW.{c} ISNULL OR ST_IsEmpty(NEW.{c}))
BEGIN
  DELETE FROM rtree_{t}_{c} WHERE id IN (OLD.{i}, NEW.{i});
END;

CREATE TRIGGER rtree_{t}_{c}_update5 AFTER UPDATE ON {t}
  WHEN OLD.{i} != NEW.{i} AND
       (NEW.{c} NOTNULL AND NOT ST_IsEmpty(NEW.{c}))
BEGIN
  DELETE FROM rtree_{t}_{c} WHERE id = OLD.{i};
  INSERT OR REPLACE INTO rtree_{t}_{c} VALUES (
    NEW.{i},
    ST_MinX(NEW.{c}), ST_MaxX(NEW.{c}),
    ST_MinY(NEW.{c}), ST_MaxY(NEW.{c})
  );
END;

CREATE TRIGGER rtree_{t}_{c}_update6 AFTER UPDATE OF {c} ON {t}
  WHEN OLD.{i} = NEW.{i} AND
       (NEW.{c} NOTNULL AND NOT ST_IsEmpty(NEW.{c})) AND
       (OLD.{c} NOTNULL AND NOT ST_IsEmpty(OLD.{c}))
BEGIN
  UPDATE rtree_{t}_{c} SET
    minx = ST_MinX(NEW.{c}),
    maxx = ST_MaxX(NEW.{c}),
    miny = ST_MinY(NEW.{c}),
    maxy = ST_MaxY(NEW.{c})
  WHERE id = NEW.{i};
END;

CREATE TRIGGER rtree_{t}_{c}_update7 AFTER UPDATE OF {c} ON {t}
  WHEN OLD.{i} = NEW.{i} AND
       (NEW.{c} NOTNULL AND NOT ST_IsEmpty(NEW.{c})) AND
       (OLD.{c} ISNULL OR ST_IsEmpty(OLD.{c}))
BEGIN
  INSERT INTO rtree_{t}_{c} VALUES (
    NEW.{i},
    ST_MinX(NEW.{c}), ST_MaxX(NEW.{c}),
    ST_MinY(NEW.{c}), ST_MaxY(NEW.{c})
  );
END;

CREATE TRIGGER rtree_{t}_{c}_delete AFTER DELETE ON {t}
  WHEN old.{c} NOT NULL
BEGIN
  DELETE FROM rtree_{t}_{c} WHERE id = OLD.{i};
END;",
        t = table,
        c = geom_column,
        i = id_column
    )
}

pub(crate) fn execute_rtree_sqls(
    conn: &rusqlite::Connection,
    table: &str,
    geom_column: &str,
    id_column: &str,
) -> rusqlite::Result<()> {
    conn.execute_batch(&gpkg_rtree_create_sql(table, geom_column))?;
    conn.execute_batch(&gpkg_rtree_load_sql(table, geom_column, id_column))?;
    conn.execute_batch(&gpkg_rtree_triggers_sql(table, geom_column, id_column))?;
    Ok(())
}
