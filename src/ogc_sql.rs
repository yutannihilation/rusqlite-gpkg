// cf. https://www.geopackage.org/spec140/index.html#table_definition_sql

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

// Unused, but necessary
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

// Unused, but necessary
pub(crate) const SQL_GPKG_TILE_MATRIX: &str = "
CREATE TABLE gpkg_tile_matrix (
  table_name TEXT NOT NULL,
  zoom_level INTEGER NOT NULL,
  matrix_width INTEGER NOT NULL,
  matrix_height INTEGER NOT NULL,
  tile_width INTEGER NOT NULL,
  tile_height INTEGER NOT NULL,
  pixel_x_size DOUBLE NOT NULL,
  pixel_y_size DOUBLE NOT NULL,
  CONSTRAINT pk_ttm PRIMARY KEY (table_name, zoom_level),
  CONSTRAINT fk_tmm_table_name FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name)
);
";

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

// cf. https://www.geopackage.org/spec140/index.html#extension_rtree
pub(crate) fn gpkg_rtree_create_sql(table: &str, column: &str) -> String {
    format!(
        "CREATE VIRTUAL TABLE rtree_{}_{} USING rtree(id, minx, maxx, miny, maxy);",
        table, column
    )
}

pub(crate) fn gpkg_rtree_load_sql(table: &str, column: &str, id_column: &str) -> String {
    format!(
        "INSERT OR REPLACE INTO rtree_{}_{}
  SELECT {}, ST_MinX({}), ST_MaxX({}), ST_MinY({}), ST_MaxY({})
  FROM {} WHERE {} NOT NULL AND NOT ST_IsEmpty({});",
        table,
        column,
        id_column,
        column,
        column,
        column,
        column,
        table,
        column,
        column
    )
}

pub(crate) fn gpkg_rtree_triggers_sql(table: &str, column: &str, id_column: &str) -> String {
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
        c = column,
        i = id_column
    )
}
