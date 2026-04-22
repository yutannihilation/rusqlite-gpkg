use crate::Value;
use crate::error::{GpkgError, Result};
use crate::ogc_sql::{sql_delete_all, sql_insert_feature, sql_select_attribute_rows};
use crate::types::ColumnSpec;
use rusqlite::types::Type;
use std::collections::HashMap;
use std::rc::Rc;

use super::attribute_row::GpkgAttributeRow;

#[derive(Debug)]
/// A GeoPackage attribute table (non-spatial, no geometry column).
pub struct GpkgAttributeTable {
    pub(super) conn: Rc<rusqlite::Connection>,
    pub(super) is_read_only: bool,
    pub table_name: String,
    pub primary_key_column: String,
    pub property_columns: Vec<ColumnSpec>,
    pub(super) property_index_by_name: Rc<HashMap<String, usize>>,
    pub(super) insert_sql: String,
    pub(super) update_sql: String,
}

impl GpkgAttributeTable {
    /// Return all rows in the table.
    pub fn rows(&self) -> Result<Vec<GpkgAttributeRow>> {
        let columns = self.property_columns.iter().map(|spec| spec.name.as_str());
        let sql =
            sql_select_attribute_rows(&self.table_name, &self.primary_key_column, columns, None);

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], |row| {
                row_to_attribute_row(
                    row,
                    &self.property_columns,
                    &self.primary_key_column,
                    &self.property_index_by_name,
                )
            })?
            .collect::<rusqlite::Result<Vec<GpkgAttributeRow>>>()?;

        Ok(rows)
    }

    /// Remove all rows from the table.
    pub fn truncate(&self) -> Result<usize> {
        self.ensure_writable()?;
        let sql = sql_delete_all(&self.table_name);
        Ok(self.conn.execute(&sql, [])?)
    }

    /// Insert a row with ordered property values (no geometry).
    pub fn insert<'p, P>(&self, properties: P) -> Result<()>
    where
        P: IntoIterator<Item = &'p Value>,
    {
        let properties: Vec<&Value> = properties.into_iter().collect();
        let expected = self.property_columns.len();
        let got = properties.len();
        if expected != got {
            return Err(GpkgError::InvalidPropertyCount { expected, got });
        }

        self.ensure_writable()?;

        let params = params_from_properties(properties, None);
        let mut stmt = self.conn.prepare_cached(&self.insert_sql)?;
        stmt.execute(params)?;
        Ok(())
    }

    /// Update the row with the given primary key.
    pub fn update<'p, P>(&self, properties: P, id: i64) -> Result<()>
    where
        P: IntoIterator<Item = &'p Value>,
    {
        let properties: Vec<&Value> = properties.into_iter().collect();
        let expected = self.property_columns.len();
        let got = properties.len();
        if expected != got {
            return Err(GpkgError::InvalidPropertyCount { expected, got });
        }

        self.ensure_writable()?;

        let params = params_from_properties(properties, Some(id));
        let mut stmt = self.conn.prepare_cached(&self.update_sql)?;
        stmt.execute(params)?;
        Ok(())
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.is_read_only {
            return Err(GpkgError::ReadOnly);
        }
        Ok(())
    }

    pub(crate) fn build_insert_sql(table_name: &str, property_columns: &[ColumnSpec]) -> String {
        if property_columns.is_empty() {
            return format!(r#"INSERT INTO "{}" DEFAULT VALUES"#, table_name);
        }

        let columns: Vec<String> = property_columns
            .iter()
            .map(|spec| format!(r#""{}""#, spec.name))
            .collect();

        let placeholders = (1..=columns.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<String>>()
            .join(",");

        sql_insert_feature(table_name, &columns.join(","), &placeholders)
    }

    pub(crate) fn build_update_sql(
        table_name: &str,
        primary_key_column: &str,
        property_columns: &[ColumnSpec],
    ) -> String {
        if property_columns.is_empty() {
            // No columns to update; use a WHERE-only statement that touches nothing.
            return format!(
                r#"SELECT 1 FROM "{}" WHERE "{}"=?1"#,
                table_name, primary_key_column
            );
        }

        let assignments = property_columns
            .iter()
            .enumerate()
            .map(|(idx, spec)| format!(r#""{}"=?{}"#, spec.name, idx + 1))
            .collect::<Vec<String>>()
            .join(",");
        let id_idx = property_columns.len() + 1;

        format!(
            r#"UPDATE "{}" SET {} WHERE "{}"=?{}"#,
            table_name, assignments, primary_key_column, id_idx
        )
    }

    pub(crate) fn build_property_index_by_name(
        property_columns: &[ColumnSpec],
    ) -> HashMap<String, usize> {
        let mut map = HashMap::with_capacity(property_columns.len());
        for (idx, column) in property_columns.iter().enumerate() {
            map.insert(column.name.clone(), idx);
        }
        map
    }
}

const PRIMARY_INDEX: usize = 0;

fn row_to_attribute_row(
    row: &rusqlite::Row<'_>,
    property_columns: &[ColumnSpec],
    primary_key_column: &str,
    property_index_by_name: &Rc<HashMap<String, usize>>,
) -> std::result::Result<GpkgAttributeRow, rusqlite::Error> {
    let mut id: Option<i64> = None;
    let mut properties = Vec::with_capacity(property_columns.len());
    let row_len = property_columns.len() + 1;

    for idx in 0..row_len {
        let value_ref = row.get_ref(idx)?;
        let value = Value::from(value_ref);

        if idx == PRIMARY_INDEX {
            match &value {
                Value::Integer(value) => id = Some(*value),
                _ => {
                    return Err(rusqlite::Error::InvalidColumnType(
                        idx,
                        primary_key_column.to_string(),
                        value_ref.data_type(),
                    ));
                }
            }
        } else {
            properties.push(value);
        }
    }

    let id = id.ok_or_else(|| {
        rusqlite::Error::InvalidColumnType(
            PRIMARY_INDEX,
            primary_key_column.to_string(),
            Type::Null,
        )
    })?;

    Ok(GpkgAttributeRow {
        id,
        properties,
        property_index_by_name: property_index_by_name.clone(),
    })
}

fn params_from_properties<'p, P>(properties: P, id: Option<i64>) -> impl rusqlite::Params
where
    P: IntoIterator<Item = &'p Value>,
{
    let params = properties
        .into_iter()
        .map(SqlParam::Borrowed)
        .chain(id.into_iter().map(|i| SqlParam::Owned(Value::Integer(i))));
    rusqlite::params_from_iter(params)
}

enum SqlParam<'a> {
    Owned(Value),
    Borrowed(&'a Value),
}

impl<'a> rusqlite::ToSql for SqlParam<'a> {
    #[inline]
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            SqlParam::Owned(value) => value.to_sql(),
            SqlParam::Borrowed(value) => value.to_sql(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::GpkgError;
    use crate::Result;
    use crate::gpkg::Gpkg;
    use crate::params;
    use crate::types::{ColumnSpec, ColumnType};

    #[test]
    fn create_and_read_attribute_table() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
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

        let table = gpkg.create_attribute_table("observations", &columns)?;
        table.insert(params!["alpha", 7_i64])?;
        table.insert(params!["beta", 9_i64])?;

        let rows = table.rows()?;
        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].id(), 1);
        let name: String = rows[0].property("name").unwrap().try_into()?;
        assert_eq!(name, "alpha");
        let value: i64 = rows[0].property("value").unwrap().try_into()?;
        assert_eq!(value, 7);

        assert_eq!(rows[1].id(), 2);
        let name: String = rows[1].property("name").unwrap().try_into()?;
        assert_eq!(name, "beta");
        let value: i64 = rows[1].property("value").unwrap().try_into()?;
        assert_eq!(value, 9);

        Ok(())
    }

    #[test]
    fn attribute_table_metadata_in_gpkg_contents() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        }];

        gpkg.create_attribute_table("observations", &columns)?;

        // Verify gpkg_contents has data_type = 'attributes' and NULL srs_id
        let (data_type, srs_id): (String, Option<i32>) = gpkg.conn.query_row(
            "SELECT data_type, srs_id FROM gpkg_contents WHERE table_name = 'observations'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(data_type, "attributes");
        assert_eq!(srs_id, None);

        // Verify NO entry in gpkg_geometry_columns
        let count: i64 = gpkg.conn.query_row(
            "SELECT COUNT(*) FROM gpkg_geometry_columns WHERE table_name = 'observations'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 0);

        Ok(())
    }

    #[test]
    fn list_attribute_tables_returns_only_attributes() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;

        // Create a feature layer
        gpkg.create_layer(
            "points",
            "geom",
            wkb::reader::GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &[],
        )?;

        // Create an attribute table
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        }];
        gpkg.create_attribute_table("observations", &columns)?;

        // list_layers returns only feature layers
        let layers = gpkg.list_layers()?;
        assert_eq!(layers, vec!["points"]);

        // list_attribute_tables returns only attribute tables
        let attr_tables = gpkg.list_attribute_tables()?;
        assert_eq!(attr_tables, vec!["observations"]);

        Ok(())
    }

    #[test]
    fn get_layer_rejects_attribute_table() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        }];
        gpkg.create_attribute_table("observations", &columns)?;

        let err = gpkg
            .get_layer("observations")
            .expect_err("should fail for attribute table");
        assert!(matches!(err, GpkgError::NotAFeatureLayer { .. }));

        Ok(())
    }

    #[test]
    fn get_attribute_table_rejects_feature_layer() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        gpkg.create_layer(
            "points",
            "geom",
            wkb::reader::GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &[],
        )?;

        let err = gpkg
            .get_attribute_table("points")
            .expect_err("should fail for feature layer");
        assert!(matches!(err, GpkgError::NotAnAttributeTable { .. }));

        Ok(())
    }

    #[test]
    fn insert_and_update_attribute_row() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
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

        let table = gpkg.create_attribute_table("observations", &columns)?;
        table.insert(params!["alpha", 7_i64])?;
        let id = table.conn.last_insert_rowid();

        table.update(params!["beta", 9_i64], id)?;

        let (name, value): (String, i64) = table.conn.query_row(
            "SELECT name, value FROM observations WHERE fid = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(name, "beta");
        assert_eq!(value, 9);

        Ok(())
    }

    #[test]
    fn truncate_attribute_table() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        }];

        let table = gpkg.create_attribute_table("observations", &columns)?;
        let a = "a".to_string();
        let b = "b".to_string();
        table.insert(params![a])?;
        table.insert(params![b])?;

        let deleted = table.truncate()?;
        assert_eq!(deleted, 2);

        let count: i64 = table
            .conn
            .query_row("SELECT COUNT(*) FROM observations", [], |row| row.get(0))?;
        assert_eq!(count, 0);

        Ok(())
    }

    #[test]
    fn delete_attribute_table() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        }];
        gpkg.create_attribute_table("observations", &columns)?;

        gpkg.delete_attribute_table("observations")?;

        let tables = gpkg.list_attribute_tables()?;
        assert!(tables.is_empty());

        Ok(())
    }

    #[test]
    fn rejects_invalid_property_count() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns = vec![
            ColumnSpec {
                name: "a".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnSpec {
                name: "b".to_string(),
                column_type: ColumnType::Integer,
            },
        ];

        let table = gpkg.create_attribute_table("test", &columns)?;
        let only = "only".to_string();
        let result = table.insert(params![only]);
        match result {
            Err(GpkgError::InvalidPropertyCount {
                expected: 2,
                got: 1,
            }) => {}
            e => panic!("expected InvalidPropertyCount error: {e:?}"),
        }

        Ok(())
    }

    #[test]
    fn nullable_properties() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns = vec![
            ColumnSpec {
                name: "a".to_string(),
                column_type: ColumnType::Double,
            },
            ColumnSpec {
                name: "b".to_string(),
                column_type: ColumnType::Integer,
            },
        ];

        let table = gpkg.create_attribute_table("nullable_test", &columns)?;
        table.insert(params![Some(1.0_f64), Option::<i64>::None])?;

        let rows = table.rows()?;
        assert_eq!(rows.len(), 1);

        let a: Option<f64> = rows[0].property("a").unwrap().try_into()?;
        assert_eq!(a, Some(1.0));

        let b: Option<i64> = rows[0].property("b").unwrap().try_into()?;
        assert_eq!(b, None);

        Ok(())
    }

    #[test]
    fn get_attribute_table_roundtrip() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
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

        let table = gpkg.create_attribute_table("observations", &columns)?;
        table.insert(params!["alpha", 7_i64])?;
        drop(table);

        // Re-open the table via get_attribute_table
        let table = gpkg.get_attribute_table("observations")?;
        assert_eq!(table.table_name, "observations");
        assert_eq!(table.property_columns.len(), 2);
        assert_eq!(table.property_columns[0].name, "name");
        assert_eq!(table.property_columns[1].name, "value");

        let rows = table.rows()?;
        assert_eq!(rows.len(), 1);
        let name: String = rows[0].property("name").unwrap().try_into()?;
        assert_eq!(name, "alpha");

        Ok(())
    }

    #[test]
    fn duplicate_name_across_features_and_attributes() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;

        gpkg.create_layer(
            "shared_name",
            "geom",
            wkb::reader::GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &[],
        )?;

        let err = gpkg
            .create_attribute_table("shared_name", &[])
            .expect_err("duplicate name should fail");
        assert!(matches!(err, GpkgError::LayerAlreadyExists { .. }));

        Ok(())
    }
}
