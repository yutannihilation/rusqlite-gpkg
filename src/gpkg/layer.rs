use crate::Value;
use crate::error::{GpkgError, Result};
use crate::ogc_sql::{sql_delete_all, sql_insert_feature, sql_select_features};
use crate::types::ColumnSpec;
use geo_traits::GeometryTrait;
use rusqlite::{params_from_iter, types::Type};
use std::collections::HashMap;
use std::sync::Arc;
use wkb::reader::Wkb;

use super::{Gpkg, GpkgFeature, GpkgFeatureIterator, wkb_to_gpkg_geometry};

#[derive(Debug)]
/// A GeoPackage layer with geometry metadata and column specs.
pub struct GpkgLayer<'a> {
    pub(super) conn: &'a Gpkg,
    pub layer_name: String,
    pub geometry_column: String,
    pub primary_key_column: String,
    pub geometry_type: wkb::reader::GeometryType,
    pub geometry_dimension: wkb::reader::Dimension,
    pub srs_id: u32,
    pub property_columns: Vec<ColumnSpec>,
    pub(super) property_index_by_name: Arc<HashMap<String, usize>>,
    pub(super) insert_sql: String,
    pub(super) update_sql: String,
}

// When issueing the SELECT query, always place these columns first so that
// we don't need to find the positions every time.
const GEOMETRY_INDEX: usize = 0;
const PRIMARY_INDEX: usize = 1;

impl<'a> GpkgLayer<'a> {
    /// Iterate over features in the layer in rowid order.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
    /// let layer = gpkg.get_layer("points")?;
    /// for feature in layer.features()? {
    ///     let _id = feature.id();
    ///     let _geom = feature.geometry()?;
    /// }
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn features(&self) -> Result<GpkgFeatureIterator> {
        let columns = self.property_columns.iter().map(|spec| spec.name.as_str());

        let sql = sql_select_features(
            &self.layer_name,
            &self.geometry_column,
            &self.primary_key_column,
            columns,
        );
        let mut stmt = self.conn.connection().prepare(&sql)?;
        let features = stmt
            .query_map([], |row| {
                let mut id: Option<i64> = None;
                let mut geometry: Option<Vec<u8>> = None;
                let mut properties = Vec::with_capacity(self.property_columns.len());
                let row_len = self.property_columns.len() + 2;

                for idx in 0..row_len {
                    let value_ref = row.get_ref(idx)?;
                    let value = Value::from(value_ref);
                    let name = if idx == GEOMETRY_INDEX {
                        self.geometry_column.as_str()
                    } else if idx == PRIMARY_INDEX {
                        self.primary_key_column.as_str()
                    } else {
                        self.property_columns[idx - 2].name.as_str()
                    };

                    if idx == GEOMETRY_INDEX {
                        match value {
                            Value::Blob(bytes) => geometry = Some(bytes),
                            Value::Null => geometry = None,
                            _ => {
                                return Err(rusqlite::Error::InvalidColumnType(
                                    idx,
                                    name.to_string(),
                                    value_ref.data_type(),
                                ));
                            }
                        }
                    } else if idx == PRIMARY_INDEX {
                        match &value {
                            Value::Integer(value) => id = Some(*value),
                            _ => {
                                return Err(rusqlite::Error::InvalidColumnType(
                                    idx,
                                    name.to_string(),
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
                        self.primary_key_column.clone(),
                        Type::Null,
                    )
                })?;

                Ok(GpkgFeature {
                    id,
                    geometry,
                    properties,
                    property_index_by_name: Arc::clone(&self.property_index_by_name),
                })
            })?
            .collect::<std::result::Result<Vec<GpkgFeature>, _>>()?;

        Ok(GpkgFeatureIterator {
            features: features.into_iter(),
        })
    }

    /// Remove all rows from the layer.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open("data/example.gpkg")?;
    /// let layer = gpkg.get_layer("points")?;
    /// layer.truncate()?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn truncate(&self) -> Result<usize> {
        self.ensure_writable()?;
        let sql = sql_delete_all(&self.layer_name);
        Ok(self.conn.connection().execute(&sql, [])?)
    }

    /// Insert a feature with geometry and ordered property values.
    ///
    /// Example:
    /// ```no_run
    /// use geo_types::Point;
    /// use rusqlite_gpkg::{Gpkg, Value};
    ///
    /// let gpkg = Gpkg::open("data/example.gpkg")?;
    /// let layer = gpkg.get_layer("points")?;
    ///
    /// let properties = vec![Value::Text("alpha".to_string()), Value::Integer(1)];
    /// layer.insert(Point::new(1.0, 2.0), properties)?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn insert<G, P>(&self, geometry: G, properties: P) -> Result<()>
    where
        G: GeometryTrait<T = f64>,
        P: IntoIterator<Item = Value>,
    {
        let geom = self.geom_from_geometry(geometry)?;

        let params = std::iter::once(Value::Geometry(geom)).chain(properties.into_iter());

        let mut stmt = self.conn.connection().prepare_cached(&self.insert_sql)?;
        stmt.execute(params_from_iter(params))?;
        Ok(())
    }

    /// Update the feature with geometry and ordered property values.
    ///
    /// Example:
    /// ```no_run
    /// use geo_types::Point;
    /// use rusqlite_gpkg::{Gpkg, Value};
    ///
    /// let gpkg = Gpkg::open("data/example.gpkg")?;
    /// let layer = gpkg.get_layer("points")?;
    /// layer.update(Point::new(3.0, 4.0), vec![Value::from("beta"), Value::from(false)], 1)?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn update<G, P>(&self, geometry: G, properties: P, id: i64) -> Result<()>
    where
        G: GeometryTrait<T = f64>,
        P: IntoIterator<Item = Value>,
    {
        let geom = self.geom_from_geometry(geometry)?;

        let id_value = id;
        let params = std::iter::once(Value::Geometry(geom))
            .chain(properties.into_iter())
            .chain(std::iter::once(Value::Integer(id_value)));

        let mut stmt = self.conn.connection().prepare_cached(&self.update_sql)?;
        stmt.execute(params_from_iter(params))?;
        Ok(())
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.conn.is_read_only() {
            return Err(GpkgError::ReadOnly);
        }
        Ok(())
    }

    pub(crate) fn build_insert_sql(
        layer_name: &str,
        geometry_column: &str,
        property_columns: &[ColumnSpec],
    ) -> String {
        let mut columns = Vec::with_capacity(property_columns.len() + 1);
        columns.push(format!(r#""{}""#, geometry_column));
        columns.extend(
            property_columns
                .iter()
                .map(|spec| format!(r#""{}""#, spec.name)),
        );

        let placeholders = (1..=columns.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<String>>()
            .join(",");

        sql_insert_feature(layer_name, &columns.join(","), &placeholders)
    }

    pub(crate) fn build_update_sql(
        layer_name: &str,
        geometry_column: &str,
        primary_key_column: &str,
        property_columns: &[ColumnSpec],
    ) -> String {
        let mut column_names = Vec::with_capacity(property_columns.len() + 1);
        column_names.push(geometry_column);
        column_names.extend(property_columns.iter().map(|spec| spec.name.as_str()));

        let assignments = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| format!(r#""{}"=?{}"#, name, idx + 1))
            .collect::<Vec<String>>()
            .join(",");
        let id_idx = column_names.len() + 1;

        format!(
            r#"UPDATE "{}" SET {} WHERE "{}"=?{}"#,
            layer_name, assignments, primary_key_column, id_idx
        )
    }

    pub(crate) fn build_property_index_by_name(
        property_columns: &[ColumnSpec],
    ) -> HashMap<String, usize> {
        let mut property_index_by_name = HashMap::with_capacity(property_columns.len());
        for (idx, column) in property_columns.iter().enumerate() {
            property_index_by_name.insert(column.name.clone(), idx);
        }
        property_index_by_name
    }

    fn geom_from_geometry<G>(&self, geometry: G) -> Result<Vec<u8>>
    where
        G: GeometryTrait<T = f64>,
    {
        self.ensure_writable()?;

        let mut buf = Vec::new();
        wkb::writer::write_geometry(&mut buf, &geometry, &Default::default())?;
        let wkb = Wkb::try_new(&buf)?;
        let geom = wkb_to_gpkg_geometry(wkb, self.srs_id)?;

        Ok(geom)
    }
}

#[cfg(test)]
mod tests {
    use crate::Result;
    use crate::Value;
    use crate::conversions::geometry_type_to_str;
    use crate::gpkg::Gpkg;
    use crate::types::{ColumnSpec, ColumnType};
    use geo_traits::GeometryTrait;
    use geo_types::{
        Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point,
        Polygon,
    };
    use std::str::FromStr;
    use wkb::reader::{GeometryType, Wkb};
    use wkt::Wkt;

    fn generated_gpkg_path() -> &'static str {
        "src/test/test_generated.gpkg"
    }

    fn gpkg_blob_from_geometry<G: GeometryTrait<T = f64>>(
        geometry: G,
        srs_id: u32,
    ) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        wkb::writer::write_geometry(&mut buf, &geometry, &Default::default())?;
        let wkb = Wkb::try_new(&buf)?;
        super::super::wkb_to_gpkg_geometry(wkb, srs_id)
    }

    fn assert_geometry_roundtrip<G: GeometryTrait<T = f64> + Clone>(
        gpkg: &Gpkg,
        layer_name: &str,
        geometry_type: GeometryType,
        geometry_dimension: wkb::reader::Dimension,
        geometry: G,
    ) -> Result<()> {
        let columns: Vec<ColumnSpec> = Vec::new();
        let layer = gpkg.create_layer(
            layer_name,
            "geom",
            geometry_type,
            geometry_dimension,
            4326,
            &columns,
        )?;

        let expected_blob = gpkg_blob_from_geometry(geometry.clone(), 4326)?;
        let mut expected_wkb_bytes = Vec::new();
        wkb::writer::write_geometry(&mut expected_wkb_bytes, &geometry, &Default::default())?;
        let expected_wkb = Wkb::try_new(&expected_wkb_bytes)?;

        layer.insert(geometry, [])?;

        let geom_blob: Vec<u8> = layer.conn.connection().query_row(
            &format!(r#"SELECT "geom" FROM "{}""#, layer_name),
            [],
            |row| row.get(0),
        )?;
        assert_eq!(geom_blob, expected_blob);

        let feature = layer.features()?.next().expect("inserted feature");
        let geom = feature.geometry()?;
        assert_eq!(geom.geometry_type(), geometry_type);
        assert_eq!(geom.dimension(), geometry_dimension);
        assert_eq!(geom.buf(), expected_wkb.buf());

        Ok(())
    }

    #[test]
    fn reads_generated_layers_and_counts() -> Result<()> {
        let gpkg = Gpkg::open_read_only(generated_gpkg_path())?;
        let mut layers = gpkg.list_layers()?;
        layers.sort();
        assert_eq!(layers, vec!["lines", "points", "polygons"]);

        let points = gpkg.get_layer("points")?;
        let lines = gpkg.get_layer("lines")?;
        let polygons = gpkg.get_layer("polygons")?;

        assert_eq!(points.features()?.count(), 5);
        assert_eq!(lines.features()?.count(), 3);
        assert_eq!(polygons.features()?.count(), 2);

        Ok(())
    }

    #[test]
    fn reads_geometry_and_properties_from_points() -> Result<()> {
        let gpkg = Gpkg::open_read_only(generated_gpkg_path())?;
        let layer = gpkg.get_layer("points")?;
        let mut iter = layer.features()?;
        let feature = iter.next().expect("first feature");

        let geom = feature.geometry()?;
        assert_eq!(geom.geometry_type(), GeometryType::Point);

        assert_eq!(feature.id(), 1);
        let name: String = feature.property("name").ok_or("missing name")?.try_into()?;
        assert_eq!(name, "alpha");

        let active: bool = feature
            .property("active")
            .ok_or("missing active")?
            .try_into()?;
        assert_eq!(active, true);

        let note = feature.property("note").ok_or("missing note")?;
        assert_eq!(note, Value::Text("first".to_string()));

        Ok(())
    }

    #[test]
    fn creates_layer_metadata() -> Result<()> {
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

        gpkg.create_layer(
            "points",
            "geom",
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        let (geometry_type_name, srs_id, z, m): (String, u32, i8, i8) =
            gpkg.connection().query_row(
                "SELECT geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name = 'points'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;

        assert_eq!(
            geometry_type_name,
            geometry_type_to_str(GeometryType::Point)
        );
        assert_eq!(srs_id, 4326);
        assert_eq!(z, 0);
        assert_eq!(m, 0);

        Ok(())
    }

    #[test]
    fn inserts_and_updates_by_primary_key() -> Result<()> {
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

        let layer = gpkg.create_layer(
            "points",
            "geom",
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        let point_a = Point::new(1.0, 2.0);
        let name_a = "alpha".to_string();
        let value_a = 7_i64;
        layer.insert(point_a, [Value::from(name_a), Value::from(value_a)])?;
        let id = layer.conn.connection().last_insert_rowid();

        let point_b = Point::new(4.0, 5.0);
        let name_b = "beta".to_string();
        let value_b = 9_i64;
        layer.update(point_b, [Value::from(name_b), Value::from(value_b)], id)?;

        let (geom_blob, name, value): (Vec<u8>, String, i64) = layer.conn.connection().query_row(
            "SELECT geom, name, value FROM points WHERE fid = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;

        let expected_geom = gpkg_blob_from_geometry(Point::new(4.0, 5.0), 4326)?;
        assert_eq!(geom_blob, expected_geom);
        assert_eq!(name, "beta");
        assert_eq!(value, 9);

        Ok(())
    }

    #[test]
    fn roundtrips_all_geometry_types() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;

        let line = LineString::from(vec![(0.0, 0.0), (1.5, 1.0), (2.0, 0.5)]);
        let line_b = LineString::from(vec![(-1.0, -1.0), (-2.0, -3.0)]);
        let exterior = LineString::from(vec![
            (0.0, 0.0),
            (3.0, 0.0),
            (3.0, 3.0),
            (0.0, 3.0),
            (0.0, 0.0),
        ]);
        let polygon = Polygon::new(exterior, vec![]);
        let polygon_b = Polygon::new(
            LineString::from(vec![
                (10.0, 10.0),
                (12.0, 10.0),
                (12.0, 12.0),
                (10.0, 12.0),
                (10.0, 10.0),
            ]),
            vec![],
        );

        assert_geometry_roundtrip(
            &gpkg,
            "rt_points",
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            Point::new(1.0, 2.0),
        )?;
        assert_geometry_roundtrip(
            &gpkg,
            "rt_lines",
            GeometryType::LineString,
            wkb::reader::Dimension::Xy,
            line.clone(),
        )?;
        assert_geometry_roundtrip(
            &gpkg,
            "rt_polygons",
            GeometryType::Polygon,
            wkb::reader::Dimension::Xy,
            polygon.clone(),
        )?;
        assert_geometry_roundtrip(
            &gpkg,
            "rt_multi_points",
            GeometryType::MultiPoint,
            wkb::reader::Dimension::Xy,
            MultiPoint::from(vec![Point::new(1.0, 1.0), Point::new(2.0, 2.0)]),
        )?;
        assert_geometry_roundtrip(
            &gpkg,
            "rt_multi_lines",
            GeometryType::MultiLineString,
            wkb::reader::Dimension::Xy,
            MultiLineString::new(vec![line.clone(), line_b]),
        )?;
        assert_geometry_roundtrip(
            &gpkg,
            "rt_multi_polygons",
            GeometryType::MultiPolygon,
            wkb::reader::Dimension::Xy,
            MultiPolygon::new(vec![polygon.clone(), polygon_b]),
        )?;
        assert_geometry_roundtrip(
            &gpkg,
            "rt_geometry_collections",
            GeometryType::GeometryCollection,
            wkb::reader::Dimension::Xy,
            GeometryCollection::from(vec![
                Geometry::Point(Point::new(-1.0, -2.0)),
                Geometry::LineString(line),
                Geometry::Polygon(polygon),
            ]),
        )?;

        Ok(())
    }

    #[test]
    fn roundtrips_z_and_m_dimensions() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;

        let point_z = Wkt::from_str("POINT Z (1 2 3)")
            .map_err(|err| crate::error::GpkgError::Message(err.to_string()))?;
        let line_m = Wkt::from_str("LINESTRING M (0 0 5, 1 1 6)")
            .map_err(|err| crate::error::GpkgError::Message(err.to_string()))?;
        let polygon_zm = Wkt::from_str("POLYGON ZM ((0 0 1 10, 2 0 2 11, 2 2 3 12, 0 0 1 10))")
            .map_err(|err| crate::error::GpkgError::Message(err.to_string()))?;

        assert_geometry_roundtrip(
            &gpkg,
            "rt_point_z",
            GeometryType::Point,
            wkb::reader::Dimension::Xyz,
            point_z,
        )?;
        assert_geometry_roundtrip(
            &gpkg,
            "rt_linestring_m",
            GeometryType::LineString,
            wkb::reader::Dimension::Xym,
            line_m,
        )?;
        assert_geometry_roundtrip(
            &gpkg,
            "rt_polygon_zm",
            GeometryType::Polygon,
            wkb::reader::Dimension::Xyzm,
            polygon_zm,
        )?;

        Ok(())
    }

    #[test]
    fn rtree_updates_on_insert_update_delete() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns: Vec<ColumnSpec> = Vec::new();
        let layer = gpkg.create_layer(
            "rtree_points",
            "geom",
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        let point_a = Point::new(1.5, -2.0);
        layer.insert(point_a, [])?;
        let id = layer.conn.connection().last_insert_rowid();

        let (minx, maxx, miny, maxy): (f64, f64, f64, f64) = layer.conn.connection().query_row(
            "SELECT minx, maxx, miny, maxy FROM rtree_rtree_points_geom WHERE id = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(minx, 1.5);
        assert_eq!(maxx, 1.5);
        assert_eq!(miny, -2.0);
        assert_eq!(maxy, -2.0);

        let point_b = Point::new(-4.0, 6.25);
        layer.update(point_b, [], id)?;
        let (minx, maxx, miny, maxy): (f64, f64, f64, f64) = layer.conn.connection().query_row(
            "SELECT minx, maxx, miny, maxy FROM rtree_rtree_points_geom WHERE id = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(minx, -4.0);
        assert_eq!(maxx, -4.0);
        assert_eq!(miny, 6.25);
        assert_eq!(maxy, 6.25);

        layer.truncate()?;
        let count: i64 = layer.conn.connection().query_row(
            "SELECT COUNT(*) FROM rtree_rtree_points_geom",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 0);

        Ok(())
    }

    #[test]
    fn truncates_rows() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        }];

        let layer = gpkg.create_layer(
            "points",
            "geom",
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        let value_a = "a".to_string();
        let value_b = "b".to_string();
        layer.insert(Point::new(0.0, 0.0), [Value::from(value_a)])?;
        layer.insert(Point::new(1.0, 1.0), [Value::from(value_b)])?;

        let deleted = layer.truncate()?;
        assert_eq!(deleted, 2);

        let count: i64 =
            layer
                .conn
                .connection()
                .query_row("SELECT COUNT(*) FROM points", [], |row| row.get(0))?;
        assert_eq!(count, 0);

        Ok(())
    }

    #[test]
    fn rejects_invalid_property_count() -> Result<()> {
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

        let layer = gpkg.create_layer(
            "points",
            "geom",
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        let only = "only".to_string();
        let result = layer.insert(Point::new(0.0, 0.0), [Value::from(only)]);
        match result {
            Err(crate::GpkgError::Sql(rusqlite::Error::InvalidParameterCount(_, _))) => {}
            e => panic!("expected InvalidParameterCount error: {e:?}"),
        }

        Ok(())
    }
}
