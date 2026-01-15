use crate::error::{GpkgError, Result};
use crate::ogc_sql::{sql_delete_all, sql_insert_feature, sql_select_features};
use crate::types::{ColumnSpec, RusqliteValues};
use geo_traits::GeometryTrait;
use rusqlite::{
    params_from_iter,
    types::{Type, Value},
};
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
}

impl<'a> GpkgLayer<'a> {
    /// Iterate over features in the layer in rowid order.
    pub fn features(&self) -> Result<GpkgFeatureIterator> {
        let column_specs = self
            .conn
            .get_column_specs(&self.layer_name, &self.geometry_column)?;
        let primary_index = column_specs
            .other_columns
            .iter()
            .position(|spec| spec.name == self.primary_key_column)
            .ok_or_else(|| rusqlite::Error::InvalidColumnName(self.primary_key_column.clone()))?;
        let geometry_index = 0usize;
        let primary_index = primary_index + 1;
        let columns = std::iter::once(column_specs.geometry_column.name.as_str())
            .chain(column_specs.other_columns.iter().map(|spec| spec.name.as_str()))
            .map(|name| format!(r#""{}""#, name))
            .collect::<Vec<String>>()
            .join(",");
        let sql = sql_select_features(&self.layer_name, &columns);
        let mut stmt = self.conn.connection().prepare(&sql)?;
        let features = stmt
            .query_map([], |row| {
                let mut id: Option<i64> = None;
                let mut geometry: Option<Vec<u8>> = None;
                let mut properties = Vec::with_capacity(column_specs.other_columns.len());

                for idx in 0..=column_specs.other_columns.len() {
                    let value_ref = row.get_ref(idx)?;
                    let value = Value::from(value_ref);
                    let spec = if idx == geometry_index {
                        &column_specs.geometry_column
                    } else {
                        &column_specs.other_columns[idx - 1]
                    };

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
        Ok(self.conn.connection().execute(&sql, [])?)
    }

    /// Insert a feature with geometry and ordered property values.
    pub fn insert<G, P>(&self, geometry: G, params: P) -> Result<()>
    where
        G: GeometryTrait<T = f64>,
        P: RusqliteValues,
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

        let mut stmt = self.conn.connection().prepare(&sql)?;
        stmt.execute(params_from_iter(values))?;
        Ok(())
    }

    /// Update the feature with geometry and ordered property values.
    pub fn update<G, P>(&self, geometry: G, params: P, id: i64) -> Result<()>
    where
        G: GeometryTrait<T = f64>,
        P: RusqliteValues,
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

        let mut stmt = self.conn.connection().prepare(&sql)?;
        stmt.execute(params_from_iter(values))?;

        Ok(())
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.conn.is_read_only() {
            return Err(GpkgError::ReadOnly);
        }
        Ok(())
    }

    fn feature_values_and_columns<G, P>(
        &self,
        geometry: G,
        params: P,
    ) -> Result<(Vec<Value>, Vec<String>)>
    where
        G: GeometryTrait<T = f64>,
        P: RusqliteValues,
    {
        self.ensure_writable()?;

        let mut buf = Vec::new();
        wkb::writer::write_geometry(&mut buf, &geometry, &Default::default())?;
        let wkb = Wkb::try_new(&buf)?;
        let geom = wkb_to_gpkg_geometry(wkb, self.srs_id)?;

        let properties: Vec<Value> = params.into_values()?;
        if properties.len() != self.property_columns.len() {
            return Err(GpkgError::InvalidPropertyCount {
                expected: self.property_columns.len(),
                got: properties.len(),
            });
        }

        let mut values = Vec::with_capacity(self.property_columns.len() + 1);
        values.push(Value::Blob(geom));
        values.extend(properties);

        let mut column_names = Vec::with_capacity(self.property_columns.len() + 1);
        column_names.push(self.geometry_column.clone());
        column_names.extend(self.property_columns.iter().map(|col| col.name.clone()));

        Ok((values, column_names))
    }
}

#[cfg(test)]
mod tests {
    use crate::Result;
    use crate::conversions::geometry_type_to_str;
    use crate::gpkg::Gpkg;
    use crate::types::{ColumnSpec, ColumnType};
    use geo_traits::GeometryTrait;
    use geo_types::{
        Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point,
        Polygon,
    };
    use rusqlite::types::Value;
    use std::str::FromStr;
    use wkb::reader::{GeometryType, Wkb};
    use wkt::Wkt;

    fn generated_gpkg_path() -> &'static str {
        "src/test/test_generated.gpkg"
    }

    fn property_index(columns: &[super::ColumnSpec], name: &str) -> Option<usize> {
        columns.iter().position(|col| col.name == name)
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
        let layer = gpkg.new_layer(
            layer_name,
            "geom".to_string(),
            geometry_type,
            geometry_dimension,
            4326,
            &columns,
        )?;

        let expected_blob = gpkg_blob_from_geometry(geometry.clone(), 4326)?;
        let mut expected_wkb_bytes = Vec::new();
        wkb::writer::write_geometry(&mut expected_wkb_bytes, &geometry, &Default::default())?;
        let expected_wkb = Wkb::try_new(&expected_wkb_bytes)?;

        layer.insert(geometry, Vec::<Value>::new())?;

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

    // This is a bit horrible part. gpkg_spatial_ref_sys requires the WKT of the SRS, but we don't have a good source for this.
    // Adding 4326 is easy, but what should I do to support other SRS?
    fn ensure_srs_4326(gpkg: &Gpkg) -> Result<()> {
        const EPSG4326_WKT: &str = r#"GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]"#;

        gpkg.connection().execute(
            "INSERT INTO gpkg_spatial_ref_sys \
            (srs_name, srs_id, organization, organization_coordsys_id, definition, description) \
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["WGS 84", 4326, "EPSG", 4326, EPSG4326_WKT, "WGS 84"],
        )?;
        Ok(())
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
        let columns = &layer.property_columns;

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

    #[test]
    fn creates_layer_metadata() -> Result<()> {
        let gpkg = Gpkg::new_in_memory()?;
        ensure_srs_4326(&gpkg)?;
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

        gpkg.new_layer(
            "points",
            "geom".to_string(),
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
        let gpkg = Gpkg::new_in_memory()?;
        ensure_srs_4326(&gpkg)?;
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
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        let point_a = Point::new(1.0, 2.0);
        layer.insert(point_a, ("alpha".to_string(), 7_i64))?;
        let id = layer.conn.connection().last_insert_rowid();

        let point_b = Point::new(4.0, 5.0);
        layer.update(point_b, ("beta".to_string(), 9_i64), id)?;

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
        let gpkg = Gpkg::new_in_memory()?;
        ensure_srs_4326(&gpkg)?;

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
        let gpkg = Gpkg::new_in_memory()?;
        ensure_srs_4326(&gpkg)?;

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
        let gpkg = Gpkg::new_in_memory()?;
        ensure_srs_4326(&gpkg)?;
        let columns: Vec<ColumnSpec> = Vec::new();
        let layer = gpkg.new_layer(
            "rtree_points",
            "geom".to_string(),
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        let point_a = Point::new(1.5, -2.0);
        layer.insert(point_a, Vec::<Value>::new())?;
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
        layer.update(point_b, Vec::<Value>::new(), id)?;
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
        let gpkg = Gpkg::new_in_memory()?;
        ensure_srs_4326(&gpkg)?;
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            column_type: ColumnType::Varchar,
        }];

        let layer = gpkg.new_layer(
            "points",
            "geom".to_string(),
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        layer.insert(Point::new(0.0, 0.0), vec![Value::Text("a".to_string())])?;
        layer.insert(Point::new(1.0, 1.0), vec![Value::Text("b".to_string())])?;

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
        let gpkg = Gpkg::new_in_memory()?;
        ensure_srs_4326(&gpkg)?;
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
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        let result = layer.insert(Point::new(0.0, 0.0), vec![Value::Text("only".to_string())]);
        match result {
            Err(crate::error::GpkgError::InvalidPropertyCount { expected, got }) => {
                assert_eq!(expected, 2);
                assert_eq!(got, 1);
            }
            _ => panic!("expected InvalidPropertyCount error"),
        }

        Ok(())
    }
}
