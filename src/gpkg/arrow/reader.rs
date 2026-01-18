use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{FieldRef, SchemaRef};
use geoarrow_array::{GeoArrowArray, builder::WkbBuilder};

use crate::{
    ColumnSpec, Gpkg, GpkgError, GpkgLayer, gpkg::feature::gpkg_geometry_to_wkb_bytes,
    ogc_sql::sql_select_features,
};

/// Iterator that yields `RecordBatch`s` of features from a layer in a Gpkg file.
pub struct GpkgRecordBatchReader<'a> {
    pub(super) stmt: rusqlite::Statement<'a>,
    pub(super) property_columns: Vec<ColumnSpec>,
    pub(super) geometry_column: String,
    pub(super) srs_id: u32,
    pub(super) batch_size: usize,
    pub(super) offset: u32,
    pub(super) end_or_invalid_state: bool,
}

impl<'a> GpkgRecordBatchReader<'a> {
    pub(crate) fn new(
        conn: &'a Arc<rusqlite::Connection>,
        layer_name: &str,
        batch_size: u32,
    ) -> crate::error::Result<Self> {
        let gpkg = Gpkg::new_from_conn(conn.clone(), true)?;
        let layer = gpkg.get_layer(layer_name)?;
        let columns = layer.property_columns.iter().map(|spec| spec.name.as_str());
        let sql = sql_select_features(
            &layer.layer_name,
            &layer.geometry_column,
            &layer.primary_key_column,
            columns,
            Some(batch_size),
        );

        let stmt = conn.prepare(&sql)?;
        Ok(Self::new_inner(stmt, &layer, batch_size))
    }

    pub(crate) fn new_inner(
        stmt: rusqlite::Statement<'a>,
        layer: &GpkgLayer,
        batch_size: u32,
    ) -> Self {
        Self {
            stmt,
            batch_size: batch_size as usize,
            property_columns: layer.property_columns.clone(),
            geometry_column: layer.geometry_column.clone(),
            srs_id: layer.srs_id.clone(),
            offset: 0,
            end_or_invalid_state: false,
        }
    }
}

impl<'a> GpkgRecordBatchReader<'a> {
    pub fn get_arrow_schema(&self) -> SchemaRef {
        let mut fields: Vec<FieldRef> = self
            .property_columns
            .iter()
            .map(|col| {
                let field = match col.column_type {
                    crate::ColumnType::Boolean => {
                        arrow_schema::Field::new(&col.name, arrow_schema::DataType::Boolean, true)
                    }
                    crate::ColumnType::Varchar => {
                        arrow_schema::Field::new(&col.name, arrow_schema::DataType::Utf8, true)
                    }
                    crate::ColumnType::Double => {
                        arrow_schema::Field::new(&col.name, arrow_schema::DataType::Float64, true)
                    }
                    crate::ColumnType::Integer => {
                        arrow_schema::Field::new(&col.name, arrow_schema::DataType::Int64, true)
                    }
                    crate::ColumnType::Geometry => {
                        wkb_geometry_field(&col.name, self.srs_id.to_string())
                    }
                };

                Arc::new(field)
            })
            .collect();

        fields.push(Arc::new(wkb_geometry_field(
            &self.geometry_column,
            self.srs_id.to_string(),
        )));

        Arc::new(arrow_schema::Schema::new(fields))
    }

    fn create_record_batch_builder(&self) -> GpkgRecordBatchBuilder {
        let builders: Vec<GpkgArrayBuilder> =
            self.property_columns
                .iter()
                .map(|col| match col.column_type {
                    crate::ColumnType::Boolean => GpkgArrayBuilder::Boolean(
                        arrow_array::builder::BooleanBuilder::with_capacity(self.batch_size),
                    ),
                    crate::ColumnType::Varchar => GpkgArrayBuilder::Varchar(
                        arrow_array::builder::StringBuilder::with_capacity(
                            self.batch_size,
                            8 * self.batch_size,
                        ),
                    ),
                    crate::ColumnType::Double => GpkgArrayBuilder::Double(
                        arrow_array::builder::Float64Builder::with_capacity(self.batch_size),
                    ),
                    crate::ColumnType::Integer => GpkgArrayBuilder::Integer(
                        arrow_array::builder::Int64Builder::with_capacity(self.batch_size),
                    ),
                    crate::ColumnType::Geometry => GpkgArrayBuilder::Geometry(
                        wkb_geometry_builder(self.srs_id.to_string(), self.batch_size),
                    ),
                })
                .collect();

        GpkgRecordBatchBuilder {
            schema_ref: self.get_arrow_schema(),
            builders,
            geo_builder: wkb_geometry_builder(self.srs_id.to_string(), self.batch_size),
        }
    }

    // This doesn't advance the offset.
    fn get_record_batch(&mut self) -> crate::error::Result<arrow_array::RecordBatch> {
        let mut builders = self.create_record_batch_builder();
        let mut rows = self.stmt.query([self.offset])?;
        while let Some(row) = rows.next()? {
            builders.push(row)?;
        }

        builders.finish()
    }
}

impl<'a> Iterator for GpkgRecordBatchReader<'a> {
    type Item = crate::error::Result<arrow_array::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end_or_invalid_state {
            return None;
        }

        let result = self.get_record_batch();

        let features = match result {
            Ok(features) => features,
            Err(e) => {
                // I don't know in what case some error happens, but I bet it's unrecoverable.
                self.end_or_invalid_state = true;
                return Some(Err(e.into()));
            }
        };

        // If the result is less than the batch size, it means it reached the end.
        let result_size = features.num_rows();
        if result_size < self.batch_size as usize {
            self.end_or_invalid_state = true;
            if result_size == 0 {
                return None;
            }
        }

        self.offset += result_size as u32;

        Some(Ok(features))
    }
}

pub enum GpkgArrayBuilder {
    Boolean(arrow_array::builder::BooleanBuilder),
    Varchar(arrow_array::builder::StringBuilder),
    Double(arrow_array::builder::Float64Builder),
    Integer(arrow_array::builder::Int64Builder),
    // Note: Since WkbBuilder doesn't implement ArrayBuilder trait, we cannot use Box<dyn ArrayBuilder> to unify this
    Geometry(WkbBuilder<i32>),
}

impl GpkgArrayBuilder {
    fn push(&mut self, value: rusqlite::types::Value) -> crate::error::Result<()> {
        match (self, value) {
            // null
            (GpkgArrayBuilder::Boolean(builder), rusqlite::types::Value::Null) => {
                builder.append_null();
            }
            (GpkgArrayBuilder::Varchar(builder), rusqlite::types::Value::Null) => {
                builder.append_null();
            }
            (GpkgArrayBuilder::Double(builder), rusqlite::types::Value::Null) => {
                builder.append_null();
            }
            (GpkgArrayBuilder::Integer(builder), rusqlite::types::Value::Null) => {
                builder.append_null();
            }
            (GpkgArrayBuilder::Geometry(builder), rusqlite::types::Value::Null) => {
                builder.push_wkb(None).unwrap();
            }
            // non-null value
            (GpkgArrayBuilder::Boolean(builder), rusqlite::types::Value::Integer(i)) => {
                builder.append_value(i == 1);
            }
            (GpkgArrayBuilder::Varchar(builder), rusqlite::types::Value::Text(t)) => {
                builder.append_value(t);
            }
            (GpkgArrayBuilder::Double(builder), rusqlite::types::Value::Real(f)) => {
                builder.append_value(f);
            }
            (GpkgArrayBuilder::Integer(builder), rusqlite::types::Value::Integer(i)) => {
                builder.append_value(i);
            }
            (GpkgArrayBuilder::Geometry(builder), rusqlite::types::Value::Blob(b)) => {
                let wkb_bytes = gpkg_geometry_to_wkb_bytes(&b)?;
                builder
                    .push_wkb(Some(wkb_bytes))
                    .map_err(|e| GpkgError::Message(format!("{e:?}")))?;
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

pub struct GpkgRecordBatchBuilder {
    pub(crate) schema_ref: SchemaRef,
    pub(crate) builders: Vec<GpkgArrayBuilder>,
    pub(crate) geo_builder: WkbBuilder<i32>,
}

impl GpkgRecordBatchBuilder {
    pub(crate) fn push(&mut self, row: &rusqlite::Row<'_>) -> crate::error::Result<()> {
        let n = self.builders.len();
        for i in 0..n {
            let column_index = i + 2;
            match row.get::<usize, rusqlite::types::Value>(column_index) {
                Ok(v) => self.builders[i].push(v)?,
                Err(e) => return Err(GpkgError::Sql(e)),
            }
        }

        match row.get::<usize, rusqlite::types::Value>(0) {
            Ok(rusqlite::types::Value::Blob(b)) => {
                let wkb_bytes = gpkg_geometry_to_wkb_bytes(&b)?;
                self.geo_builder
                    .push_wkb(Some(wkb_bytes))
                    .map_err(|e| GpkgError::Message(format!("{e:?}")))?;
            }
            Ok(rusqlite::types::Value::Null) => {
                self.geo_builder.push_wkb(None).unwrap();
            }
            Ok(_) => return Err(GpkgError::Message("Invalid value".to_string())),
            Err(e) => return Err(GpkgError::Sql(e)),
        }

        Ok(())
    }

    fn finish(self) -> crate::error::Result<arrow_array::RecordBatch> {
        let mut columns: Vec<ArrayRef> = self
            .builders
            .into_iter()
            .map(|b| match b {
                GpkgArrayBuilder::Boolean(mut builder) => {
                    arrow_array::builder::ArrayBuilder::finish(&mut builder)
                }
                GpkgArrayBuilder::Varchar(mut builder) => {
                    arrow_array::builder::ArrayBuilder::finish(&mut builder)
                }
                GpkgArrayBuilder::Double(mut builder) => {
                    arrow_array::builder::ArrayBuilder::finish(&mut builder)
                }
                GpkgArrayBuilder::Integer(mut builder) => {
                    arrow_array::builder::ArrayBuilder::finish(&mut builder)
                }
                GpkgArrayBuilder::Geometry(builder) => builder.finish().into_array_ref(),
            })
            .collect();
        columns.push(self.geo_builder.finish().into_array_ref());

        Ok(arrow_array::RecordBatch::try_new(self.schema_ref, columns).unwrap())
    }
}

// TODO: some iterator returns record batch

fn wkb_geometry_field(field_name: &str, srs_id: String) -> arrow_schema::Field {
    let geoarrow_metadata =
        geoarrow_schema::Metadata::new(geoarrow_schema::Crs::from_srid(srs_id.clone()), None);
    geoarrow_schema::GeoArrowType::Wkb(geoarrow_schema::WkbType::new(geoarrow_metadata.into()))
        .to_field(field_name, true)
}

fn wkb_geometry_builder(srs_id: String, batch_size: usize) -> WkbBuilder<i32> {
    let geoarrow_metadata =
        geoarrow_schema::Metadata::new(geoarrow_schema::Crs::from_srid(srs_id.clone()), None);
    WkbBuilder::with_capacity(
        geoarrow_schema::WkbType::new(geoarrow_metadata.into()),
        geoarrow_array::capacity::WkbCapacity::new(21 * batch_size, batch_size),
    )
}

#[cfg(all(test, feature = "arrow"))]
mod tests {
    use super::GpkgRecordBatchReader;
    use crate::Result;
    use crate::gpkg::Gpkg;
    use crate::params;
    use crate::types::{ColumnSpec, ColumnType};
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow_schema::DataType;
    use geo_types::Point;
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::WkbArray;
    use wkb::reader::GeometryType;

    fn create_test_layer(gpkg: &Gpkg) -> Result<crate::GpkgLayer> {
        let columns = vec![
            ColumnSpec {
                name: "active".to_string(),
                column_type: ColumnType::Boolean,
            },
            ColumnSpec {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnSpec {
                name: "score".to_string(),
                column_type: ColumnType::Double,
            },
            ColumnSpec {
                name: "count".to_string(),
                column_type: ColumnType::Integer,
            },
        ];

        gpkg.create_layer(
            "arrow_points",
            "geom",
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )
    }

    #[test]
    fn record_batch_has_expected_types_and_values() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let layer = create_test_layer(&gpkg)?;

        let first_geom = Point::new(1.0, 2.0);
        let second_geom = Point::new(3.0, 4.0);

        layer.insert(first_geom, params![true, "alpha", 1.25, 7])?;
        layer.insert(second_geom, params![false, "beta", 2.5, 9])?;

        let mut iter: GpkgRecordBatchReader<'_> = layer.features_record_batch(10)?;
        let batch = iter.next().transpose()?.expect("first batch");

        let schema = batch.schema();
        let fields = schema.fields();
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].name(), "active");
        assert_eq!(fields[1].name(), "name");
        assert_eq!(fields[2].name(), "score");
        assert_eq!(fields[3].name(), "count");
        assert_eq!(fields[4].name(), "geom");
        assert_eq!(fields[0].data_type(), &DataType::Boolean);
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert_eq!(fields[2].data_type(), &DataType::Float64);
        assert_eq!(fields[3].data_type(), &DataType::Int64);

        let active = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean array");
        let name = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        let score = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float array");
        let count = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int array");

        assert_eq!(active.value(0), true);
        assert_eq!(active.value(1), false);
        assert_eq!(name.value(0), "alpha");
        assert_eq!(name.value(1), "beta");
        assert_eq!(score.value(0), 1.25);
        assert_eq!(score.value(1), 2.5);
        assert_eq!(count.value(0), 7);
        assert_eq!(count.value(1), 9);

        let geom_field = fields[4].as_ref();
        let geom_array = WkbArray::try_from((batch.column(4).as_ref(), geom_field)).unwrap();
        let geom = geom_array.value(0).unwrap();
        let mut expected = Vec::new();
        wkb::writer::write_geometry(&mut expected, &Point::new(1.0, 2.0), &Default::default())?;
        assert_eq!(geom.buf(), expected.as_slice());

        Ok(())
    }

    #[test]
    fn record_batch_iterator_respects_offsets_and_limits() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns = vec![ColumnSpec {
            name: "rank".to_string(),
            column_type: ColumnType::Integer,
        }];
        let layer = gpkg.create_layer(
            "arrow_offsets",
            "geom",
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        for i in 0..5 {
            layer.insert(Point::new(i as f64, i as f64), params![i as i64])?;
        }

        let mut values = Vec::new();
        let mut batch_sizes = Vec::new();
        for batch in layer.features_record_batch(2)? {
            let batch = batch?;
            batch_sizes.push(batch.num_rows());
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int array");
            for row in 0..array.len() {
                values.push(array.value(row));
            }
        }

        assert_eq!(values, vec![0, 1, 2, 3, 4]);
        assert_eq!(batch_sizes, vec![2, 2, 1]);

        Ok(())
    }
}
