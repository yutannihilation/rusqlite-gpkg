use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatchReader};
use arrow_schema::{FieldRef, SchemaRef};

use crate::{ColumnSpec, Gpkg, GpkgAttributeTable, GpkgError, ogc_sql::sql_select_attribute_rows};

/// Iterator that yields Arrow `RecordBatch`es from a GeoPackage attribute table.
///
/// This is the attribute-table counterpart of [`ArrowGpkgReader`][crate::ArrowGpkgReader].
/// The schema contains only property columns (no geometry).
pub struct ArrowGpkgAttributeReader<'a> {
    stmt: rusqlite::Statement<'a>,
    property_columns: Vec<ColumnSpec>,
    batch_size: usize,
    offset: u32,
    end_or_invalid_state: bool,
    schema_ref: SchemaRef,
}

impl<'a> ArrowGpkgAttributeReader<'a> {
    /// Create a new Arrow reader for an attribute table.
    ///
    /// `batch_size` must be greater than zero.
    pub fn new(gpkg: &'a Gpkg, table_name: &str, batch_size: u32) -> crate::error::Result<Self> {
        if batch_size == 0 {
            return Err(GpkgError::GeoArrow(
                "batch_size must be greater than zero".to_string(),
            ));
        }
        let table = gpkg.get_attribute_table(table_name)?;
        let columns = table.property_columns.iter().map(|spec| spec.name.as_str());
        let sql = sql_select_attribute_rows(
            &table.table_name,
            &table.primary_key_column,
            columns,
            Some(batch_size),
        );

        let stmt = gpkg.conn.prepare(&sql)?;
        Ok(Self::new_inner(stmt, &table, batch_size))
    }

    fn new_inner(
        stmt: rusqlite::Statement<'a>,
        table: &GpkgAttributeTable,
        batch_size: u32,
    ) -> Self {
        let schema_ref = Self::construct_arrow_schema(&table.property_columns);

        Self {
            stmt,
            batch_size: batch_size as usize,
            property_columns: table.property_columns.clone(),
            offset: 0,
            end_or_invalid_state: false,
            schema_ref,
        }
    }

    fn construct_arrow_schema(property_columns: &[ColumnSpec]) -> SchemaRef {
        let fields: Vec<FieldRef> = property_columns
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
                    crate::ColumnType::Date | crate::ColumnType::Datetime => {
                        arrow_schema::Field::new(&col.name, arrow_schema::DataType::Utf8, true)
                    }
                    crate::ColumnType::Blob => {
                        arrow_schema::Field::new(&col.name, arrow_schema::DataType::Binary, true)
                    }
                    crate::ColumnType::Geometry => {
                        // Should not appear in attribute tables, but handle gracefully.
                        arrow_schema::Field::new(&col.name, arrow_schema::DataType::Binary, true)
                    }
                };
                Arc::new(field)
            })
            .collect();

        Arc::new(arrow_schema::Schema::new(fields))
    }

    fn create_record_batch_builder(&self) -> AttributeRecordBatchBuilder {
        let builders: Vec<AttributeArrayBuilder> = self
            .property_columns
            .iter()
            .map(|col| match col.column_type {
                crate::ColumnType::Boolean => AttributeArrayBuilder::Boolean(
                    arrow_array::builder::BooleanBuilder::with_capacity(self.batch_size),
                ),
                crate::ColumnType::Varchar
                | crate::ColumnType::Date
                | crate::ColumnType::Datetime => AttributeArrayBuilder::Varchar(
                    arrow_array::builder::StringBuilder::with_capacity(
                        self.batch_size,
                        8 * self.batch_size,
                    ),
                ),
                crate::ColumnType::Double => AttributeArrayBuilder::Double(
                    arrow_array::builder::Float64Builder::with_capacity(self.batch_size),
                ),
                crate::ColumnType::Integer => AttributeArrayBuilder::Integer(
                    arrow_array::builder::Int64Builder::with_capacity(self.batch_size),
                ),
                crate::ColumnType::Blob | crate::ColumnType::Geometry => {
                    AttributeArrayBuilder::Blob(arrow_array::builder::BinaryBuilder::with_capacity(
                        self.batch_size,
                        8 * self.batch_size,
                    ))
                }
            })
            .collect();

        AttributeRecordBatchBuilder {
            schema_ref: self.schema_ref.clone(),
            builders,
        }
    }

    fn get_record_batch(&mut self) -> crate::error::Result<arrow_array::RecordBatch> {
        let mut builders = self.create_record_batch_builder();
        let mut rows = self.stmt.query([self.offset])?;
        while let Some(row) = rows.next()? {
            builders.push(row)?;
        }
        builders.finish()
    }
}

impl<'a> Iterator for ArrowGpkgAttributeReader<'a> {
    type Item = Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end_or_invalid_state {
            return None;
        }

        let result = self.get_record_batch();

        let batch = match result {
            Ok(batch) => batch,
            Err(e) => {
                self.end_or_invalid_state = true;
                return Some(Err(e.into()));
            }
        };

        let result_size = batch.num_rows();
        if result_size < self.batch_size {
            self.end_or_invalid_state = true;
            if result_size == 0 {
                return None;
            }
        }

        self.offset += result_size as u32;

        Some(Ok(batch))
    }
}

impl<'a> RecordBatchReader for ArrowGpkgAttributeReader<'a> {
    fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
    }
}

enum AttributeArrayBuilder {
    Boolean(arrow_array::builder::BooleanBuilder),
    Varchar(arrow_array::builder::StringBuilder),
    Double(arrow_array::builder::Float64Builder),
    Integer(arrow_array::builder::Int64Builder),
    Blob(arrow_array::builder::BinaryBuilder),
}

fn rusqlite_value_type_name(value: &rusqlite::types::Value) -> &'static str {
    match value {
        rusqlite::types::Value::Null => "NULL",
        rusqlite::types::Value::Integer(_) => "INTEGER",
        rusqlite::types::Value::Real(_) => "REAL",
        rusqlite::types::Value::Text(_) => "TEXT",
        rusqlite::types::Value::Blob(_) => "BLOB",
    }
}

impl AttributeArrayBuilder {
    fn push(&mut self, value: rusqlite::types::Value) -> crate::error::Result<()> {
        match self {
            AttributeArrayBuilder::Boolean(builder) => match value {
                rusqlite::types::Value::Null => builder.append_null(),
                rusqlite::types::Value::Integer(i) => builder.append_value(i == 1),
                other => {
                    return Err(GpkgError::InvalidArrowValue {
                        expected: "INTEGER or NULL",
                        actual: rusqlite_value_type_name(&other),
                    });
                }
            },
            AttributeArrayBuilder::Varchar(builder) => match value {
                rusqlite::types::Value::Null => builder.append_null(),
                rusqlite::types::Value::Text(t) => builder.append_value(t),
                other => {
                    return Err(GpkgError::InvalidArrowValue {
                        expected: "TEXT or NULL",
                        actual: rusqlite_value_type_name(&other),
                    });
                }
            },
            AttributeArrayBuilder::Double(builder) => match value {
                rusqlite::types::Value::Null => builder.append_null(),
                rusqlite::types::Value::Real(f) => builder.append_value(f),
                other => {
                    return Err(GpkgError::InvalidArrowValue {
                        expected: "REAL or NULL",
                        actual: rusqlite_value_type_name(&other),
                    });
                }
            },
            AttributeArrayBuilder::Integer(builder) => match value {
                rusqlite::types::Value::Null => builder.append_null(),
                rusqlite::types::Value::Integer(i) => builder.append_value(i),
                other => {
                    return Err(GpkgError::InvalidArrowValue {
                        expected: "INTEGER or NULL",
                        actual: rusqlite_value_type_name(&other),
                    });
                }
            },
            AttributeArrayBuilder::Blob(builder) => match value {
                rusqlite::types::Value::Null => builder.append_null(),
                rusqlite::types::Value::Blob(b) => builder.append_value(b),
                other => {
                    return Err(GpkgError::InvalidArrowValue {
                        expected: "BLOB or NULL",
                        actual: rusqlite_value_type_name(&other),
                    });
                }
            },
        }
        Ok(())
    }
}

struct AttributeRecordBatchBuilder {
    schema_ref: SchemaRef,
    builders: Vec<AttributeArrayBuilder>,
}

impl AttributeRecordBatchBuilder {
    fn push(&mut self, row: &rusqlite::Row<'_>) -> crate::error::Result<()> {
        // Column 0 is the primary key (skipped), properties start at column 1.
        for (i, builder) in self.builders.iter_mut().enumerate() {
            let column_index = i + 1;
            match row.get::<usize, rusqlite::types::Value>(column_index) {
                Ok(v) => builder.push(v)?,
                Err(e) => return Err(GpkgError::Sql(e)),
            }
        }
        Ok(())
    }

    fn finish(self) -> crate::error::Result<arrow_array::RecordBatch> {
        let columns: Vec<ArrayRef> = self
            .builders
            .into_iter()
            .map(|b| match b {
                AttributeArrayBuilder::Boolean(mut builder) => {
                    arrow_array::builder::ArrayBuilder::finish(&mut builder)
                }
                AttributeArrayBuilder::Varchar(mut builder) => {
                    arrow_array::builder::ArrayBuilder::finish(&mut builder)
                }
                AttributeArrayBuilder::Double(mut builder) => {
                    arrow_array::builder::ArrayBuilder::finish(&mut builder)
                }
                AttributeArrayBuilder::Integer(mut builder) => {
                    arrow_array::builder::ArrayBuilder::finish(&mut builder)
                }
                AttributeArrayBuilder::Blob(mut builder) => {
                    arrow_array::builder::ArrayBuilder::finish(&mut builder)
                }
            })
            .collect();

        Ok(arrow_array::RecordBatch::try_new(self.schema_ref, columns)?)
    }
}

#[cfg(all(test, feature = "arrow"))]
mod tests {
    use super::ArrowGpkgAttributeReader;
    use crate::Result;
    use crate::gpkg::Gpkg;
    use crate::params;
    use crate::types::{ColumnSpec, ColumnType};
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::DataType;

    #[test]
    fn reads_attribute_table_as_record_batch() -> Result<()> {
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
        table.insert(params!["alpha", 10_i64])?;
        table.insert(params!["beta", 20_i64])?;

        let mut reader = ArrowGpkgAttributeReader::new(&gpkg, "observations", 100)?;
        let batch = reader.next().unwrap()?;

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alpha");
        assert_eq!(names.value(1), "beta");

        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert_eq!(values.value(1), 20);

        Ok(())
    }
}
