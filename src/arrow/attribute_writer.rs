use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::Gpkg;
use crate::error::{GpkgError, Result};
use crate::gpkg::GpkgAttributeTable;
use crate::types::{ColumnSpec, ColumnType};

/// Writes Arrow `RecordBatch`es into a GeoPackage attribute table (no geometry).
///
/// `ArrowGpkgAttributeWriter` creates a new GeoPackage attribute table from the
/// Arrow schema of the first batch and then inserts rows from each batch written
/// via [`write`][Self::write].
///
/// ## Example
///
/// ```no_run
/// use rusqlite_gpkg::{ArrowGpkgAttributeWriter, Gpkg};
/// # fn example(batch: arrow_array::RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
/// let gpkg = Gpkg::open_in_memory()?;
/// let mut writer = ArrowGpkgAttributeWriter::new(&gpkg, "my_table")?;
/// writer.write(&batch)?;
/// # Ok(())
/// # }
/// ```
pub struct ArrowGpkgAttributeWriter<'a> {
    gpkg: &'a Gpkg,
    table_name: String,
    state: Option<AttributeWriterState>,
}

struct AttributeWriterState {
    insert_sql: String,
    col_indices: Vec<usize>,
}

impl<'a> ArrowGpkgAttributeWriter<'a> {
    /// Create a new writer targeting the given attribute table name.
    ///
    /// The table is not created until the first [`write`][Self::write] call.
    pub fn new(gpkg: &'a Gpkg, table_name: &str) -> Result<Self> {
        Ok(Self {
            gpkg,
            table_name: table_name.to_string(),
            state: None,
        })
    }

    /// Write a `RecordBatch` into the GeoPackage attribute table.
    ///
    /// On the first call, the table is created from the batch's schema.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.state.is_none() {
            self.initialize(batch.schema())?;
        }
        self.write_batch(batch)
    }

    fn initialize(&mut self, schema: SchemaRef) -> Result<()> {
        let property_columns = build_property_columns(&schema)?;

        self.gpkg
            .create_attribute_table(&self.table_name, &property_columns)?;

        let insert_sql = GpkgAttributeTable::build_insert_sql(&self.table_name, &property_columns);

        let col_indices: Vec<usize> = (0..schema.fields().len()).collect();

        self.state = Some(AttributeWriterState {
            insert_sql,
            col_indices,
        });

        Ok(())
    }

    fn write_batch(&self, batch: &RecordBatch) -> Result<()> {
        let state = self
            .state
            .as_ref()
            .expect("initialize must be called first");

        let mut stmt = self.gpkg.conn.prepare_cached(&state.insert_sql)?;
        let num_params = state.col_indices.len();
        let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(num_params);

        for row_idx in 0..batch.num_rows() {
            params.clear();

            for &col_idx in &state.col_indices {
                let array = batch.column(col_idx);
                params.push(extract_value(array, row_idx)?);
            }

            stmt.execute(rusqlite::params_from_iter(&params))?;
        }
        Ok(())
    }
}

fn build_property_columns(schema: &SchemaRef) -> Result<Vec<ColumnSpec>> {
    let mut columns = Vec::new();
    for field in schema.fields().iter() {
        let column_type = arrow_type_to_column_type(field.data_type())?;
        columns.push(ColumnSpec {
            name: field.name().clone(),
            column_type,
        });
    }
    Ok(columns)
}

fn arrow_type_to_column_type(dt: &arrow_schema::DataType) -> Result<ColumnType> {
    use arrow_schema::DataType;
    match dt {
        DataType::Boolean => Ok(ColumnType::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Ok(ColumnType::Integer)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(ColumnType::Integer)
        }
        DataType::Float32 | DataType::Float64 => Ok(ColumnType::Double),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(ColumnType::Varchar),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok(ColumnType::Blob),
        other => Err(GpkgError::GeoArrow(format!(
            "Unsupported Arrow data type for GeoPackage column: {other:?}"
        ))),
    }
}

fn extract_value(array: &dyn arrow_array::Array, row_idx: usize) -> Result<rusqlite::types::Value> {
    if array.is_null(row_idx) {
        return Ok(rusqlite::types::Value::Null);
    }

    if let Some(a) = array.as_any().downcast_ref::<arrow_array::BooleanArray>() {
        return Ok(rusqlite::types::Value::Integer(if a.value(row_idx) {
            1
        } else {
            0
        }));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Int8Array>() {
        return Ok(rusqlite::types::Value::Integer(a.value(row_idx) as i64));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Int16Array>() {
        return Ok(rusqlite::types::Value::Integer(a.value(row_idx) as i64));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return Ok(rusqlite::types::Value::Integer(a.value(row_idx) as i64));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return Ok(rusqlite::types::Value::Integer(a.value(row_idx)));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::UInt8Array>() {
        return Ok(rusqlite::types::Value::Integer(a.value(row_idx) as i64));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::UInt16Array>() {
        return Ok(rusqlite::types::Value::Integer(a.value(row_idx) as i64));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::UInt32Array>() {
        return Ok(rusqlite::types::Value::Integer(a.value(row_idx) as i64));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        return Ok(rusqlite::types::Value::Integer(a.value(row_idx) as i64));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Float32Array>() {
        return Ok(rusqlite::types::Value::Real(a.value(row_idx) as f64));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
        return Ok(rusqlite::types::Value::Real(a.value(row_idx)));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
        return Ok(rusqlite::types::Value::Text(a.value(row_idx).to_string()));
    }
    if let Some(a) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        return Ok(rusqlite::types::Value::Text(a.value(row_idx).to_string()));
    }
    if let Some(a) = array
        .as_any()
        .downcast_ref::<arrow_array::StringViewArray>()
    {
        return Ok(rusqlite::types::Value::Text(a.value(row_idx).to_string()));
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::BinaryArray>() {
        return Ok(rusqlite::types::Value::Blob(a.value(row_idx).to_vec()));
    }
    if let Some(a) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeBinaryArray>()
    {
        return Ok(rusqlite::types::Value::Blob(a.value(row_idx).to_vec()));
    }
    if let Some(a) = array
        .as_any()
        .downcast_ref::<arrow_array::BinaryViewArray>()
    {
        return Ok(rusqlite::types::Value::Blob(a.value(row_idx).to_vec()));
    }

    Err(GpkgError::GeoArrow(format!(
        "Unsupported Arrow array type: {:?}",
        array.data_type()
    )))
}

#[cfg(all(test, feature = "arrow"))]
mod tests {
    use super::ArrowGpkgAttributeWriter;
    use crate::Result;
    use crate::arrow::attribute_reader::ArrowGpkgAttributeReader;
    use crate::gpkg::Gpkg;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn write_and_read_back_attribute_table() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;

        let name_array = Arc::new(StringArray::from(vec!["alpha", "beta"]));
        let value_array = Arc::new(Int64Array::from(vec![10, 20]));

        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("name", arrow_schema::DataType::Utf8, true)),
            Arc::new(Field::new("value", arrow_schema::DataType::Int64, true)),
        ]));

        let batch =
            RecordBatch::try_new(schema, vec![name_array, value_array]).expect("valid batch");

        let mut writer = ArrowGpkgAttributeWriter::new(&gpkg, "test_attrs")?;
        writer.write(&batch)?;

        // Read back
        let mut reader = ArrowGpkgAttributeReader::new(&gpkg, "test_attrs", 100)?;
        let read_batch = reader.next().unwrap()?;

        assert_eq!(read_batch.num_rows(), 2);
        assert_eq!(read_batch.num_columns(), 2);

        let names = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alpha");
        assert_eq!(names.value(1), "beta");

        let values = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert_eq!(values.value(1), 20);

        Ok(())
    }

    #[test]
    fn write_multiple_batches_attribute_table() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;

        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "rank",
            arrow_schema::DataType::Int64,
            true,
        ))]));

        let mut writer = ArrowGpkgAttributeWriter::new(&gpkg, "multi_batch_attrs")?;

        for i in 0..3 {
            let rank_array = Arc::new(Int64Array::from(vec![i as i64]));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![rank_array]).expect("valid batch");
            writer.write(&batch)?;
        }

        let table = gpkg.get_attribute_table("multi_batch_attrs")?;
        let rows = table.rows()?;
        assert_eq!(rows.len(), 3);

        Ok(())
    }
}
