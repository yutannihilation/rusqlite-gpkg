use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use geoarrow_schema::{CrsType, Metadata};

use crate::Gpkg;
use crate::error::{GpkgError, Result};
use crate::gpkg::GpkgLayer;
use crate::gpkg::wkb_to_gpkg_geometry;
use crate::types::{ColumnSpec, ColumnType};

/// Writes Arrow `RecordBatch`es into a GeoPackage layer.
///
/// `ArrowGpkgWriter` creates a new GeoPackage layer from the Arrow schema of
/// the first batch and then inserts rows from each batch written via [`write`][Self::write].
///
/// The geometry column is identified by its GeoArrow extension metadata. The
/// EPSG code (srs_id) is derived from the CRS embedded in that metadata:
///
/// - **PROJJSON** — the EPSG code is read from the `id.authority` / `id.code` fields.
/// - **WKT2** — parsed via [`epsg_utils::parse_wkt2`] and [`Crs::to_epsg`][epsg_utils::Crs::to_epsg].
/// - **authority:code** strings of the form `EPSG:<code>` are parsed directly.
/// - **SRID** strings are parsed as plain integers.
///
/// If the EPSG code cannot be resolved, layer creation will fail.
///
/// ## Example
///
/// ```no_run
/// use rusqlite_gpkg::{ArrowGpkgWriter, Gpkg};
/// # fn example(batch: arrow_array::RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
/// let gpkg = Gpkg::open_in_memory()?;
/// let mut writer = ArrowGpkgWriter::new(&gpkg, "my_layer")?;
/// writer.write(&batch)?;
/// # Ok(())
/// # }
/// ```
pub struct ArrowGpkgWriter<'a> {
    gpkg: &'a Gpkg,
    layer_name: String,
    /// Cached after the first `write()` call.
    state: Option<WriterState>,
}

/// Schema-derived state cached after initialization.
struct WriterState {
    geom_index: usize,
    srs_id: u32,
    insert_sql: String,
    /// Column indices in the Arrow schema that map to property columns (excludes geometry).
    property_col_indices: Vec<usize>,
}

impl<'a> ArrowGpkgWriter<'a> {
    /// Create a new writer targeting the given layer name.
    ///
    /// The layer is not created until the first [`write`][Self::write] call,
    /// because the schema is derived from the first `RecordBatch`.
    pub fn new(gpkg: &'a Gpkg, layer_name: &str) -> Result<Self> {
        Ok(Self {
            gpkg,
            layer_name: layer_name.to_string(),
            state: None,
        })
    }

    /// Write a `RecordBatch` into the GeoPackage layer.
    ///
    /// On the first call, the layer is created from the batch's schema. The
    /// geometry column is identified by GeoArrow extension metadata; all other
    /// columns become property columns.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.state.is_none() {
            self.initialize(batch.schema())?;
        }
        self.write_batch(batch)
    }

    fn initialize(&mut self, schema: SchemaRef) -> Result<()> {
        let (geom_index, geom_field_name, srs_id) = resolve_geometry_column(&schema)?;
        let property_columns = build_property_columns(&schema, geom_index)?;

        // Register the SRS if it's not already present (e.g. non-4326 EPSG codes).
        self.ensure_srs_registered(srs_id)?;

        self.gpkg.create_layer(
            &self.layer_name,
            &geom_field_name,
            // GeoPackage stores WKB, which carries its own geometry type per row.
            // Use GEOMETRY as the catch-all type.
            wkb::reader::GeometryType::GeometryCollection,
            wkb::reader::Dimension::Xy,
            srs_id,
            &property_columns,
        )?;

        let insert_sql =
            GpkgLayer::build_insert_sql(&self.layer_name, &geom_field_name, &property_columns);

        let property_col_indices: Vec<usize> = (0..schema.fields().len())
            .filter(|&i| i != geom_index)
            .collect();

        self.state = Some(WriterState {
            geom_index,
            srs_id,
            insert_sql,
            property_col_indices,
        });

        Ok(())
    }

    fn ensure_srs_registered(&self, srs_id: u32) -> Result<()> {
        let exists: i64 = self.gpkg.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM gpkg_spatial_ref_sys WHERE srs_id = ?1)",
            rusqlite::params![srs_id],
            |row| row.get(0),
        )?;
        if exists == 1 {
            return Ok(());
        }

        // GeoPackage requires a definition but we don't have a WKT1 source;
        // "undefined" is permitted by the spec for non-built-in SRS entries.
        let definition = "undefined";
        let name = format!("EPSG:{srs_id}");
        self.gpkg.register_srs(
            &name,
            srs_id as i32,
            "EPSG",
            srs_id as i32,
            definition,
            &name,
        )?;
        Ok(())
    }

    fn write_batch(&self, batch: &RecordBatch) -> Result<()> {
        let state = self
            .state
            .as_ref()
            .expect("initialize must be called first");

        let mut stmt = self.gpkg.conn.prepare_cached(&state.insert_sql)?;
        let num_params = 1 + state.property_col_indices.len();
        let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(num_params);

        for row_idx in 0..batch.num_rows() {
            params.clear();

            // Geometry column first (matching the layer's insert SQL column order)
            let geom_array = batch.column(state.geom_index);
            if geom_array.is_null(row_idx) {
                return Err(GpkgError::NullGeometryValue);
            }
            let wkb_bytes = extract_wkb_bytes(geom_array, row_idx)?;
            let wkb = wkb::reader::Wkb::try_new(&wkb_bytes)?;
            let geom_blob = wkb_to_gpkg_geometry(wkb, state.srs_id)?;
            params.push(rusqlite::types::Value::Blob(geom_blob));

            // Property columns in schema order (skipping the geometry column)
            for &col_idx in &state.property_col_indices {
                let array = batch.column(col_idx);
                params.push(extract_value(array, row_idx)?);
            }

            stmt.execute(rusqlite::params_from_iter(&params))?;
        }
        Ok(())
    }
}

/// Identify the geometry column from GeoArrow extension metadata and extract its EPSG srs_id.
fn resolve_geometry_column(schema: &SchemaRef) -> Result<(usize, String, u32)> {
    for (i, field) in schema.fields().iter().enumerate() {
        if field.extension_type_name().is_some() {
            let metadata = Metadata::try_from(field.as_ref())
                .map_err(|e| GpkgError::GeoArrow(format!("{e}")))?;
            let srs_id = srs_id_from_crs(metadata.crs())?;
            return Ok((i, field.name().clone(), srs_id));
        }
    }
    Err(GpkgError::GeoArrow(
        "No geometry column found in Arrow schema (missing GeoArrow extension metadata)"
            .to_string(),
    ))
}

/// Convert GeoArrow CRS metadata to an EPSG srs_id.
fn srs_id_from_crs(crs: &geoarrow_schema::Crs) -> Result<u32> {
    let (crs_value, crs_type) = match (crs.crs_value(), crs.crs_type()) {
        (Some(value), crs_type) => (value, crs_type),
        (None, _) => {
            return Err(GpkgError::GeoArrow(
                "Geometry column has no CRS metadata; cannot determine srs_id".to_string(),
            ));
        }
    };

    match crs_type {
        Some(CrsType::Projjson) => {
            // Extract EPSG code directly from the PROJJSON "id" field:
            // {"id": {"authority": "EPSG", "code": 4326}}
            let id = crs_value
                .get("id")
                .ok_or_else(|| GpkgError::GeoArrow("PROJJSON has no 'id' field".to_string()))?;
            let authority = id.get("authority").and_then(|v| v.as_str()).unwrap_or("");
            if !authority.eq_ignore_ascii_case("EPSG") {
                return Err(GpkgError::GeoArrow(format!(
                    "Unsupported PROJJSON authority '{authority}', only EPSG is supported"
                )));
            }
            id.get("code")
                .and_then(|v| v.as_u64())
                .map(|c| c as u32)
                .ok_or_else(|| {
                    GpkgError::GeoArrow("PROJJSON 'id' has no numeric 'code' field".to_string())
                })
        }
        Some(CrsType::Wkt2_2019) => {
            let wkt_str = crs_value
                .as_str()
                .ok_or_else(|| GpkgError::GeoArrow("WKT2 CRS value is not a string".to_string()))?;
            let parsed = epsg_utils::parse_wkt2(wkt_str)
                .map_err(|e| GpkgError::GeoArrow(format!("Failed to parse WKT2: {e}")))?;
            parsed.to_epsg().map(|c| c as u32).ok_or_else(|| {
                GpkgError::GeoArrow("WKT2 CRS does not contain an EPSG identifier".to_string())
            })
        }
        Some(CrsType::AuthorityCode) => {
            let code_str = crs_value.as_str().ok_or_else(|| {
                GpkgError::GeoArrow("authority:code CRS value is not a string".to_string())
            })?;
            let (authority, code) = code_str.split_once(':').ok_or_else(|| {
                GpkgError::GeoArrow(format!("Invalid authority:code format: '{code_str}'"))
            })?;
            if !authority.eq_ignore_ascii_case("EPSG") {
                return Err(GpkgError::GeoArrow(format!(
                    "Unsupported CRS authority in '{code_str}', only EPSG is supported"
                )));
            }
            code.parse::<u32>()
                .map_err(|_| GpkgError::GeoArrow(format!("Invalid EPSG code in '{code_str}'")))
        }
        Some(CrsType::Srid) | None => {
            let srid_str = crs_value
                .as_str()
                .ok_or_else(|| GpkgError::GeoArrow("SRID CRS value is not a string".to_string()))?;
            srid_str
                .parse::<u32>()
                .map_err(|_| GpkgError::GeoArrow(format!("Cannot parse SRID '{srid_str}' as u32")))
        }
    }
}

/// Build property column specs from the Arrow schema, skipping the geometry column.
fn build_property_columns(schema: &SchemaRef, geom_index: usize) -> Result<Vec<ColumnSpec>> {
    let mut columns = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        if i == geom_index {
            continue;
        }
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

/// Extract raw WKB bytes from a geometry array at the given row index.
fn extract_wkb_bytes(array: &dyn arrow_array::Array, row_idx: usize) -> Result<Vec<u8>> {
    if let Some(binary) = array.as_any().downcast_ref::<arrow_array::BinaryArray>() {
        return Ok(binary.value(row_idx).to_vec());
    }
    if let Some(binary) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeBinaryArray>()
    {
        return Ok(binary.value(row_idx).to_vec());
    }
    Err(GpkgError::GeoArrow(
        "Geometry column must be Binary or LargeBinary (WKB)".to_string(),
    ))
}

/// Extract a rusqlite-compatible value from an Arrow array at the given row index.
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
    use super::ArrowGpkgWriter;
    use crate::Result;
    use crate::arrow::reader::ArrowGpkgReader;
    use crate::gpkg::Gpkg;

    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};
    use geoarrow_array::GeoArrowArray;
    use std::sync::Arc;

    fn wkb_field_4326() -> Field {
        super::super::wkb_geometry_field("geom", 4326)
    }

    fn make_wkb_array(coords: &[(f64, f64)]) -> arrow_array::ArrayRef {
        let mut builder = super::super::wkb_geometry_builder(4326, coords.len());
        for &(x, y) in coords {
            let point = geo_types::Point::new(x, y);
            let mut wkb_bytes = Vec::new();
            wkb::writer::write_geometry(&mut wkb_bytes, &point, &Default::default()).unwrap();
            builder.push_wkb(Some(&wkb_bytes)).unwrap();
        }
        builder.finish().into_array_ref()
    }

    #[test]
    fn write_and_read_back() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;

        let geom_array = make_wkb_array(&[(1.0, 2.0), (3.0, 4.0)]);
        let name_array = Arc::new(StringArray::from(vec!["alpha", "beta"]));
        let value_array = Arc::new(Int64Array::from(vec![10, 20]));

        let schema = Arc::new(Schema::new(vec![
            Arc::new(wkb_field_4326()),
            Arc::new(Field::new("name", arrow_schema::DataType::Utf8, true)),
            Arc::new(Field::new("value", arrow_schema::DataType::Int64, true)),
        ]));

        let batch = RecordBatch::try_new(schema, vec![geom_array, name_array, value_array])
            .expect("valid batch");

        let mut writer = ArrowGpkgWriter::new(&gpkg, "test_points")?;
        writer.write(&batch)?;

        // Read back via the existing ArrowGpkgReader
        let mut reader = ArrowGpkgReader::new(&gpkg, "test_points", 100)?;
        let read_batch = reader.next().unwrap()?;

        assert_eq!(read_batch.num_rows(), 2);
        assert_eq!(read_batch.num_columns(), 3); // name, value, geom

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
    fn write_multiple_batches() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;

        let schema = Arc::new(Schema::new(vec![
            Arc::new(wkb_field_4326()),
            Arc::new(Field::new("rank", arrow_schema::DataType::Int64, true)),
        ]));

        let mut writer = ArrowGpkgWriter::new(&gpkg, "multi_batch")?;

        for i in 0..3 {
            let x = i as f64;
            let geom_array = make_wkb_array(&[(x, x + 1.0)]);
            let rank_array = Arc::new(Int64Array::from(vec![i as i64]));
            let batch = RecordBatch::try_new(schema.clone(), vec![geom_array, rank_array])
                .expect("valid batch");
            writer.write(&batch)?;
        }

        let layer = gpkg.get_layer("multi_batch")?;
        let features = layer.features()?;
        assert_eq!(features.len(), 3);

        Ok(())
    }

    #[test]
    fn write_with_float_column() -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;

        let geom_array = make_wkb_array(&[(1.0, 2.0)]);
        let score_array = Arc::new(Float64Array::from(vec![3.14]));

        let schema = Arc::new(Schema::new(vec![
            Arc::new(wkb_field_4326()),
            Arc::new(Field::new("score", arrow_schema::DataType::Float64, true)),
        ]));

        let batch =
            RecordBatch::try_new(schema, vec![geom_array, score_array]).expect("valid batch");

        let mut writer = ArrowGpkgWriter::new(&gpkg, "float_layer")?;
        writer.write(&batch)?;

        let layer = gpkg.get_layer("float_layer")?;
        let features = layer.features()?;
        assert_eq!(features.len(), 1);
        let score: f64 = features[0].property("score").unwrap().try_into()?;
        assert!((score - 3.14).abs() < f64::EPSILON);

        Ok(())
    }
}
