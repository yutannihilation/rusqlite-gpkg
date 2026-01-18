use arrow_schema::SchemaRef;

use crate::Gpkg;

pub struct ArrowGpkgWriter<'a> {
    pub(super) stmt: rusqlite::Statement<'a>,
}

impl<'a> ArrowGpkgWriter<'a> {
    pub fn new(gpkg: &'a Gpkg, layer_name: &str, schema: SchemaRef) -> crate::error::Result<Self> {
        let mut geometry_column: Option<(&str, wkb::reader::Dimension)> = None;
        for (i, field) in schema.fields().iter().enumerate() {
            if let Ok(Some(ty)) = geoarrow_schema::GeoArrowType::from_extension_field(&field) {
                let crs = ty.metadata().crs();
                let srid = match (crs.crs_type(), crs.crs_value()) {
                    (Some(geoarrow_schema::CrsType::Srid), Some(v)) => {
                        v.as_str().unwrap().to_string()
                    }
                    _ => todo!(),
                };

                let dim = match ty.dimension() {
                    Some(dim) => match dim {
                        geoarrow_schema::Dimension::XY => wkb::reader::Dimension::Xy,
                        geoarrow_schema::Dimension::XYZ => wkb::reader::Dimension::Xyz,
                        geoarrow_schema::Dimension::XYM => wkb::reader::Dimension::Xym,
                        geoarrow_schema::Dimension::XYZM => wkb::reader::Dimension::Xyzm,
                    },
                    None => {
                        // TODO: Wkb and Wkt doesn't return dimension
                        unimplemented!()
                    }
                };
                geometry_column.insert((field.name(), dim));
            }
        }

        let geom_col_indices = geometry_columns(schema);
        let geometry_column = match geom_col_indices.as_slice() {
            [] => {
                return Err(crate::GpkgError::Message("No geometry column".to_string()));
            }
            // When there are multiple geometry columns, use the first one.
            [i] | [i, ..] => schema.field(*i).name(),
        };

        let layer = gpkg.create_layer(
            layer_name,
            geometry_column,
            geometry_type,
            geometry_dimension,
            srs_id,
            other_column_specs,
        );
    }
}

fn geometry_columns(schema: SchemaRef) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .flat_map(|(idx, field)| {
            if let Ok(Some(_)) = geoarrow_schema::GeoArrowType::from_extension_field(&field) {
                Some(idx)
            } else {
                None
            }
        })
        .collect()
}
