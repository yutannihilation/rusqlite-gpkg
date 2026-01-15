use crate::error::GpkgError;
use crate::types::ColumnType;

#[inline]
pub(crate) fn geometry_type_to_str(geometry_type: wkb::reader::GeometryType) -> &'static str {
    match geometry_type {
        wkb::reader::GeometryType::GeometryCollection => "GEOMETRYCOLLECTION",
        wkb::reader::GeometryType::Point => "POINT",
        wkb::reader::GeometryType::LineString => "LINESTRING",
        wkb::reader::GeometryType::Polygon => "POLYGON",
        wkb::reader::GeometryType::MultiPoint => "MULTIPOINT",
        wkb::reader::GeometryType::MultiLineString => "MULTILINESTRING",
        wkb::reader::GeometryType::MultiPolygon => "MULTIPOLYGON",
        _ => unreachable!(),
    }
}

#[inline]
pub(crate) fn geometry_type_from_str(
    geometry_type_str: &str,
) -> Result<wkb::reader::GeometryType, GpkgError> {
    let s = geometry_type_str;
    if s.eq_ignore_ascii_case("GEOMETRY") || s.eq_ignore_ascii_case("GEOMETRYCOLLECTION") {
        Ok(wkb::reader::GeometryType::GeometryCollection)
    } else if s.eq_ignore_ascii_case("POINT") {
        Ok(wkb::reader::GeometryType::Point)
    } else if s.eq_ignore_ascii_case("LINESTRING") {
        Ok(wkb::reader::GeometryType::LineString)
    } else if s.eq_ignore_ascii_case("POLYGON") {
        Ok(wkb::reader::GeometryType::Polygon)
    } else if s.eq_ignore_ascii_case("MULTIPOINT") {
        Ok(wkb::reader::GeometryType::MultiPoint)
    } else if s.eq_ignore_ascii_case("MULTILINESTRING") {
        Ok(wkb::reader::GeometryType::MultiLineString)
    } else if s.eq_ignore_ascii_case("MULTIPOLYGON") {
        Ok(wkb::reader::GeometryType::MultiPolygon)
    } else {
        Err(GpkgError::UnsupportedGeometryType(
            geometry_type_str.to_string(),
        ))
    }
}

#[inline]
pub(crate) fn dimension_to_zm(dimension: wkb::reader::Dimension) -> (i8, i8) {
    match dimension {
        wkb::reader::Dimension::Xy => (0, 0),
        wkb::reader::Dimension::Xyz => (1, 0),
        wkb::reader::Dimension::Xym => (0, 1),
        wkb::reader::Dimension::Xyzm => (1, 1),
    }
}

#[inline]
pub(crate) fn dimension_from_zm(z: i8, m: i8) -> Result<wkb::reader::Dimension, GpkgError> {
    match (z, m) {
        (0, 0) => Ok(wkb::reader::Dimension::Xy),
        (1, 0) => Ok(wkb::reader::Dimension::Xyz),
        (0, 1) => Ok(wkb::reader::Dimension::Xym),
        (1, 1) => Ok(wkb::reader::Dimension::Xyzm),
        // Note: the spec says z and m are
        //
        //   0: z/m values prohibited
        //   1: z/m values mandatory
        //   2: z/m values optional
        //
        // but I don't know how 2 can be handled, just treat as an invalid value
        _ => Err(GpkgError::InvalidDimension { z, m }),
    }
}

#[inline]
pub(crate) fn column_type_to_str(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Integer => "INTEGER",
        ColumnType::Double => "DOUBLE",
        ColumnType::Varchar => "TEXT",
        ColumnType::Boolean => "BOOLEAN",
        ColumnType::Geometry => "GEOMETRY",
    }
}

#[inline]
pub(crate) fn column_type_from_str(column_type_str: &str) -> Option<ColumnType> {
    let s = column_type_str;
    if s.eq_ignore_ascii_case("TINYINT")
        || s.eq_ignore_ascii_case("SMALLINT")
        || s.eq_ignore_ascii_case("MEDIUMINT")
        || s.eq_ignore_ascii_case("INT")
        || s.eq_ignore_ascii_case("INTEGER")
    {
        Some(ColumnType::Integer)
    } else if s.eq_ignore_ascii_case("DOUBLE")
        || s.eq_ignore_ascii_case("FLOAT")
        || s.eq_ignore_ascii_case("REAL")
    {
        Some(ColumnType::Double)
    } else if s.eq_ignore_ascii_case("TEXT") {
        Some(ColumnType::Varchar)
    } else if s.eq_ignore_ascii_case("BOOLEAN") {
        Some(ColumnType::Boolean)
    } else if s.eq_ignore_ascii_case("BLOB") {
        Some(ColumnType::Geometry)
    } else if s.eq_ignore_ascii_case("GEOMETRY")
        || s.eq_ignore_ascii_case("POINT")
        || s.eq_ignore_ascii_case("LINESTRING")
        || s.eq_ignore_ascii_case("POLYGON")
        || s.eq_ignore_ascii_case("MULTIPOINT")
        || s.eq_ignore_ascii_case("MULTILINESTRING")
        || s.eq_ignore_ascii_case("MULTIPOLYGON")
        || s.eq_ignore_ascii_case("GEOMETRYCOLLECTION")
    {
        Some(ColumnType::Geometry)
    } else {
        None
    }
}
