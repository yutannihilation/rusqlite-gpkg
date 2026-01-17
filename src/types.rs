use crate::error::GpkgError;
use wkb::reader::{Dimension, GeometryType, Wkb};

/// Logical column types used by GeoPackage layers and DDL helpers.
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
pub enum ColumnType {
    /// Boolean value stored as an integer 0/1.
    Boolean,
    /// UTF-8 text column.
    Varchar,
    /// Floating point column (SQLite REAL).
    Double,
    /// Integer column (SQLite INTEGER).
    Integer,
    /// Geometry column stored as a GeoPackage BLOB.
    Geometry,
}

/// Column definition used when creating or describing layer properties.
#[derive(Clone, Debug)]
pub struct ColumnSpec {
    pub name: String,
    pub column_type: ColumnType,
}

/// Layer-wide metadata and property column definitions.
#[derive(Clone, Debug)]
pub struct LayerMetadata {
    pub primary_key_column: String,
    pub geometry_column: String,
    pub geometry_type: GeometryType,
    pub geometry_dimension: Dimension,
    pub srs_id: u32,
    pub other_columns: Vec<ColumnSpec>,
}

/// Owned dynamic value used for feature properties.
///
/// `Value` mirrors SQLite's dynamic types and is the primary property container
/// in this crate. Access is explicit: `GpkgFeature::property` returns
/// `Option<Value>`, and callers convert using `try_into()` or pattern matching.
///
/// Common conversions:
/// - Integers: `i64`, `i32`, `u64`, etc.
/// - Floats: `f64`, `f32`
/// - Text: `String`, `&str`
/// - Geometry: `wkb::reader::Wkb<'_>` from `Value::Geometry` or `Value::Blob`
///
/// ```no_run
/// use rusqlite_gpkg::Value;
///
/// let value = Value::Text("alpha".to_string());
/// let name: &str = (&value).try_into()?;
/// # Ok::<(), rusqlite_gpkg::GpkgError>(())
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Geometry(Vec<u8>), // we want to use Wkb struct here, but it requires a lifetime
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Text(value.to_string())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Text(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Integer(if value { 1 } else { 0 })
    }
}

macro_rules! impl_from_int {
    ($($t:ty),+ $(,)?) => {
        $(
            impl From<$t> for Value {
                #[inline]
                fn from(value: $t) -> Self {
                    Value::Integer(value as i64)
                }
            }
        )+
    };
}

macro_rules! impl_from_uint {
    ($($t:ty),+ $(,)?) => {
        $(
            impl From<$t> for Value {
                #[inline]
                fn from(value: $t) -> Self {
                    Value::Integer(value as i64)
                }
            }
        )+
    };
}

impl_from_int!(i8, i16, i32, i64, isize);
impl_from_uint!(u8, u16, u32, u64, usize);

impl From<f32> for Value {
    #[inline]
    fn from(value: f32) -> Self {
        Value::Real(value as f64)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Real(value)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

#[macro_export]
macro_rules! params {
    () => {
        &[]
    };
    ($($value:expr),+ $(,)?) => {
        &[$($crate::Value::from($value)),+]
    };
}

#[inline]
fn value_to_sql_output(value: &Value) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
    use rusqlite::types::{ToSqlOutput, ValueRef};

    let output = match value {
        Value::Null => ToSqlOutput::Borrowed(ValueRef::Null),
        Value::Integer(v) => ToSqlOutput::Borrowed(ValueRef::Integer(*v)),
        Value::Real(v) => ToSqlOutput::Borrowed(ValueRef::Real(*v)),
        Value::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
        Value::Blob(items) | Value::Geometry(items) => {
            ToSqlOutput::Borrowed(ValueRef::Blob(items.as_slice()))
        }
    };

    Ok(output)
}

impl rusqlite::ToSql for Value {
    #[inline]
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        value_to_sql_output(self)
    }
}

impl From<rusqlite::types::Value> for Value {
    #[inline]
    fn from(value: rusqlite::types::Value) -> Self {
        match value {
            rusqlite::types::Value::Null => Value::Null,
            rusqlite::types::Value::Integer(value) => Value::Integer(value),
            rusqlite::types::Value::Real(value) => Value::Real(value),
            rusqlite::types::Value::Text(value) => Value::Text(value),
            rusqlite::types::Value::Blob(value) => Value::Blob(value),
        }
    }
}

impl<'a> From<rusqlite::types::ValueRef<'a>> for Value {
    #[inline]
    fn from(value: rusqlite::types::ValueRef<'a>) -> Self {
        match value {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(value) => Value::Integer(value),
            rusqlite::types::ValueRef::Real(value) => Value::Real(value),
            rusqlite::types::ValueRef::Text(value) => {
                let s = std::str::from_utf8(value).expect("invalid UTF-8");
                Value::Text(s.to_string())
            }
            rusqlite::types::ValueRef::Blob(value) => Value::Blob(value.to_vec()),
        }
    }
}

impl From<Value> for rusqlite::types::Value {
    #[inline]
    fn from(value: Value) -> Self {
        match value {
            Value::Null => rusqlite::types::Value::Null,
            Value::Integer(value) => rusqlite::types::Value::Integer(value),
            Value::Real(value) => rusqlite::types::Value::Real(value),
            Value::Text(value) => rusqlite::types::Value::Text(value),
            Value::Blob(value) | Value::Geometry(value) => rusqlite::types::Value::Blob(value),
        }
    }
}

#[inline]
fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Integer(_) => "INTEGER",
        Value::Real(_) => "REAL",
        Value::Text(_) => "TEXT",
        Value::Blob(_) => "BLOB",
        Value::Geometry(_) => "GEOMETRY",
    }
}

#[inline]
fn invalid_type(expected: &'static str, value: &Value) -> GpkgError {
    GpkgError::Message(format!(
        "expected {expected}, got {}",
        value_type_name(value)
    ))
}

#[inline]
fn out_of_range(expected: &'static str) -> GpkgError {
    GpkgError::Message(format!("value out of range for {expected}"))
}

macro_rules! impl_try_from_int_ref {
    ($t:ty) => {
        impl TryFrom<&Value> for $t {
            type Error = GpkgError;

            #[inline]
            fn try_from(value: &Value) -> Result<Self, Self::Error> {
                match value {
                    Value::Integer(v) => {
                        <$t>::try_from(*v).map_err(|_| out_of_range(stringify!($t)))
                    }
                    _ => Err(invalid_type(stringify!($t), value)),
                }
            }
        }

        impl TryFrom<Value> for $t {
            type Error = GpkgError;

            #[inline]
            fn try_from(value: Value) -> Result<Self, Self::Error> {
                (&value).try_into()
            }
        }
    };
}

impl TryFrom<&Value> for i64 {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Integer(v) => Ok(*v),
            _ => Err(invalid_type("i64", value)),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl_try_from_int_ref!(i32);
impl_try_from_int_ref!(i16);
impl_try_from_int_ref!(i8);
impl_try_from_int_ref!(isize);
impl_try_from_int_ref!(u64);
impl_try_from_int_ref!(u32);
impl_try_from_int_ref!(u16);
impl_try_from_int_ref!(u8);
impl_try_from_int_ref!(usize);

impl TryFrom<&Value> for f64 {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Real(v) => Ok(*v),
            Value::Integer(v) => Ok(*v as f64),
            _ => Err(invalid_type("f64", value)),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&Value> for f32 {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Real(v) => Ok(*v as f32),
            Value::Integer(v) => Ok(*v as f32),
            _ => Err(invalid_type("f32", value)),
        }
    }
}

impl TryFrom<Value> for f32 {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&Value> for bool {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Integer(0) => Ok(false),
            Value::Integer(1) => Ok(true),
            _ => Err(invalid_type("bool", value)),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<Value> for String {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Text(s) => Ok(s),
            other => Err(invalid_type("String", &other)),
        }
    }
}

impl<'a> TryFrom<&'a Value> for &'a str {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
        match value {
            Value::Text(s) => Ok(s.as_str()),
            _ => Err(invalid_type("&str", value)),
        }
    }
}

impl<'a> TryFrom<&'a Value> for Wkb<'a> {
    type Error = GpkgError;

    #[inline]
    fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
        match value {
            Value::Geometry(bytes) => {
                return Ok(crate::gpkg::gpkg_geometry_to_wkb(bytes.as_slice())?);
            }
            Value::Blob(bytes) => {
                let bytes = bytes.as_slice();
                if bytes.len() >= 4 && bytes[0] == 0x47 && bytes[1] == 0x50 {
                    return Ok(crate::gpkg::gpkg_geometry_to_wkb(bytes)?);
                }
                return Ok(Wkb::try_new(bytes)?);
            }
            _ => return Err(invalid_type("Wkb", value)),
        }
    }
}
