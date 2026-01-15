use crate::Result;
use rusqlite::types::Value;

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
pub enum ColumnType {
    Boolean,
    Varchar,
    Double,
    Integer,
    Geometry,
}

#[derive(Clone, Debug)]
pub struct ColumnSpec {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Clone, Debug)]
pub struct ColumnSpecs {
    pub primary_key_column: String,
    pub geometry_column: String,
    pub other_columns: Vec<ColumnSpec>,
}

/// Supplies ordered property values for insert/update calls.
pub trait RusqliteValues {
    fn into_values(self) -> Result<Vec<Value>>;
}

/// Convert a single parameter into a rusqlite Value.
pub trait IntoRusqliteValue {
    fn into_value(self) -> Value;
}

impl IntoRusqliteValue for Value {
    fn into_value(self) -> Value {
        self
    }
}

impl IntoRusqliteValue for &str {
    fn into_value(self) -> Value {
        Value::Text(self.to_string())
    }
}

impl IntoRusqliteValue for String {
    fn into_value(self) -> Value {
        Value::Text(self)
    }
}

impl IntoRusqliteValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::Blob(self)
    }
}

impl IntoRusqliteValue for bool {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl IntoRusqliteValue for f32 {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl IntoRusqliteValue for f64 {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl IntoRusqliteValue for i8 {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl IntoRusqliteValue for i16 {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl IntoRusqliteValue for i32 {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl IntoRusqliteValue for i64 {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl IntoRusqliteValue for u8 {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl IntoRusqliteValue for u16 {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl IntoRusqliteValue for u32 {
    fn into_value(self) -> Value {
        Value::from(self)
    }
}

impl<T> IntoRusqliteValue for Option<T>
where
    T: IntoRusqliteValue,
{
    fn into_value(self) -> Value {
        match self {
            Some(value) => value.into_value(),
            None => Value::Null,
        }
    }
}

impl RusqliteValues for () {
    fn into_values(self) -> Result<Vec<Value>> {
        Ok(Vec::new())
    }
}

impl<T> RusqliteValues for Vec<T>
where
    T: IntoRusqliteValue,
{
    fn into_values(self) -> Result<Vec<Value>> {
        Ok(self
            .into_iter()
            .map(IntoRusqliteValue::into_value)
            .collect())
    }
}

impl<T> RusqliteValues for &[T]
where
    T: Clone + IntoRusqliteValue,
{
    fn into_values(self) -> Result<Vec<Value>> {
        Ok(self
            .iter()
            .cloned()
            .map(IntoRusqliteValue::into_value)
            .collect())
    }
}

macro_rules! value_params_array {
    ($($count:literal),+ $(,)?) => {
        $(
            impl<T> RusqliteValues for [T; $count]
            where
                T: IntoRusqliteValue,
            {
                fn into_values(self) -> Result<Vec<Value>> {
                    Ok(self.into_iter().map(IntoRusqliteValue::into_value).collect())
                }
            }
        )+
    }
}

value_params_array!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);

macro_rules! rusqlite_values_tuple {
    ($count:literal : $(($field:tt $ftype:ident)),* $(,)?) => {
        impl<$($ftype,)*> RusqliteValues for ($($ftype,)*) where $($ftype: IntoRusqliteValue,)* {
            fn into_values(self) -> Result<Vec<Value>> {
                Ok(vec![$(self.$field.into_value(),)*])
            }
        }
    }
}

rusqlite_values_tuple!(1: (0 A));
rusqlite_values_tuple!(2: (0 A), (1 B));
rusqlite_values_tuple!(3: (0 A), (1 B), (2 C));
rusqlite_values_tuple!(4: (0 A), (1 B), (2 C), (3 D));
rusqlite_values_tuple!(5: (0 A), (1 B), (2 C), (3 D), (4 E));
rusqlite_values_tuple!(6: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F));
rusqlite_values_tuple!(7: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G));
rusqlite_values_tuple!(8: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H));
rusqlite_values_tuple!(9: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I));
rusqlite_values_tuple!(10: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J));
rusqlite_values_tuple!(11: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K));
rusqlite_values_tuple!(12: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L));
rusqlite_values_tuple!(13: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M));
rusqlite_values_tuple!(14: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N));
rusqlite_values_tuple!(15: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O));
rusqlite_values_tuple!(16: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O), (15 P));
