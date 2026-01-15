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
    pub primary_key: String,
    pub other_columns: Vec<ColumnSpec>,
}

/// Supplies ordered property values for insert/update calls.
pub trait RusqliteValues {
    fn into_values(self) -> Result<Vec<Value>>;
}

impl RusqliteValues for () {
    fn into_values(self) -> Result<Vec<Value>> {
        Ok(Vec::new())
    }
}

impl<T> RusqliteValues for Vec<T>
where
    T: Into<Value>,
{
    fn into_values(self) -> Result<Vec<Value>> {
        Ok(self.into_iter().map(Into::into).collect())
    }
}

impl<T> RusqliteValues for &[T]
where
    T: Clone + Into<Value>,
{
    fn into_values(self) -> Result<Vec<Value>> {
        Ok(self.iter().cloned().map(Into::into).collect())
    }
}

macro_rules! value_params_array {
    ($($count:literal),+ $(,)?) => {
        $(
            impl<T> RusqliteValues for [T; $count]
            where
                T: Into<Value>,
            {
                fn into_values(self) -> Result<Vec<Value>> {
                    Ok(self.into_iter().map(Into::into).collect())
                }
            }
        )+
    }
}

value_params_array!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);

macro_rules! rusqlite_values_tuple {
    ($count:literal : $(($field:tt $ftype:ident)),* $(,)?) => {
        impl<$($ftype,)*> RusqliteValues for ($($ftype,)*) where $($ftype: Into<Value>,)* {
            fn into_values(self) -> Result<Vec<Value>> {
                Ok(vec![$(self.$field.into(),)*])
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
