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
    pub primary_key: bool,
}
