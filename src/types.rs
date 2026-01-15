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
