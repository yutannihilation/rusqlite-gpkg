use std::sync::Arc;
use std::sync::Mutex;

use crate::gpkg::GpkgDataSource;

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
#[repr(C)]
pub struct ColumnSpec {
    pub name: String,
    pub column_type: ColumnType,
}

#[repr(C)]
pub struct GpkgBindData {
    pub sources: Vec<GpkgDataSource>,
    pub column_specs: Vec<ColumnSpec>,
}

pub struct Cursor {
    pub source_idx: usize,
    pub offset: usize,
}

#[repr(C)]
pub struct StReadMultiInitData {
    pub cursor: Arc<Mutex<Cursor>>,
}

impl Cursor {
    pub fn new() -> Self {
        Self {
            source_idx: 0,
            offset: 0,
        }
    }
}
