use crate::Value;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug)]
/// A single row from a GeoPackage attribute table (no geometry).
pub struct GpkgAttributeRow {
    pub(super) id: i64,
    pub(super) properties: Vec<Value>,
    pub(super) property_index_by_name: Rc<HashMap<String, usize>>,
}

impl GpkgAttributeRow {
    /// The primary key (rowid) of this row.
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Look up a property value by column name.
    pub fn property(&self, name: &str) -> Option<Value> {
        let idx = *self.property_index_by_name.get(name)?;
        Some(self.properties[idx].clone())
    }

    /// All property values in schema order.
    pub fn properties(&self) -> &[Value] {
        &self.properties
    }
}
