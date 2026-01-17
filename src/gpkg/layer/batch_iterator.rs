use crate::Result;
use crate::gpkg::GpkgFeature;
use crate::types::ColumnSpec;
use rusqlite;
use std::collections::HashMap;
use std::sync::Arc;

use super::row_to_feature;

/// Iterator that yields batches of features from a layer.
///
/// Each call to `next()` returns a `Result<Vec<GpkgFeature>>` containing up to
/// `batch_size` features. This provides a chunked alternative to `features()`,
/// which always allocates a single vector for the whole layer.
pub struct GpkgFeatureBatchIterator<'a> {
    pub(super) stmt: rusqlite::Statement<'a>,
    pub(super) property_columns: Vec<ColumnSpec>,
    pub(super) geometry_column: String,
    pub(super) primary_key_column: String,
    pub(super) property_index_by_name: Arc<HashMap<String, usize>>,
    pub(super) batch_size: u32,
    pub(super) offset: u32,
    pub(super) end_or_invalid_state: bool,
}

impl<'a> Iterator for GpkgFeatureBatchIterator<'a> {
    type Item = Result<Vec<GpkgFeature>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end_or_invalid_state {
            return None;
        }

        let result = self.stmt.query_map([self.offset], |row| {
            row_to_feature(
                row,
                &self.property_columns,
                &self.geometry_column,
                &self.primary_key_column,
                &self.property_index_by_name,
            )
        });

        let collected_result = match result {
            Ok(mapped_rows) => mapped_rows.collect::<rusqlite::Result<Vec<GpkgFeature>>>(),
            Err(e) => {
                // I don't know in what case some error happens, but I bet it's unrecoverable.
                self.end_or_invalid_state = true;
                return Some(Err(e.into()));
            }
        };

        let features = match collected_result {
            Ok(features) => features,
            Err(e) => {
                // I don't know in what case some error happens, but I bet it's unrecoverable.
                self.end_or_invalid_state = true;
                return Some(Err(e.into()));
            }
        };

        // If the result is less than the batch size, it means it reached the end.
        let result_size = features.len();
        if result_size < self.batch_size as usize {
            self.end_or_invalid_state = true;
            if features.is_empty() {
                return None;
            }
        }

        self.offset += result_size as u32;

        Some(Ok(features))
    }
}

#[cfg(test)]
mod tests {
    use crate::Result;
    use crate::Value;
    use crate::gpkg::Gpkg;
    use crate::types::ColumnSpec;
    use geo_types::Point;
    use wkb::reader::GeometryType;

    fn assert_batch_iteration(total: usize, batch_size: u32) -> Result<()> {
        let gpkg = Gpkg::open_in_memory()?;
        let columns: Vec<ColumnSpec> = Vec::new();
        let layer = gpkg.create_layer(
            "batch_points",
            "geom",
            GeometryType::Point,
            wkb::reader::Dimension::Xy,
            4326,
            &columns,
        )?;

        for i in 0..total {
            layer.insert(Point::new(i as f64, i as f64), std::iter::empty::<&Value>())?;
        }

        let mut counts = Vec::new();
        for batch in layer.features_batch(batch_size)? {
            let features = batch?;
            counts.push(features.len());
        }

        let total_seen: usize = counts.iter().sum();
        assert_eq!(total_seen, total);

        if total == 0 {
            assert!(counts.is_empty());
            return Ok(());
        }

        let expected_full_batches = total / batch_size as usize;
        let expected_remainder = total % batch_size as usize;

        for (idx, count) in counts.iter().enumerate() {
            let is_last = idx == counts.len() - 1;
            if !is_last || expected_remainder == 0 {
                assert_eq!(*count, batch_size as usize);
            } else {
                assert_eq!(*count, expected_remainder);
            }
        }

        assert_eq!(
            counts.len(),
            expected_full_batches + if expected_remainder == 0 { 0 } else { 1 }
        );

        Ok(())
    }

    #[test]
    fn batch_iterator_handles_empty_layer() -> Result<()> {
        assert_batch_iteration(0, 3)?;
        Ok(())
    }

    #[test]
    fn batch_iterator_handles_smaller_than_batch() -> Result<()> {
        assert_batch_iteration(2, 5)?;
        Ok(())
    }

    #[test]
    fn batch_iterator_handles_exact_multiple() -> Result<()> {
        assert_batch_iteration(6, 3)?;
        Ok(())
    }

    #[test]
    fn batch_iterator_handles_remainder() -> Result<()> {
        assert_batch_iteration(7, 3)?;
        Ok(())
    }

    #[test]
    fn batch_iterator_handles_single_item_batches() -> Result<()> {
        assert_batch_iteration(4, 1)?;
        Ok(())
    }
}
