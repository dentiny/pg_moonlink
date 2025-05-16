use crate::error::Result;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow_array::builder::BooleanBuilder;
use roaring::RoaringTreemap;

/// A thin wrapper for roaring bitmap.
pub struct RoaringBitmapDV {
    bitmap: RoaringTreemap,
}

impl RoaringBitmapDV {
    pub fn new() -> Self {
        Self {
            bitmap: RoaringTreemap::new(),
        }
    }

    /// Return the row is successfully marked as "deleted".
    /// Return false if it's already deleted.
    pub fn delete_row(&mut self, row_idx: usize) -> bool {
        self.bitmap.insert(row_idx as u64)
    }

    /// Apply the deletion vector to filter a record batch
    pub fn apply_to_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let total_rows = batch.num_rows();
        let mut builder = BooleanBuilder::new();
        for i in 0..total_rows {
            builder.append_value(!self.bitmap.contains(i as u64));
        }
        let filter = builder.finish();
        let filter_batch = compute::filter_record_batch(batch, &filter)?;
        Ok(filter_batch)
    }

    /// Return whether the given row is deleted.
    pub fn is_deleted(&self, row_idx: usize) -> bool {
        self.bitmap.contains(row_idx as u64)
    }

    /// Return all active row indexes.
    pub fn collect_active_rows(&self, total_rows: usize) -> Vec<usize> {
        (0..total_rows)
            .filter(|i| !self.bitmap.contains(*i as u64))
            .collect()
    }

    /// Return all deleted row indexes.
    pub fn collect_deleted_rows(&self) -> Vec<u64> {
        self.bitmap.iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_delete_buffer() -> Result<()> {
        // Create a delete vector
        let mut buffer = RoaringBitmapDV::new();
        // Delete some rows
        buffer.delete_row(1);
        buffer.delete_row(3);

        // Check deletion status
        assert!(!buffer.is_deleted(0));
        assert!(buffer.is_deleted(1));
        assert!(!buffer.is_deleted(2));
        assert!(buffer.is_deleted(3));
        assert!(!buffer.is_deleted(4));

        // Create a test batch
        let name_array = Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E"])) as ArrayRef;
        let age_array = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                    "PARQUET:field_id".to_string(),
                    "1".to_string(),
                )])),
                Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
                    "PARQUET:field_id".to_string(),
                    "2".to_string(),
                )])),
            ])),
            vec![name_array, age_array],
        )?;

        // Apply deletion filter
        let filtered = buffer.apply_to_batch(&batch)?;

        // Check filtered batch
        assert_eq!(filtered.num_rows(), 3);
        let filtered_names = filtered
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let filtered_ages = filtered
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(filtered_names.value(0), "A");
        assert_eq!(filtered_names.value(1), "C");
        assert_eq!(filtered_names.value(2), "E");

        assert_eq!(filtered_ages.value(0), 10);
        assert_eq!(filtered_ages.value(1), 30);
        assert_eq!(filtered_ages.value(2), 50);

        Ok(())
    }

    #[test]
    fn test_into_iter() {
        // Create a delete vector
        let mut buffer = RoaringBitmapDV::new();

        // Before deletion all rows are active
        let active_rows: Vec<usize> = buffer.collect_active_rows(10);
        assert_eq!(active_rows, (0..10).collect::<Vec<_>>());

        // Delete rows 1, 3, and 8
        buffer.delete_row(1);
        buffer.delete_row(3);
        buffer.delete_row(8);

        // Check that the iterator returns those positions
        let active_rows: Vec<usize> = buffer.collect_active_rows(10);
        assert_eq!(active_rows, vec![0, 2, 4, 5, 6, 7, 9]);
    }

    #[test]
    fn test_apply_to_batch_with_deletions() {
        // Create a sample RecordBatch with 5 rows
        let values = Int32Array::from(vec![10, 20, 30, 40, 50]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();

        // Create a BatchDeletionVector for 5 rows
        let mut del_vec = RoaringBitmapDV::new();

        // Mark rows 1 and 3 (values 20 and 40) as deleted
        del_vec.delete_row(1);
        del_vec.delete_row(3);

        // Apply the deletion vector
        let filtered = del_vec.apply_to_batch(&batch).unwrap();

        // Extract the resulting array
        let filtered_values = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Should contain values: 10, 30, 50
        assert_eq!(filtered.num_rows(), 3);
        assert_eq!(filtered_values.value(0), 10);
        assert_eq!(filtered_values.value(1), 30);
        assert_eq!(filtered_values.value(2), 50);
    }

    #[test]
    fn test_apply_to_batch_without_deletions() {
        // Create a RecordBatch of 3 rows
        let values = Int32Array::from(vec![100, 200, 300]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();

        // No deletions
        let del_vec = RoaringBitmapDV::new();

        let filtered = del_vec.apply_to_batch(&batch).unwrap();

        let filtered_values = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Expect original values
        assert_eq!(filtered.num_rows(), 3);
        assert_eq!(filtered_values.value(0), 100);
        assert_eq!(filtered_values.value(1), 200);
        assert_eq!(filtered_values.value(2), 300);
    }
}
