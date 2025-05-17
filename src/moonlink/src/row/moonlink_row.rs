use super::moonlink_type::RowValue;
use ahash::AHasher;
use arrow::array::Array;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use std::hash::{Hash, Hasher};
use std::mem::take;

#[derive(Debug)]
pub struct MoonlinkRow {
    pub values: Vec<RowValue>,
}

impl MoonlinkRow {
    pub fn new(values: Vec<RowValue>) -> Self {
        Self { values }
    }

    /// Returns an iterator over values for columns included in the projection mask
    fn projected_iter<'a>(
        &'a self,
        projection_mask: &'a ProjectionMask,
    ) -> impl Iterator<Item = &'a RowValue> + 'a {
        let col_num = self.values.len();
        (0..col_num)
            .filter(move |&idx| projection_mask.leaf_included(idx))
            .map(move |idx| &self.values[idx])
    }

    pub fn equals_record_batch_at_offset(
        &self,
        batch: &RecordBatch,
        offset: usize,
        identity: &IdentityProp,
    ) -> bool {
        self.equals_record_batch_at_offset_impl(batch, offset, identity, /*proj_mask=*/ None)
    }

    fn equals_record_batch_at_offset_impl(
        &self,
        batch: &RecordBatch,
        offset: usize,
        identity: &IdentityProp,
        proj_mask: Option<ProjectionMask>,
    ) -> bool {
        if offset >= batch.num_rows() {
            panic!("Offset is out of bounds");
        }

        // Helper closure to compare a value with a column at offset
        let value_matches_column = |value: &RowValue, column: &arrow::array::ArrayRef| -> bool {
            match value {
                RowValue::Int32(v) => {
                    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int32Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Int64(v) => {
                    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Float32(v) => {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::Float32Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Float64(v) => {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::Float64Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Decimal(v) => {
                    if let Some(array) = column
                        .as_any()
                        .downcast_ref::<arrow::array::Decimal128Array>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::Bool(v) => {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::BooleanArray>()
                    {
                        array.value(offset) == *v
                    } else {
                        false
                    }
                }
                RowValue::ByteArray(v) => {
                    if let Some(array) = column.as_any().downcast_ref::<arrow::array::BinaryArray>()
                    {
                        array.value(offset) == v.as_slice()
                    } else if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        array.value(offset).as_bytes() == v.as_slice()
                    } else {
                        false
                    }
                }
                RowValue::FixedLenByteArray(v) => {
                    if let Some(array) = column
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                    {
                        array.value(offset) == v.as_slice()
                    } else {
                        false
                    }
                }
                RowValue::Array(_) => {
                    panic!("Array not supported");
                }
                RowValue::Struct(_) => {
                    panic!("Struct not supported");
                }
                RowValue::Null => column.is_null(offset),
            }
        };

        if let IdentityProp::Keys(keys) = identity {
            if keys.len() != batch.columns().len() {
                return self
                    .values
                    .iter()
                    .zip(keys.iter())
                    .all(|(value, &col_idx)| {
                        let column = batch.column(col_idx);
                        value_matches_column(value, column)
                    });
            }
        }

        if let Some(mask) = proj_mask {
            self.projected_iter(&mask)
                .zip(batch.columns())
                .all(|(value, column)| value_matches_column(value, column))
        } else {
            self.values
                .iter()
                .zip(batch.columns())
                .all(|(value, column)| value_matches_column(value, column))
        }
    }

    pub async fn equals_parquet_at_offset(
        &self,
        file_name: &str,
        offset: usize,
        identity: &IdentityProp,
    ) -> bool {
        let file = tokio::fs::File::open(file_name).await.unwrap();
        let stream_builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
        let row_groups = stream_builder.metadata().row_groups();
        let mut target_row_group = 0;
        let mut row_count: usize = 0;
        for row_group in row_groups {
            if row_count + row_group.num_rows() as usize > offset {
                break;
            }
            row_count += row_group.num_rows() as usize;
            target_row_group += 1;
        }
        let proj_mask = ProjectionMask::roots(
            stream_builder.metadata().file_metadata().schema_descr(),
            identity.get_key_indices(self.values.len()),
        );
        let mut reader = stream_builder
            .with_row_groups(vec![target_row_group])
            .with_offset(offset - row_count)
            .with_limit(1)
            .with_batch_size(1)
            .with_projection(proj_mask.clone())
            .build()
            .unwrap();
        let mut batch_reader = reader.next_row_group().await.unwrap().unwrap();
        let batch = batch_reader.next().unwrap().unwrap();

        println!("batch = {:?}", batch);

        self.equals_record_batch_at_offset_impl(&batch, 0, identity, Some(proj_mask))
    }
}

impl MoonlinkRow {
    pub fn equals_full_row(&self, other: &Self, identity: &IdentityProp) -> bool {
        match identity {
            IdentityProp::Keys(keys) => {
                assert_eq!(self.values.len(), keys.len());
                self.values
                    .iter()
                    .eq(keys.iter().map(|k| &other.values[*k]))
            }
            IdentityProp::FullRow => self.values.iter().eq(other.values.iter()),
            IdentityProp::SinglePrimitiveKey(_) => {
                panic!("Never required for equality checkx")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityProp {
    SinglePrimitiveKey(usize),
    Keys(Vec<usize>),
    FullRow,
}

impl IdentityProp {
    pub fn new_key(columns: Vec<usize>, fields: &[Field]) -> Self {
        if columns.len() == 1 {
            let width = fields[columns[0]].data_type().primitive_width();
            if width == Some(1) || width == Some(2) || width == Some(4) || width == Some(8) {
                IdentityProp::SinglePrimitiveKey(columns[0])
            } else {
                IdentityProp::Keys(columns)
            }
        } else {
            IdentityProp::Keys(columns)
        }
    }

    /// Get columns for the current identity property.
    pub fn get_key_indices(&self, col_num: usize) -> Vec<usize> {
        match self {
            IdentityProp::SinglePrimitiveKey(index) => vec![*index],
            IdentityProp::Keys(key_indices) => key_indices.clone(),
            IdentityProp::FullRow => (0..col_num).collect(),
        }
    }

    pub fn extract_identity_columns(&self, mut row: MoonlinkRow) -> Option<MoonlinkRow> {
        match self {
            IdentityProp::SinglePrimitiveKey(_) => None,
            IdentityProp::Keys(keys) => {
                let mut identity_columns = Vec::with_capacity(keys.len());
                for key in keys {
                    identity_columns.push(take(&mut row.values[*key]));
                }
                Some(MoonlinkRow::new(identity_columns))
            }
            IdentityProp::FullRow => Some(row),
        }
    }

    pub fn extract_identity_for_key(&self, row: &MoonlinkRow) -> Option<MoonlinkRow> {
        if let IdentityProp::Keys(keys) = self {
            let mut identity_columns = Vec::with_capacity(keys.len());
            for key in keys {
                identity_columns.push(row.values[*key].clone());
            }
            Some(MoonlinkRow::new(identity_columns))
        } else {
            None
        }
    }

    pub fn get_lookup_key(&self, row: &MoonlinkRow) -> u64 {
        match self {
            IdentityProp::SinglePrimitiveKey(key) => row.values[*key].to_u64_key(),
            IdentityProp::Keys(keys) => {
                let mut hasher = AHasher::default();
                for key in keys {
                    row.values[*key].hash(&mut hasher);
                }
                hasher.finish()
            }
            IdentityProp::FullRow => {
                let mut hasher = AHasher::default();
                for value in row.values.iter() {
                    value.hash(&mut hasher);
                }
                hasher.finish()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowValue;

    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use arrow_array::{Int32Array, Int64Array};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use tempfile::tempdir;

    impl Clone for MoonlinkRow {
        fn clone(&self) -> Self {
            MoonlinkRow::new(self.values.clone())
        }
    }

    #[test]
    fn test_equals_full_row_with_identity() {
        use RowValue::*;

        let row1 = MoonlinkRow::new(vec![Int32(1), Float32(2.0), ByteArray(b"abc".to_vec())]);
        let row2 = MoonlinkRow::new(vec![Int32(1), Float32(2.0), ByteArray(b"abc".to_vec())]);
        let row3 = MoonlinkRow::new(vec![Int32(1), Float32(2.0), ByteArray(b"def".to_vec())]);
        let row4 = MoonlinkRow::new(vec![
            Int32(1),
            Float32(2.0),
            Int32(3), // different type
        ]);

        // Identity using all columns
        let identity_all = IdentityProp::FullRow;
        // Identity using only the first column
        let identity_first = IdentityProp::Keys(vec![0]);

        // All columns: identity_row and full_row
        let id_row1_all = identity_all.extract_identity_columns(row1.clone()).unwrap();

        assert!(id_row1_all.equals_full_row(&row2, &identity_all));
        assert!(!id_row1_all.equals_full_row(&row3, &identity_all));
        assert!(!id_row1_all.equals_full_row(&row4, &identity_all));

        // Only first column matters: all rows should be equal
        let id_row1_first = identity_first
            .extract_identity_columns(row1.clone())
            .unwrap();

        assert!(id_row1_first.equals_full_row(&row2, &identity_first));
        assert!(id_row1_first.equals_full_row(&row3, &identity_first));
        assert!(id_row1_first.equals_full_row(&row4, &identity_first));

        // You can also check that the identity_row equals its own full row
        assert!(id_row1_all.equals_full_row(&row1, &identity_all));
        assert!(id_row1_first.equals_full_row(&row1, &identity_first));
    }

    #[tokio::test]
    async fn test_equals_parquet_at_offset() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int64, false),
        ]));
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("output.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        // Create moonlink row to match against, which matches the second row in the parquet file.
        let row = MoonlinkRow::new(vec![RowValue::Int32(2), RowValue::Int64(20)]);

        // Check cases with full row as identity property.
        assert!(
            row.equals_parquet_at_offset(
                file_path.to_str().unwrap(),
                /*offset=*/ 1,
                &IdentityProp::FullRow
            )
            .await
        );
        assert!(
            !row.equals_parquet_at_offset(
                file_path.to_str().unwrap(),
                /*offset=*/ 0,
                &IdentityProp::FullRow
            )
            .await
        );

        // Check cases with single primary key as identity propety.
        assert!(
            row.equals_parquet_at_offset(
                file_path.to_str().unwrap(),
                /*offset=*/ 1,
                &IdentityProp::SinglePrimitiveKey(1)
            )
            .await
        );
        assert!(
            !row.equals_parquet_at_offset(
                file_path.to_str().unwrap(),
                /*offset=*/ 0,
                &IdentityProp::SinglePrimitiveKey(1)
            )
            .await
        );

        // Check cases with specified keys.
        assert!(
            row.equals_parquet_at_offset(
                file_path.to_str().unwrap(),
                /*offset=*/ 1,
                &IdentityProp::Keys(vec![1])
            )
            .await
        );
        assert!(
            !row.equals_parquet_at_offset(
                file_path.to_str().unwrap(),
                /*offset=*/ 0,
                &IdentityProp::Keys(vec![1])
            )
            .await
        );
    }
}
