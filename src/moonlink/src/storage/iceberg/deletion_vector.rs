use crate::storage::iceberg::puffin_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;

use std::collections::HashMap;

use iceberg::io::FileIO;
use iceberg::puffin::Blob;
use iceberg::spec::DataFile;
use iceberg::{Error as IcebergError, Result as IcebergResult};
use roaring::RoaringBitmap;

// Magic bytes for deletion vector for puffin file.
const DELETION_VECTOR_MAGIC_BYTES: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

// Min length for serialized blob for deletion vector.
const MIN_SERIALIZED_DELETION_VECTOR_BLOB: usize = 12;

// Deletion vector puffin blob properties which must be contained.
pub(crate) static DELETION_VECTOR_CADINALITY: &str = "cardinality";
pub(crate) static DELETION_VECTOR_REFERENCED_DATA_FILE: &str = "referenced-data-file";

// Max number of rows in a batch. Use to convert puffin deletion vector to moonlink batch delete vector.
// TODO(hjiang): Confirm max batch size when integrate iceberg system with moonlink.
const HARD_CODE_DELETE_VECTOR_MAX_ROW: usize = 4096;

pub(crate) struct DeletionVector {
    /// Bitmap representing deleted rows.
    pub(crate) bitmap: RoaringBitmap,
}

// TODO(hjiang): Ideally moonlink doesn't need to operate on `Blob` directly, iceberg-rust should provide high-level interface to operate on deleted rows, but before it's supported officially, we use this hacky way to construct a `iceberg::puffin::blob::Blob`.
#[allow(dead_code)]
struct IcebergBlobProxy {
    pub(crate) r#type: String,
    pub(crate) fields: Vec<i32>,
    pub(crate) snapshot_id: i64,
    pub(crate) sequence_number: i64,
    pub(crate) data: Vec<u8>,
    pub(crate) properties: HashMap<String, String>,
}

// TODO(hjiang): Current we can only take row index as u32, should implement a u64 version.
impl DeletionVector {
    /// Creates a new empty deletion vector.
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }

    /// Marks a row as deleted.
    pub fn mark_rows_deleted(&mut self, rows: Vec<usize>) {
        let rows_as_u32: Vec<u32> = rows
            .into_iter()
            .map(|x| {
                assert!(
                    x <= u32::MAX as usize,
                    "Row index is larger than max value of u32"
                );
                x as u32
            })
            .collect();
        self.bitmap.extend(rows_as_u32);
    }

    /// Serializes the deletion vector into a byte vector.
    fn serialize_roaring_bitmap(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.bitmap.serialize_into(&mut bytes).unwrap();
        bytes
    }

    /// Deserializes a byte vector into a DeletionVector.
    fn deserialize_roaring_map(data: &[u8]) -> IcebergResult<Self> {
        RoaringBitmap::deserialize_from(data)
            .map(|bitmap| Self { bitmap })
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to deserialize DeletionVector: {}", e),
                )
            })
    }

    /// Sanity check required blob properties have been properly set.
    fn check_properties(properties: &HashMap<String, String>) {
        assert!(
            properties.contains_key(DELETION_VECTOR_CADINALITY),
            "Deletion vector blob properties should contain {}",
            DELETION_VECTOR_CADINALITY
        );
        assert!(
            properties.contains_key(DELETION_VECTOR_REFERENCED_DATA_FILE),
            "Deletion vector blob properties should contain {}",
            DELETION_VECTOR_REFERENCED_DATA_FILE
        );
    }

    /// Serialize the deletion vector into `IcebergBlobProxy` to write to puffin files.
    ///
    /// Serialization storage format:
    /// | len for magic and vector | magic | vector | crc32c |
    /// crc32c field is checksum of the magic bytes and serialized vector as 4 bytes in big-endian.
    pub fn serialize(&self, properties: HashMap<String, String>) -> Blob {
        DeletionVector::check_properties(&properties);

        let serialized_bitmap = self.serialize_roaring_bitmap();

        // Calculate combined length (magic bytes + bitmap).
        let combined_length = (DELETION_VECTOR_MAGIC_BYTES.len() + serialized_bitmap.len()) as u32;

        // Create a buffer to hold all the data.
        let mut data = Vec::with_capacity(
            std::mem::size_of_val(&combined_length) + // length
            DELETION_VECTOR_MAGIC_BYTES.len() + // magic sequence
            serialized_bitmap.len() + // serialized roaring bitmap
            4, // crc
        );

        // Write combined length.
        data.extend_from_slice(&combined_length.to_be_bytes());

        // Write magic bytes.
        data.extend_from_slice(&DELETION_VECTOR_MAGIC_BYTES);

        // Write serialized bitmap.
        data.extend_from_slice(&serialized_bitmap);

        // Calculate CRC (magic bytes + serialized bitmap).
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&DELETION_VECTOR_MAGIC_BYTES);
        hasher.update(&serialized_bitmap);
        let crc = hasher.finalize();

        // Write CRC.
        data.extend_from_slice(&crc.to_be_bytes());

        let blob_proxy = IcebergBlobProxy {
            r#type: "deletion-vector-v1".to_string(),
            fields: vec![],
            snapshot_id: 0, // TODO: Set appropriate values, we should pass in TableMetadata here.
            sequence_number: 0, // TODO: Set appropriate values, we should pass in TableMetadata here.
            data,
            properties,
        };
        unsafe { std::mem::transmute::<IcebergBlobProxy, Blob>(blob_proxy) }
    }

    /// Deserialize from `IcebergBlobProxy` to deletion vector.
    pub fn deserialize(blob: Blob) -> IcebergResult<Self> {
        let blob_proxy = unsafe { std::mem::transmute::<Blob, IcebergBlobProxy>(blob) };
        let data = &blob_proxy.data;

        // Minimum length for serialized blob is 12 bytes (4 length + 4 magic + 4 crc).
        if data.len() < MIN_SERIALIZED_DELETION_VECTOR_BLOB {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                "Serialized deletion vector blob should be at least 12 bytes.".to_string(),
            ));
        }

        // Check magic bytes.
        let magic_in_data = &data[4..8];
        if magic_in_data != DELETION_VECTOR_MAGIC_BYTES {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                "Data corruption detected for serialized deletion vector blob.".to_string(),
            ));
        }

        // Check combined length.
        let combined_length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if std::mem::size_of_val(&combined_length) + (combined_length as usize) + 4 != data.len() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "Serialized deletion vector blob length mismatch: expected {}, actual {}",
                    std::mem::size_of_val(&combined_length) + (combined_length as usize) + 4, /*crc32c*/
                    data.len()
                ),
            ));
        }

        // The rest between magic bytes and CRC is the serialized bitmap.
        let bitmap_data_start = 8;
        let bitmap_data_end = data.len() - 4;
        let bitmap_data = &data[bitmap_data_start..bitmap_data_end];

        // Check CRC.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&DELETION_VECTOR_MAGIC_BYTES);
        hasher.update(bitmap_data);
        let expected_crc = hasher.finalize();

        let stored_crc = u32::from_be_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]);

        if expected_crc != stored_crc {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!("Within serialized deletion vector blob persisted crc32c is {expected_crc}, actual crc32c is {stored_crc}."),
            ));
        }

        // Deserialize the bitmap.
        DeletionVector::deserialize_roaring_map(bitmap_data)
    }

    /// Load deletion vector from puffin file blob.
    ///
    /// TODO(hjiang): Add unit test for load blob from local filesystem.
    pub async fn load_from_dv_blob(file_io: FileIO, puffin_file: &DataFile) -> IcebergResult<Self> {
        let blob = puffin_utils::load_blob_from_puffin_file(file_io, puffin_file).await?;
        DeletionVector::deserialize(blob)
    }

    /// Convert self to `BatchDeletionVector`, after which self ownership is terminated.
    pub fn take_as_batch_delete_vector(self) -> BatchDeletionVector {
        let mut batch_delete_vector = BatchDeletionVector::new(HARD_CODE_DELETE_VECTOR_MAX_ROW);
        for row_idx in self.bitmap.iter() {
            batch_delete_vector.delete_row(row_idx as usize);
        }
        batch_delete_vector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_blob_properties(deleted_rows: usize) -> HashMap<String, String> {
        let mut properties = HashMap::new();
        properties.insert(
            DELETION_VECTOR_CADINALITY.to_string(),
            deleted_rows.to_string(),
        );
        properties.insert(
            DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(),
            "/tmp/iceberg/data/filename".to_string(),
        );
        properties
    }

    #[test]
    fn test_empty_deletion_vector() {
        let dv = DeletionVector::new();
        let blob = dv.serialize(create_test_blob_properties(/*deleted_rows=*/ 0));
        let deserialized_dv = DeletionVector::deserialize(blob).unwrap();
        assert!(dv.bitmap.is_empty());
        assert!(deserialized_dv.bitmap.is_empty());
    }

    #[test]
    fn test_mark_and_serialize_deserialize_deletion_vector() {
        let mut dv = DeletionVector::new();
        let deleted_rows = vec![1, 3, 5, 7, 1000];
        dv.mark_rows_deleted(deleted_rows.clone());
        let blob = dv.serialize(create_test_blob_properties(
            /*deleted_rows=*/ deleted_rows.len(),
        ));
        let deserialized_dv = DeletionVector::deserialize(blob).unwrap();
        for row in deleted_rows {
            assert!(deserialized_dv.bitmap.contains(row as u32));
        }
        assert_eq!(dv.bitmap.len(), deserialized_dv.bitmap.len());
    }
}
