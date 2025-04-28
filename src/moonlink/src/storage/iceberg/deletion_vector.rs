use iceberg::{Error as IcebergError, Result as IcebergResult};
use roaring::RoaringBitmap;
use std::collections::HashMap;

// Magic bytes for deletion vector for puffin file.
#[allow(dead_code)]
const DELETION_VECTOR_MAGIC_BYTES: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

// Min length for serialized blob for deletion vector.
#[allow(dead_code)]
const MIN_SERIALIZED_DELETION_VECTOR_BLOB: usize = 12;

#[allow(dead_code)]
pub(crate) struct DeletionVector {
    /// Bitmap representing deleted rows.
    bitmap: RoaringBitmap,
}

// TODO(hjiang): Ideally moonlink doesn't need to operate on `Blob` directly, iceberg-rust should provide high-level interface to operate on deleted rows, but before it's supported officially, we use this hacky way to construct a `iceberg::puffin::blob::Blob`.
#[allow(dead_code)]
pub struct IcebergBlobProxy {
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
        let rows_as_u32: Vec<u32> = rows.into_iter().map(|x| x as u32).collect();
        self.bitmap.extend(rows_as_u32);
    }

    /// Serializes the deletion vector into a byte vector.
    fn _serialize_roaring_bitmap(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.bitmap.serialize_into(&mut bytes).unwrap();
        bytes
    }

    /// Deserializes a byte vector into a DeletionVector.
    fn _deserialize_roaring_map(data: &[u8]) -> IcebergResult<Self> {
        RoaringBitmap::deserialize_from(data)
            .map(|bitmap| Self { bitmap })
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to deserialize DeletionVector: {}", e),
                )
            })
    }

    /// Serialize the deletion vector into `IcebergBlobProxy` to write to puffin files.
    ///
    /// Serialization storage format:
    /// | len for magic and vector | magic | vector | crc32c |
    pub fn _serialize(&self) -> IcebergBlobProxy {
        let serialized_bitmap = self._serialize_roaring_bitmap();

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

        IcebergBlobProxy {
            r#type: "deletion-vector-v1".to_string(),
            fields: vec![],
            snapshot_id: 0, // TODO: Set appropriate values, we should pass in TableMetadata here.
            sequence_number: 0, // TODO: Set appropriate values, we should pass in TableMetadata here.
            data,
            properties: HashMap::new(),
        }
    }

    /// Deserialize from `IcebergBlobProxy` to deletion vector.
    pub fn _deserialize(blob: IcebergBlobProxy) -> IcebergResult<Self> {
        let data = &blob.data;

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
        DeletionVector::_deserialize_roaring_map(bitmap_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_deletion_vector() {
        let dv = DeletionVector::new();
        let blob = dv._serialize();
        let deserialized_dv = DeletionVector::_deserialize(blob).unwrap();
        assert!(dv.bitmap.is_empty());
        assert!(deserialized_dv.bitmap.is_empty());
    }

    #[test]
    fn test_mark_and_serialize_deserialize_deletion_vector() {
        let mut dv = DeletionVector::new();
        let deleted_rows = vec![1, 3, 5, 7, 1000];
        dv.mark_rows_deleted(deleted_rows.clone());
        let blob = dv._serialize();
        let deserialized_dv = DeletionVector::_deserialize(blob).unwrap();
        for row in deleted_rows {
            assert!(deserialized_dv.bitmap.contains(row as u32));
        }
        assert_eq!(dv.bitmap.len(), deserialized_dv.bitmap.len());
    }
}
