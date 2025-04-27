use roaring::RoaringBitmap;

pub(crate) struct DeletionVector {
    /// Bitmap representing deleted rows.
    bitmap: RoaringBitmap,
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
    pub fn mark_deleted(&mut self, row: u32) {
        self.bitmap.insert(row);
    }

    /// Serializes the deletion vector into a byte vector.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // RoaringBitmap has a built-in serialize method
        self.bitmap.serialize_into(&mut bytes).unwrap();
        bytes
    }

    /// Deserializes a byte vector into a DeletionVector.
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        match RoaringBitmap::deserialize_from(data) {
            Ok(bitmap) => Ok(Self { bitmap }),
            Err(e) => Err(format!("Failed to deserialize DeletionVector: {}", e)),
        }
    }
}
