/// iceberg-rust `Blob` type is not publicly accessible, so we make a proxy type and reinterpret.
use std::collections::HashMap;

#[allow(dead_code)]
pub(crate) struct IcebergBlobProxy {
    pub(crate) r#type: String,
    pub(crate) fields: Vec<i32>,
    pub(crate) snapshot_id: i64,
    pub(crate) sequence_number: i64,
    pub(crate) data: Vec<u8>,
    pub(crate) properties: HashMap<String, String>,
}
