use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
/// Items needed for iceberg snapshot.
use crate::storage::index::FileIndex as MooncakeFileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::TableManager;

use std::collections::HashMap;

#[derive(Debug)]
pub struct IcebergSnapshotPayload {
    /// Flush LSN.
    pub(crate) flush_lsn: u64,
    /// New data files to introduce to iceberg table.
    pub(crate) data_files: Vec<MooncakeDataFileRef>,
    /// Maps from data filepath to its latest deletion vector.
    pub(crate) new_deletion_vector: Vec<(MooncakeDataFileRef, BatchDeletionVector)>,
    /// All file indices which have been persisted locally.
    ///
    /// TODO(hjiang): It's ok to take only new moooncake file index, instead of all file indices.
    pub(crate) file_indices: Vec<MooncakeFileIndex>,
}

/// Return type of async iceberg snapshot creation.
pub(crate) struct IcebergSnapshotResult {
    /// Table manager is (1) not `Sync` safe; (2) only used at iceberg snapshot creation, so we `move` it around every snapshot.
    pub(crate) table_manager: Box<dyn TableManager>,
    /// Iceberg flush LSN.
    pub(crate) flush_lsn: u64,
    /// Persisted data files.
    pub(crate) new_data_files: Vec<MooncakeDataFileRef>,
    /// Persisted puffin blob reference.
    pub(crate) puffin_blob_ref: HashMap<MooncakeDataFileRef, PuffinBlobRef>,
}
