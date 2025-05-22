/// Items needed for iceberg snapshot.
use std::collections::HashMap;
use std::path::PathBuf;

use crate::storage::index::FileIndex as MooncakeFileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;

#[derive(Debug)]
pub struct IcebergSnapshotPayload {
    /// Flush LSN.
    pub(crate) flush_lsn: u64,
    /// New data files to introduce to iceberg table.
    pub(crate) data_files: Vec<PathBuf>,
    /// Maps from data filepath to its latest deletion vector.
    pub(crate) new_deletion_vector: HashMap<PathBuf, BatchDeletionVector>,
    /// All file indices which have been persisted locally.
    ///
    /// TODO(hjiang): It's ok to take only new moooncake file index, instead of all file indices.
    pub(crate) file_indices: Vec<MooncakeFileIndex>,
}
