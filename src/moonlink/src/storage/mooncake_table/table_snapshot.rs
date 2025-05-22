/// Items needed for icebern snapshot.

use std::collections::HashMap;

use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;

pub(crate) struct IcebergSnapshotItem {
    /// Flush LSN.
    flush_lsn: u64,
    /// New data files to introduce to iceberg table.
    data_files: Vec<PathBuf>,
    /// Maps from data filepath to its latest deletion vector.
    new_deletion_vector: HashMap<PathBuf, BatchDeletionVector>,    
}
