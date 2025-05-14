use std::path::PathBuf;
use std::sync::Arc;

use crate::row::MoonlinkRow;
use super::mooncake_table::delete_vector::BatchDeletionVector;

// UNDONE(UPDATE_DELETE): a better way to handle file ids
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileId(pub(crate) Arc<PathBuf>);

#[derive(Debug, Clone, PartialEq)]
pub enum RecordLocation {
    /// Record is in a memory batch
    /// (batch_id, row_offset)
    MemoryBatch(u64, usize),

    /// Record is in a disk file
    /// (file_id, row_offset)
    DiskFile(FileId, usize),
}

#[derive(Debug)]
pub struct RawDeletionRecord {
    pub(crate) lookup_key: u64,
    pub(crate) row_identity: Option<MoonlinkRow>,
    pub(crate) pos: Option<(u64, usize)>,
    pub(crate) lsn: u64,
}

#[derive(Clone, Debug)]
pub struct ProcessedDeletionRecord {
    pub(crate) _lookup_key: u64,
    pub(crate) pos: RecordLocation,
    pub(crate) lsn: u64,
}

impl From<RecordLocation> for (u64, usize) {
    fn from(val: RecordLocation) -> Self {
        match val {
            RecordLocation::MemoryBatch(batch_id, row_offset) => (batch_id, row_offset),
            _ => panic!("Cannot convert RecordLocation to (u64, usize)"),
        }
    }
}

impl From<(u64, usize)> for RecordLocation {
    fn from(value: (u64, usize)) -> Self {
        RecordLocation::MemoryBatch(value.0, value.1)
    }
}

/// TODO(hjiang): Need to pass down mem slice size.
/// Aggregate all committed deletion logs into <data file, batch deletion vector>, whose LSN is less or equal to [`lsn`].
pub(crate) fn aggregate_committed_deletion_logs(committed_deletion_logs: &Vec<ProcessedDeletionRecord>, lsn: u64) 
    -> std::collections::HashMap<PathBuf, BatchDeletionVector> {
    let mut aggregated_deletion_logs = std::collections::HashMap::new();
    for cur_deletion_log in committed_deletion_logs.iter() {
        if cur_deletion_log.lsn > lsn {
            continue;
        }
        if let RecordLocation::DiskFile(file_id, row_idx) = &cur_deletion_log.pos {
            let filepath = (*file_id.0).clone();
            let deletion_vector = aggregated_deletion_logs.entry(filepath).or_insert_with(|| BatchDeletionVector::new(1000));
            assert!(deletion_vector.delete_row(*row_idx));
        }
    }
    aggregated_deletion_logs
}
