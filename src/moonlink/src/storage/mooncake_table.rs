mod data_batches;
pub(crate) mod delete_vector;
mod disk_slice;
mod mem_slice;
mod shared_array;
mod snapshot;

use super::iceberg::iceberg_table_manager::IcebergTableConfig;
use super::iceberg::puffin_utils::PuffinBlobRef;
use super::index::{FileIndex, MemIndex, MooncakeIndex};
use super::storage_utils::{RawDeletionRecord, RecordLocation};
use crate::error::{Error, Result};
use crate::row::{IdentityProp, MoonlinkRow};
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
use futures::executor::block_on;
use std::collections::HashMap;
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use delete_vector::BatchDeletionVector;
pub(crate) use disk_slice::DiskSliceWriter;
use mem_slice::MemSlice;
pub(crate) use snapshot::{PuffinDeletionBlobAtRead, SnapshotTableState};
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub struct TableConfig {
    /// Number of batch records which decides when to flush records from MemSlice to disk.
    pub mem_slice_size: usize,
    /// Max number of rows in MemSlice.
    pub batch_size: usize,
    /// Number of new data files to trigger an iceberg snapshot.
    pub iceberg_snapshot_new_data_file_count: usize,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl TableConfig {
    #[cfg(debug_assertions)]
    const DEFAULT_MEM_SLICE_SIZE: usize = 4 * 16;
    #[cfg(debug_assertions)]
    const DEFAULT_BATCH_SIZE: usize = 4;
    #[cfg(debug_assertions)]
    const DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT: usize = 1;

    #[cfg(not(debug_assertions))]
    const DEFAULT_MEM_SLICE_SIZE: usize = 2048 * 16;
    #[cfg(not(debug_assertions))]
    const DEFAULT_BATCH_SIZE: usize = 2048;
    #[cfg(not(debug_assertions))]
    const DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT: usize = 1;

    pub fn new() -> Self {
        Self {
            mem_slice_size: Self::DEFAULT_MEM_SLICE_SIZE,
            batch_size: Self::DEFAULT_BATCH_SIZE,
            iceberg_snapshot_new_data_file_count: Self::DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT,
        }
    }
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
    pub fn iceberg_snapshot_new_data_file_count(&self) -> usize {
        self.iceberg_snapshot_new_data_file_count
    }
}

#[derive(Debug)]
pub struct TableMetadata {
    /// table name
    pub(crate) name: String,
    /// table id
    pub(crate) id: u64,
    /// table schema
    pub(crate) schema: Arc<Schema>,
    /// table config
    pub(crate) config: TableConfig,
    /// storage path
    pub(crate) path: PathBuf,
    /// function to get lookup key from row
    pub(crate) identity: IdentityProp,
}

#[derive(Clone, Debug)]
pub(crate) struct DiskFileDeletionVector {
    /// In-memory deletion vector, used for new deletion records process in-memory.
    pub(crate) batch_deletion_vector: BatchDeletionVector,
    /// Persisted iceberg deletion vector puffin blob.
    pub(crate) puffin_deletion_blob: Option<PuffinBlobRef>,
}

/// Snapshot contains state of the table at a given time.
/// A snapshot maps directly to an iceberg snapshot.
///
pub struct Snapshot {
    /// table metadata
    pub(crate) metadata: Arc<TableMetadata>,
    /// datafile and their deletion vector.
    /// TODO(hjiang): For the initial release and before we figure out a cache design, disk files are always local ones.
    pub(crate) disk_files: HashMap<PathBuf, DiskFileDeletionVector>,
    /// Current snapshot version, which is the mooncake table commit point.
    pub(crate) snapshot_version: u64,
    /// LSN which last data file flush operation happens.
    ///
    /// There're two important time points: commit and flush.
    /// - Data files are persisted at flush point, which could span across multiple commit points;
    /// - Batch deletion vector, which is the value for `Snapshot::disk_files` updates at commit points.
    ///   So likely they are not consistent from LSN's perspective.
    ///
    /// At iceberg snapshot creation, we should only dump consistent data files and deletion logs.
    /// Data file flush LSN is recorded here, to get correponding deletion logs from "committed deletion logs".
    pub(crate) data_file_flush_lsn: Option<u64>,
    /// indices
    pub(crate) indices: MooncakeIndex,
}

impl Snapshot {
    pub(crate) fn new(metadata: Arc<TableMetadata>) -> Self {
        Self {
            metadata,
            disk_files: HashMap::new(),
            snapshot_version: 0,
            data_file_flush_lsn: None,
            indices: MooncakeIndex::new(),
        }
    }

    /// Get file indices for the current snapshot.
    pub(crate) fn get_file_indices(&self) -> &[FileIndex] {
        self.indices.file_indices.as_slice()
    }

    pub fn get_name_for_inmemory_file(&self) -> PathBuf {
        Path::join(
            &self.metadata.path,
            format!(
                "inmemory_{}_{}_{}.parquet",
                self.metadata.name, self.metadata.id, self.snapshot_version
            ),
        )
    }
}

#[derive(Default)]
pub struct SnapshotTask {
    /// Current task
    ///
    new_disk_slices: Vec<DiskSliceWriter>,
    new_deletions: Vec<RawDeletionRecord>,
    /// Pair of <batch id, record batch>.
    new_record_batches: Vec<(u64, Arc<RecordBatch>)>,
    new_rows: Option<SharedRowBufferSnapshot>,
    new_mem_indices: Vec<Arc<MemIndex>>,
    /// Assigned (non-zero) after a commit event.
    new_lsn: u64,
    new_commit_point: Option<RecordLocation>,
}

impl SnapshotTask {
    pub fn new() -> Self {
        Self {
            new_disk_slices: Vec::new(),
            new_deletions: Vec::new(),
            new_record_batches: Vec::new(),
            new_rows: None,
            new_mem_indices: Vec::new(),
            new_lsn: 0,
            new_commit_point: None,
        }
    }

    pub fn should_create_snapshot(&self) -> bool {
        self.new_lsn > 0 || !self.new_disk_slices.is_empty() || self.new_deletions.len() > 1000
    }

    /// Get newly created data files.
    pub(crate) fn get_new_data_files(&self) -> Vec<PathBuf> {
        let mut new_files = vec![];
        for cur_disk_slice in self.new_disk_slices.iter() {
            new_files.extend(
                cur_disk_slice
                    .output_files()
                    .iter()
                    .map(|(p, _)| p.clone())
                    .collect::<Vec<_>>(),
            );
        }
        new_files
    }
}

/// Used to track the state of a streamed transaction
/// Holds the memslice and pending deletes
struct TransactionStreamState {
    mem_slice: MemSlice,
    new_deletions: Vec<RawDeletionRecord>,
    new_disk_slices: Vec<DiskSliceWriter>,
}

impl TransactionStreamState {
    fn new(schema: Arc<Schema>, batch_size: usize, identity: IdentityProp) -> Self {
        Self {
            mem_slice: MemSlice::new(schema, batch_size, identity),
            new_deletions: Vec::new(),
            new_disk_slices: Vec::new(),
        }
    }
}

/// MooncakeTable is a disk table + mem slice.
/// Transactions will append data to the mem slice.
///
/// And periodically disk slices will be merged and compacted.
/// Single thread is used to write to the table.
///
pub struct MooncakeTable {
    /// Current metadata of the table.
    ///
    metadata: Arc<TableMetadata>,

    /// The mem slice
    ///
    mem_slice: MemSlice,

    /// Current snapshot of the table
    snapshot: Arc<RwLock<SnapshotTableState>>,

    table_snapshot_watch_sender: watch::Sender<u64>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,

    /// Records all the write operations since last snapshot.
    next_snapshot_task: SnapshotTask,

    /// Stream state per transaction, keyed by xact-id.
    transaction_stream_states: HashMap<u32, TransactionStreamState>,
}

impl MooncakeTable {
    /// foreground functions
    ///
    pub async fn new(
        schema: Schema,
        name: String,
        version: u64,
        base_path: PathBuf,
        identity: IdentityProp,
        iceberg_table_config: IcebergTableConfig,
        table_config: TableConfig,
    ) -> Self {
        let schema = Arc::new(schema);
        let metadata = Arc::new(TableMetadata {
            name,
            id: version,
            schema,
            config: table_config,
            path: base_path,
            identity,
        });
        let (table_snapshot_watch_sender, table_snapshot_watch_receiver) = watch::channel(0);
        Self {
            mem_slice: MemSlice::new(
                metadata.schema.clone(),
                metadata.config.batch_size,
                metadata.identity.clone(),
            ),
            metadata: metadata.clone(),
            snapshot: Arc::new(RwLock::new(
                SnapshotTableState::new(metadata, iceberg_table_config).await,
            )),
            next_snapshot_task: SnapshotTask::new(),
            transaction_stream_states: HashMap::new(),
            table_snapshot_watch_sender,
            table_snapshot_watch_receiver,
        }
    }

    pub(crate) fn get_state_for_reader(
        &self,
    ) -> (Arc<RwLock<SnapshotTableState>>, watch::Receiver<u64>) {
        (
            self.snapshot.clone(),
            self.table_snapshot_watch_receiver.clone(),
        )
    }

    pub fn append(&mut self, row: MoonlinkRow) -> Result<()> {
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let identity_for_key = self.metadata.identity.extract_identity_for_key(&row);
        if let Some(batch) = self.mem_slice.append(lookup_key, row, identity_for_key)? {
            self.next_snapshot_task.new_record_batches.push(batch);
        }
        Ok(())
    }

    pub fn delete(&mut self, row: MoonlinkRow, lsn: u64) {
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn,
            pos: None,
            row_identity: self.metadata.identity.extract_identity_columns(row),
        };
        let pos = self.mem_slice.delete(&record, &self.metadata.identity);
        record.pos = pos;
        self.next_snapshot_task.new_deletions.push(record);
    }

    pub fn commit(&mut self, lsn: u64) {
        self.next_snapshot_task.new_lsn = lsn;
        self.next_snapshot_task.new_commit_point = Some(self.mem_slice.get_commit_check_point());
    }

    pub fn should_flush(&self) -> bool {
        self.mem_slice.get_num_rows() >= self.metadata.config.mem_slice_size
    }

    fn get_or_create_stream_state<'a>(
        transaction_stream_states: &'a mut HashMap<u32, TransactionStreamState>,
        metadata: &Arc<TableMetadata>,
        xact_id: u32,
    ) -> &'a mut TransactionStreamState {
        transaction_stream_states.entry(xact_id).or_insert_with(|| {
            TransactionStreamState::new(
                metadata.schema.clone(),
                metadata.config.batch_size,
                metadata.identity.clone(),
            )
        })
    }

    pub fn should_transaction_flush(&self, xact_id: u32) -> bool {
        self.transaction_stream_states
            .get(&xact_id)
            .unwrap()
            .mem_slice
            .get_num_rows()
            >= self.metadata.config.batch_size
    }

    pub fn append_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) -> Result<()> {
        let stream_state = Self::get_or_create_stream_state(
            &mut self.transaction_stream_states,
            &self.metadata,
            xact_id,
        );

        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let identity_for_key = self.metadata.identity.extract_identity_for_key(&row);
        stream_state
            .mem_slice
            .append(lookup_key, row, identity_for_key)?;

        Ok(())
    }

    pub fn delete_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) {
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn: u64::MAX, // Updated at commit time
            pos: None,
            row_identity: self.metadata.identity.extract_identity_columns(row),
        };

        let stream_state = Self::get_or_create_stream_state(
            &mut self.transaction_stream_states,
            &self.metadata,
            xact_id,
        );
        if let Some(pos) = stream_state
            .mem_slice
            .delete(&record, &self.metadata.identity)
        {
            record.pos = Some(pos);
        } else {
            // Edge‑case: txn deletes a row that's still in the main mem_slice
            // TODO(nbiscaro): This is a bit of a hack. We can likely resolve this in a cleaner way during snapshot.
            // [https://github.com/Mooncake-Labs/moonlink/issues/126]
            record.pos = self
                .mem_slice
                .find_non_deleted_position(&record, &self.metadata.identity);
            // NOTE: There is still a remaining edge case that is not yet supported:
            // In the event that we have two identical, rows A and B, with no primary key (using full row as
            // identifier). We may have a situation where we delete A during some streaming
            // transaction, and then delete A in a non-streaming transaction. In this case, we will
            // delete A twice instead of deleting A then B. A potential solution is to have the
            // main mem slice delete from the top of the matches rows and the streamin transaction
            // to delete from the bottom, but this needs a closer look.
        }
        self.transaction_stream_states
            .get_mut(&xact_id)
            .unwrap()
            .new_deletions
            .push(record);
    }

    pub fn abort_in_stream_batch(&mut self, xact_id: u32) {
        // Record abortion in snapshot task so we can remove any uncomitted deletions
        self.transaction_stream_states.remove(&xact_id);
    }

    /// Flush the given MemSlice and in-memory record batches into data files.
    async fn inner_flush_data_files(
        mem_slice: &mut MemSlice,
        snapshot_task: &mut SnapshotTask,
        metadata: &Arc<TableMetadata>,
        lsn: u64,
    ) -> Result<DiskSliceWriter> {
        // Finalize the current batch (if needed)
        let (new_batch, batches, index) = mem_slice.drain().unwrap();

        if let Some(batch) = new_batch {
            snapshot_task.new_record_batches.push(batch);
        }

        let index = Arc::new(index);
        snapshot_task.new_mem_indices.push(index.clone());

        let metadata_clone = metadata.clone();
        let path_clone = metadata.path.clone();

        let disk_slice_writer_join_handle = tokio::task::spawn_blocking(move || {
            let mut disk_slice = DiskSliceWriter::new(
                metadata_clone.schema.clone(),
                path_clone,
                batches,
                Some(lsn),
                index,
            );
            block_on(disk_slice.write())?;
            Ok(disk_slice)
        });

        match disk_slice_writer_join_handle.await {
            Ok(Ok(disk_slice)) => Ok(disk_slice),
            Ok(Err(e)) => Err(e),
            Err(join_error) => Err(Error::TokioJoinError(join_error.to_string())),
        }
    }

    async fn stream_flush(
        mem_slice: &mut MemSlice,
        metadata: &Arc<TableMetadata>,
    ) -> Result<DiskSliceWriter> {
        // Finalize the current batch (if needed)
        let (_, batches, index) = mem_slice.drain().unwrap();

        let index = Arc::new(index);
        let mut disk_slice = DiskSliceWriter::new(
            metadata.schema.clone(),
            metadata.path.clone(),
            batches,
            None,
            index,
        );
        // TODO(nbiscaro): Find longer term solution that allows aysnc write
        block_on(disk_slice.write())?;
        Ok(disk_slice)
    }

    pub async fn flush_transaction_stream(&mut self, xact_id: u32) -> Result<()> {
        if let Some(stream_state) = self.transaction_stream_states.get_mut(&xact_id) {
            let disk_slice =
                Self::stream_flush(&mut stream_state.mem_slice, &self.metadata).await?;

            stream_state.new_disk_slices.push(disk_slice);

            return Ok(());
        }
        Ok(())
    }

    pub async fn commit_transaction_stream(&mut self, xact_id: u32, lsn: u64) -> Result<()> {
        if let Some(mut stream_state) = self.transaction_stream_states.remove(&xact_id) {
            let xact_mem_slice = &mut stream_state.mem_slice;

            let snapshot_task = &mut self.next_snapshot_task;
            snapshot_task.new_lsn = lsn;

            // We update our delete records with the last lsn of the transaction
            // Note that in the stream case we dont have this until commit time
            for deletion in stream_state.new_deletions.iter_mut() {
                deletion.lsn = lsn;
            }

            // add transaction deletions to snapshot task
            snapshot_task
                .new_deletions
                .append(&mut stream_state.new_deletions);

            // Flush any remaining rows in the xact mem slice
            let disk_slice = Self::stream_flush(xact_mem_slice, &self.metadata).await?;
            stream_state.new_disk_slices.push(disk_slice);

            // Update the LSN of all disk slices from pre-commit flushes
            for disk_slice in stream_state.new_disk_slices.iter_mut() {
                disk_slice.set_lsn(Some(lsn));
            }

            // Add the disk slices to the snapshot task
            snapshot_task
                .new_disk_slices
                .append(&mut stream_state.new_disk_slices);

            Ok(())
        } else {
            Err(Error::TransactionNotFound(xact_id))
        }
    }

    // UNDONE(BATCH_INSERT):
    // flush uncommitted batches from big batch insert
    pub async fn flush(&mut self, lsn: u64) -> Result<()> {
        // Marks a checkpoint for iceberg snapshot creation.
        self.snapshot.write().await.update_flush_lsn(lsn);

        if self.mem_slice.is_empty() {
            return Ok(());
        }

        // Flush data files into iceberb table.
        let disk_slice = Self::inner_flush_data_files(
            &mut self.mem_slice,
            &mut self.next_snapshot_task,
            &self.metadata,
            lsn,
        )
        .await?;
        self.next_snapshot_task.new_disk_slices.push(disk_slice);

        Ok(())
    }

    // Create a snapshot of the last committed version, return current snapshot's version.
    //
    pub fn create_snapshot(&mut self) -> Option<JoinHandle<u64>> {
        if !self.next_snapshot_task.should_create_snapshot() {
            return None;
        }
        self.next_snapshot_task.new_rows = Some(self.mem_slice.get_latest_rows());
        let next_snapshot_task = take(&mut self.next_snapshot_task);
        Some(tokio::task::spawn(Self::create_snapshot_async(
            self.snapshot.clone(),
            next_snapshot_task,
        )))
    }

    pub(crate) fn notify_snapshot_reader(&self, lsn: u64) {
        self.table_snapshot_watch_sender.send(lsn).unwrap();
    }

    async fn create_snapshot_async(
        snapshot: Arc<RwLock<SnapshotTableState>>,
        next_snapshot_task: SnapshotTask,
    ) -> u64 {
        snapshot
            .write()
            .await
            .update_snapshot(next_snapshot_task)
            .await
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_utils;
