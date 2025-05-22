use crate::row::{IdentityProp, MoonlinkRow, RowValue};
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::IcebergTableConfig;
use crate::storage::{verify_files_and_deletions, MooncakeTable};
use crate::table_handler::{TableEvent, TableHandler}; // Ensure this path is correct
use crate::union_read::{decode_read_state_for_testing, ReadStateManager};
use crate::{IcebergSnapshotStateManager, IcebergTableManager, TableConfig as MooncakeTableConfig};

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

use arrow::datatypes::{DataType, Field, Schema};
use tokio::sync::{mpsc, watch};

/// Creates a default schema for testing.
pub fn default_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )])),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "2".to_string(),
        )])),
        Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "3".to_string(),
        )])),
    ])
}

/// Creates a `MoonlinkRow` for testing purposes.
pub fn create_row(id: i32, name: &str, age: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(name.as_bytes().to_vec()),
        RowValue::Int32(age),
    ])
}

/// Get iceberg table manager config.
pub fn get_iceberg_manager_config(table_name: String, warehouse_uri: String) -> IcebergTableConfig {
    IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["default".to_string()],
        table_name,
        drop_table_if_exists: false,
    }
}

/// Holds the common environment components for table handler tests.
pub struct TestEnvironment {
    pub handler: TableHandler,
    event_sender: mpsc::Sender<TableEvent>,
    read_state_manager: Arc<ReadStateManager>,
    replication_tx: watch::Sender<u64>,
    table_commit_tx: watch::Sender<u64>,
    iceberg_snapshot_manager: IcebergSnapshotStateManager,
    pub(crate) temp_dir: TempDir,
}

impl TestEnvironment {
    /// Creates a default test environment with default settings.
    pub async fn default() -> Self {
        Self::new(MooncakeTableConfig::default()).await
    }

    /// Creates a new test environment with default settings.
    pub async fn new(mooncake_table_config: MooncakeTableConfig) -> Self {
        let schema = default_schema();
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // TODO(hjiang): Hard-code iceberg table namespace and table name.
        let table_name = "table_name";
        let iceberg_table_config =
            get_iceberg_manager_config(table_name.to_string(), path.to_str().unwrap().to_string());
        let mooncake_table = MooncakeTable::new(
            schema,
            table_name.to_string(),
            1,
            path,
            IdentityProp::Keys(vec![0]),
            iceberg_table_config,
            mooncake_table_config,
        )
        .await;

        let (replication_tx, replication_rx) = watch::channel(0u64);
        let (table_commit_tx, table_commit_rx) = watch::channel(0u64);

        let read_state_manager = Arc::new(ReadStateManager::new(
            &mooncake_table,
            replication_rx,
            table_commit_rx,
        ));

        let (snapshot_completion_tx, snapshot_completion_rx) = mpsc::channel(1);
        let handler = TableHandler::new(mooncake_table, snapshot_completion_tx);
        let iceberg_snapshot_manager =
            IcebergSnapshotStateManager::new(handler.get_event_sender(), snapshot_completion_rx);
        let event_sender = handler.get_event_sender();

        Self {
            handler,
            event_sender,
            read_state_manager,
            replication_tx,
            table_commit_tx,
            iceberg_snapshot_manager,
            temp_dir,
        }
    }

    /// Create iceberg table manager.
    pub fn create_iceberg_table_manager(
        &self,
        mooncake_table_config: MooncakeTableConfig,
    ) -> IcebergTableManager {
        let table_name = "table_name";
        let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
            name: table_name.to_string(),
            id: 0,
            schema: Arc::new(default_schema()),
            config: mooncake_table_config.clone(),
            path: self.temp_dir.path().to_path_buf(),
            identity: IdentityProp::Keys(vec![0]),
        });
        let iceberg_table_config = get_iceberg_manager_config(
            table_name.to_string(),
            self.temp_dir.path().to_str().unwrap().to_string(),
        );
        IcebergTableManager::new(mooncake_table_metadata, iceberg_table_config)
    }

    async fn send_event(&self, event: TableEvent) {
        self.event_sender
            .send(event)
            .await
            .expect("Failed to send event");
    }

    // --- Util functions for iceberg snapshot creation ---

    /// Initiate an iceberg snapshot event at best effort.
    pub async fn initiate_snapshot(&mut self) {
        self.iceberg_snapshot_manager.initiate_snapshot().await
    }

    /// Wait iceberg snapshot creation completion.
    pub async fn sync_snapshot_completion(&mut self) {
        self.iceberg_snapshot_manager
            .sync_snapshot_completion()
            .await
    }

    // --- Operation Helpers ---

    pub async fn append_row(&self, id: i32, name: &str, age: i32, xact_id: Option<u32>) {
        let row = create_row(id, name, age);
        self.send_event(TableEvent::Append { row, xact_id }).await;
    }

    pub async fn delete_row(&self, id: i32, name: &str, age: i32, lsn: u64, xact_id: Option<u32>) {
        let row = create_row(id, name, age);
        self.send_event(TableEvent::Delete { row, lsn, xact_id })
            .await;
    }

    pub async fn commit(&self, lsn: u64) {
        self.send_event(TableEvent::Commit { lsn }).await;
    }

    pub async fn stream_commit(&self, lsn: u64, xact_id: u32) {
        self.send_event(TableEvent::StreamCommit { lsn, xact_id })
            .await;
    }

    pub async fn stream_abort(&self, xact_id: u32) {
        self.send_event(TableEvent::StreamAbort { xact_id }).await;
    }

    pub async fn flush_table(&self, lsn: u64) {
        self.send_event(TableEvent::Flush { lsn }).await;
    }

    pub async fn stream_flush(&self, xact_id: u32) {
        self.send_event(TableEvent::StreamFlush { xact_id }).await;
    }

    // --- LSN and Verification Helpers ---

    /// Sets both table commit and replication LSN to the same value.
    /// This makes data up to `lsn` potentially readable.
    pub fn set_readable_lsn(&self, lsn: u64) {
        self.table_commit_tx
            .send(lsn)
            .expect("Failed to send table commit LSN");
        self.replication_tx
            .send(lsn)
            .expect("Failed to send replication LSN");
    }

    /// Sets table commit LSN and a potentially higher replication LSN.
    pub fn set_readable_lsn_with_cap(&self, table_commit_lsn: u64, replication_cap_lsn: u64) {
        self.table_commit_tx
            .send(table_commit_lsn)
            .expect("Failed to send table commit LSN");
        self.replication_tx
            .send(replication_cap_lsn.max(table_commit_lsn))
            .expect("Failed to send replication LSN");
    }

    /// Directly set the table commit LSN watch channel.
    pub fn set_table_commit_lsn(&self, lsn: u64) {
        self.table_commit_tx
            .send(lsn)
            .expect("Failed to send table commit LSN");
    }

    /// Directly set the replication LSN watch channel.
    pub fn set_replication_lsn(&self, lsn: u64) {
        self.replication_tx
            .send(lsn)
            .expect("Failed to send replication LSN");
    }

    pub async fn verify_snapshot(&self, target_lsn: u64, expected_ids: &[i32]) {
        check_read_snapshot(&self.read_state_manager, target_lsn, expected_ids).await;
    }

    // --- Lifecycle Helper ---
    pub async fn shutdown(&mut self) {
        self.send_event(TableEvent::_Shutdown).await;
        if let Some(handle) = self.handler._event_handle.take() {
            handle.await.expect("TableHandler task panicked");
        }
    }
}

/// Verifies the state of a read snapshot against expected row IDs.
pub async fn check_read_snapshot(
    read_manager: &ReadStateManager,
    target_lsn: u64,
    expected_ids: &[i32],
) {
    let read_state = read_manager.try_read(Some(target_lsn)).await.unwrap();
    let (files, deletion_vectors, position_deletes) = decode_read_state_for_testing(&read_state);

    if files.is_empty() && !expected_ids.is_empty() {
        unreachable!(
            "No snapshot files returned for LSN {} when rows (IDs: {:?}) were expected. Expected files because expected_ids is not empty.",
            target_lsn, expected_ids
        );
    }
    verify_files_and_deletions(&files, position_deletes, deletion_vectors, expected_ids).await;
}
