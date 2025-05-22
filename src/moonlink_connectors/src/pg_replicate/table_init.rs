use crate::pg_replicate::replication_state::ReplicationState;
use crate::pg_replicate::table::TableSchema;
use crate::pg_replicate::util::postgres_schema_to_moonlink_schema;
use moonlink::{
    IcebergSnapshotStateManager, IcebergTableConfig, MooncakeTable, ReadStateManager, TableConfig,
    TableEvent, TableHandler,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::{mpsc::Sender, watch};

/// Components required to replicate a single table.
/// Components that the [`Sink`] needs for processing CDC events.
pub struct TableComponents {
    pub handler: TableHandler,
    pub event_sender: Sender<TableEvent>,
    pub commit_lsn_sender: watch::Sender<u64>,
}

/// Resources that should be returned to the caller when a table is initialised.
pub struct TableResources {
    pub sink_components: TableComponents,
    pub read_state_manager: ReadStateManager,
    pub iceberg_snapshot_manager: IcebergSnapshotStateManager,
}

/// Build all components needed to replicate `table_schema`.
pub async fn build_table_components(
    table_schema: &TableSchema,
    base_path: &Path,
    replication_state: &ReplicationState,
) -> TableResources {
    let table_path = PathBuf::from(base_path).join(table_schema.table_name.to_string());
    tokio::fs::create_dir_all(&table_path).await.unwrap();
    let (arrow_schema, identity) = postgres_schema_to_moonlink_schema(table_schema);
    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri: base_path.to_str().unwrap().to_string(),
        namespace: vec!["default".to_string()],
        table_name: table_schema.table_name.to_string(),
        // TODO(hjiang): Disable recovery in production, at the moment we only support create new table from scratch.
        drop_table_if_exists: true,
    };
    let table = MooncakeTable::new(
        arrow_schema,
        table_schema.table_name.to_string(),
        table_schema.table_id as u64,
        table_path,
        identity,
        iceberg_table_config,
        TableConfig::new(),
    )
    .await;

    let (commit_tx, commit_rx) = watch::channel(0u64);
    let read_state_manager =
        ReadStateManager::new(&table, replication_state.subscribe(), commit_rx);

    let (snapshot_completion_tx, snapshot_completion_rx) = mpsc::channel(1);
    let handler = TableHandler::new(table, snapshot_completion_tx);
    let iceberg_snapshot_manager =
        IcebergSnapshotStateManager::new(handler.get_event_sender(), snapshot_completion_rx);
    let event_sender = handler.get_event_sender();

    let sink_components = TableComponents {
        handler,
        event_sender,
        commit_lsn_sender: commit_tx,
    };

    TableResources {
        sink_components,
        read_state_manager,
        iceberg_snapshot_manager,
    }
}
