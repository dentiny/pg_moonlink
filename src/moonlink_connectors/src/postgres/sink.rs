use crate::pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    replication_state::ReplicationState,
    table::{TableId, TableSchema},
};
use crate::postgres::util::postgres_schema_to_moonlink_schema;
use crate::postgres::util::PostgresTableRow;
use moonlink::IcebergSnapshotStateManager;
use moonlink::IcebergTableConfig;
use moonlink::ReadStateManager;
use moonlink::{MooncakeTable, TableConfig, TableEvent, TableHandler};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;

#[derive(Default)]
struct TransactionState {
    final_lsn: u64,
    touched_tables: HashSet<TableId>,
}

pub struct Sink {
    table_handlers: HashMap<TableId, TableHandler>,
    last_committed_lsn_per_table: HashMap<TableId, watch::Sender<u64>>,
    event_senders: HashMap<TableId, Sender<TableEvent>>,
    streaming_transactions_state: HashMap<u32, TransactionState>,
    transaction_state: TransactionState,
    reader_notifier: Sender<ReadStateManager>,
    iceberg_snapshot_notifier: Sender<IcebergSnapshotStateManager>,
    base_path: PathBuf,
    replication_state: Arc<ReplicationState>,
}

impl Sink {
    pub fn new(
        reader_notifier: Sender<ReadStateManager>,
        iceberg_snapshot_notifier: Sender<IcebergSnapshotStateManager>,
        base_path: PathBuf,
    ) -> Self {
        Self {
            table_handlers: HashMap::new(),
            last_committed_lsn_per_table: HashMap::new(),
            event_senders: HashMap::new(),
            streaming_transactions_state: HashMap::new(),
            transaction_state: TransactionState {
                final_lsn: 0,
                touched_tables: HashSet::new(),
            },
            reader_notifier,
            iceberg_snapshot_notifier,
            base_path,
            replication_state: ReplicationState::new(),
        }
    }
}

impl Sink {
    pub async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Infallible> {
        let table_handlers = &mut self.table_handlers;
        for (table_id, table_schema) in table_schemas {
            let table_path =
                PathBuf::from(&self.base_path).join(table_schema.table_name.to_string());
            tokio::fs::create_dir_all(&table_path).await.unwrap();
            let (arrow_schema, identity) = postgres_schema_to_moonlink_schema(&table_schema);
            let iceberg_table_config = IcebergTableConfig {
                warehouse_uri: self.base_path.to_str().unwrap().to_string(),
                namespace: vec!["default".to_string()],
                table_name: table_schema.table_name.to_string(),
                // TODO(hjiang): Disable recovery in production, at the moment we only support create new table from scratch.
                drop_table_if_exists: true,
            };
            let table = MooncakeTable::new(
                arrow_schema,
                table_schema.table_name.to_string(),
                table_id as u64,
                table_path,
                identity,
                iceberg_table_config,
                TableConfig::new(),
            )
            .await;
            let (table_commit_tx, table_commit_rx) = watch::channel(0u64);
            self.last_committed_lsn_per_table
                .insert(table_id, table_commit_tx);
            let read_state_manager =
                ReadStateManager::new(&table, self.replication_state.subscribe(), table_commit_rx);
            let mut iceberg_snapshot_state_manager = IcebergSnapshotStateManager::new();
            let table_handler = TableHandler::new(table, &mut iceberg_snapshot_state_manager);
            self.event_senders
                .insert(table_id, table_handler.get_event_sender());
            self.reader_notifier.send(read_state_manager).await.unwrap();
            self.iceberg_snapshot_notifier
                .send(iceberg_snapshot_state_manager)
                .await
                .unwrap();
            table_handlers.insert(table_id, table_handler);
        }
        Ok(())
    }

    // TODO: Use this when copying the intial table with data. Currently we assume the table to be empty and start streaming cdc events immediately for simplicity.
    #[allow(dead_code)]
    async fn write_table_row(
        &mut self,
        row: TableRow,
        table_id: TableId,
    ) -> Result<(), Infallible> {
        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
        event_sender
            .send(TableEvent::Append {
                row: PostgresTableRow(row).into(),
                xact_id: None,
            })
            .await
            .unwrap();
        event_sender
            .send(TableEvent::Commit { lsn: 0 })
            .await
            .unwrap();
        Ok(())
    }

    pub async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, Infallible> {
        match event {
            CdcEvent::Begin(begin_body) => {
                self.transaction_state.final_lsn = begin_body.final_lsn();
            }
            CdcEvent::StreamStart(_stream_start_body) => {}
            CdcEvent::Commit(commit_body) => {
                for table_id in &self.transaction_state.touched_tables {
                    let event_sender = self.event_senders.get_mut(table_id).unwrap();
                    event_sender
                        .send(TableEvent::Commit {
                            lsn: commit_body.commit_lsn(),
                        })
                        .await
                        .unwrap();
                    self.last_committed_lsn_per_table
                        .get_mut(table_id)
                        .unwrap()
                        .send(commit_body.commit_lsn())
                        .unwrap();
                }
                self.transaction_state.touched_tables.clear();
            }
            CdcEvent::StreamCommit(stream_commit_body) => {
                let xact_id = stream_commit_body.xid();
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = self.event_senders.get_mut(table_id).unwrap();
                        event_sender
                            .send(TableEvent::StreamCommit {
                                lsn: stream_commit_body.commit_lsn(),
                                xact_id,
                            })
                            .await
                            .unwrap();
                        self.last_committed_lsn_per_table
                            .get_mut(table_id)
                            .unwrap()
                            .send(stream_commit_body.commit_lsn())
                            .unwrap();
                    }
                }
                self.streaming_transactions_state.remove(&xact_id);
            }
            CdcEvent::Insert((table_id, table_row, xact_id)) => {
                let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                event_sender
                    .send(TableEvent::Append {
                        row: PostgresTableRow(table_row).into(),
                        xact_id,
                    })
                    .await
                    .unwrap();
                if let Some(xid) = xact_id {
                    self.streaming_transactions_state
                        .entry(xid)
                        .or_default()
                        .touched_tables
                        .insert(table_id);
                } else {
                    self.transaction_state.touched_tables.insert(table_id);
                }
            }
            CdcEvent::Update((table_id, old_table_row, new_table_row, xact_id)) => {
                let final_lsn = if let Some(xid) = xact_id {
                    self.streaming_transactions_state
                        .entry(xid)
                        .or_default()
                        .touched_tables
                        .insert(table_id);
                    self.streaming_transactions_state
                        .get(&xid)
                        .unwrap()
                        .final_lsn
                } else {
                    self.transaction_state.touched_tables.insert(table_id);
                    self.transaction_state.final_lsn
                };

                let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                event_sender
                    .send(TableEvent::Delete {
                        row: PostgresTableRow(old_table_row.unwrap()).into(),
                        lsn: final_lsn,
                        xact_id,
                    })
                    .await
                    .unwrap();
                event_sender
                    .send(TableEvent::Append {
                        row: PostgresTableRow(new_table_row).into(),
                        xact_id,
                    })
                    .await
                    .unwrap();
            }
            CdcEvent::Delete((table_id, table_row, xact_id)) => {
                let final_lsn = if let Some(xid) = xact_id {
                    self.streaming_transactions_state
                        .entry(xid)
                        .or_default()
                        .touched_tables
                        .insert(table_id);
                    self.streaming_transactions_state
                        .get(&xid)
                        .unwrap()
                        .final_lsn
                } else {
                    self.transaction_state.touched_tables.insert(table_id);
                    self.transaction_state.final_lsn
                };

                let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                event_sender
                    .send(TableEvent::Delete {
                        row: PostgresTableRow(table_row).into(),
                        lsn: final_lsn,
                        xact_id,
                    })
                    .await
                    .unwrap();
            }
            CdcEvent::Relation(relation_body) => println!("Relation {relation_body:?}"),
            CdcEvent::Type(type_body) => println!("Type {type_body:?}"),
            CdcEvent::PrimaryKeepAlive(primary_keepalive_body) => {
                self.replication_state
                    .mark(PgLsn::from(primary_keepalive_body.wal_end()));
            }
            CdcEvent::StreamStop(_stream_stop_body) => {}
            CdcEvent::StreamAbort(stream_abort_body) => {
                let xact_id = stream_abort_body.xid();
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = self.event_senders.get_mut(table_id).unwrap();
                        event_sender
                            .send(TableEvent::StreamAbort { xact_id })
                            .await
                            .unwrap();
                    }
                }
                self.streaming_transactions_state.remove(&xact_id);
            }
        }
        Ok(PgLsn::from(0))
    }

    // TODO: Use this when we add back table copy.
    #[allow(dead_code)]
    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Infallible> {
        println!("table {table_id} copied");
        Ok(())
    }
}
