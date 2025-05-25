use crate::pg_replicate::util::PostgresTableRow;
use crate::pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    replication_state::ReplicationState,
    table::TableId,
};
use moonlink::{TableEvent, TableHandler};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;

use super::table_init::TableComponents;

#[derive(Default)]
struct TransactionState {
    final_lsn: u64,
    touched_tables: HashSet<TableId>,
}

pub struct Sink {
    table_handlers: HashMap<TableId, TableHandler>,
    event_senders: Arc<RwLock<HashMap<TableId, Sender<TableEvent>>>>,
    streaming_transactions_state: HashMap<u32, TransactionState>,
    transaction_state: TransactionState,
    replication_state: Arc<ReplicationState>,
}

impl Sink {
    pub fn new(
        replication_state: Arc<ReplicationState>,
        event_senders: Arc<RwLock<HashMap<TableId, Sender<TableEvent>>>>,
    ) -> Self {
        Self {
            table_handlers: HashMap::new(),
            event_senders,
            streaming_transactions_state: HashMap::new(),
            transaction_state: TransactionState {
                final_lsn: 0,
                touched_tables: HashSet::new(),
            },
            replication_state,
        }
    }
}

impl Sink {
    // TODO: Use this when copying the intial table with data. Currently we assume the table to be empty and start streaming cdc events immediately for simplicity.
    #[allow(dead_code)]
    async fn write_table_row(
        &mut self,
        row: TableRow,
        table_id: TableId,
    ) -> Result<(), Infallible> {
        let event_sender = {
            let event_senders_guard = self.event_senders.read().unwrap();
            event_senders_guard.get(&table_id).cloned()
        };
        if let Some(event_sender) = event_sender {
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
        }
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
                    let event_sender = {
                        let event_senders_guard = self.event_senders.read().unwrap();
                        event_senders_guard.get(table_id).cloned()
                    };
                    if let Some(event_sender) = event_sender {
                        event_sender
                            .send(TableEvent::Commit {
                                lsn: commit_body.commit_lsn(),
                            })
                            .await
                            .unwrap();
                    }
                }
                self.transaction_state.touched_tables.clear();
            }
            CdcEvent::StreamCommit(stream_commit_body) => {
                let xact_id = stream_commit_body.xid();
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = {
                            let event_senders_guard = self.event_senders.read().unwrap();
                            event_senders_guard.get(table_id).cloned()
                        };
                        if let Some(event_sender) = event_sender {
                            event_sender
                                .send(TableEvent::StreamCommit {
                                    lsn: stream_commit_body.commit_lsn(),
                                    xact_id,
                                })
                                .await
                                .unwrap();
                        }
                    }
                    self.streaming_transactions_state.remove(&xact_id);
                }
            }
            CdcEvent::Insert((table_id, table_row, xact_id)) => {
                let event_sender = {
                    let event_senders_guard = self.event_senders.read().unwrap();
                    event_senders_guard.get(&table_id).cloned()
                };
                if let Some(event_sender) = event_sender {
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

                let event_sender = {
                    let event_senders_guard = self.event_senders.read().unwrap();
                    event_senders_guard.get(&table_id).cloned()
                };
                if let Some(event_sender) = event_sender {
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

                let event_sender = {
                    let event_senders_guard = self.event_senders.read().unwrap();
                    event_senders_guard.get(&table_id).cloned()
                };
                if let Some(event_sender) = event_sender {
                    event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                        })
                        .await
                        .unwrap();
                }
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
                        let event_sender = {
                            let event_senders_guard = self.event_senders.read().unwrap();
                            event_senders_guard.get(table_id).cloned()
                        };
                        if let Some(event_sender) = event_sender {
                            event_sender
                                .send(TableEvent::StreamAbort { xact_id })
                                .await
                                .unwrap();
                        }
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
