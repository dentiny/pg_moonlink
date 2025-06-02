use crate::row::MoonlinkRow;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::IcebergSnapshotResult;
use crate::storage::MooncakeTable;
use crate::{Error, Result};
use std::collections::BTreeMap;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

/// Event types that can be processed by the TableHandler
#[derive(Debug)]
pub enum TableEvent {
    /// Append a row to the table
    Append {
        row: MoonlinkRow,
        xact_id: Option<u32>,
    },
    /// Delete a row from the table
    Delete {
        row: MoonlinkRow,
        lsn: u64,
        xact_id: Option<u32>,
    },
    /// Commit all pending operations with a given LSN
    Commit { lsn: u64 },
    /// Commit all pending operations with given LSN and xact_id
    StreamCommit { lsn: u64, xact_id: u32 },
    /// Abort current stream with given xact_id
    StreamAbort { xact_id: u32 },
    /// Flush the table to disk
    Flush { lsn: u64 },
    /// Flush the transaction stream with given xact_id
    StreamFlush { xact_id: u32 },
    /// Shutdown the handler
    _Shutdown,
    /// Force a mooncake and iceberg snapshot.
    ForceSnapshot { lsn: u64, tx: Sender<Result<()>> },
    /// Drop table.
    DropIcebergTable,
}

/// Handler for table operations
pub struct TableHandler {
    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the event queue
    event_sender: Sender<TableEvent>,
}

/// Contains a few senders, which notifies after certain iceberg events completion.
pub struct IcebergEventSyncSender {
    /// Notifies when iceberg drop table completes.
    pub iceberg_drop_table_completion_tx: mpsc::Sender<Result<()>>,
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub fn new(table: MooncakeTable, iceberg_event_sync_sender: IcebergEventSyncSender) -> Self {
        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(async move {
            Self::event_loop(iceberg_event_sync_sender, event_receiver, table).await;
        }));

        // Create the handler
        Self {
            _event_handle: event_handle,
            event_sender,
        }
    }

    /// Get the event sender to send events to this handler
    pub fn get_event_sender(&self) -> Sender<TableEvent> {
        self.event_sender.clone()
    }

    /// Main event processing loop
    async fn event_loop(
        iceberg_event_sync_sender: IcebergEventSyncSender,
        mut event_receiver: Receiver<TableEvent>,
        mut table: MooncakeTable,
    ) {
        let mut periodic_snapshot_interval = time::interval(Duration::from_millis(500));

        // Join handle for mooncake snapshot.
        let mut mooncake_snapshot_handle: Option<
            JoinHandle<(u64, Option<IcebergSnapshotPayload>)>,
        > = None;

        // Join handle for iceberg snapshot.
        let mut iceberg_snapshot_handle: Option<JoinHandle<Result<IcebergSnapshotResult>>> = None;

        // Requested minimum LSN for a force snapshot request.
        let mut force_snapshot_lsns: BTreeMap<u64, Sender<Result<()>>> = BTreeMap::new();

        // Record LSN if the last handled table event is committed, which indicates mooncake table stays at a consistent view, so table could be flushed safely.
        let mut table_consistent_view_lsn: Option<u64> = None;

        // Whether current table receives any update events.
        let mut table_updated = false;

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    table_consistent_view_lsn = match event {
                        TableEvent::Commit { lsn } => Some(lsn),
                        TableEvent::StreamCommit { lsn, .. } => Some(lsn),
                        // `ForceSnapshot` event doesn't affect whether mooncake is at a committed state.
                        TableEvent::ForceSnapshot { .. } => table_consistent_view_lsn,
                        _ => None,
                    };
                    table_updated = match event {
                        TableEvent::ForceSnapshot { .. } => table_updated,
                        _ => true,
                    };

                    match event {
                        TableEvent::Append { row, xact_id } => {
                            let result = match xact_id {
                                Some(xact_id) => {
                                    let res = table.append_in_stream_batch(row, xact_id);
                                    if table.should_transaction_flush(xact_id) {
                                        if let Err(e) = table.flush_transaction_stream(xact_id).await {
                                            println!("Flush failed in Append: {}", e);
                                        }
                                    }
                                    res
                                },
                                None => table.append(row),
                            };

                            if let Err(e) = result {
                                println!("Failed to append row: {}", e);
                            }
                        }
                        TableEvent::Delete { row, lsn, xact_id } => {
                            match xact_id {
                                Some(xact_id) => table.delete_in_stream_batch(row, xact_id).await,
                                None => table.delete(row, lsn).await,
                            };
                        }
                        TableEvent::Commit { lsn } => {
                            table.commit(lsn);

                            // Force create snapshot if
                            // 1. force snapshot is requested
                            // and 2. LSN which meets force snapshot requirement has appeared, before that we still allow buffering
                            // and 3. there's no snapshot creation operation ongoing
                            let force_snapshot = !force_snapshot_lsns.is_empty()
                                && lsn >= *force_snapshot_lsns.iter().next().as_ref().unwrap().0
                                && mooncake_snapshot_handle.is_none();

                            if table.should_flush() || force_snapshot {
                                if let Err(e) = table.flush(lsn).await {
                                    println!("Flush failed in Commit: {}", e);
                                }
                            }
                            if force_snapshot {
                                mooncake_snapshot_handle = table.force_create_snapshot();
                            }
                        }
                        TableEvent::StreamCommit { lsn, xact_id } => {
                            // Force create snapshot if
                            // 1. force snapshot is requested
                            // and 2. LSN which meets force snapshot requirement has appeared, before that we still allow buffering
                            // and 3. there's no snapshot creation operation ongoing
                            let force_snapshot = !force_snapshot_lsns.is_empty()
                            && lsn >= *force_snapshot_lsns.iter().next().as_ref().unwrap().0
                            && mooncake_snapshot_handle.is_none();

                            if let Err(e) = table.commit_transaction_stream(xact_id, lsn).await {
                                println!("Stream commit flush failed: {}", e);
                            }
                            if force_snapshot {
                                mooncake_snapshot_handle = table.force_create_snapshot();
                            }
                        }
                        TableEvent::StreamAbort { xact_id } => {
                            table.abort_in_stream_batch(xact_id);
                        }
                        TableEvent::Flush { lsn } => {
                            if let Err(e) = table.flush(lsn).await {
                                println!("Explicit Flush failed: {}", e);
                            }
                        }
                        TableEvent::StreamFlush { xact_id } => {
                            if let Err(e) = table.flush_transaction_stream(xact_id).await {
                                println!("Stream flush failed: {}", e);
                            }
                        }
                        TableEvent::_Shutdown => {
                            println!("Shutting down table handler");
                            break;
                        }
                        TableEvent::ForceSnapshot { lsn, tx } => {
                            // A workaround to avoid create snapshot call gets stuck, when there's no write operations to the table.
                            if !table_updated {
                                tx.send(Ok(())).await.unwrap();
                                continue;
                            }

                            // Fast-path: if iceberg snapshot requirement is already satisfied, notify directly.
                            let last_iceberg_snapshot_lsn = table.get_iceberg_snapshot_lsn();
                            if last_iceberg_snapshot_lsn.is_some() && lsn <= last_iceberg_snapshot_lsn.unwrap() {
                                tx.send(Ok(())).await.unwrap();
                            }
                            // Iceberg snapshot LSN requirement is not met, record the required LSN, so later commit will pick up.
                            else {
                                force_snapshot_lsns.insert(lsn, tx);
                            }
                        }
                        // Branch to drop the iceberg table, only used when the whole table requested to drop.
                        // So we block wait for asynchronous request completion.
                        TableEvent::DropIcebergTable => {
                            let res = table.drop_iceberg_table().await;
                            iceberg_event_sync_sender.iceberg_drop_table_completion_tx.send(res).await.unwrap();
                        }
                    }
                }
                // Wait for the mooncake snapshot to complete.
                Some((lsn, iceberg_snapshot_payload)) = async {
                    if let Some(handle) = &mut mooncake_snapshot_handle {
                        match handle.await {
                            Ok((lsn, iceberg_snapshot_payload)) => {
                                Some((lsn, iceberg_snapshot_payload))
                            }
                            Err(e) => {
                                println!("Snapshot task was cancelled: {}", e);
                                None
                            }
                        }
                    } else {
                        futures::future::pending::<Option<_>>().await
                    }
                } => {
                    // Notify read the mooncake table commit of LSN.
                    table.notify_snapshot_reader(lsn);

                    // Process iceberg snapshot and trigger iceberg snapshot if necessary.
                    //
                    // TODO(hjiang): same as mooncake snapshot, there should be at most one iceberg snapshot ongoing; for simplicity, we skip iceberg snapshot if there's already one.
                    // An optimization is skip generating iceberg snapshot payload at mooncake snapshot if we know it's already taking place, but the risk is missing iceberg snapshot.
                    if iceberg_snapshot_handle.is_none() {
                        if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
                            iceberg_snapshot_handle = Some(table.persist_iceberg_snapshot(iceberg_snapshot_payload));
                        }
                    }

                    mooncake_snapshot_handle = None;
                }
                // Wait for iceberg snapshot flush operation to finish.
                Some(iceberg_snapshot_res) = async {
                    if let Some(handle) = &mut iceberg_snapshot_handle {
                        match handle.await {
                            Ok(iceberg_snapshot_res) => {
                                Some(iceberg_snapshot_res)
                            }
                            Err(e) => {
                                println!("Iceberg snapshot task gets cancelled: {:?}", e);
                                None
                            }
                        }
                    } else {
                        futures::future::pending::<Option<_>>().await
                    }
                } => {
                    iceberg_snapshot_handle = None;

                    match iceberg_snapshot_res {
                        Ok(snapshot_res) => {
                            let iceberg_flush_lsn = snapshot_res.flush_lsn;
                            table.set_iceberg_snapshot_res(snapshot_res);

                            // Notify all waiters with LSN satisfied.
                            let new_map = force_snapshot_lsns.split_off(&iceberg_flush_lsn);
                            for (_, tx) in force_snapshot_lsns.iter() {
                                tx.send(Ok(())).await.unwrap()
                            }
                            force_snapshot_lsns = new_map;
                        }
                        Err(e) => {
                            for (_, tx) in force_snapshot_lsns.iter() {
                                let err = Error::IcebergMessage(format!("Iceberg failure: {}", e));
                                tx.send(Err(err)).await.unwrap()
                            }
                            force_snapshot_lsns.clear();
                        }
                    }
                }
                // Periodic snapshot based on time
                _ = periodic_snapshot_interval.tick() => {
                    // Only create a periodic snapshot if there isn't already one in progress
                    if mooncake_snapshot_handle.is_some() {
                        continue;
                    }

                    // Check whether a flush and force snapshot is needed.
                    if !force_snapshot_lsns.is_empty() {
                        if let Some(commit_lsn) = table_consistent_view_lsn {
                            if *force_snapshot_lsns.iter().next().as_ref().unwrap().0 <= commit_lsn {
                                table.flush(/*lsn=*/ commit_lsn).await.unwrap();
                                mooncake_snapshot_handle = table.force_create_snapshot();
                                continue;
                            }
                        }
                    }

                    // Fallback to normal periodic snapshot.
                    mooncake_snapshot_handle = table.create_snapshot();
                }
                // If all senders have been dropped, exit the loop
                else => {
                    println!("All event senders have been dropped, shutting down table handler");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_utils;
