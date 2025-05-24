use crate::row::MoonlinkRow;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::MooncakeTable;
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
    ForceSnapshot { lsn: u64 },
}

/// Handler for table operations
pub struct TableHandler {
    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the event queue
    event_sender: Sender<TableEvent>,
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub fn new(table: MooncakeTable, iceberg_snapshot_completion_tx: mpsc::Sender<()>) -> Self {
        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(async move {
            Self::event_loop(iceberg_snapshot_completion_tx, event_receiver, table).await;
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
        iceberg_snapshot_completion_tx: Sender<()>,
        mut event_receiver: Receiver<TableEvent>,
        mut table: MooncakeTable,
    ) {
        let mut snapshot_handle: Option<JoinHandle<(u64, Option<IcebergSnapshotPayload>)>> = None;
        let mut periodic_snapshot_interval = time::interval(Duration::from_millis(500));
        // Requested minimum LSN for a force snapshot request.
        let mut force_snapshot_lsn: Option<u64> = None;

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    match event {
                        TableEvent::Append { row, xact_id } => {
                            let result = match xact_id {
                                Some(xact_id) => {
                                    let res = table.append_in_stream_batch(row, xact_id);
                                    if table.should_transaction_flush(xact_id) {
                                        println!("Flushing transaction stream");
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
                            // and 2. there's no snapshot creation operation ongoing
                            let need_force_snapshot = force_snapshot_lsn.is_some() && force_snapshot_lsn.unwrap() <= lsn && snapshot_handle.is_none();
                            if table.should_flush() || need_force_snapshot {
                                if let Err(e) = table.flush(lsn).await {
                                    println!("Flush failed in Commit: {}", e);
                                }
                            }
                            if need_force_snapshot {
                                snapshot_handle = table.create_snapshot();
                            }
                        }
                        TableEvent::StreamCommit { lsn, xact_id } => {
                            // TODO(hiang): Support force snapshot creation.
                            if let Err(e) = table.commit_transaction_stream(xact_id, lsn).await {
                                println!("Stream commit flush failed: {}", e);
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
                        TableEvent::ForceSnapshot { lsn } => {
                            assert!(force_snapshot_lsn.is_none());
                            force_snapshot_lsn = Some(lsn);
                        }
                    }
                }
                // Wait for the snapshot to complete.
                Some((lsn, iceberg_snapshot_payload)) = async {
                    if let Some(handle) = &mut snapshot_handle {
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

                    // Process iceberg snapshot.
                    // TODO(hjiang): Move iceberg snapshot creation out of eventloop.
                    let mut cur_flush_lsn : Option<u64> = None;
                    if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
                        cur_flush_lsn = Some(iceberg_snapshot_payload.flush_lsn);
                        table.persist_iceberg_snapshot(iceberg_snapshot_payload).await;
                    }

                    // Notify completion of iceberg snapshot, with flush LSN no earlier than requested LSN.
                    if let Some(request_snapshot_lsn) = force_snapshot_lsn {
                        if let Some(cur_flush_lsn) = cur_flush_lsn {
                            if request_snapshot_lsn <= cur_flush_lsn {
                                iceberg_snapshot_completion_tx.send(()).await.unwrap();
                                force_snapshot_lsn = None;
                            }
                        }
                    }
                    snapshot_handle = None;
                }
                // Periodic snapshot based on time
                _ = periodic_snapshot_interval.tick() => {
                    // Only create a periodic snapshot if there isn't already one in progress
                    if snapshot_handle.is_none() {
                        snapshot_handle = table.create_snapshot();
                    }
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
