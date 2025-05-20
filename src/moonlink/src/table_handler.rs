use crate::row::MoonlinkRow;
use crate::storage::IcebergSnapshotStateManager;
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
}

/// Handler for table operations
pub struct TableHandler {
    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the event queue
    event_sender: Sender<TableEvent>,
    // /// Receiver for iceberg snaphot initiation event.
    // iceberg_snapshot_initiation_receiver: Receiver<()>,
    // /// Sender for iceberg snapshot completion event.
    // iceberg_snapshot_completion_sender: Sender<()>,
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub fn new(
        table: MooncakeTable,
        iceberg_snapshot_state_manager: &mut IcebergSnapshotStateManager,
    ) -> Self {
        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        let iceberg_snapshot_initiation_receiver = iceberg_snapshot_state_manager
            .take_snapshot_initiation_receiver()
            .unwrap();
        let iceberg_snapshot_completion_sender =
            iceberg_snapshot_state_manager.get_snapshot_completion_sender();

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(async move {
            Self::event_loop(
                iceberg_snapshot_initiation_receiver,
                iceberg_snapshot_completion_sender,
                event_receiver,
                table,
            )
            .await;
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
        mut iceberg_snapshot_initiation_receiver: Receiver<()>,
        iceberg_snaphot_completion_sender: Sender<()>,
        mut event_receiver: Receiver<TableEvent>,
        mut table: MooncakeTable,
    ) {
        let mut snapshot_handle: Option<JoinHandle<u64>> = None;
        let mut periodic_snapshot_interval = time::interval(Duration::from_millis(500));
        let mut has_outstanding_iceberg_snapshot_request = false;

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
                                Some(xact_id) => table.delete_in_stream_batch(row, xact_id),
                                None => table.delete(row, lsn),
                            };
                        }
                        TableEvent::Commit { lsn } => {
                            table.commit(lsn);
                            if table.should_flush() {
                                if let Err(e) = table.flush(lsn).await {
                                    println!("Flush failed in Commit: {}", e);
                                }
                            }
                        }
                        TableEvent::StreamCommit { lsn, xact_id } => {
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
                    }
                }
                // wait for iceberg snapshot requests.
                Some(()) = iceberg_snapshot_initiation_receiver.recv() => {
                    assert!(!has_outstanding_iceberg_snapshot_request, "There should be at most one outstanding iceberg snapshot request for one table!");
                    has_outstanding_iceberg_snapshot_request = true;
                    // Only create a periodic snapshot if there isn't already one in progress
                    if snapshot_handle.is_none() {
                        snapshot_handle = table.create_snapshot();
                    }
                }
                // wait for the snapshot to complete
                Some(()) = async {
                    if let Some(handle) = &mut snapshot_handle {
                        match handle.await {
                            Ok(lsn) => {
                                table.notify_snapshot_reader(lsn);
                                if has_outstanding_iceberg_snapshot_request {
                                    iceberg_snaphot_completion_sender.send(()).await.unwrap();
                                }
                            }
                            Err(e) => {
                                println!("Snapshot task was cancelled: {}", e);
                            }
                        }
                        Some(())
                    } else {
                        futures::future::pending::<Option<_>>().await
                    }
                } => {
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
