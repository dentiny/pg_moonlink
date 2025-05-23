/// This module interacts with iceberg snapshot status.
use tokio::sync::mpsc;

use crate::TableEvent;

/// At most one outstanding snapshot request is allowed.
pub struct IcebergSnapshotStateManager {
    /// Used to initiate a mooncake and iceberg snapshot operation.
    table_event_tx: mpsc::Sender<TableEvent>,
    /// Used to synchronize on the completion of an iceberg snapshot.
    snapshot_completion_rx: mpsc::Receiver<()>,
}

impl IcebergSnapshotStateManager {
    pub fn new(
        table_event_tx: mpsc::Sender<TableEvent>,
        snapshot_completion_rx: mpsc::Receiver<()>,
    ) -> Self {
        Self {
            table_event_tx,
            snapshot_completion_rx,
        }
    }

    /// Synchronize on iceberg snapshot completion.
    pub async fn sync_snapshot_completion(&mut self) {
        self.snapshot_completion_rx.recv().await.unwrap()
    }

    /// Initiate an iceberg snapshot event.
    /// Notice: at most one force snapshot is supported.
    pub async fn initiate_snapshot(&mut self, lsn: u64) {
        self.table_event_tx
            .send(TableEvent::ForceSnapshot { lsn })
            .await
            .unwrap()
    }
}
