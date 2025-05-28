/// This module interacts with iceberg snapshot status.
use tokio::sync::mpsc;

use crate::TableEvent;

/// Contains a few receivers, which get notified after certain iceberg events completion.
pub struct IcebergEventSyncReceiver {
    /// Gets notified when iceberg snapshot completes.
    pub iceberg_snapshot_completion_rx: mpsc::Receiver<()>,

    /// Get notified when iceberg drop table completes.
    pub iceberg_drop_table_completion_rx: mpsc::Receiver<()>,
}

/// At most one outstanding snapshot request is allowed.
pub struct IcebergTableEventManager {
    /// Used to initiate a mooncake and iceberg snapshot operation.
    table_event_tx: mpsc::Sender<TableEvent>,
    /// Used to synchronize on the completion of an iceberg snapshot.
    snapshot_completion_rx: mpsc::Receiver<()>,
    /// Used to synchronize on the completion of an iceberg drop table.
    drop_table_completion_rx: mpsc::Receiver<()>,
}

impl IcebergTableEventManager {
    pub fn new(
        table_event_tx: mpsc::Sender<TableEvent>,
        iceberg_event_sync_rx: IcebergEventSyncReceiver,
    ) -> Self {
        Self {
            table_event_tx,
            snapshot_completion_rx: iceberg_event_sync_rx.iceberg_snapshot_completion_rx,
            drop_table_completion_rx: iceberg_event_sync_rx.iceberg_drop_table_completion_rx,
        }
    }

    /// Synchronize on iceberg snapshot completion.
    pub async fn sync_snapshot_completion(&mut self) {
        self.snapshot_completion_rx.recv().await.unwrap()
    }

    /// Initiate an iceberg snapshot event.
    /// TODO(hjiang): For now at most one force snapshot is supported, but request could come from different pg backends, should support multiple snapshot requests.
    pub async fn initiate_snapshot(&mut self, lsn: u64) {
        self.table_event_tx
            .send(TableEvent::ForceSnapshot { lsn })
            .await
            .unwrap()
    }

    /// Drop an iceberg table.
    pub async fn drop_table(&mut self) {
        self.table_event_tx
            .send(TableEvent::DropIcebergTable)
            .await
            .unwrap();
        self.drop_table_completion_rx.recv().await.unwrap();
    }
}
