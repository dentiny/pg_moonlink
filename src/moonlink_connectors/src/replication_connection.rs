use crate::pg_replicate::conversions::cdc_event::{CdcEvent, CdcEventConversionError};
use crate::pg_replicate::moonlink_sink::Sink;
use crate::pg_replicate::postgres_source::CdcStream;
use crate::pg_replicate::postgres_source::{CdcStreamError, PostgresSource, TableNamesFrom};
use crate::pg_replicate::table_init::build_table_components;
use crate::Result;
use moonlink::{IcebergTableEventManager, ReadStateManager};
use std::sync::{Arc, RwLock};
use tokio::pin;

use crate::pg_replicate::replication_state::ReplicationState;
use crate::pg_replicate::table::{TableId, TableSchema};
use futures::StreamExt;
use moonlink::TableEvent;
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_postgres::{connect, Client, NoTls};

pub struct ReplicationConnection {
    uri: String,
    table_base_path: String,
    postgres_client: Client,
    handle: Option<JoinHandle<Result<()>>>,
    table_readers: HashMap<TableId, ReadStateManager>,
    iceberg_snapshot_managers: HashMap<TableId, IcebergTableEventManager>,
    event_senders: Arc<RwLock<HashMap<TableId, Sender<TableEvent>>>>,
    commit_lsn_txs: Arc<RwLock<HashMap<TableId, watch::Sender<u64>>>>,
    replication_state: Arc<ReplicationState>,
    source: PostgresSource,
}

impl ReplicationConnection {
    pub async fn new(uri: String, table_base_path: String) -> Result<Self> {
        let (postgres_client, connection) = connect(&uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("connection error: {}", e);
            }
        });
        postgres_client
            .simple_query(
                "DROP PUBLICATION IF EXISTS moonlink_pub; CREATE PUBLICATION moonlink_pub;",
            )
            .await
            .unwrap();

        let postgres_source = PostgresSource::new(
            &uri,
            Some("moonlink_slot".to_string()),
            TableNamesFrom::Publication("moonlink_pub".to_string()),
        )
        .await
        .unwrap();

        Ok(Self {
            uri,
            table_base_path,
            postgres_client,
            handle: None,
            table_readers: HashMap::new(),
            iceberg_snapshot_managers: HashMap::new(),
            event_senders: Arc::new(RwLock::new(HashMap::new())),
            commit_lsn_txs: Arc::new(RwLock::new(HashMap::new())),
            replication_state: ReplicationState::new(),
            source: postgres_source,
        })
    }

    async fn alter_table_replica_identity(&self, table_name: &str) -> Result<()> {
        self.postgres_client
            .simple_query(&format!(
                "ALTER TABLE {} REPLICA IDENTITY FULL;",
                table_name
            ))
            .await
            .unwrap();
        Ok(())
    }

    async fn add_table_to_publication(&self, table_name: &str) -> Result<()> {
        self.postgres_client
            .simple_query(&format!(
                "ALTER PUBLICATION moonlink_pub ADD TABLE {};",
                table_name
            ))
            .await
            .unwrap();
        self.postgres_client
            .simple_query(&format!(
                "ALTER TABLE {} REPLICA IDENTITY FULL;",
                table_name
            ))
            .await
            .unwrap();
        Ok(())
    }

    async fn remove_table_from_publication(&self, table_name: &str) -> Result<()> {
        self.postgres_client
            .simple_query(&format!(
                "ALTER PUBLICATION moonlink_pub DROP TABLE {};",
                table_name
            ))
            .await
            .unwrap();

        // Optionally reset REPLICA IDENTITY to default
        self.postgres_client
            .simple_query(&format!(
                "ALTER TABLE {} REPLICA IDENTITY DEFAULT;",
                table_name
            ))
            .await
            .unwrap();

        Ok(())
    }

    pub fn get_table_reader(&self, table_id: TableId) -> &ReadStateManager {
        self.table_readers.get(&table_id).unwrap()
    }

    pub fn get_iceberg_snapshot_manager(
        &mut self,
        table_id: TableId,
    ) -> &mut IcebergTableEventManager {
        self.iceberg_snapshot_managers.get_mut(&table_id).unwrap()
    }

    async fn spawn_replication_task(&mut self, sink: Sink) -> JoinHandle<Result<()>> {
        self.source
            .commit_transaction()
            .await
            .expect("failed to commit transaction");

        // TODO: start from the last lsn in replication state for recovery.
        // TODO: track tables copied in replication state and recover these before starting the event loop.
        let mut last_lsn: u64 = 0;
        last_lsn += 1;
        let stream = self
            .source
            .get_cdc_stream(last_lsn.into())
            .await
            .expect("failed to get cdc stream");

        tokio::spawn(async move { run_event_loop(stream, sink).await })
    }

    async fn add_table_to_replication(&mut self, schema: &TableSchema) -> Result<()> {
        let table_id = schema.table_id;
        let resources = build_table_components(
            schema,
            Path::new(&self.table_base_path),
            &self.replication_state,
        )
        .await?;

        self.table_readers
            .insert(table_id, resources.read_state_manager);
        self.iceberg_snapshot_managers
            .insert(table_id, resources.iceberg_snapshot_manager);
        self.event_senders
            .write()
            .unwrap()
            .insert(table_id, resources.event_sender);
        self.commit_lsn_txs
            .write()
            .unwrap()
            .insert(table_id, resources.commit_lsn_tx);

        Ok(())
    }

    async fn remove_table_from_replication(&mut self, table_id: u32) -> Result<()> {
        self.drop_iceberg_table(table_id).await?;

        self.table_readers.remove_entry(&table_id).unwrap();
        self.iceberg_snapshot_managers
            .remove_entry(&table_id)
            .unwrap();
        self.event_senders
            .write()
            .unwrap()
            .remove_entry(&table_id)
            .unwrap();
        self.commit_lsn_txs
            .write()
            .unwrap()
            .remove_entry(&table_id)
            .unwrap();

        Ok(())
    }

    /// Clean up iceberg table in a blocking manner.
    async fn drop_iceberg_table(&mut self, table_id: u32) -> Result<()> {
        let iceberg_state_manager = self.iceberg_snapshot_managers.get_mut(&table_id).unwrap();
        iceberg_state_manager.drop_table().await?;
        Ok(())
    }

    pub async fn start_replication(&mut self) -> Result<()> {
        let table_schemas = self.source.get_table_schemas().await;
        for schema in table_schemas.values() {
            self.add_table_to_replication(schema).await?;
        }

        let sink = Sink::new(
            self.replication_state.clone(),
            self.event_senders.clone(),
            self.commit_lsn_txs.clone(),
        );
        self.handle = Some(self.spawn_replication_task(sink).await);

        Ok(())
    }

    pub async fn add_table(&mut self, table_name: &str) -> Result<TableId> {
        // TODO: We should not naively alter the replica identity of a table. We should only do this if we are sure that the table does not already have a FULL replica identity. [https://github.com/Mooncake-Labs/moonlink/issues/104]
        self.alter_table_replica_identity(table_name).await?;
        let table_schema = self.source.fetch_table_schema(table_name, None).await?;

        self.add_table_to_replication(&table_schema).await?;

        self.add_table_to_publication(table_name).await?;

        Ok(table_schema.table_id)
    }

    /// Remove the given table from connection.
    pub async fn drop_table(&mut self, table_id: u32) -> Result<()> {
        let table_name = self.source.remove_table_schema(table_id);
        self.remove_table_from_publication(&table_name).await?;
        self.remove_table_from_replication(table_id).await?;
        Ok(())
    }

    pub fn check_table_belongs_to_source(&self, uri: &str) -> bool {
        self.uri == uri
    }
}

async fn run_event_loop(stream: CdcStream, mut sink: Sink) -> Result<()> {
    //TODO: add separate stream for initial table copy.
    // This assumes the table is empty and we can naively begin copying rows immediatley. If a table already contains data, this is not the case. We will need to use the get_table_copy_stream method to get the initial rows and write them into a new table. In parallel, we will need to buffer the cdc events for this table until the initial rows are written. For simplicity, we will assume that the table is empty at this point, and add a new stream for initial table copy later.

    pin!(stream);

    while let Some(event) = stream.next().await {
        let mut send_status_update = false;
        if let Err(CdcStreamError::CdcEventConversion(CdcEventConversionError::MissingSchema(_))) =
            &event
        {
            continue;
        }
        if let Ok(CdcEvent::PrimaryKeepAlive(body)) = &event {
            send_status_update = body.reply() == 1;
        }
        let event = event?;
        let last_lsn = sink.write_cdc_event(event).await.unwrap();
        if send_status_update {
            let _ = stream.as_mut().send_status_update(last_lsn).await;
        }
    }

    Ok(())
}
