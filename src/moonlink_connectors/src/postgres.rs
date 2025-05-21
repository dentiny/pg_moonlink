use crate::pg_replicate::conversions::cdc_event::{CdcEvent, CdcEventConversionError};
use crate::pg_replicate::moonlink_sink::Sink;
use crate::pg_replicate::postgres_source::{
    CdcStreamError, PostgresSource, PostgresSourceError, TableNamesFrom,
};
use moonlink::{IcebergSnapshotStateManager, ReadStateManager};
use tokio::pin;

use futures::StreamExt;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::{connect, Client, NoTls};

pub struct MoonlinkPostgresSource {
    uri: String,
    table_base_path: String,
    postgres_client: Client,
    handle: Option<JoinHandle<Result<(), PostgresSourceError>>>,
}

impl MoonlinkPostgresSource {
    pub async fn new(uri: String, table_base_path: String) -> Result<Self, PostgresSourceError> {
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
        Ok(Self {
            uri,
            table_base_path,
            postgres_client,
            handle: None,
        })
    }

    pub async fn add_table(
        &mut self,
        table_name: &str,
    ) -> Result<(ReadStateManager, IcebergSnapshotStateManager), PostgresSourceError> {
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
        let source = PostgresSource::new(
            &self.uri,
            Some("moonlink_slot".to_string()),
            TableNamesFrom::Publication("moonlink_pub".to_string()),
        )
        .await?;
        // Use channel to blockingly asynchronously create and get ReadStateManager and IcebergSnapshotStateManager.
        let (reader_notifier, mut reader_notifier_receiver) = mpsc::channel(1);
        let (iceberg_snapshot_notifier, mut iceberg_snapshot_notifier_receiver) = mpsc::channel(1);

        let sink = Sink::new(
            reader_notifier,
            iceberg_snapshot_notifier,
            PathBuf::from(self.table_base_path.clone()),
        );
        let handle = tokio::spawn(async move { run_replication(source, sink).await });
        self.handle = Some(handle);

        let read_state_manager = reader_notifier_receiver.recv().await;
        let iceberg_snapshot_manager = iceberg_snapshot_notifier_receiver.recv().await;
        Ok((
            read_state_manager.unwrap(),
            iceberg_snapshot_manager.unwrap(),
        ))
    }

    pub fn check_table_belongs_to_source(&self, uri: &str) -> bool {
        self.uri == uri
    }
}

async fn run_replication(
    mut source: PostgresSource,
    mut sink: Sink,
) -> Result<(), PostgresSourceError> {
    sink.write_table_schemas(source.get_table_schemas().clone())
        .await
        .unwrap();

    //TODO: add separate stream for initial table copy.
    // This assumes the table is empty and we can naively begin copying rows immediatley. If a table already contains data, this is not the case. We will need to use the get_table_copy_stream method to get the initial rows and write them into a new table. In parallel, we will need to buffer the cdc events for this table until the initial rows are written. For simplicity, we will assume that the table is empty at this point, and add a new stream for initial table copy later.

    source.commit_transaction().await?;

    // TODO: start from the last lsn in replication state for recovery.
    // TODO: track tables copied in replication state.
    let mut last_lsn: u64 = 0;
    last_lsn += 1;
    let stream = source.get_cdc_stream(last_lsn.into()).await?;

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
