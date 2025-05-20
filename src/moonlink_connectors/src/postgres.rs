mod sink;
mod util;
use crate::pg_replicate::pipeline::{
    batching::data_pipeline::BatchDataPipeline,
    batching::BatchConfig,
    sinks::InfallibleSinkError,
    sources::postgres::{PostgresSource, PostgresSourceError, TableNamesFrom},
    PipelineAction, PipelineError,
};
use moonlink::{IcebergSnapshotStateManager, ReadStateManager};

use sink::*;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::{connect, Client, NoTls};

pub struct MoonlinkPostgresSource {
    uri: String,
    table_base_path: String,
    postgres_client: Client,
    handle: Option<
        JoinHandle<
            std::result::Result<(), PipelineError<PostgresSourceError, InfallibleSinkError>>,
        >,
    >,
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
        let (iceberg_snapshot_state_notifier, mut iceberg_snapshot_state_notifier_receiver) =
            mpsc::channel(1);

        let sink = Sink::new(
            reader_notifier,
            iceberg_snapshot_state_notifier,
            PathBuf::from(self.table_base_path.clone()),
        );
        let batch_config = BatchConfig::new(1000, Duration::from_secs(1));
        let mut pipeline =
            BatchDataPipeline::new(source, sink, PipelineAction::CdcOnly, batch_config);
        let pipeline_handle = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move { pipeline.start().await })
        });
        self.handle = Some(pipeline_handle);

        let read_state_manager = reader_notifier_receiver.recv().await;
        let iceberg_snapshot_manager = iceberg_snapshot_state_notifier_receiver.recv().await;
        Ok((
            read_state_manager.unwrap(),
            iceberg_snapshot_manager.unwrap(),
        ))
    }

    pub fn check_table_belongs_to_source(&self, uri: &str) -> bool {
        self.uri == uri
    }
}
