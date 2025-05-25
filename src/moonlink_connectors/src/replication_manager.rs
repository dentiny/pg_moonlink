use crate::pg_replicate::table::TableId;
use crate::{PostgresSourceError, ReplicationConnection};
use moonlink::{IcebergSnapshotStateManager, ReadStateManager};
use std::collections::HashMap;
use std::hash::Hash;

/// Manage replication sources keyed by their connection URI.
///
/// This struct abstracts the lifecycle of `MoonlinkPostgresSource` and
/// provides a single entry point to add new tables to a running
/// replication. A new replication will automatically be started when a
/// table is added for a URI that is not currently being replicated.
pub struct ReplicationManager<T: Eq + Hash> {
    connections: HashMap<String, ReplicationConnection>,
    table_base_path: String,
    table_info: HashMap<T, (String, TableId)>,
}

impl<T: Eq + Hash> ReplicationManager<T> {
    pub fn new(table_base_path: String) -> Self {
        Self {
            connections: HashMap::new(),
            table_base_path,
            table_info: HashMap::new(),
        }
    }

    /// Add a table to be replicated from the given `uri`.
    ///
    /// If replication for this `uri` is not yet running a new replication
    /// source will be created.
    pub async fn add_table(
        &mut self,
        uri: &str,
        external_table_id: T,
        table_name: &str,
    ) -> Result<(), PostgresSourceError> {
        if !self.connections.contains_key(uri) {
            // Lazily create the directory that will hold all tables.
            // This will not overwrite any existing directory.
            tokio::fs::create_dir_all(&self.table_base_path)
                .await
                .map_err(PostgresSourceError::Io)?;
            let base_path = tokio::fs::canonicalize(&self.table_base_path)
                .await
                .map_err(PostgresSourceError::Io)?;
            let mut replication_connection =
                ReplicationConnection::new(uri.to_owned(), base_path.to_str().unwrap().to_string())
                    .await?;
            replication_connection.start_replication().await?;
            self.connections
                .insert(uri.to_string(), replication_connection);
        }
        let replication_connection = self.connections.get_mut(uri).unwrap();

        let table_id = replication_connection.add_table(table_name).await?;
        self.table_info
            .insert(external_table_id, (uri.to_string(), table_id));

        Ok(())
    }

    pub fn get_table_reader(&self, table_id: &T) -> &ReadStateManager {
        let (uri, table_id) = self.table_info.get(table_id).expect("table not found");
        let connection = self.connections.get(uri).expect("connection not found");
        connection.get_table_reader(*table_id)
    }

    pub fn get_iceberg_snapshot_manager(
        &mut self,
        table_id: &T,
    ) -> &mut IcebergSnapshotStateManager {
        let (uri, table_id) = self.table_info.get(table_id).expect("table not found");
        let connection = self.connections.get_mut(uri).expect("connection not found");
        connection.get_iceberg_snapshot_manager(*table_id)
    }
}
