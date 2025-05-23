pub use moonlink::ReadState;
mod error;

pub use error::Error;
use error::Result;
use moonlink_connectors::ReplicationManager;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;

// Default local filesystem directory where all tables data will be stored under.
const DEFAULT_MOONLINK_TABLE_BASE_PATH: &str = "./mooncake/";

pub struct MoonlinkBackend<T: Eq + Hash> {
    // Could be either relative or absolute path.
    replication_manager: RwLock<ReplicationManager<T>>,
}

impl<T: Eq + Hash + Clone> Default for MoonlinkBackend<T> {
    fn default() -> Self {
        Self::new(DEFAULT_MOONLINK_TABLE_BASE_PATH.to_string())
    }
}

impl<T: Eq + Hash + Clone> MoonlinkBackend<T> {
    pub fn new(base_path: String) -> Self {
        Self {
            replication_manager: RwLock::new(ReplicationManager::new(base_path.clone())),
        }
    }

    pub async fn create_table(&self, table_id: T, table_name: &str, uri: &str) -> Result<()> {
        let mut manager = self.replication_manager.write().await;
        manager.add_table(uri, table_id, table_name).await?;
        Ok(())
    }

    pub async fn drop_table(&self, _table_id: T) -> Result<()> {
        todo!()
    }

    pub async fn scan_table(&self, table_id: &T, lsn: Option<u64>) -> Result<Arc<ReadState>> {
        let manager = self.replication_manager.read().await;

        let table_reader = manager.get_table_reader(table_id);
        let read_state = table_reader.try_read(lsn).await?;
        Ok(read_state)
    }

    /// Create an iceberg snapshot with the given LSN, return when the a snapshot is successfully created.
    pub async fn create_iceberg_snapshot(&self, table_id: &T, lsn: u64) -> Result<()> {
        let mut manager = self.replication_manager.write().await;

        let writer = manager.get_iceberg_snapshot_manager(table_id);
        writer.initiate_snapshot(lsn).await;
        writer.sync_snapshot_completion().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio_postgres::{connect, NoTls};

    #[tokio::test]
    async fn test_moonlink_service() {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let uri = "postgresql://postgres:postgres@postgres:5432/postgres";
        let service =
            MoonlinkBackend::<&'static str>::new(temp_dir.path().to_str().unwrap().to_string());
        // connect to postgres and create a table
        let (client, connection) = connect(uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client.simple_query("DROP TABLE IF EXISTS test; CREATE TABLE test (id bigint PRIMARY KEY, name VARCHAR(255));").await.unwrap();
        service
            .create_table("test", "public.test", uri)
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test VALUES (1 ,'foo');")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test VALUES (2 ,'bar');")
            .await
            .unwrap();
        let old = service.scan_table(&"test", None).await.unwrap();
        // wait 2 second
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let new = service.scan_table(&"test", None).await.unwrap();
        assert_ne!(old.data, new.data);
    }
}
