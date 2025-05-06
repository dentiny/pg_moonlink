use crate::storage::iceberg::catalog_utils;
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
/// IcebergTableManager is responsible to interact with an iceberg table, including
/// - read and write operations for all types of storage
/// - maintain metadata for table write status; for example, persisted data files and their corresponding deletion vector
/// - local caching
use crate::storage::iceberg::file_catalog::FileSystemCatalog;
use crate::storage::iceberg::puffin_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::TableMetadata as MoonlinkTableMetadata;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use futures::executor::block_on;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::DataFile;
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::Result as IcebergResult;
use mockall::*;
use uuid::Uuid;

#[allow(dead_code)]
pub struct IcebergTableManagerConfig {
    /// Table warehouse location.
    warehouse_uri: String,
    /// Moonlink table metadata.
    table_metadata: Arc<MoonlinkTableMetadata>,
    /// Namespace for the iceberg table.
    namespace: Vec<String>,
    /// Iceberg table name.
    table: String,
}

#[allow(dead_code)]
#[automock]
pub(crate) trait IcebergOperation {
    /// Write a new snapshot to iceberg table.
    async fn commit_snapshot(
        &mut self,
        snapshot_disk_files: HashMap<PathBuf, BatchDeletionVector>,
    ) -> IcebergResult<()>;
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct DataFileEntry {
    /// Iceberg data file.
    data_file: DataFile,
    /// In-memory deletion vector.
    deletion_vector: BatchDeletionVector,
}

/// TODO(hjiang):
/// 1. Craft a trait for table manager so we could mock later.
/// 2. Support a data file handle, which is a remote file path, plus an optional local cache filepath.
/// 3. Support a deletion vector handle, which is a remote file path, with an optional in-memory buffer and a local cache filepath.
#[allow(dead_code)]
pub struct IcebergTableManager {
    /// TODO(hjiang): A workaround iceberg-rust doesn't support deletion vector yet.
    /// Support only filesystem catalog for now, will add object storage catalog.
    filesystem_catalog: FileSystemCatalog,

    /// The iceberg table it's managing.
    iceberg_table: IcebergTable,

    /// Moonlink table metadata.
    mooncake_table_metadata: Arc<MoonlinkTableMetadata>,

    /// Maps from already persisted data file filepath to its deletion vector, and iceberg `DataFile`.
    persisted_items: HashMap<PathBuf, DataFileEntry>,
}

impl IcebergTableManager {
    pub fn new(config: IcebergTableManagerConfig) -> IcebergResult<IcebergTableManager> {
        let filesystem_catalog = FileSystemCatalog::new(config.warehouse_uri.clone());
        let iceberg_table = block_on(catalog_utils::get_or_create_iceberg_table(
            &filesystem_catalog,
            &config.warehouse_uri,
            &config.namespace,
            &config.table.clone(),
            config.table_metadata.schema.as_ref(),
        ))?;

        Ok(Self {
            filesystem_catalog,
            iceberg_table,
            mooncake_table_metadata: config.table_metadata,
            persisted_items: HashMap::new(),
        })
    }

    /// Write deletion vector to puffin file.
    async fn write_deletion_vector(
        &mut self,
        data_file: String,
        deletion_vector: BatchDeletionVector,
    ) -> IcebergResult<()> {
        let deleted_rows = deletion_vector.collect_deleted_rows();
        if deleted_rows.is_empty() {
            return Ok(());
        }

        if !deleted_rows.is_empty() {
            // TODO(hjiang): Currently one deletion vector is stored in one puffin file, need to revisit later.
            let deleted_row_count = deleted_rows.len();
            let mut iceberg_deletion_vector = DeletionVector::new();
            iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
            let blob_properties = HashMap::from([
                (DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(), data_file),
                (
                    DELETION_VECTOR_CADINALITY.to_string(),
                    deleted_row_count.to_string(),
                ),
            ]);
            let table_metadata = self.iceberg_table.metadata();
            let blob = iceberg_deletion_vector.serialize(
                table_metadata.current_snapshot_id().unwrap_or(-1),
                table_metadata.next_sequence_number(),
                blob_properties,
            );

            // TODO(hjiang): Current iceberg-rust doesn't support deletion vector officially, so we do our own hack to rewrite manifest file by our own catalog implementation.
            let location_generator =
                DefaultLocationGenerator::new(self.iceberg_table.metadata().clone())?;
            let puffin_filepath =
                location_generator.generate_location(&format!("{}-puffin.bin", Uuid::new_v4()));
            let mut puffin_writer = puffin_utils::create_puffin_writer(
                self.iceberg_table.file_io(),
                puffin_filepath.clone(),
            )
            .await?;
            // TODO(hjiang): Provide option to enable compression for puffin blob.
            puffin_writer.add(blob, CompressionCodec::None).await?;

            self.filesystem_catalog
                .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
                .await?;
        }

        Ok(())
    }
}

impl IcebergOperation for IcebergTableManager {
    /// TODO(hjiang): Parallelize all IO operations.
    async fn commit_snapshot(
        &mut self,
        disk_files: HashMap<PathBuf, BatchDeletionVector>,
    ) -> IcebergResult<()> {
        let mut new_data_files = vec![];
        let mut new_persisted_items: HashMap<PathBuf, DataFileEntry> = HashMap::new();
        let old_persisted_items = self.persisted_items.clone();
        for (local_path, deletion_vector) in disk_files.into_iter() {
            match old_persisted_items.get(&local_path) {
                Some(entry) => {
                    if entry.deletion_vector == deletion_vector {
                        let mut new_data_file_entry: DataFileEntry = entry.clone();
                        new_data_file_entry.deletion_vector = deletion_vector;
                        new_persisted_items.insert(local_path, new_data_file_entry);
                        continue;
                    }
                    let path_str = entry.data_file.file_path().to_string();
                    self.write_deletion_vector(path_str, deletion_vector.clone())
                        .await?;

                    let mut new_data_file_entry: DataFileEntry = entry.clone();
                    new_data_file_entry.deletion_vector = deletion_vector;
                    new_persisted_items.insert(local_path, new_data_file_entry);
                }
                None => {
                    let data_file = catalog_utils::write_record_batch_to_iceberg(
                        &self.iceberg_table,
                        &local_path,
                    )
                    .await?;
                    let data_filepath = data_file.file_path().to_string();
                    new_data_files.push(data_file.clone());
                    self.write_deletion_vector(data_filepath, deletion_vector.clone())
                        .await?;
                    new_persisted_items.insert(
                        local_path,
                        DataFileEntry {
                            data_file,
                            deletion_vector,
                        },
                    );
                }
            }
        }
        self.persisted_items = new_persisted_items;

        let txn = Transaction::new(&self.iceberg_table);
        let mut action =
            txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;
        action.add_data_files(new_data_files)?;
        let txn = action.apply().await?;
        txn.commit(&self.filesystem_catalog).await?;
        self.filesystem_catalog.clear_puffin_metadata();

        Ok(())
    }
}
