use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
use crate::storage::iceberg::index::FileIndexBlob;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::recovery_utils::DataFileEntry;
use crate::storage::iceberg::utils;
use crate::storage::iceberg::validation as IcebergValidation;
use crate::storage::index::{FileIndex as MooncakeFileIndex, MooncakeIndex};
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::vec;

use iceberg::io::FileIO;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::DataFileFormat;
use iceberg::spec::{DataFile, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::Result as IcebergResult;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[cfg(test)]
use mockall::*;

use super::recovery_utils;

#[allow(dead_code)]
#[derive(Clone, Debug, TypedBuilder)]
pub struct IcebergTableManagerConfig {
    /// Table warehouse location.
    #[builder(default = "/tmp/moonlink_iceberg".to_string())]
    pub warehouse_uri: String,
    /// Mooncake table metadata.
    pub mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    /// Namespace for the iceberg table.
    #[builder(default = vec!["default".to_string()])]
    pub namespace: Vec<String>,
    /// Iceberg table name.
    #[builder(default = "table".to_string())]
    pub table_name: String,
}

#[allow(dead_code)]
#[cfg_attr(test, automock)]
pub(crate) trait IcebergOperation {
    /// Write a new snapshot to iceberg table.
    /// It could be called for multiple times to write and commit multiple snapshots.
    ///
    /// # Arguments
    ///
    /// * disk_files: a map from data file paths to batch deletion vector.
    ///   Data file path could be either local (when upload local data file to iceberg), or remote (after recovery from iceberg table).
    /// * file_indices: all file indexes which have been persisted, could be either local or remote.
    ///
    /// Please notice, it takes full content to commit snapshot.
    ///
    /// TODO(hjiang): We're storing the iceberg table status in two places, one for iceberg table manager, another at snapshot.
    /// Provide delta change interface, so snapshot doesn't need to store everything.
    async fn sync_snapshot(
        &mut self,
        disk_files: HashMap<PathBuf, BatchDeletionVector>,
        file_indices: &[MooncakeFileIndex],
    ) -> IcebergResult<()>;

    // /// Load latest snapshot from iceberg table. Used for recovery and initialization.
    // async fn load_snapshot_from_table(&mut self) -> IcebergResult<MooncakeSnapshot>
    // where
    //     Self: Sized;
}

/// TODO(hjiang):
/// 1. Support a data file handle, which is a remote file path, plus an optional local cache filepath.
/// 2. Support a deletion vector handle, which is a remote file path, with an optional in-memory buffer and a local cache filepath.
#[allow(dead_code)]
#[derive(Debug)]
pub struct IcebergTableManager {
    /// Iceberg table configuration.
    config: IcebergTableManagerConfig,

    /// Iceberg catalog, which interacts with the iceberg table.
    catalog: Box<dyn MoonlinkCatalog>,

    /// The iceberg table it's managing.
    iceberg_table: Option<IcebergTable>,

    /// Maps from already persisted data file filepath to its deletion vector, and iceberg `DataFile`.
    persisted_data_files: HashMap<PathBuf, DataFileEntry>,

    /// A set of file index id which has been managed by the iceberg table.
    persisted_file_index_ids: HashSet<u32>,
}

impl IcebergTableManager {
    #[allow(dead_code)]
    pub async fn new(config: IcebergTableManagerConfig) -> (IcebergTableManager, MooncakeSnapshot) {
        let catalog = utils::create_catalog(&config.warehouse_uri).unwrap();
        let mut iceberg_table = utils::get_or_create_iceberg_table(
            &*catalog,
            &config.warehouse_uri,
            &config.namespace,
            &config.table_name.clone(),
            config.mooncake_table_metadata.schema.as_ref(),
        )
        .await
        .unwrap();
        let recovered_items = recovery_utils::load_snapshot_from_table(
            &mut iceberg_table,
            config.mooncake_table_metadata.clone(),
        )
        .await
        .unwrap();

        let manager = Self {
            config,
            catalog,
            iceberg_table: None,
            persisted_data_files: recovered_items.persisted_data_files,
            persisted_file_index_ids: recovered_items.persisted_file_index_ids,
        };

        (manager, recovered_items.snapshot)
    }

    /// Get a unique puffin filepath under table warehouse uri.
    fn get_unique_puffin_filepath(&self) -> String {
        let location_generator =
            DefaultLocationGenerator::new(self.iceberg_table.as_ref().unwrap().metadata().clone())
                .unwrap();
        location_generator.generate_location(&format!("{}-puffin.bin", Uuid::new_v4()))
    }

    /// ---------- store snapshot ----------
    ///
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
            let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
            // TODO(hjiang): Fix sequence number and snapshot id, which should be -1.
            let blob = iceberg_deletion_vector.serialize(
                table_metadata.current_snapshot_id().unwrap_or(-1),
                table_metadata.next_sequence_number(),
                blob_properties,
            );
            let puffin_filepath = self.get_unique_puffin_filepath();
            let mut puffin_writer = puffin_utils::create_puffin_writer(
                self.iceberg_table.as_ref().unwrap().file_io(),
                puffin_filepath.clone(),
            )
            .await?;
            puffin_writer.add(blob, CompressionCodec::None).await?;

            self.catalog
                .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
                .await?;
        }

        Ok(())
    }

    /// Dump in-memory data files and their deletion vector into iceberg table,
    /// only the changed part (i.e. new data files, updated deletion vectors) will be persisted into the table.
    /// Returns new data files to append into iceberg table.
    async fn sync_data_files_and_deletion_vector(
        &mut self,
        mut disk_files: HashMap<PathBuf, BatchDeletionVector>,
    ) -> IcebergResult<(
        Vec<DataFile>,                   /*new data files*/
        HashMap<PathBuf, DataFileEntry>, /*new data file entries*/
    )> {
        let mut new_data_files = vec![];
        let mut new_persisted_data_files: HashMap<PathBuf, DataFileEntry> = HashMap::new();
        let old_persisted_data_files = self.persisted_data_files.clone();
        for (local_path, deletion_vector) in disk_files.drain() {
            match old_persisted_data_files.get(&local_path) {
                Some(entry) => {
                    if entry.deletion_vector == deletion_vector {
                        let mut new_data_file_entry: DataFileEntry = entry.clone();
                        new_data_file_entry.deletion_vector = deletion_vector;
                        new_persisted_data_files.insert(local_path, new_data_file_entry);
                        continue;
                    }
                    let path_str = entry.data_file.file_path().to_string();
                    self.write_deletion_vector(path_str, deletion_vector.clone())
                        .await?;

                    let mut new_data_file_entry: DataFileEntry = entry.clone();
                    new_data_file_entry.deletion_vector = deletion_vector;
                    new_persisted_data_files.insert(local_path, new_data_file_entry);
                }
                None => {
                    let data_file = utils::write_record_batch_to_iceberg(
                        self.iceberg_table.as_ref().unwrap(),
                        &local_path,
                    )
                    .await?;
                    let data_filepath = data_file.file_path().to_string();
                    new_data_files.push(data_file.clone());
                    self.write_deletion_vector(data_filepath, deletion_vector.clone())
                        .await?;
                    new_persisted_data_files.insert(
                        local_path,
                        DataFileEntry {
                            data_file,
                            deletion_vector,
                        },
                    );
                }
            }
        }
        Ok((new_data_files, new_persisted_data_files))
    }

    /// Dump file indexes into the iceberg table, only new file indexes will be persisted into the table.
    /// Return file index ids which should be added into iceberg table.
    ///
    /// TODO(hjiang): Need to configure (1) the number of blobs in a puffin file; and (2) the number of file index in a puffin blob.
    /// For implementation simpicity, put everything in a single file and a single blob.
    async fn sync_file_indices(&mut self, file_indices: &[MooncakeFileIndex]) -> IcebergResult<()> {
        if file_indices.is_empty() {
            return Ok(());
        }

        let puffin_filepath = self.get_unique_puffin_filepath();
        let mut puffin_writer = puffin_utils::create_puffin_writer(
            self.iceberg_table.as_ref().unwrap().file_io(),
            puffin_filepath.clone(),
        )
        .await?;

        // TODO(hjiang): Maps from local filepath to remote filepath.
        // After sync, file index still stores local index file location.
        // After cache design, we should be able to provide a "handle" abstraction, which could be either local or remote.
        // The hash map here is merely a workaround to pass remote path to iceberg file index structure.
        let mut local_index_file_to_remote = HashMap::new();

        let mut new_file_indices: Vec<&MooncakeFileIndex> =
            Vec::with_capacity(file_indices.len() - self.persisted_file_index_ids.len());
        for cur_file_index in file_indices.iter() {
            if self
                .persisted_file_index_ids
                .insert(cur_file_index.global_index_id)
            {
                // Record new index file id.
                new_file_indices.push(cur_file_index);
                // Upload new index file to iceberg table.
                for cur_index_block in cur_file_index.index_blocks.iter() {
                    let remote_index_block = utils::upload_index_file(
                        self.iceberg_table.as_ref().unwrap(),
                        &cur_index_block.file_path,
                    )
                    .await?;
                    local_index_file_to_remote
                        .insert(cur_index_block.file_path.clone(), remote_index_block);
                }
            }
        }

        let file_index_blob = FileIndexBlob::new(new_file_indices, local_index_file_to_remote);
        let puffin_blob = file_index_blob.as_blob()?;
        puffin_writer
            .add(puffin_blob, iceberg::puffin::CompressionCodec::None)
            .await?;
        self.catalog
            .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
            .await?;

        Ok(())
    }
}

/// TODO(hjiang): Parallelize all IO operations.
impl IcebergOperation for IcebergTableManager {
    async fn sync_snapshot(
        &mut self,
        disk_files: HashMap<PathBuf, BatchDeletionVector>,
        file_indices: &[MooncakeFileIndex],
    ) -> IcebergResult<()> {
        // Persist data files and deletion vector changes.
        let (new_data_files, new_persisted_data_files) =
            self.sync_data_files_and_deletion_vector(disk_files).await?;

        // If no change for both data files and deletion vectors (it also implies no new index files), no need to initiate a new transaction.
        if self.persisted_data_files == new_persisted_data_files {
            return Ok(());
        }
        self.persisted_data_files = new_persisted_data_files;

        // Persist file index changes.
        self.sync_file_indices(file_indices).await?;

        // Only start append action when there're new data files.
        let mut txn = Transaction::new(self.iceberg_table.as_ref().unwrap());
        if !new_data_files.is_empty() {
            let mut action =
                txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;
            action.add_data_files(new_data_files)?;
            txn = action.apply().await?;
        }

        self.iceberg_table = Some(txn.commit(&*self.catalog).await?);
        self.catalog.clear_puffin_metadata();

        Ok(())
    }
}

unsafe impl Send for IcebergTableManager {}
unsafe impl Sync for IcebergTableManager {}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::row::Identity as RowIdentity;
    use crate::row::MoonlinkRow;
    use crate::row::{Identity, RowValue};
    use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
    #[cfg(feature = "storage-s3")]
    use crate::storage::iceberg::s3_test_utils;
    use crate::storage::mooncake_table::{
        TableConfig as MooncakeTableConfig, TableMetadata as MooncakeTableMetadata,
    };
    use crate::storage::MooncakeTable;

    use std::collections::HashMap;
    use std::fs::File;

    use std::sync::Arc;
    use tempfile::tempdir;

    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::datatypes::{DataType, Field};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use iceberg::io::FileRead;
    use iceberg::Error as IcebergError;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;

    /// Create test batch deletion vector.
    fn test_deletion_vector_1() -> BatchDeletionVector {
        let mut deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
        deletion_vector.delete_row(2);
        deletion_vector
    }
    /// Test deletion vector 2 includes deletion vector 1, used to mimic new data file rows deletion situation.
    fn test_deletion_vector_2() -> BatchDeletionVector {
        let mut deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
        deletion_vector.delete_row(1);
        deletion_vector.delete_row(2);
        deletion_vector
    }

    /// Test util function to create arrow schema.
    fn create_test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
            Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "3".to_string(),
            )])),
        ]))
    }

    /// Test util function to create mooncake table metadata.
    fn create_test_table_metadata(local_table_directory: String) -> Arc<MooncakeTableMetadata> {
        Arc::new(MooncakeTableMetadata {
            name: "test_table".to_string(),
            id: 0,
            schema: create_test_arrow_schema(),
            config: MooncakeTableConfig::new(),
            path: PathBuf::from(local_table_directory),
            identity: RowIdentity::FullRow,
        })
    }

    /// Test util function to create arrow record batch.
    fn test_batch_1(arrow_schema: Arc<ArrowSchema>) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
                Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
                Arc::new(Int32Array::from(vec![10, 20, 30])), // age column
            ],
        )
        .unwrap()
    }
    fn test_batch_2(arrow_schema: Arc<ArrowSchema>) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])), // id column
                Arc::new(StringArray::from(vec!["d", "e", "f"])), // name column
                Arc::new(Int32Array::from(vec![40, 50, 60])), // age column
            ],
        )
        .unwrap()
    }

    /// Test util function to write arrow record batch into local file.
    async fn write_arrow_record_batch_to_local<P: AsRef<std::path::Path>>(
        path: P,
        schema: Arc<ArrowSchema>,
        batch: &RecordBatch,
    ) -> IcebergResult<()> {
        let file = File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }

    /// Test util function to load all arrow batch from the given parquet file.
    async fn load_arrow_batch(file_io: &FileIO, filepath: &str) -> IcebergResult<RecordBatch> {
        let input_file = file_io.new_input(filepath)?;
        let input_file_metadata = input_file.metadata().await?;
        let reader = input_file.reader().await?;
        let bytes = reader.read(0..input_file_metadata.size).await?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        let mut reader = builder.build()?;
        let batch = reader.next().transpose()?.expect("Should have one batch");
        Ok(batch)
    }

    /// Test util to get file indices filepaths and their corresponding data filepaths.
    fn get_file_indices_filepath_and_data_filepaths(
        mooncake_index: &MooncakeIndex,
    ) -> (
        Vec<String>, /*file indices filepath*/
        Vec<String>, /*data filepaths*/
    ) {
        let file_indices = &mooncake_index.file_indices;

        let mut data_files: Vec<String> = vec![];
        let mut index_files: Vec<String> = vec![];
        for cur_file_index in file_indices.iter() {
            data_files.extend(
                cur_file_index
                    .files
                    .iter()
                    .map(|cur_file| cur_file.as_path().to_str().unwrap().to_string())
                    .collect::<Vec<_>>(),
            );
            index_files.extend(
                cur_file_index
                    .index_blocks
                    .iter()
                    .map(|cur_index_block| cur_index_block.file_path.clone())
                    .collect::<Vec<_>>(),
            );
        }

        (data_files, index_files)
    }

    /// Test snapshot store and load for different types of catalogs based on the given warehouse.
    async fn test_store_and_load_snapshot_impl(
        iceberg_table_manager: &mut IcebergTableManager,
    ) -> IcebergResult<()> {
        // At the beginning of the test, there's nothing in table.
        assert!(iceberg_table_manager.persisted_data_files.is_empty());

        // Create arrow schema and table.
        let arrow_schema = create_test_arrow_schema();
        let tmp_dir = tempdir()?;

        // Write first snapshot to iceberg table (with deletion vector).
        let data_filename_1 = "data-1.parquet";
        let batch = test_batch_1(arrow_schema.clone());
        let parquet_path = tmp_dir.path().join(data_filename_1);
        write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch)
            .await?;
        let mut snapshot_disk_files: HashMap<PathBuf, BatchDeletionVector> = HashMap::new();
        snapshot_disk_files.insert(parquet_path.clone(), test_deletion_vector_1());
        iceberg_table_manager
            .sync_snapshot(
                snapshot_disk_files.clone(),
                /*file_indices=*/ vec![].as_slice(),
            )
            .await?;

        // Write second snapshot to iceberg table, with updated deletion vector and new data file.
        snapshot_disk_files.insert(parquet_path.clone(), test_deletion_vector_2());

        let data_filename_2 = "data-2.parquet";
        let batch = test_batch_2(arrow_schema.clone());
        let parquet_path = tmp_dir.path().join(data_filename_2);
        write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch)
            .await?;
        snapshot_disk_files.insert(parquet_path.clone(), test_deletion_vector_1());
        iceberg_table_manager
            .sync_snapshot(
                snapshot_disk_files,
                /*file_indices=*/ vec![].as_slice(),
            )
            .await?;

        // Check persisted items in the iceberg table.
        assert_eq!(
            iceberg_table_manager.persisted_data_files.len(),
            2,
            "Persisted items for table manager is {:?}",
            iceberg_table_manager.persisted_data_files
        );

        // Check the loaded data file is of the expected format and content.
        let file_io = iceberg_table_manager
            .iceberg_table
            .as_ref()
            .unwrap()
            .file_io();
        for (loaded_path, data_entry) in iceberg_table_manager.persisted_data_files.iter() {
            let loaded_arrow_batch =
                load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;
            let deleted_rows = data_entry.deletion_vector.collect_deleted_rows();
            assert_eq!(
                *loaded_arrow_batch.schema_ref(),
                arrow_schema,
                "Expect arrow schema {:?}, actual arrow schema {:?}",
                arrow_schema,
                loaded_arrow_batch.schema_ref()
            );

            // Check second data file and its deletion vector.
            if loaded_path.to_str().unwrap().ends_with(data_filename_2) {
                assert_eq!(
                    loaded_arrow_batch,
                    test_batch_2(arrow_schema.clone()),
                    "Expect arrow record batch {:?}, actual arrow record batch {:?}",
                    loaded_arrow_batch,
                    test_batch_2(arrow_schema.clone())
                );
                assert_eq!(
                    deleted_rows,
                    test_deletion_vector_1().collect_deleted_rows(),
                    "Loaded deletion vector is not the same as the one gets stored."
                );
                continue;
            }

            // Check first data file and its deletion vector.
            assert!(loaded_path.to_str().unwrap().ends_with(data_filename_1));
            assert_eq!(
                loaded_arrow_batch,
                test_batch_1(arrow_schema.clone()),
                "Expect arrow record batch {:?}, actual arrow record batch {:?}",
                loaded_arrow_batch,
                test_batch_1(arrow_schema.clone())
            );
            assert_eq!(
                deleted_rows,
                test_deletion_vector_2().collect_deleted_rows(),
                "Loaded deletion vector is not the same as the one gets stored."
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_snapshots() -> IcebergResult<()> {
        // Create arrow schema and table.
        let tmp_dir = tempdir()?;
        let mooncake_table_metadata =
            create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
        let config = IcebergTableManagerConfig {
            warehouse_uri: tmp_dir.path().to_str().unwrap().to_string(),
            mooncake_table_metadata,
            namespace: vec!["namespace".to_string()],
            table_name: "test_table".to_string(),
        };
        let mut iceberg_table_manager = IcebergTableManager::new(config);
        test_store_and_load_snapshot_impl(&mut iceberg_table_manager).await?;
        Ok(())
    }

    // TODO(hjiang): Figure out a way to check file index content; for example, search for an item.
    async fn mooncake_table_snapshot_persist_impl(warehouse_uri: String) -> IcebergResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();
        let iceberg_table_config = IcebergTableManagerConfig {
            warehouse_uri,
            mooncake_table_metadata: create_test_table_metadata(path.to_str().unwrap().to_string()),
            namespace: vec!["namespace".to_string()],
            table_name: "test_table".to_string(),
        };
        let schema = create_test_arrow_schema();
        let mut table = MooncakeTable::new(
            schema.as_ref().clone(),
            "test_table".to_string(),
            /*version=*/ 1,
            path,
            Identity::Keys(vec![0]),
            Some(iceberg_table_config.clone()),
        );

        // Perform a few table write operations.
        //
        // Operation series 1: append three rows, delete one of them, flush, commit and create snapshot.
        // Expects to see one data file with no deletion vector, because mooncake table handle deletion inline before persistence, and all record batches are dumped into one single data file.
        // The three rows are deleted in three operations series respectively.
        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("John".as_bytes().to_vec()),
            RowValue::Int32(30),
        ]);
        table.append(row1.clone()).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to append row1 {:?} to mooncake table because {:?}",
                    row1, e
                ),
            )
        })?;
        let row2 = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Alice".as_bytes().to_vec()),
            RowValue::Int32(10),
        ]);
        table.append(row2.clone()).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to append row2 {:?} to mooncake table because {:?}",
                    row2, e
                ),
            )
        })?;
        let row3 = MoonlinkRow::new(vec![
            RowValue::Int32(3),
            RowValue::ByteArray("Bob".as_bytes().to_vec()),
            RowValue::Int32(50),
        ]);
        table.append(row3.clone()).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to append row3 {:?} to mooncake table because {:?}",
                    row3, e
                ),
            )
        })?;
        // First deletion of row2, which happens in MemSlice.
        table.delete(row1.clone(), /*lsn=*/ 100);
        table.flush(/*lsn=*/ 200).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to flush records to mooncake table because {:?}", e),
            )
        })?;
        table.commit(/*lsn=*/ 200);
        table.create_snapshot().unwrap().await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create snapshot to iceberg table because {:?}", e),
            )
        })?;

        // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
        let mut iceberg_table_manager = IcebergTableManager::new(iceberg_table_config.clone());
        let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
        assert_eq!(
            snapshot.disk_files.len(),
            1,
            "Persisted items for table manager is {:?}",
            snapshot.disk_files
        );
        assert_eq!(
            snapshot.indices.file_indices.len(),
            1,
            "Snapshot data files and file indices are {:?}",
            get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
        );

        // Check the loaded data file is of the expected format and content.
        let file_io = iceberg_table_manager
            .iceberg_table
            .as_ref()
            .unwrap()
            .file_io();
        let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;
        let expected_arrow_batch = RecordBatch::try_new(
            schema.clone(),
            // row2 and row3
            vec![
                Arc::new(Int32Array::from(vec![2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(Int32Array::from(vec![10, 50])),
            ],
        )
        .unwrap();
        assert_eq!(
            loaded_arrow_batch, expected_arrow_batch,
            "Expected arrow data is {:?}, actual data is {:?}",
            expected_arrow_batch, loaded_arrow_batch
        );

        let deleted_rows = deletion_vector.collect_deleted_rows();
        assert!(
            deleted_rows.is_empty(),
            "There should be no deletion vector in iceberg table."
        );

        // --------------------------------------
        // Operation series 2: no more additional rows appended, only to delete the first row in the table.
        // Expects to see a new deletion vector, because its corresponding data file has been persisted.
        table.delete(row2.clone(), /*lsn=*/ 300);
        table.flush(/*lsn=*/ 300).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to flush records {:?} to mooncake table because {:?}",
                    row2, e
                ),
            )
        })?;
        table.commit(/*lsn=*/ 300);
        table.create_snapshot().unwrap().await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create snapshot to iceberg because {:?}", e),
            )
        })?;

        // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
        let mut iceberg_table_manager = IcebergTableManager::new(iceberg_table_config.clone());
        let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
        assert_eq!(
            snapshot.disk_files.len(),
            1,
            "Persisted items for table manager is {:?}",
            snapshot.disk_files
        );
        assert_eq!(
            snapshot.indices.file_indices.len(),
            1,
            "Snapshot data files and file indices are {:?}",
            get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
        );

        // Check the loaded data file is of the expected format and content.
        let file_io = iceberg_table_manager
            .iceberg_table
            .as_ref()
            .unwrap()
            .file_io();
        let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;
        assert_eq!(
            loaded_arrow_batch, expected_arrow_batch,
            "Expected arrow data is {:?}, actual data is {:?}",
            expected_arrow_batch, loaded_arrow_batch
        );

        let deleted_rows = deletion_vector.collect_deleted_rows();
        let expected_deleted_rows = vec![0_u64];
        assert_eq!(
            deleted_rows, expected_deleted_rows,
            "Expected deletion vector {:?}, actual deletion vector {:?}",
            expected_deleted_rows, deleted_rows
        );

        // --------------------------------------
        // Operation series 3: no more additional rows appended, only to delete the last row in the table.
        // Expects to see the existing deletion vector updated, because its corresponding data file has been persisted.
        table.delete(row3.clone(), /*lsn=*/ 400);
        table.flush(/*lsn=*/ 400).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to flush records to mooncake table because {:?}", e),
            )
        })?;
        table.commit(/*lsn=*/ 400);
        table.create_snapshot().unwrap().await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create snapshot to iceberg because {:?}", e),
            )
        })?;

        // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
        let mut iceberg_table_manager = IcebergTableManager::new(iceberg_table_config.clone());
        let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
        assert_eq!(
            snapshot.disk_files.len(),
            1,
            "Persisted items for table manager is {:?}",
            snapshot.disk_files
        );
        assert_eq!(
            snapshot.indices.file_indices.len(),
            1,
            "Snapshot data files and file indices are {:?}",
            get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
        );

        // Check the loaded data file is of the expected format and content.
        let file_io = iceberg_table_manager
            .iceberg_table
            .as_ref()
            .unwrap()
            .file_io();
        let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;
        assert_eq!(
            loaded_arrow_batch, expected_arrow_batch,
            "Expected arrow data is {:?}, actual data is {:?}",
            expected_arrow_batch, loaded_arrow_batch
        );

        let deleted_rows = deletion_vector.collect_deleted_rows();
        let expected_deleted_rows = vec![0_u64, 1_u64];
        assert_eq!(
            deleted_rows, expected_deleted_rows,
            "Expected deletion vector {:?}, actual deletion vector {:?}",
            expected_deleted_rows, deleted_rows
        );
        let (_, data_entry) = iceberg_table_manager
            .persisted_data_files
            .iter()
            .next()
            .unwrap();

        // --------------------------------------
        // Operation series 4: append a new row, and don't delete any rows.
        // Expects to see the existing deletion vector unchanged and new data file created.
        let row4 = MoonlinkRow::new(vec![
            RowValue::Int32(4),
            RowValue::ByteArray("Tom".as_bytes().to_vec()),
            RowValue::Int32(40),
        ]);
        table.append(row4.clone()).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to append row4 {:?} to mooncake table because {:?}",
                    row4, e
                ),
            )
        })?;
        table.flush(/*lsn=*/ 500).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to flush records to mooncake table because {:?}", e),
            )
        })?;
        table.commit(/*lsn=*/ 500);
        table.create_snapshot().unwrap().await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create snapshot to iceberg table because {:?}", e),
            )
        })?;

        // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
        let mut iceberg_table_manager = IcebergTableManager::new(iceberg_table_config.clone());
        let mut snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
        assert_eq!(
            snapshot.disk_files.len(),
            2,
            "Persisted items for table manager is {:?}",
            snapshot.disk_files
        );
        assert_eq!(
            snapshot.indices.file_indices.len(),
            2,
            "Snapshot data files and file indices are {:?}",
            get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
        );

        // The old data file and deletion vector is unchanged.
        let old_data_entry = iceberg_table_manager
            .persisted_data_files
            .remove(loaded_path);
        assert!(
            old_data_entry.is_some(),
            "Add new data file shouldn't change existing persisted items"
        );
        assert_eq!(
            &old_data_entry.unwrap(),
            data_entry,
            "Add new data file shouldn't change existing persisted items"
        );
        snapshot.disk_files.remove(loaded_path);

        // Check new data file is correctly managed by iceberg table with no deletion vector.
        let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;

        let expected_arrow_batch = RecordBatch::try_new(
            schema.clone(),
            // row4
            vec![
                Arc::new(Int32Array::from(vec![4])),
                Arc::new(StringArray::from(vec!["Tom"])),
                Arc::new(Int32Array::from(vec![40])),
            ],
        )
        .unwrap();
        assert_eq!(
            loaded_arrow_batch, expected_arrow_batch,
            "Expected arrow data is {:?}, actual data is {:?}",
            expected_arrow_batch, loaded_arrow_batch
        );

        let deleted_rows = deletion_vector.collect_deleted_rows();
        assert!(
            deleted_rows.is_empty(),
            "The new appended data file should have no deletion vector aside, but actually it contains deletion vector {:?}",
            deleted_rows
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filesystem_sync_snapshots() -> IcebergResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap().to_string();
        mooncake_table_snapshot_persist_impl(path).await
    }

    #[tokio::test]
    #[cfg(feature = "storage-s3")]
    async fn test_object_storage_sync_snapshots() -> IcebergResult<()> {
        let (bucket_name, warehouse_uri) = s3_test_utils::get_test_minio_bucket_and_warehouse();
        s3_test_utils::object_store_test_utils::create_test_s3_bucket(bucket_name.clone()).await?;
        mooncake_table_snapshot_persist_impl(warehouse_uri).await
    }
}
