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

use crate::storage::iceberg::validation as IcebergValidation;
use futures::executor::block_on;
use iceberg::io::FileIO;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::DataFileFormat;
use iceberg::spec::ManifestContentType;
use iceberg::spec::{DataFile, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::Error as IcebergError;
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

    /// Load snapshot from iceberg table. Used for recovery.
    async fn load_from_table(&mut self) -> IcebergResult<()>
    where
        Self: Sized;
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct DataFileEntry {
    /// Iceberg data file, used to decide what to persist at new commit requests.
    data_file: DataFile,
    /// In-memory deletion vector.
    deletion_vector: BatchDeletionVector,
}

/// TODO(hjiang):
/// 1. Craft a trait for table manager so we could mock later.
/// 2. Support a data file handle, which is a remote file path, plus an optional local cache filepath.
/// 3. Support a deletion vector handle, which is a remote file path, with an optional in-memory buffer and a local cache filepath.
#[allow(dead_code)]
#[derive(Debug)]
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
        let filesystem_catalog = FileSystemCatalog::new(config.warehouse_uri.clone())?;
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

    /// Load data file into table manager from the current manifest entry.
    async fn load_data_file_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
    ) -> IcebergResult<()> {
        let data_file = entry.data_file();
        if data_file.file_format() == DataFileFormat::Puffin {
            return Ok(());
        }

        let file_path = PathBuf::from(data_file.file_path().to_string());
        assert_eq!(
            data_file.file_format(),
            DataFileFormat::Parquet,
            "Data file is of file format parquet for entry {:?}.",
            entry,
        );
        let new_data_file_entry = DataFileEntry {
            data_file: data_file.clone(),
            deletion_vector: BatchDeletionVector::new(/*max_rows=*/ 0),
        };
        let old_entry = self.persisted_items.insert(file_path, new_data_file_entry);
        assert!(old_entry.is_none());
        Ok(())
    }

    /// Load deletion vector into table manager from the current manifest entry.
    async fn load_deletion_vector_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        let data_file = entry.data_file();
        if data_file.file_format() == DataFileFormat::Parquet {
            return Ok(());
        }

        let referenced_path_buf: PathBuf = data_file.referenced_data_file().unwrap().into();

        let data_file_entry = self.persisted_items.get_mut(&referenced_path_buf);
        assert!(
            data_file_entry.is_some(),
            "At recovery, the data file path for {:?} doesn't exist",
            referenced_path_buf
        );

        IcebergValidation::validate_puffin_manifest_entry(&entry)?;
        let deletion_vector = DeletionVector::load_from_dv_blob(file_io.clone(), data_file).await?;
        let batch_deletion_vector = deletion_vector.take_as_batch_delete_vector();
        data_file_entry.unwrap().deletion_vector = batch_deletion_vector;

        Ok(())
    }
}

/// TODO(hjiang): Parallelize all IO operations and reduce a few copies.
impl IcebergOperation for IcebergTableManager {
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
        self.iceberg_table = txn.commit(&self.filesystem_catalog).await?;
        self.filesystem_catalog.clear_puffin_metadata();

        Ok(())
    }

    async fn load_from_table(&mut self) -> IcebergResult<()> {
        let table_metadata = self.iceberg_table.metadata();
        let snapshot_meta = table_metadata.current_snapshot().unwrap();
        let manifest_list = snapshot_meta
            .load_manifest_list(self.iceberg_table.file_io(), table_metadata)
            .await?;

        let file_io = self.iceberg_table.file_io().clone();
        for manifest_file in manifest_list.entries().iter() {
            assert_eq!(
                manifest_file.content,
                ManifestContentType::Data,
                "Iceberg table should only contain data type."
            );

            // All files (i.e. data files, deletion vector, manifest files) under the same snapshot are assigned with the same sequence number.
            // Reference: https://iceberg.apache.org/spec/?h=content#sequence-numbers
            let manifest = manifest_file.load_manifest(&file_io).await?;
            let snapshot_seq_no = manifest.entries().first().unwrap().sequence_number();

            for entry in manifest.entries() {
                // Sanity check all manifest entries are of the sequence number.
                let cur_entry_seq_no = entry.sequence_number();
                if snapshot_seq_no != cur_entry_seq_no {
                    return Err(IcebergError::new(
                        iceberg::ErrorKind::DataInvalid,
                        format!("When reading from iceberg table, snapshot sequence id inconsistency found {:?} vs {:?}", snapshot_seq_no, cur_entry_seq_no),
                    ));
                }
                // On load, we do two pass on all entries, to check whether all deletion vector has a corresponding data file.
                self.load_data_file_from_manifest_entry(entry.as_ref())
                    .await?;
                self.load_deletion_vector_from_manifest_entry(entry.as_ref(), &file_io)
                    .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::iceberg::test_utils;
    use crate::storage::{
        iceberg::catalog_utils::create_catalog,
        mooncake_table::{Snapshot, TableConfig, TableMetadata},
    };

    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::tempdir;

    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use iceberg::io::FileRead;
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
            ArrowField::new("id", ArrowDataType::Int32, /*nullable=*/ false).with_metadata(
                HashMap::from([("PARQUET:field_id".to_string(), "1".to_string())]),
            ),
            ArrowField::new("name", ArrowDataType::Utf8, /*nullable=*/ false).with_metadata(
                HashMap::from([("PARQUET:field_id".to_string(), "2".to_string())]),
            ),
        ]))
    }

    /// Test util function to create arrow record batch.
    fn test_batch_1(arrow_schema: Arc<ArrowSchema>) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
                Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
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

    #[tokio::test]
    async fn test_invalid_warehouse_uri() -> IcebergResult<()> {
        // Create arrow schema and table.
        let arrow_schema = create_test_arrow_schema();
        let tmp_dir = tempdir()?;
        let metadata = Arc::new(TableMetadata {
            name: "test_table".to_string(),
            schema: arrow_schema.clone(),
            id: 0, // unused.
            config: TableConfig::new(),
            path: tmp_dir.path().to_path_buf(),
            get_lookup_key: |_row| 1, // unused.
        });
        let config = IcebergTableManagerConfig {
            warehouse_uri: "invalid_warehouse_uri".to_string(),
            table_metadata: metadata,
            namespace: vec!["namespace".to_string()],
            table: "test_table".to_string(),
        };
        let iceberg_table_manager = IcebergTableManager::new(config);
        assert!(
            iceberg_table_manager.is_err(),
            "Snapshot with invalid warehouse should fail when store."
        );
        let err = iceberg_table_manager.unwrap_err();
        assert_eq!(err.kind(), iceberg::ErrorKind::Unexpected);
        Ok(())
    }
}
