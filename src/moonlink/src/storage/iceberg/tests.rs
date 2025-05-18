use crate::row::IdentityProp as RowIdentity;
use crate::row::MoonlinkRow;
use crate::row::RowValue;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::iceberg_table_manager::*;
#[cfg(feature = "storage-s3")]
use crate::storage::iceberg::s3_test_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::{
    TableConfig as MooncakeTableConfig, TableMetadata as MooncakeTableMetadata,
};

use crate::storage::index::MooncakeIndex;
use crate::storage::MooncakeTable;

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::tempdir;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::io::FileIO;
use iceberg::io::FileRead;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

/// Create test batch deletion vector.
fn test_committed_deletion_log_1(data_filepath: PathBuf) -> HashMap<PathBuf, BatchDeletionVector> {
    let mut deletion_vector = BatchDeletionVector::new(MooncakeTableConfig::new().batch_size());
    deletion_vector.delete_row(0);

    let mut deletion_log = HashMap::new();
    deletion_log.insert(data_filepath.clone(), deletion_vector);
    deletion_log
}
/// Test deletion vector 2 includes deletion vector 1, used to mimic new data file rows deletion situation.
fn test_committed_deletion_log_2(data_filepath: PathBuf) -> HashMap<PathBuf, BatchDeletionVector> {
    let mut deletion_vector = BatchDeletionVector::new(MooncakeTableConfig::new().batch_size());
    deletion_vector.delete_row(1);
    deletion_vector.delete_row(2);

    let mut deletion_log = HashMap::new();
    deletion_log.insert(data_filepath.clone(), deletion_vector);
    deletion_log
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
    write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch).await?;

    iceberg_table_manager
        .sync_snapshot(
            /*flush_lsn=*/ 0,
            /*new_disk_files=*/ vec![parquet_path.clone()],
            /*committed_deletion_log=*/
            test_committed_deletion_log_1(parquet_path.clone()),
            /*file_indices=*/ vec![].as_slice(),
        )
        .await?;

    // Write second snapshot to iceberg table, with updated deletion vector and new data file.
    let data_filename_2 = "data-2.parquet";
    let batch = test_batch_2(arrow_schema.clone());
    let parquet_path = tmp_dir.path().join(data_filename_2);
    write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch).await?;
    iceberg_table_manager
        .sync_snapshot(
            /*flush_lsn=*/ 1,
            /*new_disk_files=*/ vec![parquet_path.clone()],
            /*committed_deletion_log=*/
            test_committed_deletion_log_2(parquet_path.clone()),
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
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;
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
                vec![1, 2],
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
            vec![0],
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
    let config = IcebergTableConfig {
        warehouse_uri: tmp_dir.path().to_str().unwrap().to_string(),
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
    };
    let mut iceberg_table_manager = IcebergTableManager::new(mooncake_table_metadata, config);
    test_store_and_load_snapshot_impl(&mut iceberg_table_manager).await?;
    Ok(())
}

/// Testing scenario: attempt an iceberg snapshot when no data file, deletion vector or index files generated.
#[tokio::test]
async fn test_empty_content_snapshot_creation() -> IcebergResult<()> {
    let tmp_dir = tempdir()?;
    let mooncake_table_metadata =
        create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
    let config = IcebergTableConfig {
        warehouse_uri: tmp_dir.path().to_str().unwrap().to_string(),
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
    };
    let mut iceberg_table_manager =
        IcebergTableManager::new(mooncake_table_metadata.clone(), config.clone());
    iceberg_table_manager
        .sync_snapshot(
            /*flush_lsn=*/ 0,
            /*disk_files=*/ vec![],
            /*desired_deletion_vector=*/ HashMap::new(),
            /*file_indices=*/ vec![].as_slice(),
        )
        .await?;

    // Recover from iceberg snapshot, and check mooncake table snapshot version.
    let mut iceberg_table_manager =
        IcebergTableManager::new(mooncake_table_metadata.clone(), config.clone());
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    Ok(())
}

// TODO(hjiang): Figure out a way to check file index content; for example, search for an item.
async fn mooncake_table_snapshot_persist_impl(warehouse_uri: String) -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
    };
    let schema = create_test_arrow_schema();
    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mut mooncake_table_config = MooncakeTableConfig::new();
    mooncake_table_config.iceberg_snapshot_new_data_file_count = 0;
    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*version=*/ 1,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
    )
    .await;

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
    table.delete(row1.clone(), /*flush_lsn=*/ 100);
    table.flush(/*flush_lsn=*/ 200).await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to flush records to mooncake table because {:?}", e),
        )
    })?;
    table.commit(/*flush_lsn=*/ 200);
    table.create_snapshot().unwrap().await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to create snapshot to iceberg table because {:?}", e),
        )
    })?;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );
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
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);

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
    table.delete(row2.clone(), /*flush_lsn=*/ 300);
    table.flush(/*flush_lsn=*/ 300).await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!(
                "Failed to flush records {:?} to mooncake table because {:?}",
                row2, e
            ),
        )
    })?;
    table.commit(/*flush_lsn=*/ 300);
    table.create_snapshot().unwrap().await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to create snapshot to iceberg because {:?}", e),
        )
    })?;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );
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
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);

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
    table.delete(row3.clone(), /*flush_lsn=*/ 400);
    table.flush(/*flush_lsn=*/ 400).await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to flush records to mooncake table because {:?}", e),
        )
    })?;
    table.commit(/*flush_lsn=*/ 400);
    table.create_snapshot().unwrap().await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to create snapshot to iceberg because {:?}", e),
        )
    })?;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );
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
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 400);

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
    table.flush(/*flush_lsn=*/ 500).await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to flush records to mooncake table because {:?}", e),
        )
    })?;
    table.commit(/*flush_lsn=*/ 500);
    table.create_snapshot().unwrap().await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to create snapshot to iceberg table because {:?}", e),
        )
    })?;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );
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
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);

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
