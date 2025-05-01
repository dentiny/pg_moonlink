use super::catalog_utils::create_catalog;
use super::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
use crate::storage::iceberg::file_catalog::FileSystemCatalog;
use crate::storage::iceberg::object_storage_catalog::{S3Catalog, S3CatalogConfig};
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::validation::validate_puffin_manifest_entry;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::Snapshot;
use crate::storage::iceberg::test_utils;

use futures::executor::block_on;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;

use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow as IcebergArrow;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::ManifestContentType;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::Error as IcebergError;
use iceberg::NamespaceIdent;
use iceberg::TableCreation;
use iceberg::{Catalog, Result as IcebergResult, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use url::Url;
use uuid::Uuid;

// UNDONE(Iceberg):
// 1. Implement deletion file related load and store operations.
// (unrelated to functionality) 2. Update rest catalog service ip/port, currently it's hard-coded to devcontainer's config, which should be parsed from env variable or config files.
// (unrelated to functionality) 3. Add timeout to rest catalog access.
// (unrelated to functionality) 4. Use real namespace and table name, which we should be able to get it from moonlink, it's hard-coded to "default" and "test_table" for now.

// Get or create an iceberg table in the given catalog from the given namespace and table name.
async fn get_or_create_iceberg_table<C: Catalog + ?Sized>(
    catalog: &C,
    namespace: &[&str],
    table_name: &str,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {
    let namespace_ident =
        NamespaceIdent::from_vec(namespace.iter().map(|s| s.to_string()).collect()).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());
    match catalog.load_table(&table_ident).await {
        Ok(table) => {
            println!("when load table, it already exists: {:?}", table);
            Ok(table)
        },
        Err(_) => {
            let namespace_already_exists = catalog.namespace_exists(&namespace_ident).await?;
            if !namespace_already_exists {
                catalog
                    .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
                    .await?;
            }

            let iceberg_schema = IcebergArrow::arrow_schema_to_schema(arrow_schema)?;
            let tbl_creation = TableCreation::builder()
                .name(table_name.to_string())
                .location(format!(
                    "s3://test-bucket/{}/{}",
                    namespace_ident.to_url_string(),
                    table_name
                ))
                .schema(iceberg_schema)
                .properties(HashMap::new())
                .build();

            let table = catalog
                .create_table(&table_ident.namespace, tbl_creation)
                .await?;
            Ok(table)
        }
    }
}

// Write the given record batch to the iceberg table.
async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    parquet_filepath: &PathBuf,
) -> IcebergResult<DataFile> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;

    println!("\n\nlocation generator = {:?}\n\n", location_generator);

    let file_name_generator = DefaultFileNameGenerator::new(
        /*prefix=*/ Uuid::new_v4().to_string(),
        /*suffix=*/ None,
        /*format=*/ DataFileFormat::Parquet,
    );

    let file = std::fs::File::open(parquet_filepath)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut arrow_reader = builder.build()?;

    let parquet_writer_builder = ParquetWriterBuilder::new(
        /*props=*/ WriterProperties::default(),
        /*schame=*/ table.metadata().current_schema().clone(),
        /*file_io=*/ table.file_io().clone(),
        /*location_generator=*/ location_generator,
        /*file_name_generator=*/ file_name_generator,
    );

    // TOOD(hjiang): Add support for partition values.
    let data_file_writer_builder = DataFileWriterBuilder::new(
        parquet_writer_builder,
        /*partition_value=*/ None,
        /*partition_spec_id=*/ 0,
    );
    let mut data_file_writer = data_file_writer_builder.build().await?;
    while let Some(record_batch) = arrow_reader.next().transpose()? {
        data_file_writer.write(record_batch).await?;
    }

    let data_files = data_file_writer.close().await?;
    assert_eq!(
        data_files.len(),
        1,
        "Should only have one parquet file written"
    );

    Ok(data_files[0].clone())
}

pub trait IcebergSnapshot {
    // Write the current version to iceberg
    async fn _export_to_iceberg(&mut self) -> IcebergResult<()>;

    // Create a snapshot by reading from iceberg in async style.
    //
    // TODO(hjiang): Expose an async function only for tokio-based test, otherwise will suffer deadlock with executor.
    // Reference: https://greptime.com/blogs/2023-03-09-bridging-async-and-sync-rust
    async fn _async_load_from_iceberg(&self) -> IcebergResult<Self>
    where
        Self: Sized;

    // Create a snapshot by reading from iceberg
    fn _load_from_iceberg(&self) -> IcebergResult<Self>
    where
        Self: Sized;
}

impl IcebergSnapshot for Snapshot {
    // TODO(hjiang):
    // 1. Extract into multiple functions, for example, data file persistence, deletion vector persistence, iceberg table transaction.
    // 2. Parallalize IO operations, now they're implemented in sequential style.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn _export_to_iceberg(&mut self) -> IcebergResult<()> {
        let url = Url::parse(&self.warehouse_uri)
            .or_else(|_| Url::from_file_path(self.warehouse_uri.clone()))
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Invalid warehouse URI {}: {:?}",
                        self.warehouse_uri.clone(),
                        e
                    ),
                )
            })?;

        // Create catalog based on warehouse uri.
        //
        // TODO(hjiang):
        // 1. This is a hacky way to check whether catalog is moonlink self-implemented ones, which support deletion vector.
        // We should revisit to check more rust-idiomatic way.
        // 2. Logic to decide which catalog to use should be rewritten here; when we release the pg_mooncake and moonlink,
        // likely only filesystem catalog and object storage catalog will be used due to deletion vector support.
        #[allow(unused_assignments)]
        let mut opt_catalog: Option<Rc<RefCell<dyn Catalog>>> = None;
        let mut filesystem_catalog: Option<Rc<RefCell<FileSystemCatalog>>> = None;
        let mut object_storage_catalog: Option<Rc<RefCell<S3Catalog>>> = None;
        if self.warehouse_uri.starts_with("s3://test-bucket") {

            println!("on store, we are hitting s3 catalog!!!1");

            let config = S3CatalogConfig::new(
                /*warehouse_location=*/ "s3://test-bucket".to_string(),
                /*access_key_id=*/ "minioadmin".to_string(),
                /*secret_access_key=*/ "minioadmin".to_string(),
                /*region=*/ "auto".to_string(), // minio doesn't care about region.
                /*bucket=*/ "test-bucket".to_string(),
                /*endpoint=*/ "http://minio:9000".to_string(),
            );
            let internal_s3_config = Rc::new(RefCell::new(S3Catalog::new(config)));
            object_storage_catalog = Some(internal_s3_config.clone());
            let catalog_rc: Rc<RefCell<dyn Catalog>> = internal_s3_config.clone();
            opt_catalog = Some(catalog_rc);
        } else if url.scheme() == "file" {
            let absolute_path = url.path();
            let internal_fs_catalog = Rc::new(RefCell::new(FileSystemCatalog::new(
                absolute_path.to_string(),
            )));
            filesystem_catalog = Some(internal_fs_catalog.clone());
            let catalog_rc: Rc<RefCell<dyn Catalog>> = internal_fs_catalog.clone();
            opt_catalog = Some(catalog_rc);
        } else {
            // Fallback all other warehouse uri to rest catalog.
            opt_catalog = Some(Rc::new(RefCell::new(RestCatalog::new(
                RestCatalogConfig::builder()
                    .uri(self.warehouse_uri.clone())
                    .build(),
            ))));
        }
        // `catalog` is guaranteed to be valid.
        let catalog: Rc<RefCell<dyn Catalog>> = opt_catalog.unwrap();

        let table_name = self.metadata.name.clone();
        let namespace = vec!["default"];
        let arrow_schema = self.metadata.schema.as_ref();
        let iceberg_table =
            get_or_create_iceberg_table(&*catalog.borrow(), &namespace, &table_name, arrow_schema)
                .await?;

        let txn = Transaction::new(&iceberg_table);
        let mut action =
            txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;

        let mut new_disk_files = HashMap::with_capacity(self.disk_files.len());
        let mut new_data_files = Vec::with_capacity(self.disk_files.len());
        for (file_path, deletion_vector) in self.disk_files.drain() {
            // Write data files to iceberg table.
            let data_file =
                write_record_batch_to_iceberg(&iceberg_table.clone(), &file_path).await?;
            let new_path = PathBuf::from(data_file.file_path());

            // Record deleted rows in the deletion vector.
            let deleted_rows = deletion_vector.collect_deleted_rows();
            if !deleted_rows.is_empty() {
                // TODO(hjiang): Currently one deletion vector is stored in one puffin file, need to revisit later.
                let deleted_row_count = deleted_rows.len();
                let mut iceberg_deletion_vector = DeletionVector::new();
                iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
                let blob_properties = HashMap::from([
                    (
                        DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(),
                        new_path.to_str().unwrap().to_string(),
                    ),
                    (
                        DELETION_VECTOR_CADINALITY.to_string(),
                        deleted_row_count.to_string(),
                    ),
                ]);
                let blob = iceberg_deletion_vector.serialize(blob_properties);

                // TODO(hjiang): Current iceberg-rust doesn't support deletion vector officially, so we do our own hack to rewrite manifest file by our own catalog implementation.
                let location_generator =
                    DefaultLocationGenerator::new(iceberg_table.metadata().clone())?;
                let puffin_filepath =
                    location_generator.generate_location(&format!("{}-puffin.bin", Uuid::new_v4()));
                let puffin_filepath = "s3://test-bucket/default/test_table/data/786628da-283e-4611-af23-b7ef925469f0-puffin.bin".to_string();
                let mut puffin_writer = puffin_utils::create_puffin_writer(
                    iceberg_table.file_io(),
                    puffin_filepath.clone(),
                )
                .await?;
                // TODO(hjiang): Provide option to enable compression for puffin blob.
                puffin_writer.add(blob, CompressionCodec::None).await?;

                if let Some(filesystem_catalog_val) = &mut filesystem_catalog {
                    filesystem_catalog_val
                        .borrow_mut()
                        .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
                        .await?;
                } else if let Some(object_storage_catalog_val) = &mut object_storage_catalog {
                    object_storage_catalog_val
                        .borrow_mut()
                        .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
                        .await?;
                }
            }

            new_disk_files.insert(new_path, deletion_vector);
            new_data_files.push(data_file);
        }

        // TODO(hjiang): Likely we should bookkeep the old files so they could be deleted later.
        self.disk_files = new_disk_files;
        action.add_data_files(new_data_files)?;

        // Commit write transaction, which internally creates manifest files and manifest list files.
        let txn = action.apply().await?;
        txn.commit(&*catalog.borrow()).await?;

        Ok(())
    }

    async fn _async_load_from_iceberg(&self) -> IcebergResult<Self> {
        let namespace = vec!["default"];
        let table_name = self.metadata.name.clone();
        let arrow_schema = self.metadata.schema.as_ref();
        let catalog = create_catalog(&self.warehouse_uri)?;
        let iceberg_table =
            get_or_create_iceberg_table(&*catalog, &namespace, &table_name, arrow_schema).await?;

        let table_metadata = iceberg_table.metadata();
        let snapshot_meta = table_metadata.current_snapshot().unwrap();
        let file_io = iceberg_table.file_io();
        let manifest_list = snapshot_meta
            .load_manifest_list(file_io, table_metadata)
            .await?;
        let mut snapshot = Snapshot::new(self.metadata.clone());

        // Maps from data filepath to batch deletion vector loaded from puffin blob.
        let mut disk_file_to_deletion_vector: HashMap<PathBuf, BatchDeletionVector> =
            HashMap::new();
        for manifest_file in manifest_list.entries().iter() {
            // We're only interested in DATA files when recover snapshot status.
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                let data_file = entry.data_file();
                let file_path = PathBuf::from(data_file.file_path().to_string());

                // Record batch deletion.
                if data_file.file_format() == DataFileFormat::Puffin {
                    validate_puffin_manifest_entry(entry)?;
                    let deletion_vector =
                        DeletionVector::load_from_dv_blob(file_io.clone(), data_file).await?;
                    let batch_deletion_vector = deletion_vector.take_as_batch_delete_vector();
                    let referenced_path_buf: PathBuf =
                        data_file.referenced_data_file().unwrap().into();
                    disk_file_to_deletion_vector.insert(referenced_path_buf, batch_deletion_vector);
                    continue;
                }

                // Handle data files.
                assert_eq!(
                    data_file.file_format(),
                    DataFileFormat::Parquet,
                    "Data file is of file format parquet."
                );
                disk_file_to_deletion_vector
                    .entry(file_path)
                    .or_insert_with(|| BatchDeletionVector::new(/*max_rows=*/ 0));
            }
        }

        snapshot.disk_files = disk_file_to_deletion_vector;

        Ok(snapshot)
    }

    fn _load_from_iceberg(&self) -> IcebergResult<Self> {
        block_on(self._async_load_from_iceberg())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    use parquet::arrow::ArrowWriter;

    fn create_test_batch_deletion_vector() -> BatchDeletionVector {
        let mut deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
        deletion_vector.delete_row(2);
        deletion_vector
    }

    async fn delete_all_tables<C: Catalog + ?Sized>(catalog: &C) -> IcebergResult<()> {
        let namespaces = catalog.list_namespaces(/*parent=*/ None).await?;
        for namespace in namespaces {
            let tables = catalog.list_tables(&namespace).await?;
            for table in tables {
                catalog.drop_table(&table).await.ok();
                println!("Deleted table: {:?} in namespace {:?}", table, namespace);
            }
        }

        Ok(())
    }

    async fn delete_all_namespaces<C: Catalog + ?Sized>(catalog: &C) -> IcebergResult<()> {
        let namespaces = catalog.list_namespaces(/*parent=*/ None).await?;
        for namespace in namespaces {
            catalog.drop_namespace(&namespace).await.ok();
            println!("Deleted namespace: {:?}", namespace);
        }

        Ok(())
    }

    /// Test util function to create arrow schema.
    fn create_test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false).with_metadata(HashMap::from([
                ("PARQUET:field_id".to_string(), "1".to_string()),
            ])),
            ArrowField::new("name", ArrowDataType::Utf8, false).with_metadata(HashMap::from([
                ("PARQUET:field_id".to_string(), "2".to_string()),
            ])),
        ]))
    }

    // Test util function to create arrow record batch.
    fn create_test_arrow_record_batch(arrow_schema: Arc<ArrowSchema>) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
                Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
            ],
        ).unwrap()
    }

    /// Test snapshot store and load for different types of catalogs based on the given warehouse.
    ///
    /// * `deletion_vector_supported` - whether the given catalog supports deletion vector; if false, skip checking deletion vector load.
    ///
    /// TODO(hjiang): This test case only write once, thus one snapshot; should test situations where we write multiple times.
    async fn test_store_and_load_snapshot_impl(
        warehouse_uri: &str,
        deletion_vector_supported: bool,
    ) -> IcebergResult<()> {
        // Create Arrow schema and record batch.
        let arrow_schema = create_test_arrow_schema();
        let batch = create_test_arrow_record_batch(arrow_schema.clone());

        // Cleanup namespace and table before testing.
        let catalog = create_catalog(warehouse_uri)?;

        // Cleanup states before testing.
        // delete_all_tables(&*catalog).await?;
        // delete_all_namespaces(&*catalog).await?;

        println!("delete all tables and namespaces at {:?}:{:?}", file!(), line!());

        // Write record batch to Parquet file.
        let tmp_dir = tempdir()?;
        let parquet_path = tmp_dir.path().join("data.parquet");
        let file = File::create(&parquet_path)?;
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None)?;
        writer.write(&batch)?;
        writer.close()?;

        println!("arrow write {:?}:{:?}", file!(), line!());

        let metadata = Arc::new(TableMetadata {
            name: "test_table".to_string(),
            schema: arrow_schema.clone(),
            id: 0, // unused.
            config: TableConfig::new(),
            path: tmp_dir.path().to_path_buf(),
            get_lookup_key: |_row| 1, // unused.
        });

        let mut disk_files = HashMap::new();
        disk_files.insert(parquet_path.clone(), create_test_batch_deletion_vector());

        let mut snapshot = Snapshot::new(metadata);
        snapshot._set_warehouse_info(warehouse_uri.to_string());
        snapshot.disk_files = disk_files;

        snapshot._export_to_iceberg().await?;
        let loaded_snapshot = snapshot._async_load_from_iceberg().await?;
        assert_eq!(
            loaded_snapshot.disk_files.len(),
            1,
            "Should have exactly one file, but disk files have {:?}.",
            loaded_snapshot.disk_files.keys()
        );

        let namespace = vec!["default_namespace"];
        let table_name = "test_table";
        let iceberg_table =
            get_or_create_iceberg_table(&*catalog, &namespace, table_name, &arrow_schema).await?;
        let file_io = iceberg_table.file_io();

        // Check the loaded data file exists.
        let (loaded_path, loaded_deletion_vector) =
            loaded_snapshot.disk_files.iter().next().unwrap();
        assert!(
            file_io.exists(loaded_path.to_str().unwrap()).await?,
            "File should exist"
        );

        // Check the loaded data file is of the expected format and content.
        let input_file = file_io.new_input(loaded_path.to_str().unwrap())?;
        let input_file_metadata = input_file.metadata().await?;
        let reader = input_file.reader().await?;
        let bytes = reader.read(0..input_file_metadata.size).await?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        let mut reader = builder.build()?;
        let batch = reader.next().transpose()?.expect("Should have one batch");

        let actual_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            *actual_ids,
            Int32Array::from(vec![1, 2, 3]),
            "ID data should match"
        );

        // Check loaded deletion vector.
        if deletion_vector_supported {
            let collect_deleted_rows = loaded_deletion_vector.collect_deleted_rows();
            assert_eq!(
                collect_deleted_rows,
                create_test_batch_deletion_vector().collect_deleted_rows(),
                "Loaded deletion vector is not the same as the one gets stored."
            );
        }

        // Verify second column (name).
        let actual_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            *actual_names,
            StringArray::from(vec!["a", "b", "c"]),
            "Name data should match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_store_and_load_snapshot_with_rest_catalog() -> IcebergResult<()> {
        let warehouse_uri = "http://iceberg:8181";
        test_store_and_load_snapshot_impl(warehouse_uri, /*deletion_vector_supported=*/ false)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_store_and_load_snapshot_with_filesystem_catalog() -> IcebergResult<()> {
        let tmp_dir = tempdir()?;
        let warehouse_path = tmp_dir.path().to_str().unwrap();
        test_store_and_load_snapshot_impl(warehouse_path, /*deletion_vector_supported=*/ true)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_store_and_load_snapshot_with_minio_catalog() -> IcebergResult<()> {
        test_utils::create_test_s3_bucket().await?;
        let warehouse_uri = "s3://test-bucket";
        test_store_and_load_snapshot_impl(warehouse_uri, /*deletion_vector_supported=*/ false)
            .await?;
        Ok(())
    }

}
