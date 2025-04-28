use super::catalog_utils::create_catalog;
use super::deletion_vector::DeletionVector;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::Snapshot;

use futures::executor::block_on;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowType;
use arrow_schema::Schema as ArrowSchema;
use iceberg::spec::ManifestContentType;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::spec::{
    NestedField, NestedFieldRef, PrimitiveType, Schema as IcebergSchema, Type as IcebergType,
};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::NamespaceIdent;
use iceberg::TableCreation;
use iceberg::{Catalog, Result as IcebergResult, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

// UNDONE(Iceberg):
// 1. Implement deletion file related load and store operations.
// (unrelated to functionality) 2. Support all data types, other than major primitive types.
// (unrelated to functionality) 3. Update rest catalog service ip/port, currently it's hard-coded to devcontainer's config, which should be parsed from env variable or config files.
// (unrelated to functionality) 4. Add timeout to rest catalog access.
// (unrelated to functionality) 5. Use real namespace and table name, which we should be able to get it from moonlink, it's hard-coded to "default" and "test_table" for now.

// Convert arrow schema to icerberg schema.
fn arrow_to_iceberg_schema(arrow_schema: &ArrowSchema) -> IcebergSchema {
    let mut field_id_counter = 1;

    let iceberg_fields: Vec<NestedFieldRef> = arrow_schema
        .fields
        .iter()
        .map(|f| {
            let iceberg_type = arrow_type_to_iceberg_type(f.data_type());
            let field = if f.is_nullable() {
                NestedField::optional(field_id_counter, f.name().clone(), iceberg_type)
            } else {
                NestedField::required(field_id_counter, f.name().clone(), iceberg_type)
            };
            field_id_counter += 1;
            Arc::new(field)
        })
        .collect();

    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(iceberg_fields)
        .build()
        .expect("Failed to build Iceberg schema")
}

// Convert arrow data type to iceberg data type.
fn arrow_type_to_iceberg_type(data_type: &ArrowType) -> IcebergType {
    match data_type {
        ArrowType::Boolean => IcebergType::Primitive(PrimitiveType::Boolean),
        ArrowType::Int32 => IcebergType::Primitive(PrimitiveType::Int),
        ArrowType::Int64 => IcebergType::Primitive(PrimitiveType::Long),
        ArrowType::Float32 => IcebergType::Primitive(PrimitiveType::Float),
        ArrowType::Float64 => IcebergType::Primitive(PrimitiveType::Double),
        ArrowType::Utf8 => IcebergType::Primitive(PrimitiveType::String),
        ArrowType::Binary => IcebergType::Primitive(PrimitiveType::Binary),
        ArrowType::Timestamp(_, _) => IcebergType::Primitive(PrimitiveType::Timestamp),
        ArrowType::Date32 => IcebergType::Primitive(PrimitiveType::Date),
        // TODO(hjiang): Support more arrow types.
        _ => panic!("Unsupported Arrow data type: {:?}", data_type),
    }
}

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
        Ok(table) => Ok(table),
        Err(_) => {
            // TOOD(hjiang): Temporary workaround for iceberg-rust bug, see https://github.com/apache/iceberg-rust/issues/1234
            let namespaces = catalog.list_namespaces(None).await?;
            let namespace_already_exists = namespaces.contains(&namespace_ident);
            if !namespace_already_exists {
                catalog
                    .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
                    .await?;
            }

            let iceberg_schema = arrow_to_iceberg_schema(arrow_schema);
            let tbl_creation = TableCreation::builder()
                .name(table_name.to_string())
                .location(format!(
                    "file:///tmp/iceberg-test/{}/{}",
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
    assert!(
        data_files.len() == 1,
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
    // TODO(hjiang): Extract into multiple functions.
    async fn _export_to_iceberg(&mut self) -> IcebergResult<()> {
        let table_name = self.metadata.name.clone();
        let namespace = vec!["default"];
        let arrow_schema = self.metadata.schema.as_ref();
        let catalog = create_catalog(&self.warehouse_uri)?;
        let iceberg_table =
            get_or_create_iceberg_table(&*catalog, &namespace, &table_name, arrow_schema).await?;

        let txn = Transaction::new(&iceberg_table);
        let mut action =
            txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;

        let mut new_disk_files = HashMap::with_capacity(self.disk_files.len());
        let mut new_data_files = Vec::with_capacity(self.disk_files.len());
        for (file_path, deletion_vector) in self.disk_files.drain() {
            // Record deleted rows in the deletion vector.
            let deleted_rows = deletion_vector.collect_deleted_rows();
            if !deleted_rows.is_empty() {
                let mut _iceberg_deletion_vector = DeletionVector::new();
                _iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
            }

            // Write data files to iceberg table.
            let data_file =
                write_record_batch_to_iceberg(&iceberg_table.clone(), &file_path).await?;
            let new_path = PathBuf::from(data_file.file_path());
            new_disk_files.insert(new_path, deletion_vector);
            new_data_files.push(data_file);
        }

        // TODO(hjiang): Likely we should bookkeep the old files so they could be deleted later.
        self.disk_files = new_disk_files;
        action.add_data_files(new_data_files)?;

        // Commit write transaction, which internally creates manifest files and manifest list files.
        let txn = action.apply().await?;
        txn.commit(&*catalog).await?;

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
        for manifest_file in manifest_list.entries().iter() {
            // We're only interested in DATA files when recover snapshot status.
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                let data_file = entry.data_file();
                let file_path = PathBuf::from(data_file.file_path().to_string());
                snapshot
                    .disk_files
                    .insert(file_path, BatchDeletionVector::new(/*max_rows=*/ 0));
            }
        }

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

    async fn test_store_and_load_snapshot_impl(warehouse_uri: &str) -> IcebergResult<()> {
        // Create Arrow schema and record batch.
        let arrow_schema =
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false).with_metadata(HashMap::from([
                    ("PARQUET:field_id".to_string(), "1".to_string()),
                ])),
                ArrowField::new("name", ArrowDataType::Utf8, false).with_metadata(HashMap::from([
                    ("PARQUET:field_id".to_string(), "2".to_string()),
                ])),
            ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
                Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
            ],
        )?;

        // Cleanup namespace and table before testing.
        let catalog = create_catalog(warehouse_uri)?;
        // Cleanup states before testing.
        delete_all_tables(&*catalog).await?;
        delete_all_namespaces(&*catalog).await?;

        // Write record batch to Parquet file.
        let tmp_dir = tempdir()?;
        let parquet_path = tmp_dir.path().join("data.parquet");
        let file = File::create(&parquet_path)?;
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None)?;
        writer.write(&batch)?;
        writer.close()?;

        let metadata = Arc::new(TableMetadata {
            name: "test_table".to_string(),
            schema: arrow_schema.clone(),
            id: 0, // unused.
            config: TableConfig::new(),
            path: tmp_dir.path().to_path_buf(),
            get_lookup_key: |_row| 1, // unused.
        });

        let mut disk_files = HashMap::new();
        disk_files.insert(parquet_path.clone(), BatchDeletionVector::new(0));

        let mut snapshot = Snapshot::new(metadata);
        snapshot._set_warehouse_info(warehouse_uri.to_string());
        snapshot.disk_files = disk_files;

        snapshot._export_to_iceberg().await?;
        let loaded_snapshot = snapshot._async_load_from_iceberg().await?;
        assert_eq!(
            loaded_snapshot.disk_files.len(),
            1,
            "Should have exactly one file."
        );

        let namespace = vec!["default"];
        let table_name = "test_table";
        let iceberg_table =
            get_or_create_iceberg_table(&*catalog, &namespace, table_name, &arrow_schema).await?;
        let file_io = iceberg_table.file_io();

        // Check the loaded data file exists.
        let (loaded_path, _) = loaded_snapshot.disk_files.iter().next().unwrap();
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

        // Verify second column (name)
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
        let warehouse_uri = "http://localhost:8181";
        test_store_and_load_snapshot_impl(warehouse_uri).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_store_and_load_snapshot_with_filesystem_catalog() -> IcebergResult<()> {
        let tmp_dir = tempdir()?;
        let warehouse_path = tmp_dir.path().to_str().unwrap();
        test_store_and_load_snapshot_impl(warehouse_path).await?;
        Ok(())
    }
}
