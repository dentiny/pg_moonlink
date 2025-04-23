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
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

// UNDONE(Iceberg):
// 1. Implement filesystem catalog for (1) unit test; (2) local quick experiment for pg_mooncake.
// 2. Implement deletion file related load and store operations.
// (unrelated to functionality) 3. Support all data types, other than major primitive types.
// (unrelated to functionality) 4. Update rest catalog service ip/port, which should be parsed from env variable or config files.
// (unrelated to functionality) 5. Add timeout to rest catalog access.
// (unrelated to functionality) 6. Use real namespace and table name, it's hard-coded to "default" and "test_table" for now.

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
async fn get_or_create_iceberg_table<C: Catalog>(
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
        /*prefix=*/ "iceberg-data".to_string(),
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

    // Create a snapshot by reading from iceberg in async style
    //
    // TODO(hjiang): Expose an async function only for tokio-based test, otherwise will suffer deadlock with executor.
    // Reference: https://greptime.com/blogs/2023-03-09-bridging-async-and-sync-rust
    async fn async_load_from_iceberg(&self) -> IcebergResult<Self>
    where
        Self: Sized;

    // Create a snapshot by reading from iceberg
    fn _load_from_iceberg(&self) -> IcebergResult<Self>
    where
        Self: Sized;
}

impl IcebergSnapshot for Snapshot {
    async fn _export_to_iceberg(&mut self) -> IcebergResult<()> {
        let table_name = self.metadata.name.clone();
        let namespace = vec!["default"];
        let arrow_schema = self.metadata.schema.as_ref();
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri("http://localhost:8181".to_string())
                .build(),
        );
        let iceberg_table =
            get_or_create_iceberg_table(&catalog, &namespace, &table_name, arrow_schema).await?;

        let mut new_disk_files = HashMap::new();
        let txn = Transaction::new(&iceberg_table);
        let mut action =
            txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;

        for (file_path, deletion_vector) in self.disk_files.drain() {
            let data_file =
                write_record_batch_to_iceberg(&iceberg_table.clone(), &file_path).await?;
            let new_path = PathBuf::from(data_file.file_path());
            new_disk_files.insert(new_path, deletion_vector);
            action.add_data_files(vec![data_file])?;
        }
        self.disk_files = new_disk_files;

        // Commit write transaction.
        let txn = action.apply().await?;
        txn.commit(&catalog).await?;

        Ok(())
    }

    async fn async_load_from_iceberg(&self) -> IcebergResult<Self> {
        let namespace = vec!["default"];
        let table_name = self.metadata.name.clone();
        let arrow_schema = self.metadata.schema.as_ref();
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri("http://localhost:8181".to_string())
                .build(),
        );
        let iceberg_table =
            get_or_create_iceberg_table(&catalog, &namespace, &table_name, arrow_schema).await?;

        let table_metadata = iceberg_table.metadata();
        let snapshot_meta = table_metadata.current_snapshot().unwrap();
        let file_io = iceberg_table.file_io();
        let manifest_list = snapshot_meta
            .load_manifest_list(file_io, table_metadata)
            .await?;
        let mut snapshot = Snapshot::new(self.metadata.clone());
        for manifest_file in manifest_list.entries().iter() {
            // We're only interested in DATA files.
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
        block_on(self.async_load_from_iceberg())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::mooncake_table::{Snapshot, TableConfig, TableMetadata};

    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::tempdir;

    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use arrow_array::{Int32Array, RecordBatch};
    use iceberg::io::FileRead;
    use iceberg::table::Table;
    use iceberg::{NamespaceIdent, TableCreation};
    use parquet::arrow::ArrowWriter;

    async fn delete_all_tables<C: Catalog>(catalog: &C) -> IcebergResult<()> {
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

    async fn delete_all_namespaces<C: Catalog>(catalog: &C) -> IcebergResult<()> {
        let namespaces = catalog.list_namespaces(/*parent=*/ None).await?;
        for namespace in namespaces {
            catalog.drop_namespace(&namespace).await.ok();
            println!("Deleted namespace: {:?}", namespace);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_write_record_batch_to_iceberg() -> IcebergResult<()> {
        // Create Arrow schema and record batch.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )
        .with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )]))]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        // Write record batch to Parquet file.
        let tmp_dir = tempdir()?;
        let parquet_path = tmp_dir.path().join("data.parquet");
        let file = File::create(&parquet_path)?;
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None)?;
        writer.write(&batch)?;
        writer.close()?;

        // Build Iceberg schema.
        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema);

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri("http://localhost:8181".to_string())
                .build(),
        );

        // Cleanup states before testing.
        delete_all_tables(&catalog).await?;
        delete_all_namespaces(&catalog).await?;

        let namespace_ident = NamespaceIdent::from_strs(&["default"])?;
        let table_name = "test_table";

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
        let table: Table = catalog.create_table(&namespace_ident, tbl_creation).await?;
        let data_file = write_record_batch_to_iceberg(&table, &parquet_path).await?;
        let input_file = table.file_io().new_input(data_file.file_path())?;
        let bytes = input_file.read().await?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        let mut arrow_reader = builder.build()?;

        let mut total_rows = 0;
        while let Some(batch) = arrow_reader.next().transpose()? {
            // Check schema.
            let schema = batch.schema();
            assert_eq!(schema.fields().len(), 1, "Expected 1 column in schema");
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(0).data_type(), &ArrowDataType::Int32);

            // Check columns.
            assert_eq!(batch.num_columns(), 1, "Expected 1 column");
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Expected Int32Array");

            // Check column values.
            let expected = vec![1, 2, 3];
            for (i, value) in expected.iter().enumerate() {
                assert_eq!(array.value(i), *value);
            }

            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 3, "Expected total 3 rows");

        Ok(())
    }

    #[tokio::test]
    async fn test_store_and_load_snapshot() -> IcebergResult<()> {
        // Create Arrow schema and record batch.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )
        .with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )]))]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        // Cleanup namespace and table before testing.
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri("http://localhost:8181".to_string())
                .build(),
        );
        // Cleanup states before testing.
        delete_all_tables(&catalog).await?;
        delete_all_namespaces(&catalog).await?;

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
        snapshot.disk_files = disk_files;

        snapshot._export_to_iceberg().await?;
        let loaded_snapshot = snapshot.async_load_from_iceberg().await?;
        assert_eq!(
            loaded_snapshot.disk_files.len(),
            1,
            "Should have exactly one file."
        );

        let namespace = vec!["default"];
        let table_name = "test_table";
        let iceberg_table =
            get_or_create_iceberg_table(&catalog, &namespace, table_name, &arrow_schema).await?;
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
        let actual_data = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            *actual_data,
            Int32Array::from(vec![1, 2, 3]),
            "Data should match [1, 2, 3]"
        );

        Ok(())
    }
}
