use crate::error::Result;
use crate::storage::mooncake_table::Snapshot;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::storage::delete_vector::BatchDeletionVector;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use arrow::ipc::reader::FileReader;
use arrow_array::RecordBatch;
use arrow_array::{Int32Array, StringArray};
use arrow_schema::Schema as ArrowSchema;
use futures::executor::block_on;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::ManifestContentType;
use iceberg::spec::TableMetadata;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::spec::{
    NestedField, NestedFieldRef, PrimitiveType, Schema as IcebergSchema, SchemaId, StructType,
    Type as IcebergType,
};
use iceberg::table::Table as IcebergTable;
use iceberg::table::TableBuilder as IcebergTableBuilder;
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
use iceberg::{Catalog, Result as IcebergResult, TableIdent, TableUpdate};
use iceberg_catalog_memory::MemoryCatalog;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::path::{Path, PathBuf};

static NAMESPACE: &str = "default";
static TABLE_NAME: &str = "t1";

// UNDONE(Iceberg):
// 1. Implement deletion file related load and store operations.
// 2. Current implementation only works for in-memory catalog, we need to support local filesystem and remote access;
// 2.1 Setup local minio for integration testing purpose.
// 3. Support all data types, other than major primitive types.

fn write_test_parquet_file(file_path: &Path) -> Result<()> {
    // Define Arrow schema
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));

    // Create a batch of dummy data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Charlie"),
            ])),
        ],
    )?;

    // Write to Parquet
    let file = std::fs::File::create(file_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

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

fn arrow_type_to_iceberg_type(data_type: &ArrowDataType) -> IcebergType {
    match data_type {
        ArrowDataType::Boolean => IcebergType::Primitive(PrimitiveType::Boolean),
        ArrowDataType::Int32 => IcebergType::Primitive(PrimitiveType::Int),
        ArrowDataType::Int64 => IcebergType::Primitive(PrimitiveType::Long),
        ArrowDataType::Float32 => IcebergType::Primitive(PrimitiveType::Float),
        ArrowDataType::Float64 => IcebergType::Primitive(PrimitiveType::Double),
        ArrowDataType::Utf8 => IcebergType::Primitive(PrimitiveType::String),
        ArrowDataType::Binary => IcebergType::Primitive(PrimitiveType::Binary),
        ArrowDataType::Timestamp(_, _) => IcebergType::Primitive(PrimitiveType::Timestamp),
        ArrowDataType::Date32 => IcebergType::Primitive(PrimitiveType::Date),
        // TODO(hjiang): Support more arrow types.
        _ => panic!("Unsupported Arrow data type: {:?}", data_type),
    }
}

async fn get_or_create_iceberg_table(
    catalog: &MemoryCatalog,
    namespace: &[&str],
    table_name: &str,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {

    // 3. Build table identifier
    let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), TABLE_NAME.to_string());

    // let namespace_ident =
    //     NamespaceIdent::from_vec(namespace_ident_vec.iter().map(|s| s.to_string()).collect())?;

    // 4. Try to load the table
    match catalog.load_table(&table_ident).await {
        Ok(table) => Ok(table),
        Err(_) => {
            let existing_namespaces = catalog.list_namespaces(None).await.unwrap();
            println!(
                "Namespaces alreading in the existing catalog: {:?}",
                existing_namespaces
            );

            if catalog.namespace_exists(&namespace_ident).await.unwrap() {
                println!("Namespace already exists, dropping now.",);
                catalog.drop_namespace(&namespace_ident).await.unwrap();
            }

            let _created_namespace = catalog
                .create_namespace(
                    &namespace_ident,
                    HashMap::from([("key1".to_string(), "value1".to_string())]),
                )
                .await
                .unwrap();
            println!("Namespace {:?} created!", namespace_ident);

            // You can also use the `from_strs` method on `TableIdent` to create the table identifier.
            // let table_ident = TableIdent::from_strs([NAMESPACE, TABLE_NAME]).unwrap();

            // 5. Create the table if it doesn't exist
            let iceberg_schema = arrow_to_iceberg_schema(arrow_schema);

            let tbl_creation = TableCreation::builder()
                .name(table_name.to_string())
                .location(format!(
                    "memory://{}/{}",
                    namespace_ident.to_url_string(),
                    table_name
                ))
                .schema(iceberg_schema)
                .properties(HashMap::new())
                .build();

            /// No such namespace: NamespaceIdent(["default"])
            let table = catalog
                .create_table(&table_ident.namespace, tbl_creation)
                .await?;

            Ok(table)
        }
    }
}

async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    parquet_filepath: &PathBuf,
) -> IcebergResult<Vec<DataFile>> {
    // 1. Read the parquet file into RecordBatches
    let file = std::fs::File::open(parquet_filepath)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut arrow_reader = builder.build()?;

    // 1. Setup location and file name generators
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        /*prefix=*/ "snapshot".to_string(),
        /*suffix=*/ None,
        /*format=*/ DataFileFormat::Parquet,
    );

    // 2. Build a Parquet writer
    let parquet_writer_builder = ParquetWriterBuilder::new(
        /*props=*/ WriterProperties::default(),
        /*schame=*/ table.metadata().current_schema().clone(),
        /*file_io=*/ table.file_io().clone(),
        /*location_generator=*/ location_generator.clone(),
        /*file_name_generator=*/ file_name_generator.clone(),
    );

    // 3. Build a data file writer (no partition spec, partition_id = 0)
    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None);
    let mut data_file_writer = data_file_writer_builder.build().await?;

    // 4. Write the batch
    while let Some(record_batch) = arrow_reader.next().transpose()? {
        print!(
            "[{}:{}] write {:?} to iceberg\n",
            file!(),
            line!(),
            record_batch
        );

        data_file_writer.write(record_batch).await?;
    }

    // 5. Finalize and collect data files
    let data_files = data_file_writer.close().await?;

    // TODO(hjiang): Check whether we need to commit the write operations to iceberg.
    Ok(data_files)
}

pub trait IcebergSnapshot {
    // Write the current version to iceberg
    async fn _export_to_iceberg(&self) -> IcebergResult<()>;

    // Create a snapshot by reading from iceberg
    fn _load_from_iceberg(&self) -> IcebergResult<Self>
    where
        Self: Sized;
}

impl IcebergSnapshot for Snapshot {
    async fn _export_to_iceberg(&self) -> IcebergResult<()> {
        // Step 1: Extract metadata
        let table_name = self.metadata.name.clone();
        let namespace = vec!["default"];
        let arrow_schema = self.metadata.schema.as_ref();

        println!("[{}:{}] Something happened!", file!(), line!());

         // 1. Build file I/O
        let file_io = FileIOBuilder::new("memory").build()?;

        // 2. Create a memory catalog
        let catalog = MemoryCatalog::new(file_io.clone(), None);

        // Step 2: Get or create Iceberg table
        let iceberg_table =
            get_or_create_iceberg_table(&catalog, &namespace, &table_name, arrow_schema).await?;

        println!("[{}:{}] Something happened!", file!(), line!());

        // Step 3: Convert Snapshot content to RecordBatch (you might have this already)
        let disk_files_ref = &self.disk_files;

        // Step 4: Write batch into Iceberg table
        let txn = Transaction::new(&iceberg_table);
        let mut action =
            txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;

        for (file_path, _deletion_vector) in disk_files_ref {
            println!("[{}:{}] write {:?} to iceberg", file!(), line!(), file_path);

            let data_files =
                write_record_batch_to_iceberg(&iceberg_table.clone(), file_path).await?;
            action.add_data_files(data_files)?;
        }

        // Step 5: Commit the transaction.
        // let txn = action.apply().await?;
        // txn.commit(&catalog).await?;

        println!("[{}:{}] Something happened!", file!(), line!());

        println!(
            "Exported snapshot to Iceberg table: {}/{}",
            namespace.join("."),
            table_name
        );

        println!("[{}:{}] Something happened!", file!(), line!());

        ///// debug
        {
            
        }

        ///// DEBUG: check content right after store.
        {
            let new_iceberg_table = block_on(get_or_create_iceberg_table(
                &catalog,
                &namespace,
                &table_name,
                arrow_schema,
            ))?;

            println!("[{}:{}] Something happened!", file!(), line!());

            let table_metadata = iceberg_table.metadata();

            println!(
                "[{}:{}] Something happened!, righr after store {:?}",
                file!(),
                line!(),
                table_metadata
            );

            /// Nothing created!!!
            let snapshot_meta = table_metadata.current_snapshot().unwrap();

            println!(
                "[{}:{}] Something happened!, righr after store {:?}",
                file!(),
                line!(),
                snapshot_meta
            );
        }

        Ok(())
    }

    fn _load_from_iceberg(&self) -> IcebergResult<Self> {
        let namespace = vec!["default"];
        let table_name = self.metadata.name.clone();
        let arrow_schema = self.metadata.schema.as_ref();

        // 1. Build file I/O
        let file_io = FileIOBuilder::new("memory").build()?;

        // 2. Create a memory catalog
        let catalog = MemoryCatalog::new(file_io.clone(), None);

        let iceberg_table = block_on(get_or_create_iceberg_table(
            &catalog,
            &namespace,
            &table_name,
            arrow_schema,
        ))?;

        println!("[{}:{}] Something happened!", file!(), line!());

        let table_metadata = iceberg_table.metadata();

        /// Nothing created!!!
        let snapshot_meta = table_metadata.current_snapshot().unwrap();

        let file_io = iceberg_table.file_io();

        println!("[{}:{}] Something happened!", file!(), line!());

        // Load the manifest list (async inside block_on)
        let manifest_list = block_on(snapshot_meta.load_manifest_list(file_io, table_metadata))?;

        let mut snapshot = Snapshot::new(self.metadata.clone());

        println!("[{}:{}] Something happened!", file!(), line!());

        for manifest_file in manifest_list.entries().iter() {
            // We're only interested in DATA files, not DELETES
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            println!("[{}:{}] Something happened!", file!(), line!());

            let manifest = block_on(manifest_file.load_manifest(file_io))?;
            for entry in manifest.entries() {
                let data_file = entry.data_file();
                let file_path = PathBuf::from(data_file.file_path().to_string());

                // Read parquet file
                let file = std::fs::File::open(&file_path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                let mut reader = builder.build()?;

                println!("[{}:{}] Something happened!", file!(), line!());

                while let Some(record_batch) = reader.next().transpose()? {
                    // Register file with deletion vector
                    snapshot
                        .disk_files
                        .insert(file_path.clone(), BatchDeletionVector::new(0));
                }
            }
        }

        println!("[{}:{}] Something happened!", file!(), line!());

        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mooncake_table::{Snapshot, TableConfig, TableMetadata};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_store_and_load_snapshot() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));

        let metadata = Arc::new(TableMetadata {
            name: "test_table".to_string(),
            schema: arrow_schema.clone(),
            id: 0,
            config: TableConfig::new(),
            path: PathBuf::from("test_table"),
            get_lookup_key: |row| 1,
        });

        // Step 1: Write real Parquet file
        let parquet_file_path = PathBuf::from("/tmp/test_snapshot.parquet");
        write_test_parquet_file(&parquet_file_path).expect("Failed to write test parquet file");

        // Step 2: Create snapshot and register file
        let mut snapshot = Snapshot::new(metadata);
        snapshot
            .disk_files
            .insert(parquet_file_path.clone(), BatchDeletionVector::new(0));
        let snapshot = Arc::new(snapshot);

        // Step 3: Export
        snapshot._export_to_iceberg().await.unwrap();

        // Step 3: Load the Snapshot back from Iceberg
        let loaded_snapshot = snapshot._load_from_iceberg().unwrap();

        // Step 4: Compare the loaded content with the original Snapshot
        assert_eq!(loaded_snapshot.metadata.name, snapshot.metadata.name);
        assert_eq!(
            loaded_snapshot.metadata.schema.as_ref(),
            snapshot.metadata.schema.as_ref()
        );
        assert_eq!(
            loaded_snapshot.disk_files.keys().collect::<Vec<_>>(),
            snapshot.disk_files.keys().collect::<Vec<_>>()
        );

        println!("Test passed: Stored and loaded snapshot matches the original.");
    }
}
