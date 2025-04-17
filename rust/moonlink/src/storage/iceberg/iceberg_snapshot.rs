use crate::error::Result;
use crate::storage::mooncake_table::Snapshot;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use arrow::ipc::reader::FileReader;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::spec::{
    NestedField, NestedFieldRef, PrimitiveType, Schema as IcebergSchema, SchemaId, StructType,
    Type as IcebergType,
};
use iceberg::table::Table as IcebergTable;
use iceberg::table::TableBuilder as IcebergTableBuilder;
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
use iceberg_catalog_memory::MemoryCatalog;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use std::path::{Path, PathBuf};

// UNDONE(Iceberg):

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
    namespace: &[&str],
    table_name: &str,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {
    // 1. Build file I/O
    let file_io = FileIOBuilder::new("memory").build()?;

    // 2. Create a memory catalog
    let catalog = MemoryCatalog::new(file_io.clone(), None);

    // 3. Build table identifier
    let mut namespace_ident_vec = namespace.to_vec();
    namespace_ident_vec.push(table_name);
    let table_ident: TableIdent = TableIdent::from_strs(namespace_ident_vec.clone())?;

    let namespace_ident =
        NamespaceIdent::from_vec(namespace_ident_vec.iter().map(|s| s.to_string()).collect())?;

    // 4. Try to load the table
    match catalog.load_table(&table_ident).await {
        Ok(table) => Ok(table),
        Err(_) => {
            // 5. Create the table if it doesn't exist
            let iceberg_schema = arrow_to_iceberg_schema(arrow_schema);

            let tbl_creation = TableCreation::builder()
                .name(table_name.to_string())
                .schema(iceberg_schema)
                .properties(HashMap::new())
                .build();

            let table = catalog.create_table(&namespace_ident, tbl_creation).await?;
            Ok(table)
        }
    }
}

async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    parquet_filepath: &PathBuf,
) -> IcebergResult<()> {
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
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );

    // 3. Build a data file writer (no partition spec, partition_id = 0)
    //
    // TODO(hjiang): Figure out the partition value.
    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None);
    let mut data_file_writer = data_file_writer_builder.build().await?;

    // 4. Write the batch
    while let Some(record_batch) = arrow_reader.next().transpose()? {
        data_file_writer.write(record_batch).await?;
    }

    // 5. Finalize and collect data files
    let data_files = data_file_writer.close().await?;

    // TODO(hjiang): Check whether we need to commit the write operations to iceberg.

    Ok(())
}

pub trait IcebergSnapshot {
    // Write the current version to iceberg
    async fn _export_to_iceberg(&self) -> IcebergResult<()>;

    // Create a snapshot by reading from iceberg
    fn _load_from_iceberg(&self) -> Self;
}

/// TODO(hjiang): Currently use in-memory IO, instead persist it somewhere.
impl IcebergSnapshot for Snapshot {
    async fn _export_to_iceberg(&self) -> IcebergResult<()> {
        // Step 1: Extract metadata
        let table_name = self.metadata.name.clone();
        let namespace = vec!["inmemory"];
        let arrow_schema = self.metadata.schema.as_ref();

        // Step 2: Get or create Iceberg table
        let iceberg_table =
            get_or_create_iceberg_table(&namespace, &table_name, arrow_schema).await?;

        // Step 3: Convert Snapshot content to RecordBatch (you might have this already)
        let disk_files_ref = &self.disk_files;

        // Step 4: Write batch into Iceberg table
        for (file_path, _deletion_vector) in disk_files_ref {
            write_record_batch_to_iceberg(&iceberg_table.clone(), file_path).await?;
        }

        println!(
            "Exported snapshot to Iceberg table: {}/{}",
            namespace.join("."),
            table_name
        );

        Ok(())
    }

    fn _load_from_iceberg(&self) -> Self {
        todo!()
    }
}
