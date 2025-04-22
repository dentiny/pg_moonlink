use crate::storage::mooncake_table::Snapshot;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowType;
use arrow_schema::Schema as ArrowSchema;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::spec::{
    NestedField, NestedFieldRef, PrimitiveType, Schema as IcebergSchema, Type as IcebergType,
};
use iceberg::table::Table as IcebergTable;
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
use std::path::PathBuf;

// UNDONE(Iceberg):
// 1. Implement deletion file related load and store operations.
// 2. Implement store and load for manifest files, manifest lists, and catalog metadata, which is not supported in iceberg-rust.
// 3. Current implementation only works for in-memory catalog, we need to support remote access.
// 3.1 Before development, setup local minio for integration testing purpose.
// (unrelated) 4. Support all data types, other than major primitive types.

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
async fn get_or_create_iceberg_table(
    catalog: &RestCatalog,
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
            let namespace_already_exists = catalog.namespace_exists(&namespace_ident).await?;
            if !namespace_already_exists {
                let _created_namespace = catalog
                    .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
                    .await
                    .unwrap();
            }

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
        /*prefix=*/ "snapshot".to_string(),
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
        /*partition_valu=*/ None,
        /*partition_val=*/ 0,
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
        for (file_path, deletion_vector) in self.disk_files.drain() {
            let data_file =
                write_record_batch_to_iceberg(&iceberg_table.clone(), &file_path).await?;

            let new_path = PathBuf::from(data_file.file_path());
            new_disk_files.insert(new_path, deletion_vector);
        }
        self.disk_files = new_disk_files;

        Ok(())
    }

    fn _load_from_iceberg(&self) -> IcebergResult<Self> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_write_record_batch_to_iceberg() -> IcebergResult<()> {
        use std::collections::HashMap;
        use std::fs::File;
        use std::sync::Arc;
        use tempfile::tempdir;

        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use iceberg::io::FileIOBuilder;
        use iceberg::table::Table;
        use iceberg::{NamespaceIdent, TableCreation};
        use iceberg_catalog_memory::MemoryCatalog;
        use parquet::arrow::ArrowWriter;

        // Create Arrow schema and record batch.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
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

        // Create in-memory catalog and table.
        let file_io = FileIOBuilder::new("memory").build()?;
        let catalog = MemoryCatalog::new(file_io.clone(), None);
        let namespace_ident = NamespaceIdent::from_strs(&["default"])?;
        let table_name = "test_table";

        // Create namespace and table.
        catalog
            .create_namespace(&namespace_ident, HashMap::new())
            .await
            .ok();

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
        let table: Table = catalog.create_table(&namespace_ident, tbl_creation).await?;

        let data_file = write_record_batch_to_iceberg(&table, &parquet_path).await?;
        let input_file = table
            .file_io()
            .new_input(data_file.file_path())
            .expect("Failed to create InputFile");
        let bytes = input_file.read().await.expect("Failed to read memory file");

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        let mut arrow_reader = builder.build()?;

        let mut total_rows = 0;
        while let Some(batch) = arrow_reader.next().transpose()? {
            // Check schema.
            let schema = batch.schema();
            assert_eq!(schema.fields().len(), 1, "Expected 1 column in schema");
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int32);

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
}
