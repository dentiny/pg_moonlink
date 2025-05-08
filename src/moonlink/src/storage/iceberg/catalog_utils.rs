use crate::storage::iceberg::file_catalog::FileSystemCatalog;
use crate::storage::iceberg::test_utils;

use std::collections::HashMap;
use std::path::PathBuf;
use url::Url;
use uuid::Uuid;

use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow as IcebergArrow;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table as IcebergTable;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::{
    Catalog, Error as IcebergError, NamespaceIdent, Result as IcebergResult, TableCreation,
    TableIdent,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

/// Create a catelog based on the provided type.
/// There're only two catalogs supported: filesystem catalog and object storage, all other catalogs either don't support transactional commit, or deletion vector.
///
/// It's worth noting catalog and warehouse uri are not 1-1 mapping; for example, rest catalog could handle warehouse.
/// Here we simply deduce catalog type from warehouse because both filesystem and object storage catalog are only able to handle certain scheme.
pub fn create_catalog(warehouse_uri: &str) -> IcebergResult<Box<dyn Catalog>> {
    // Special handle testing situation.
    if warehouse_uri.starts_with(test_utils::MINIO_TEST_WAREHOUSE_URI_PREFIX) {
        let test_bucket = test_utils::get_test_minio_bucket(warehouse_uri);
        return Ok(Box::new(test_utils::create_minio_s3_catalog(
            &test_bucket,
            warehouse_uri,
        )));
    }

    let url = Url::parse(warehouse_uri)
        .or_else(|_| Url::from_file_path(warehouse_uri))
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Invalid warehouse URI {}: {:?}", warehouse_uri, e),
            )
        })?;

    if url.scheme() == "file" {
        let absolute_path = url.path();
        return Ok(Box::new(FileSystemCatalog::new(absolute_path.to_string())));
    }

    // TODO(hjiang): Fallback to object storage for all warehouse uris.
    todo!("Need to take secrets from client side and create object storage catalog.")
}

// Get or create an iceberg table in the given catalog from the given namespace and table name.
pub(crate) async fn get_or_create_iceberg_table<C: Catalog + ?Sized>(
    catalog: &C,
    warehouse_uri: &str,
    namespace: &Vec<String>,
    table_name: &str,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {
    let namespace_ident = NamespaceIdent::from_strs(namespace).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());
    match catalog.load_table(&table_ident).await {
        Ok(table) => Ok(table),
        // TODO(hjiang): Better error handling.
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
                    "{}/{}/{}",
                    warehouse_uri,
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

/// Write the given record batch in the given local file to the iceberg table (parquet file keeps unchanged).
///
/// TODO(hjiang): Uploading local file to remote is inefficient, it reads local arrow batches and write them one by one.
/// The reason we keep the dummy style, instead of copying the file directly to target is we need the `DataFile` struct,
/// which is used when upload to iceberg table.
/// One way to resolve is to use DataFileWrite on local write, and remember the `DataFile` returned.
pub(crate) async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    parquet_filepath: &PathBuf,
) -> IcebergResult<DataFile> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        // Use UUID as prefix to avoid hotspotting on storage access.
        /*prefix=*/
        Uuid::new_v4().to_string(),
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

    println!("\n\nwriting {:?} to {:?}\n\n", parquet_filepath, data_files[0].file_path());

    assert_eq!(
        data_files.len(),
        1,
        "Should only have one parquet file written"
    );

    Ok(data_files[0].clone())
}
