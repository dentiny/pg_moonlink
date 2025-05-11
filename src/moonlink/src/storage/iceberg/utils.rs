use crate::storage::iceberg::file_catalog::{CatalogConfig, FileCatalog};
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
#[cfg(feature = "storage-s3")]
use crate::storage::iceberg::s3_test_utils;
/// Util functions related to iceberg.
use iceberg::spec::{DataContentType, DataFileFormat, ManifestEntry};

use iceberg::io::FileIOBuilder;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use url::Url;
use uuid::Uuid;

use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow as IcebergArrow;
use iceberg::spec::DataFile;
use iceberg::table::Table as IcebergTable;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::{
    Error as IcebergError, NamespaceIdent, Result as IcebergResult, TableCreation, TableIdent,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

/// Return whether the given manifest entry represents data files.
pub fn is_data_file_entry(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_data_file =
        f.content_type() == DataContentType::Data && f.file_format() == DataFileFormat::Parquet;
    if !is_data_file {
        return false;
    }
    assert!(f.referenced_data_file().is_none());
    assert!(f.content_offset().is_none());
    assert!(f.content_size_in_bytes().is_none());
    true
}

/// Return whether the given manifest entry represents deletion vector.
pub fn is_deletion_vector_entry(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_deletion_vector = f.content_type() == DataContentType::PositionDeletes;
    if !is_deletion_vector {
        return false;
    }
    assert_eq!(f.file_format(), DataFileFormat::Puffin);
    assert!(f.referenced_data_file().is_some());
    assert!(f.content_offset().is_some());
    assert!(f.content_size_in_bytes().is_some());
    true
}

/// Return whether the given manifest entry represents file index.
pub fn is_file_index(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_file_index =
        f.content_type() == DataContentType::Data && f.file_format() == DataFileFormat::Puffin;
    if !is_file_index {
        return false;
    }
    assert!(f.referenced_data_file().is_none());
    assert!(f.content_offset().is_none());
    assert!(f.content_size_in_bytes().is_none());
    true
}

/// Create a catelog based on the provided type.
///
/// It's worth noting catalog and warehouse uri are not 1-1 mapping; for example, rest catalog could handle warehouse.
/// Here we simply deduce catalog type from warehouse because both filesystem and object storage catalog are only able to handle certain scheme.
pub fn create_catalog(warehouse_uri: &str) -> IcebergResult<Box<dyn MoonlinkCatalog>> {
    // Special handle testing situation.
    #[cfg(feature = "storage-s3")]
    {
        if warehouse_uri.starts_with(s3_test_utils::MINIO_TEST_WAREHOUSE_URI_PREFIX) {
            let test_bucket = s3_test_utils::get_test_minio_bucket(warehouse_uri);
            return Ok(Box::new(s3_test_utils::create_minio_s3_catalog(
                &test_bucket,
                warehouse_uri,
            )));
        }
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
        return Ok(Box::new(FileCatalog::new(
            absolute_path.to_string(),
            CatalogConfig::FileSystem {},
        )));
    }

    // TODO(hjiang): Fallback to object storage for all warehouse uris.
    todo!("Need to take secrets from client side and create object storage catalog.")
}

// Get or create an iceberg table in the given catalog from the given namespace and table name.
pub(crate) async fn get_or_create_iceberg_table<C: MoonlinkCatalog + ?Sized>(
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
//
// TODO(hjiang):
// 1. Uploading local file to remote is inefficient, it reads local arrow batches and write them one by one.
// The reason we keep the dummy style, instead of copying the file directly to target is we need the `DataFile` struct,
// which is used when upload to iceberg table.
// One way to resolve is to use DataFileWrite on local write, and remember the `DataFile` returned.
//
// 2. A few data file properties need to respect and consider.
// Reference:
// - https://iceberg.apache.org/docs/latest/configuration/#table-properties
// - https://iceberg.apache.org/docs/latest/configuration/#table-behavior-properties
//
// - write.parquet.row-group-size-bytes
// - write.parquet.page-size-bytes
// - write.parquet.page-row-limit
// - write.parquet.dict-size-bytes
// - write.parquet.compression-codec
// - write.parquet.compression-level
// - write.parquet.bloom-filter-max-bytes
// - write.metadata.compression-codec
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

    // TODO(hjiang): Fix synchronous parquet write.
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

/// Get URL scheme for the given path.
fn get_url_scheme(url: &str) -> String {
    let url = Url::parse(url)
        .or_else(|_| Url::from_file_path(url))
        .unwrap_or_else(|_| panic!("Cannot get URL scheme from {:?}", url));
    url.scheme().to_string()
}

/// Copy the given local index file to iceberg table, and return filepath within iceberg table.
pub(crate) async fn upload_index_file(
    table: &IcebergTable,
    local_index_filepath: &str,
) -> IcebergResult<String> {
    let filename = Path::new(local_index_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let remote_filepath = location_generator.generate_location(&filename);
    let src = FileIOBuilder::new_fs_io()
        .build()?
        .new_input(local_index_filepath)?;
    let dst = FileIOBuilder::new(get_url_scheme(&remote_filepath))
        .build()?
        .new_output(remote_filepath.clone())?;

    // TODO(hjiang): Switch to parallel chunk-based reading.
    let bytes = src.read().await?;
    dst.write(bytes).await?;

    Ok(remote_filepath)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_url_scheme() {
        assert_eq!(get_url_scheme("/tmp/iceberg_table"), "file");
        assert_eq!(get_url_scheme("file:///tmp/iceberg_table"), "file");
        assert_eq!(get_url_scheme("s3://bucket/iceberg_table"), "s3");
    }
}
