use crate::storage::iceberg::file_catalog::FileSystemCatalog;
use crate::storage::iceberg::object_storage_catalog::{S3Catalog, S3CatalogConfig};

use iceberg::Error as IcebergError;
use iceberg::{Catalog, Result as IcebergResult};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use url::Url;

/// Create a catelog based on the provided type.
///
/// TODO(hjiang): Support security configuration for REST catalog.
pub fn create_catalog(warehouse_uri: &str) -> IcebergResult<Box<dyn Catalog>> {
    // Same as iceberg-rust imlementation, use URL parsing to decide which catalog to use.
    let url = Url::parse(warehouse_uri)
        .or_else(|_| Url::from_file_path(warehouse_uri))
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Invalid warehouse URI {}: {:?}", warehouse_uri, e),
            )
        })?;

    // There're only three catalogs supported: filesystem, object storage and rest, all other catalogs don't support transactional commit.
    if warehouse_uri.starts_with("s3://test-bucket") {
        let config = S3CatalogConfig::new(
            /*warehouse_location=*/ "s3://test-bucket".to_string(),
            /*access_key_id=*/ "minioadmin".to_string(),
            /*secret_access_key=*/ "minioadmin".to_string(),
            /*region=*/ "auto".to_string(), // minio doesn't care about region.
            /*bucket=*/ "test-bucket".to_string(),
            /*endpoint=*/ "http://minio:9000".to_string(),
        );

        println!("we are creating the correct config!!");

        return Ok(Box::new(S3Catalog::new(config)))
    }

    if url.scheme() == "file" {
        let absolute_path = url.path();
        return Ok(Box::new(FileSystemCatalog::new(absolute_path.to_string())));
    }

    // Delegate all other warehouse URIs to the REST catalog.
    Ok(Box::new(RestCatalog::new(
        RestCatalogConfig::builder()
            .uri(warehouse_uri.to_string())
            .build(),
    )))
}
