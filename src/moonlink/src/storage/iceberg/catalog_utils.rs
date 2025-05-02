use crate::storage::iceberg::file_catalog::FileSystemCatalog;
use crate::storage::iceberg::test_utils;

use iceberg::Error as IcebergError;
use iceberg::{Catalog, Result as IcebergResult};
use url::Url;

/// Create a catelog based on the provided type.
/// There're only two catalogs supported: filesystem catalog and object storage, all other catalogs either don't support transactional commit, or deletion vector.
pub fn create_catalog(warehouse_uri: &str) -> IcebergResult<Box<dyn Catalog>> {
    // Special handle testing situation.
    if warehouse_uri == test_utils::MINIO_TEST_WAREHOUSE_URI {
        return Ok(Box::new(test_utils::create_minio_s3_catalog()));
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
