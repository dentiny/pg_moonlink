use crate::storage::iceberg::file_catalog::FileSystemCatalog;

use iceberg::Error as IcebergError;
use iceberg::{Catalog, Result as IcebergResult};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use url::Url;

#[derive(Debug, Clone)]
pub enum CatalogInfo {
    /// Connection to a REST catalog server.
    Rest { uri: String },
    /// Local filesystem based catalog.
    FileSystem { warehouse_location: String },
}

// Default to use filesystem catalog with a default warehouse location.
impl Default for CatalogInfo {
    fn default() -> Self {
        CatalogInfo::FileSystem {
            warehouse_location: "/tmp/moonlink_iceberg_warehouse".to_string(),
        }
    }
}

impl CatalogInfo {
    pub fn display(&self) -> String {
        match self {
            CatalogInfo::Rest { uri } => format!("REST catalog uri: {}", uri),
            CatalogInfo::FileSystem { warehouse_location } => format!(
                "Filesytem catalog with warehouse location: {}",
                warehouse_location
            ),
        }
    }
}

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

    // There're only two catalogs supported: filesystem and rest, all other catalogs don't support transactional commit.
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
