use crate::storage::iceberg::file_catalog::FileSystemCatalog;

use iceberg::Catalog;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

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
pub fn create_catalog(catalog_info: CatalogInfo) -> Box<dyn Catalog> {
    match catalog_info {
        CatalogInfo::Rest { uri } => Box::new(RestCatalog::new(
            RestCatalogConfig::builder().uri(uri).build(),
        )),
        CatalogInfo::FileSystem { warehouse_location } => {
            Box::new(FileSystemCatalog::new(warehouse_location))
        }
    }
}
