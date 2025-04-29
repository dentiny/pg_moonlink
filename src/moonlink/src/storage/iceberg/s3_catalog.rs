use std::collections::HashMap;
/// This module contains the S3 catalog implementation.
///
/// TODO(hjiang):
/// 0. Better error handling.
/// 1. Implement property related functionalities.
/// 2. The initial version access everything via filesystem, for performance consideration we should cache metadata in memory.
/// 3. (not related to functionality) Set snapshot retention policy at metadata.
///
/// Iceberg table format from object storage's perspective:
/// - namespace_indicator.txt
///   - An empty file, indicates it's a valid namespace
/// - data
///   - parquet files
/// - metdata
///   - version hint file
///     + version-hint.text
///     + contains the latest version number for metadata
///   - metadata file
///     + v0.metadata.json ... vn.metadata.json
///     + records table schema
///   - snapshot file / manifest list
///     + snap-<snapshot-id>-<attempt-id>-<commit-uuid>.avro
///     + points to manifest files and record actions
///   - manifest files
///     + <commit-uuid>-m<manifest-counter>.avro
///     + which points to data files and stats
///
/// For S3 catalog, all files are stored under the warehouse location, in detail: <warehouse-location>/<namespace>/<table-name>/.
use std::error::Error;
use std::path::PathBuf;

use async_trait::async_trait;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client as S3Client;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableUpdate,
};

const NAMESPACE_INDICATOR_OBJECT_NAME: &str = "indicator.text";
const PROVIDER: &str = "s3-catalog-provider";

// TODO(hjiang): Able to take credential related information.
pub struct S3CatalogConfig {
    warehouse_location: String,
    access_key_id: String,
    secret_access_key: String,
    provider_name: String,
    region: String,
    bucket: String,
}

#[derive(Debug)]
pub struct S3Catalog {
    s3_client: S3Client,
    bucket: String,
}

impl S3Catalog {
    pub fn new(config: S3CatalogConfig) -> Self {
        let bucket = config.bucket;
        let creds = Credentials::new(
            config.access_key_id,
            config.secret_access_key,
            /*session_token=*/ None,
            /*expires_after=*/ None,
            /*provider_name*/ PROVIDER,
        );
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .credentials_provider(creds.clone())
            .region(Region::new(config.region))
            .endpoint_url(config.warehouse_location)
            .force_path_style(true)
            .build();
        let client = S3Client::from_conf(config);
        Self {
            s3_client: client,
            bucket: bucket,
        }
    }

    /// Get object name of the indicator object for the given namespace.
    fn get_namespace_indicator_name(&self, namespace: &iceberg::NamespaceIdent) -> String {
        let mut path = PathBuf::from(namespace.to_url_string());
        path.push(NAMESPACE_INDICATOR_OBJECT_NAME);
        path.to_string_lossy().into_owned()
    }

    /// Check whether the given object exists in the bucket.
    async fn object_exists(&self, key: &str) -> Result<bool, Box<dyn Error>> {
        match self
            .s3_client
            .head_object()
            .bucket(self.bucket.clone())
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.into_service_error().is_not_found() {
                    Ok(false)
                } else {
                    Err("Failed to check object existence".into())
                }
            }
        }
    }
}

#[async_trait]
impl Catalog for S3Catalog {
    /// List namespaces under the parent namespace, return error if parent namespace doesn't exist.
    /// It's worth noting only one layer of namespace will be returned.
    /// For example, suppose we create three namespaces: (1) "a", (2) "a/b", (3) "b".
    /// List all namespaces under root namespace will return "a" and "b", but not "a/b".
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        todo!()
    }

    /// Create a new namespace inside the catalog, return error if namespace already exists, or any parent namespace doesn't exist.
    ///
    /// TODO(hjiang): Implement properties handling.
    async fn create_namespace(
        &self,
        namespace_ident: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        self.s3_client
            .put_object()
            .bucket(self.bucket.clone())
            .key(self.get_namespace_indicator_name(namespace_ident))
            .body(ByteStream::default())
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to create namespace: {}", e),
                )
            })?;
        Ok(Namespace::new(namespace_ident.clone()))
    }

    /// Get a namespace information from the catalog, return error if requested namespace doesn't exist.
    async fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<Namespace> {
        let exists = self.namespace_exists(namespace_ident).await?;
        if exists {
            return Ok(Namespace::new(namespace_ident.clone()));
        }
        Err(IcebergError::new(
            iceberg::ErrorKind::NamespaceNotFound,
            format!("Namespace {:?} does not exist", namespace_ident),
        ))
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<bool> {
        let exists = self
            .object_exists(&self.get_namespace_indicator_name(namespace_ident))
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to check object existence at `namespace_exists`: {}",
                        e
                    ),
                )
            })?;
        Ok(exists)
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<()> {
        self.s3_client
            .delete_object()
            .bucket(self.bucket.clone())
            .key(self.get_namespace_indicator_name(namespace_ident))
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to drop namespace: {}", e),
                )
            })?;
        Ok(())
    }

    /// List tables from namespace, return error if the given namespace doesn't exist.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        todo!()
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        todo!()
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        todo!()
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        todo!()
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        todo!()
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        todo!()
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        todo!()
    }

    /// Update a table to the catalog, which writes metadata file and version hint file.
    ///
    /// TODO(hjiang): Implement table requirements, which indicates user-defined compare-and-swap logic.
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        todo!()
    }
}
