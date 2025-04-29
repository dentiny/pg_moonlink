use std::collections::HashMap;
/// This module contains the S3 catalog implementation.
///
/// TODO(hjiang):
/// 1. Better error handling.
/// 2. Implement property related functionalities.
/// 3. (low priority) Implement `list_namespace` function, for now it's not required in snapshot <-> iceberg interaction.
/// 4. (low priority) Implement `rename_table` function, for now it's not required in snapshot <-> iceberg interaction.
/// 5. The initial version access everything via filesystem, for performance consideration we should cache metadata in memory.
/// 6. (not related to functionality) Set snapshot retention policy at metadata.
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
use std::fmt::format;
use std::path::PathBuf;
use std::vec;

use async_trait::async_trait;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client as S3Client;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableUpdate,
};
use parquet::file::metadata;

const NAMESPACE_INDICATOR_OBJECT_NAME: &str = "indicator.text";
const PROVIDER: &str = "s3-catalog-provider";

// TODO(hjiang): Able to take credential related information.
pub struct S3CatalogConfig {
    warehouse_location: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    bucket: String,
}

impl S3CatalogConfig {
    pub fn new(
        warehouse_location: String,
        access_key_id: String,
        secret_access_key: String,
        region: String,
        bucket: String,
    ) -> Self {
        Self {
            warehouse_location,
            access_key_id,
            secret_access_key,
            region,
            bucket,
        }
    }
}

#[derive(Debug)]
pub struct S3Catalog {
    file_io: FileIO,
    s3_client: S3Client,
    bucket: String,
}

impl S3Catalog {
    pub fn new(config: S3CatalogConfig) -> Self {
        let bucket = config.bucket;
        let warehouse_location = config.warehouse_location;
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
            .endpoint_url(warehouse_location.clone())
            .force_path_style(true)
            .build();
        let client = S3Client::from_conf(config);
        Self {
            file_io: FileIOBuilder::new("s3").build().unwrap(),
            s3_client: client,
            bucket: bucket,
        }
    }

    /// Get object name of the indicator object for the given namespace.
    fn get_namespace_indicator_name(namespace: &iceberg::NamespaceIdent) -> String {
        let mut path = PathBuf::new();
        for part in namespace.as_ref() {
            path.push(part);
        }
        path.push(NAMESPACE_INDICATOR_OBJECT_NAME);
        path.to_str().unwrap().to_string()
    }

    /// Check whether the given object exists in the bucket.
    async fn object_exists(&self, object: &str) -> Result<bool, Box<dyn Error>> {
        match self
            .s3_client
            .head_object()
            .bucket(self.bucket.clone())
            .key(object)
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

    /// Create the bucket which is managed by s3 catalog. OK if it already exists.
    ///
    /// TODO(hjiang): Error handling, temporarily ignord because it's only used in unit test with local minio server.
    async fn create_bucket(&self) -> Result<(), Box<dyn Error>> {
        let _ = self
            .s3_client
            .create_bucket()
            .bucket(self.bucket.clone())
            .send()
            .await;
        Ok(())
    }

    /// Delete the bucket for the catalog.
    /// Expose only for testing purpose.
    async fn cleanup_bucket(&self) -> Result<(), Box<dyn Error>> {
        self.create_bucket().await?;

        let mut continuation_token = None;
        loop {
            let mut builder = self.s3_client.list_objects_v2().bucket(self.bucket.clone());
            if let Some(token) = continuation_token {
                builder = builder.continuation_token(token);
            }
            let response = builder.send().await?;

            if let Some(contents) = response.contents {
                let delete_objects: Vec<ObjectIdentifier> = contents
                    .into_iter()
                    .filter_map(|o| o.key)
                    .map(|key| {
                        ObjectIdentifier::builder()
                            .key(key)
                            .build()
                            .expect("Failed to build ObjectIdentifier")
                    })
                    .collect();

                if !delete_objects.is_empty() {
                    self.s3_client
                        .delete_objects()
                        .bucket(self.bucket.clone())
                        .delete(
                            Delete::builder()
                                .set_objects(Some(delete_objects))
                                .build()
                                .expect("Failed to build Delete objects"),
                        )
                        .send()
                        .await?;
                }
            }

            continuation_token = response.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(())
    }

    /// List all direct sub-directory under the given directory.
    ///
    /// For example, we have directory "a", "a/b", "a/b/c", listing direct subdirectories for "a" will return "a/b".
    async fn list_direct_subdirectories(
        &self,
        folder: &str,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let mut directories = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut builder = self
                .s3_client
                .list_objects_v2()
                .bucket(self.bucket.clone())
                .prefix(folder)
                .delimiter("/");
            if let Some(token) = continuation_token {
                builder = builder.continuation_token(token);
            }
            let response = builder.send().await?;
            if let Some(prefixes) = response.common_prefixes {
                for prefix in prefixes {
                    if let Some(prefix_str) = prefix.prefix {
                        // Remove the trailing "/" and the initial folder/ prefix (if it exists)
                        let cleaned_prefix = if folder.is_empty() {
                            prefix_str.trim_end_matches('/').to_string()
                        } else {
                            prefix_str
                                .trim_start_matches(folder)
                                .trim_end_matches('/')
                                .to_string()
                        };

                        // Check if the cleaned prefix itself contains a "/", if it does, it is not a direct subdirectory
                        if !cleaned_prefix.contains('/') {
                            directories.push(cleaned_prefix);
                        }
                    }
                }
            }

            continuation_token = response.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(directories)
    }
}

#[async_trait]
impl Catalog for S3Catalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
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
        let segments = namespace_ident.clone().inner();
        let mut segment_vec = vec![];
        for cur_segment in &segments[..segments.len().saturating_sub(1)] {
            segment_vec.push(cur_segment.clone());
            let parent_namespace_ident = NamespaceIdent::from_vec(segment_vec.clone())?;
            let exists = self.namespace_exists(&parent_namespace_ident).await?;
            if !exists {
                return Err(IcebergError::new(
                    iceberg::ErrorKind::NamespaceNotFound,
                    format!(
                        "Parent Namespace {:?} doesn't exists",
                        parent_namespace_ident
                    ),
                ));
            }
        }

        self.s3_client
            .put_object()
            .bucket(self.bucket.clone())
            .key(S3Catalog::get_namespace_indicator_name(namespace_ident))
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
            .object_exists(&S3Catalog::get_namespace_indicator_name(namespace_ident))
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
            .key(S3Catalog::get_namespace_indicator_name(namespace_ident))
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
    ///
    /// TODO(hjiang): We should check namespace existence before list.
    async fn list_tables(
        &self,
        namespace_ident: &NamespaceIdent,
    ) -> IcebergResult<Vec<TableIdent>> {
        let parent_directory = namespace_ident.to_url_string();
        let subdirectories = self
            .list_direct_subdirectories(&parent_directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to list tables: {}", e),
                )
            })?;

        let mut table_idents = Vec::with_capacity(subdirectories.len());
        for cur_subdir in subdirectories.iter() {
            table_idents.push(TableIdent::new(namespace_ident.clone(), cur_subdir.clone()));
        }
        Ok(table_idents)
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        todo!()
    }

    /// Create a new table inside the namespace.
    ///
    /// TODO(hjiang): Confirm the location field inside of `TableCreation`.
    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let directory = namespace_ident.to_url_string();
        let table_ident = TableIdent::new(namespace_ident.clone(), creation.name.clone());

        // Create version hint file.
        let version_hint_filepath =
            format!("{}/{}/metadata/version-hint.text", directory, creation.name);

        self.s3_client
            .put_object()
            .bucket(self.bucket.clone())
            .key(version_hint_filepath)
            .body(ByteStream::from("0".as_bytes().to_vec()))
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to create version hint file on table creation: {}",
                        e
                    ),
                )
            })?;

        // Create metadata file.
        let metadata_filepath = format!(
            "{}/{}/metadata/v0.metadata.json",
            directory,
            creation.name.clone()
        );

        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;
        let metadata_json = serde_json::to_string(&table_metadata.metadata)?;
        self.s3_client
            .put_object()
            .bucket(self.bucket.clone())
            .key(metadata_filepath)
            .body(ByteStream::from(metadata_json.as_bytes().to_vec()))
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to create metadata file on table creation: {}", e),
                )
            })?;

        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(table_ident)
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
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
        let mut version_hint_filepath = PathBuf::from(table.namespace.to_url_string());
        version_hint_filepath.push(table.name());
        version_hint_filepath.push("metadata");
        version_hint_filepath.push("version-hint.text");

        let exists = self
            .object_exists(&version_hint_filepath.to_str().unwrap().to_owned())
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to check object existence: {}", e),
                )
            })?;
        Ok(exists)
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        todo!()
    }

    /// Update a table to the catalog, which writes metadata file and version hint file.
    ///
    /// TODO(hjiang): Implement table requirements, which indicates user-defined compare-and-swap logic.
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, SnapshotReference, SnapshotRetention,
        Type as IcebergType, MAIN_BRANCH,
    };
    use iceberg::NamespaceIdent;
    use iceberg::Result as IcebergResult;

    const TEST_BUCKET: &str = "test-bucket";

    // Create S3 catalog with local minio deployment.
    async fn create_s3_catalog() -> S3Catalog {
        let config = S3CatalogConfig::new(
            /*warehouse_location=*/ "http://minio:9000".to_string(),
            /*access_key_id=*/ "minioadmin".to_string(),
            /*secret_access_key=*/ "minioadmin".to_string(),
            /*region=*/
            "us-west1".to_string(), // doesn't matter for minio local deployment.
            /*bucket=*/ TEST_BUCKET.to_string(),
        );
        let s3_catalog = S3Catalog::new(config);
        s3_catalog.cleanup_bucket().await.unwrap();
        s3_catalog
    }

    // Test util function to get iceberg schema,
    async fn get_test_schema() -> IcebergResult<Schema> {
        let field = NestedField::required(
            /*id=*/ 1,
            "field_name".to_string(),
            IcebergType::Primitive(PrimitiveType::Int),
        );
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(field)])
            .build()?;

        Ok(schema)
    }

    // Test util function to create a new table.
    async fn create_test_table(catalog: &S3Catalog) -> IcebergResult<()> {
        // Define namespace and table.
        let namespace = NamespaceIdent::from_strs(["default"])?;
        let table_name = "test_table".to_string();

        let schema = get_test_schema().await?;
        let table_creation = TableCreation::builder()
            .name(table_name.clone())
            .location(format!("s3://{}/{}", namespace.to_url_string(), table_name))
            .schema(schema.clone())
            .build();

        catalog
            .create_namespace(&namespace, /*properties=*/ HashMap::new())
            .await?;
        catalog.create_table(&namespace, table_creation).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_s3_catalog_namespace_operations() -> IcebergResult<()> {
        let catalog = create_s3_catalog().await;
        let namespace = NamespaceIdent::from_vec(vec!["default".to_string(), "ns".to_string()])?;

        // Ensure namespace does not exist.
        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(!exists, "Namespace should not exist before creation");

        // Create parent namespace.
        catalog
            .create_namespace(
                &NamespaceIdent::from_vec(vec!["default".to_string()]).unwrap(),
                /*properties=*/ HashMap::new(),
            )
            .await?;

        // Create namespace and check.
        catalog
            .create_namespace(&namespace, /*properties=*/ HashMap::new())
            .await?;

        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(exists, "Namespace should exist after creation");

        // Get the namespace and check.
        let ns = catalog.get_namespace(&namespace).await?;
        assert_eq!(ns.name(), &namespace, "Namespace should match created one");

        // Drop the namespace and check.
        catalog.drop_namespace(&namespace).await?;
        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(!exists, "Namespace should not exist after drop");

        Ok(())
    }

    #[tokio::test]
    async fn test_s3_catalog_table_operations() -> IcebergResult<()> {
        let catalog = create_s3_catalog().await;

        // Define namespace and table.
        let namespace = NamespaceIdent::from_vec(vec!["default".to_string()])?;
        let table_name = "test_table".to_string();
        let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

        // Ensure table does not exist.
        let table_already_exists = catalog.table_exists(&table_ident).await?;
        assert!(
            !table_already_exists,
            "Table should not exist before creation"
        );

        // TODO(hjiang): Add testcase to check list table here.

        create_test_table(&catalog).await?;
        let table_already_exists = catalog.table_exists(&table_ident).await?;
        assert!(table_already_exists, "Table should exist after creation");

        Ok(())
    }
}
