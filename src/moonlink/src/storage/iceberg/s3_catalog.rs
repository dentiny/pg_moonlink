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

static NAMESPACE_INDICATOR_OBJECT_NAME: &str = "indicator.text";
static PROVIDER: &str = "s3-catalog-provider";

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
    warehouse_location: String,
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
            warehouse_location: warehouse_location,
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

    /// List all objects under the given directory.
    async fn list_objects_under_directory(
        &self,
        folder: &str,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let mut objects_list = Vec::new();
        let mut continuation_token = None;
        let query_prefix = format!("{folder}/");
        loop {
            let mut builder = self
                .s3_client
                .list_objects_v2()
                .bucket(self.bucket.clone())
                .prefix(&query_prefix);
            if let Some(token) = continuation_token {
                builder = builder.continuation_token(token);
            }
            let response = builder.send().await?;
            if let Some(objects) = response.contents {
                for cur_object in objects {
                    if let Some(object_name) = cur_object.key {
                        objects_list.push(object_name.clone());
                    }
                }
            }

            continuation_token = response.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(objects_list)
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
        let query_prefix = format!("{folder}/");
        loop {
            let mut builder = self
                .s3_client
                .list_objects_v2()
                .bucket(self.bucket.clone())
                .prefix(&query_prefix)
                .delimiter("/");
            if let Some(token) = continuation_token {
                builder = builder.continuation_token(token);
            }
            let response = builder.send().await?;
            if let Some(prefixes) = response.common_prefixes {
                for prefix in prefixes {
                    if let Some(cur_prefix) = prefix.prefix {
                        // Only care about subdirectory, not objects.
                        //
                        // Example query:
                        // Suppose we have namespace "default" and table "test_table", cur prefix returned "default/test_table/".
                        // Listing subdirectories need to strip query prefix and slash suffix, which is "test_table" in this case.
                        if !cur_prefix.ends_with("/") {
                            continue;
                        }
                        directories.push(
                            cur_prefix
                                .strip_suffix('/')
                                .unwrap()
                                .strip_prefix(&query_prefix)
                                .unwrap()
                                .to_string(),
                        );
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

    /// Read the whole content for the given object.
    /// Notice, it's not suitable to read large files; as of now it's made for metadata files.
    async fn read_object(&self, object: &str) -> Result<String, Box<dyn Error>> {
        let response = self
            .s3_client
            .get_object()
            .bucket(self.bucket.clone())
            .key(object)
            .send()
            .await;
        let bytes = response.unwrap().body.collect().await?;
        let content = String::from_utf8(bytes.to_vec())?;
        Ok(content)
    }

    /// Write the whole content to the given file.
    async fn write_object(
        &self,
        object_filepath: &str,
        content: &str,
    ) -> Result<(), Box<dyn Error>> {
        self.s3_client
            .put_object()
            .bucket(self.bucket.clone())
            .key(object_filepath)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write content to s3: {}", e),
                )
            })?;
        Ok(())
    }

    /// Delete all given objects.
    /// TODO(hjiang): Add test and documentation on partial failure.
    async fn delete_all_objects(
        &self,
        objects_to_delete: Vec<String>,
    ) -> Result<(), Box<dyn Error>> {
        let objects_to_delete: Vec<ObjectIdentifier> = objects_to_delete
            .into_iter()
            .map(|key| {
                ObjectIdentifier::builder()
                    .key(key)
                    .build()
                    .expect("Failed to build ObjectIdentifier")
            })
            .collect();

        self.s3_client
            .delete_objects()
            .bucket(self.bucket.clone())
            .delete(
                Delete::builder()
                    .set_objects(Some(objects_to_delete))
                    .build()
                    .expect("Failed to build Delete objects"),
            )
            .send()
            .await?;

        Ok(())
    }

    /// Load metadata and its location foe the given table.
    async fn load_metadata(
        &self,
        table_ident: &TableIdent,
    ) -> IcebergResult<(String /*metadata_filepath*/, TableMetadata)> {
        // Read version hint for the table to get latest version.
        let version_hint_filepath = format!(
            "{}/{}/metadata/version-hint.text",
            table_ident.namespace().to_url_string(),
            table_ident.name(),
        );
        let version_str = self
            .read_object(&version_hint_filepath)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read version hint file on load table: {}", e),
                )
            })?;
        let version = version_str
            .trim()
            .parse::<u32>()
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        // Read and parse table metadata.
        let metadata_filepath = format!(
            "{}/{}/metadata/v{}.metadata.json",
            table_ident.namespace().to_url_string(),
            table_ident.name(),
            version,
        );
        let metadata_str = self.read_object(&metadata_filepath).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to read table metadata file on load table: {}", e),
            )
        })?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_str.as_bytes())
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        Ok((metadata_filepath, metadata))
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
        self.write_object(
            &S3Catalog::get_namespace_indicator_name(namespace_ident),
            /*content=*/ "",
        )
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to write metadata file at namespace creation: {}", e),
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

        let mut table_idents: Vec<TableIdent> = Vec::with_capacity(subdirectories.len());
        for cur_subdir in subdirectories.iter() {
            let cur_table_ident = TableIdent::new(namespace_ident.clone(), cur_subdir.clone());
            let exists = self.table_exists(&cur_table_ident).await?;
            if exists {
                table_idents.push(cur_table_ident);
            }
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
        self.write_object(&version_hint_filepath, /*content=*/ "0")
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write version hint file at table creation: {}", e),
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
        self.write_object(&metadata_filepath, /*content=*/ &metadata_json)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write metadata file at namespace creation: {}", e),
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
    async fn load_table(&self, table_ident: &TableIdent) -> IcebergResult<Table> {
        let (metadata_filepath, metadata) = self.load_metadata(&table_ident).await?;

        // Build and return the table.
        let metadata_path = format!(
            "{}/{}/{}",
            self.warehouse_location, self.bucket, metadata_filepath
        );
        let table = Table::builder()
            .metadata_location(metadata_path)
            .metadata(metadata)
            .identifier(table_ident.clone())
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let directory = format!("{}/{}", table.namespace().to_url_string(), table.name());
        let objects = self
            .list_objects_under_directory(&directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to get objects under {}: {}", directory, e),
                )
            })?;
        self.delete_all_objects(objects).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to delete objects: {}", e),
            )
        })?;
        Ok(())
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
        let (metadata_filepath, metadata) = self.load_metadata(commit.identifier()).await?;
        let version = metadata.next_sequence_number();
        let mut builder = TableMetadataBuilder::new_from_metadata(
            metadata.clone(),
            /*current_file_location=*/ Some(metadata_filepath.clone()),
        );

        // TODO(hjiang): As of now, we only take one AddSnapshot update.
        let updates = commit.take_updates();
        for update in &updates {
            match update {
                TableUpdate::AddSnapshot { snapshot } => {
                    builder = builder.add_snapshot(snapshot.clone())?;
                }
                TableUpdate::SetSnapshotRef {
                    ref_name,
                    reference,
                } => {
                    builder = builder.set_ref(ref_name, reference.clone())?;
                }
                _ => {
                    unreachable!("Only snapshot updates are expected in this implementation");
                }
            }
        }

        // Construct new metadata with updates.
        let metadata = builder.build()?.metadata;

        // Write metadata file.
        let metadata_directory = format!(
            "{}/{}/metadata",
            commit.identifier().namespace().to_url_string(),
            commit.identifier().name()
        );
        let new_metadata_filepath = format!("{}/v{}.metadata.json", metadata_directory, version,);
        let metadata_json = serde_json::to_string(&metadata)?;
        self.write_object(&new_metadata_filepath, &metadata_json)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write metadata file at table update: {}", e),
                )
            })?;

        // Write version hint file.
        let version_hint_path = format!("{}/version-hint.text", metadata_directory);
        self.write_object(&version_hint_path, &format!("{version}"))
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write version hint file at table update: {}", e),
                )
            })?;

        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(self.file_io.clone())
            .metadata(metadata)
            .metadata_location(metadata_filepath)
            .build()
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

        let tables = catalog.list_tables(&namespace).await?;
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&table_ident));

        // Load table and check.
        let table = catalog.load_table(&table_ident).await?;
        let expected_schema = get_test_schema().await?;
        assert_eq!(
            table.identifier(),
            &table_ident,
            "Loaded table identifier should match"
        );
        assert_eq!(
            *table.metadata().current_schema().as_ref(),
            expected_schema,
            "Loaded table schema should match"
        );

        // Drop the table and check.
        catalog.drop_table(&table_ident).await?;
        let table_already_exists = catalog.table_exists(&table_ident).await?;
        assert!(!table_already_exists, "Table should not exist after drop");

        Ok(())
    }

    #[tokio::test]
    async fn test_update_table() -> IcebergResult<()> {
        let catalog = create_s3_catalog().await;
        create_test_table(&catalog).await?;

        let namespace = NamespaceIdent::from_strs(["default"])?;
        let table_name = "test_table".to_string();
        let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
        catalog.load_metadata(&table_ident).await?;

        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut table_updates = vec![];
        table_updates.append(&mut vec![
            TableUpdate::AddSnapshot {
                snapshot: iceberg::spec::Snapshot::builder()
                    .with_snapshot_id(1)
                    .with_sequence_number(1)
                    .with_timestamp_ms(millis as i64)
                    .with_schema_id(0)
                    .with_manifest_list(format!(
                        "s3://{}/{}/snap-8161620281254644995-0-01966b87-6e93-7bc1-9e12-f1980d9737d3.avro",
                        namespace.to_url_string(),
                        table_name
                    ))
                    .with_parent_snapshot_id(None)
                    .with_summary(iceberg::spec::Summary {
                        operation: iceberg::spec::Operation::Append,
                        additional_properties: HashMap::new(),
                    })
                    .build(),
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            }
        ]);

        // TODO(hjiang): This is a hack to create `TableCommit`, because its builder is only exposed to crate instead of public.
        #[repr(C)]
        struct TableCommitProxy {
            ident: TableIdent,
            requirements: Vec<iceberg::TableRequirement>,
            updates: Vec<TableUpdate>,
        }
        let table_commit_proxy = TableCommitProxy {
            ident: table_ident.clone(),
            requirements: vec![],
            updates: table_updates,
        };
        let table_commit =
            unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(table_commit_proxy) };

        // Check table metadata.
        let table = catalog.update_table(table_commit).await?;
        let table_metadata = table.metadata();
        assert_eq!(
            **table_metadata.current_schema(),
            get_test_schema().await.unwrap(),
            "Schema should match"
        );
        assert_eq!(
            table.identifier(),
            &table_ident,
            "Updated table identifier should match"
        );
        assert_eq!(
            table_metadata.current_snapshot_id(),
            Some(1),
            "Current snapshot ID should be 1"
        );

        Ok(())
    }
}
