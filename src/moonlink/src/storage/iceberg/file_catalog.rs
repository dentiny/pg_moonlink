use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableUpdate,
};

/// This module contains the filesystem catalog implementation, which serves for local development and hermetic unit test purpose;
/// For initial versions, it's focusing more on simplicity and correctness rather than performance.
/// Different with `MemoryCatalog`, `FileSystemCatalog` could be used in production environment.
///
/// TODO(hjiang):
/// 1. Implement property related functionalities.
/// 2. The initial version access everything via filesystem, for performance consideration we should cache metadata in memory.
/// 3. (not related to functionality) Set snapshot retention policy at metadata.
///
/// Iceberg table format from filesystem's perspective:
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
/// For filesystem catalog, all files are stored under the warehouse location, in detail: <warehouse-location>/<namespace>/<table-name>/.

#[derive(Debug)]
pub struct FileSystemCatalog {
    file_io: FileIO,
    warehouse_location: String,
}

impl FileSystemCatalog {
    /// Creates a rest catalog from config, if the given warehouse location doesn't exist, it will be created.
    pub fn new(warehouse_location: String) -> Self {
        std::fs::create_dir_all(warehouse_location.clone()).unwrap();
        Self {
            file_io: FileIO::from_path(warehouse_location.clone())
                .unwrap()
                .build()
                .unwrap(),
            warehouse_location,
        }
    }

    /// Load metadata for the given table.
    async fn load_metadata(
        &self,
        table_ident: &TableIdent,
    ) -> IcebergResult<(String /*metadata_file_path*/, TableMetadata)> {
        let metadata_directory = format!(
            "{}/{}/{}/metadata",
            self.warehouse_location,
            table_ident.namespace.to_url_string(),
            table_ident.name()
        );

        // Get metadata file path via version hint file.
        let version_hint_path = format!("{}/version-hint.text", metadata_directory);
        let input_file: iceberg::io::InputFile = self.file_io.new_input(&version_hint_path)?;
        let version = String::from_utf8(input_file.read().await?.to_vec()).expect("");

        // Get table metadata.
        let metadata_file_path = format!("{}/v{}.metadata.json", metadata_directory, version);
        let input_file: iceberg::io::InputFile = self.file_io.new_input(&metadata_file_path)?;
        let metadata_content = input_file.read().await?.to_vec();
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_content)?;
        Ok((metadata_file_path, metadata))
    }

    /// Convert namespace identifier to filesystem paths.
    fn get_namespace_path(&self, namespace: Option<&NamespaceIdent>) -> PathBuf {
        let mut path = PathBuf::from(&self.warehouse_location);
        if namespace.is_none() {
            return path;
        }
        for part in namespace.unwrap().as_ref() {
            path.push(part);
        }
        path
    }
}

#[async_trait]
impl Catalog for FileSystemCatalog {
    /// List namespaces under the parent namespace, return error if parent namespace doesn't exist.
    /// It's worth noting only one layer of namespace will be returned.
    /// For example, suppose we create three namespaces: (1) "a", (2) "a/b", (3) "b".
    /// List all namespaces under root namespace will return "a" and "b", but not "a/b".
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        // Check parent namespace existence.
        let base_path = self.get_namespace_path(parent);
        if !base_path.exists() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!("Parent namespace {:?} does not exist", parent),
            ));
        }

        // Iterate all directories under the parent directory.
        let mut namespaces = vec![];
        for entry in std::fs::read_dir(&base_path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to read directory {:?}: {}", base_path, e),
            )
        })? {
            let entry = entry.map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read entry: {}", e),
                )
            })?;

            let path = entry.path();
            if path.is_dir() {
                let name = entry.file_name().into_string().map_err(|_| {
                    IcebergError::new(
                        iceberg::ErrorKind::Unexpected,
                        "Failed to parse directory name as UTF-8",
                    )
                })?;
                namespaces.push(NamespaceIdent::new(name));
            }
        }

        Ok(namespaces)
    }

    /// Create a new namespace inside the catalog, return error if namespace already exists, or any parent namespace doesn't exist.
    ///
    /// TODO(hjiang): Implement properties handling.
    async fn create_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        // Check whether the namespace already exists.
        let path = self.get_namespace_path(Some(namespace));
        if path.exists() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceAlreadyExists,
                format!("Namespace {:?} already exists", namespace),
            ));
        }

        // If parent directory doesn't exist, new directory creation will fail.
        std::fs::create_dir(&path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to create namespace directory {}: {}",
                    namespace.to_url_string(),
                    e
                ),
            )
        })?;

        // Return the created namespace.
        Ok(Namespace::new(namespace.clone()))
    }

    /// Get a namespace information from the catalog, return error if requested namespace doesn't exist.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        let path = self.get_namespace_path(Some(namespace));
        if path.is_dir() {
            Ok(Namespace::new(namespace.clone()))
        } else {
            Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!("Namespace {:?} does not exist", namespace),
            ))
        }
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        let path = self.get_namespace_path(Some(namespace));
        Ok(path.is_dir())
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        let path = self.get_namespace_path(Some(namespace));
        if !path.exists() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!("Namespace {:?} does not exist", namespace),
            ));
        }

        std::fs::remove_dir_all(&path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("failed to remove namespace directory: {}", e),
            )
        })?;
        Ok(())
    }

    /// List tables from namespace, return error if the given namespace doesn't exist.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        // Check if the given namespace exists.
        let namespace_path = self.get_namespace_path(Some(namespace));
        if !namespace_path.is_dir() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!("Namespace {:?} does not exist", namespace),
            ));
        }

        // Read the namespace directory.
        let mut tables = Vec::new();
        for entry in std::fs::read_dir(&namespace_path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to read namespace directory: {}", e),
            )
        })? {
            let entry = entry.map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read directory entry: {}", e),
                )
            })?;

            let path = entry.path();
            if path.is_dir() {
                let table_name = entry.file_name().into_string().map_err(|_| {
                    IcebergError::new(
                        iceberg::ErrorKind::Unexpected,
                        "Failed to parse table name as UTF-8",
                    )
                })?;

                // Check if this is a valid table (has metadata directory).
                let metadata_path = path.join("metadata");
                if metadata_path.is_dir() {
                    tables.push(TableIdent::new(namespace.clone(), table_name));
                }
            }
        }

        Ok(tables)
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
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());
        // TODO(hjiang): Confirm the location field inside of `TableCreation`.
        let warehouse_location = self.warehouse_location.clone();
        let metadata_path = format!(
            "{}/{}/{}/metadata",
            warehouse_location,
            namespace.to_url_string(),
            creation.name,
        );
        let version_hint_path = format!(
            "{}/{}/{}/metadata/version-hint.text",
            warehouse_location,
            namespace.to_url_string(),
            creation.name
        );

        // Create the initial table metadata.
        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;
        // Write the initial metadata file.
        let metadata_file_path = format!("{metadata_path}/v0.metadata.json");
        let metadata_json = serde_json::to_string(&table_metadata.metadata)?;
        let output = self.file_io.new_output(&metadata_file_path)?;
        output.write(metadata_json.into()).await?;

        // Write the version hint file.
        let version_hint_output = self.file_io.new_output(&version_hint_path)?;
        version_hint_output.write("0".into()).await?;

        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(table_ident)
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        // Construct the path to the version hint file
        let version_hint_path = format!(
            "{}/{}/{}{}",
            self.warehouse_location,
            table.namespace().to_url_string(),
            table.name(),
            "/metadata/version-hint.text"
        );

        // Read the version hint to get the latest metadata version
        let version_hint_input = self.file_io.new_input(&version_hint_path)?;
        let version_bytes = version_hint_input.read().await?;
        let version_str = std::str::from_utf8(&version_bytes)
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
        let version = version_str
            .trim()
            .parse::<u32>()
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        // Construct the path to the metadata file
        let metadata_path = format!(
            "{}/{}/{}/metadata/v{}.metadata.json",
            self.warehouse_location,
            table.namespace().to_url_string(),
            table.name(),
            version
        );

        // Read and parse table metadata.
        let input_file = self.file_io.new_input(&metadata_path)?;
        let metadata_content = input_file.read().await?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_content)
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        // Build and return the table
        let table = Table::builder()
            .metadata_location(metadata_path)
            .metadata(metadata)
            .identifier(table.clone())
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        // Construct the path to the table directory
        let table_path = format!(
            "{}/{}/{}",
            self.warehouse_location,
            table.namespace().to_url_string(),
            table.name()
        );

        // Check if path exists first.
        let path = std::path::PathBuf::from(&table_path);
        if !path.exists() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!("Table path does not exist: {}", table_path),
            ));
        }

        // Remove the directory and all its contents
        std::fs::remove_dir_all(&path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to delete table directory: {}", e),
            )
        })?;

        Ok(())
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let mut metadata_path = PathBuf::from(&self.warehouse_location);
        for part in table.namespace().as_ref() {
            metadata_path.push(part);
        }
        metadata_path.push(table.name());
        metadata_path.push("metadata");

        // Check if directory exists
        if !metadata_path.is_dir() {
            return Ok(false);
        }

        // Check if at least one .metadata.json file exists.
        let exists = std::fs::read_dir(&metadata_path)
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read metadata dir: {}", e),
                )
            })?
            .any(|entry| {
                if let Ok(entry) = entry {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    name.ends_with(".metadata.json")
                } else {
                    false
                }
            });

        Ok(exists)
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        // Construct source and destination base paths.
        let src_directory = format!(
            "{}/{}/{}",
            self.warehouse_location,
            src.namespace().to_url_string(),
            src.name()
        );
        let dst_directory = format!(
            "{}/{}/{}",
            self.warehouse_location,
            dest.namespace().to_url_string(),
            dest.name()
        );
        let src_path = PathBuf::from(&src_directory);
        let dst_path = PathBuf::from(&dst_directory);

        // Check if source table exists (look for metadata directory).
        let src_metadata = src_path.join("metadata");
        if !src_metadata.exists() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::TableNotFound,
                format!("Source table does not exist: {:?}", src),
            ));
        }

        // Check if destination already exists.
        if dst_path.exists() {
            return Err(IcebergError::new(
                iceberg::ErrorKind::TableAlreadyExists,
                format!("Destination table already exists: {:?}", dest),
            ));
        }

        // Create parent namespace directories if they don't exist.
        if let Some(parent) = dst_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to create destination namespace directory: {}", e),
                )
            })?;
        }

        // Move all files under
        std::fs::rename(&src_path, &dst_path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to rename table directory from {:?} to {:?}: {}",
                    src, dest, e
                ),
            )
        })?;

        Ok(())
    }

    /// Update a table to the catalog, which writes metadata file and version hint file.
    ///
    /// TODO(hjiang): Implement table requirements, which indicates user-defined compare-and-swap logic.
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        let (metadata_file_path, metadata) = self.load_metadata(commit.identifier()).await?;
        let version = metadata.next_sequence_number();
        let mut builder = TableMetadataBuilder::new_from_metadata(
            metadata.clone(),
            /*current_file_location=*/ Some(metadata_file_path.clone()),
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
            "{}/{}/{}/metadata",
            self.warehouse_location,
            commit.identifier().namespace().to_url_string(),
            commit.identifier().name()
        );
        let metadata_file_path = format!("{}/v{}.metadata.json", metadata_directory, version);
        let metadata_json = serde_json::to_string(&metadata)?;
        let output = self.file_io.new_output(&metadata_file_path)?;
        output.write(metadata_json.into()).await?;

        // Write version hint file.
        let version_hint_path = format!("{}/version-hint.text", metadata_directory);
        let version_hint_output = self.file_io.new_output(&version_hint_path)?;
        version_hint_output
            .write(format!("{version}").into())
            .await?;

        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(self.file_io.clone())
            .metadata(metadata)
            .metadata_location(metadata_file_path)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, SnapshotReference, SnapshotRetention,
        Type as IcebergType, MAIN_BRANCH,
    };
    use iceberg::NamespaceIdent;
    use iceberg::Result as IcebergResult;

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
    async fn create_test_table(catalog: &FileSystemCatalog) -> IcebergResult<()> {
        // Define namespace and table.
        let namespace = NamespaceIdent::from_strs(["default"])?;
        let table_name = "test_table".to_string();

        let schema = get_test_schema().await?;
        let table_creation = TableCreation::builder()
            .name(table_name.clone())
            .location(format!(
                "file:///tmp/iceberg-test/{}/{}",
                namespace.to_url_string(),
                table_name
            ))
            .schema(schema.clone())
            .build();

        catalog
            .create_namespace(&namespace, /*properties=*/ HashMap::new())
            .await?;
        catalog.create_table(&namespace, table_creation).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_filesystem_catalog_create_namespace() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileSystemCatalog::new(warehouse_path.to_string());
        let namespace = NamespaceIdent::from_strs(["default", "ns"])?;

        // Namespace creation fails because parent namespace doesn't exist.
        let result = catalog
            .create_namespace(&namespace, /*properties=*/ HashMap::new())
            .await;
        let err = result.unwrap_err();
        assert_eq!(err.kind(), iceberg::ErrorKind::Unexpected);

        Ok(())
    }

    #[tokio::test]
    async fn test_filesystem_catalog_drop_namespace() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileSystemCatalog::new(warehouse_path.to_string());
        let parent_namespace = NamespaceIdent::from_strs(["default"])?;
        let child_namespace = NamespaceIdent::from_strs(["default", "ns"])?;

        catalog
            .create_namespace(&parent_namespace, /*properties=*/ HashMap::new())
            .await?;
        catalog
            .create_namespace(&child_namespace, /*properties=*/ HashMap::new())
            .await?;

        // Drop parent namespace indicates drop the child namespace(s) as well.
        catalog.drop_namespace(&parent_namespace).await?;
        let exists = catalog.namespace_exists(&child_namespace).await?;
        assert!(
            !exists,
            "Child namespace should not exist after dropping parent namespace"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filesystem_catalog_namespace_operations() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileSystemCatalog::new(warehouse_path.to_string());
        let namespace = NamespaceIdent::from_strs(["default", "ns"])?;

        // List namespace before creation.
        let namespaces = catalog.list_namespaces(/*parent=*/ None).await?;
        assert!(
            namespaces.is_empty(),
            "No namespaces should exist before creation"
        );

        // Ensure namespace does not exist.
        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(!exists, "Namespace should not exist before creation");

        // Create parent namespace.
        catalog
            .create_namespace(
                &NamespaceIdent::from_strs(["default"]).unwrap(),
                /*properties=*/ HashMap::new(),
            )
            .await?;
        // Create namespace and check.
        catalog
            .create_namespace(&namespace, /*properties=*/ HashMap::new())
            .await?;
        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(exists, "Namespace should exist after creation");

        // List all existing namespaces.
        let namespaces = catalog
            .list_namespaces(
                /*parent=*/ Some(&NamespaceIdent::from_strs(["default"]).unwrap()),
            )
            .await?;
        assert_eq!(namespaces.len(), 1, "There should be one root namespace");
        assert_eq!(
            namespaces[0],
            NamespaceIdent::from_strs(["ns"]).unwrap(),
            "Namespace should match created one"
        );

        let namespaces = catalog.list_namespaces(/*parent=*/ None).await?;
        assert_eq!(
            namespaces.len(),
            1,
            "There should be one sub-namespace under root namespace"
        );
        assert_eq!(
            namespaces[0],
            NamespaceIdent::from_strs(["default"]).unwrap(),
            "Namespace should match created one"
        );

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
    async fn test_filesystem_catalog_table_operations() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileSystemCatalog::new(warehouse_path.to_string());

        // Define namespace and table.
        let namespace = NamespaceIdent::from_strs(["default"])?;
        let table_name = "test_table".to_string();
        let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

        // Ensure table does not exist.
        let table_exists = catalog.table_exists(&table_ident).await?;
        assert!(!table_exists, "Table should not exist before creation");
        let results = catalog.list_tables(&namespace).await;
        let err = results.unwrap_err();
        assert_eq!(err.kind(), iceberg::ErrorKind::NamespaceNotFound);

        create_test_table(&catalog).await?;
        let table_exists = catalog.table_exists(&table_ident).await?;
        assert!(table_exists, "Table should exist after creation");

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

        // Rename table and check.
        let old_table_ident = table_ident;
        let new_table_ident = TableIdent::new(namespace.clone(), "new_test_table".to_string());
        catalog
            .rename_table(
                /*src=*/ &old_table_ident,
                /*dest=*/ &new_table_ident,
            )
            .await?;
        let table_exists = catalog.table_exists(&new_table_ident).await?;
        assert!(table_exists, "Table should exist after rename");
        let table_exists = catalog.table_exists(&old_table_ident).await?;
        assert!(!table_exists, "Old table should not exist after rename");

        let tables = catalog.list_tables(&namespace).await?;
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&new_table_ident));
        assert!(!tables.contains(&old_table_ident));

        // Drop the table and check.
        catalog.drop_table(&new_table_ident).await?;
        let table_exists = catalog.table_exists(&new_table_ident).await?;
        assert!(!table_exists, "Table should not exist after drop");

        Ok(())
    }

    #[tokio::test]
    async fn test_update_table() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileSystemCatalog::new(warehouse_path.to_string());
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
                        "file:///tmp/iceberg-test/{}/{}/snap-8161620281254644995-0-01966b87-6e93-7bc1-9e12-f1980d9737d3.avro",
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
