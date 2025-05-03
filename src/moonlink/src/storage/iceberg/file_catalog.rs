use super::puffin_writer_proxy::append_puffin_metadata_and_rewrite;
use crate::storage::iceberg::puffin_writer_proxy::{
    get_puffin_metadata_and_close, PuffinBlobMetadataProxy,
};

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableUpdate,
};
use opendal::services::Fs;
use opendal::ErrorKind as OpendalErrorKind;
use opendal::{EntryMode, Operator};

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
//
// Get parent directory name with "/" suffixed.
// If the given string represents root directory ("/"), return "/" as well.
fn get_parent_directory(directory: &str) -> String {
    assert!(!directory.is_empty());
    let parent = Path::new(directory)
        .parent()
        .and_then(|p| p.to_str())
        .unwrap_or("/");
    if parent == "/" {
        return parent.to_string();
    }
    format!("{}/", parent)
}

// Normalize directory string to makes sure it ends with "/", which is the requirment for opendal.
fn normalize_directory(mut path: PathBuf) -> String {
    let mut os_string = path.as_mut_os_str().to_os_string();
    if os_string.to_str().unwrap().ends_with("/") {
        return os_string.to_str().unwrap().to_string();
    }
    os_string.push("/");
    os_string.to_str().unwrap().to_string()
}

#[derive(Debug)]
pub struct FileSystemCatalog {
    file_io: FileIO,
    warehouse_location: String,
    op: Operator,
    // Used to record puffin blob metadata.
    // Maps from "puffin filepath" to "puffin blob metadata".
    puffin_blobs: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
}

impl FileSystemCatalog {
    /// Creates a rest catalog from config, if the given warehouse location doesn't exist, it will be created.
    pub fn new(warehouse_location: String) -> Self {
        // opendal prepends root directory to all paths for all operations, here we do path manipulation ourselves and set root directory as filesystem root.
        let builder = Fs::default().root("/");
        let op = Operator::new(builder).unwrap().finish();
        futures::executor::block_on(
            op.create_dir(&normalize_directory(PathBuf::from(&warehouse_location))),
        )
        .unwrap();
        Self {
            file_io: FileIO::from_path(warehouse_location.clone())
                .unwrap()
                .build()
                .unwrap(),
            warehouse_location,
            op,
            puffin_blobs: HashMap::new(),
        }
    }

    // Get puffin metadata from the writer, and close it.
    //
    // TODO(hjiang): iceberg-rust currently doesn't support puffin write, to workaround and reduce code change,
    // we record puffin metadata ourselves and rewrite manifest file before transaction commits.
    pub(crate) async fn record_puffin_metadata_and_close(
        &mut self,
        puffin_filepath: String,
        puffin_writer: PuffinWriter,
    ) -> IcebergResult<()> {
        self.puffin_blobs.insert(
            puffin_filepath,
            get_puffin_metadata_and_close(puffin_writer).await?,
        );
        Ok(())
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
        namespace: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        // Check whether the namespace already exists.
        let path_buf = self.get_namespace_path(Some(namespace));
        let normalized_directory = normalize_directory(path_buf.clone());
        let exists = self.op.exists(&normalized_directory).await?;
        if exists {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceAlreadyExists,
                format!("Namespace {:?} already exists", namespace),
            ));
        }

        // Different with filesystem's behavior, opendal `create_dir` creates directories recursively.
        // To create the given namespace, check its the parent directory first.
        let parent_directory = get_parent_directory(&normalized_directory);
        let exists = self.op.exists(&parent_directory).await?;
        if !exists {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!(
                    "Parent namespace for the given one {:?} already exists",
                    namespace
                ),
            ));
        }

        // If parent directory doesn't exist, new directory creation will fail.
        self.op
            .create_dir(&normalized_directory)
            .await
            .map_err(|e| {
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
    async fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<Namespace> {
        let exists = self.namespace_exists(namespace_ident).await?;
        if !exists {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!("Namespace {:?} doesn't exist", namespace_ident),
            ));
        }
        Ok(Namespace::new(namespace_ident.clone()))
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<bool> {
        let path_buf = self.get_namespace_path(Some(namespace_ident));
        let entry_metadata = self.op.stat(&normalize_directory(path_buf.clone())).await;

        // Fails to stat entry metadata, check whether error type is NotFound or other IO errors.
        if entry_metadata.is_err() {
            let stats_error = entry_metadata.unwrap_err();
            if stats_error.kind() == OpendalErrorKind::NotFound {
                return Ok(false);
            }
            return Err(IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to stat directory {:?} indicated by namespace {:?}: {:?}",
                    path_buf, namespace_ident, stats_error
                ),
            ));
        }

        // Check whether filesystem entry is of directory type.
        let entry_metadata = entry_metadata.unwrap();
        if entry_metadata.mode() == EntryMode::DIR {
            return Ok(true);
        }
        return Err(IcebergError::new(
            iceberg::ErrorKind::DataInvalid,
            format!(
                "Namespace directory {:?} indicated by {:?} is not directory at local filesystem",
                path_buf, namespace_ident
            ),
        ));
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<()> {
        // Construct a `PathBuf` doesn't lead to IO operation, while `PathBuf::exists` does.
        let path_buf = self.get_namespace_path(Some(namespace_ident));
        let path = path_buf.to_str().unwrap();
        let entry_metadata = self.op.stat(&normalize_directory(path_buf.clone())).await?;
        if entry_metadata.mode() != EntryMode::DIR {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!("Namespace {:?} doesn't exist", namespace_ident),
            ));
        }

        self.op.remove_all(path).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("failed to remove namespace directory {}: {}", path, e),
            )
        })?;
        Ok(())
    }

    /// List tables from namespace, return error if the given namespace doesn't exist.
    async fn list_tables(
        &self,
        namespace_ident: &NamespaceIdent,
    ) -> IcebergResult<Vec<TableIdent>> {
        // Check if the given namespace exists.
        let exists = self.namespace_exists(namespace_ident).await?;
        if !exists {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!(
                    "Namespace {:?} doesn't exist when list tables within.",
                    namespace_ident
                ),
            ));
        }

        // Read the namespace directory.
        let mut tables = Vec::new();
        let path_buf = self.get_namespace_path(Some(namespace_ident));
        let entries = self.op.list(&normalize_directory(path_buf.clone())).await?;

        // opendal list on prefix, and returns namespace as well, skip here.
        let subfolders = namespace_ident.to_vec();
        // Subfolder indicated by namespace identity without slash suffix.
        let subfolder_to_exclude = subfolders.last().unwrap();

        for cur_entry in entries.iter() {
            if cur_entry.metadata().mode() != EntryMode::DIR {
                continue;
            }
            // Two things to notice here:
            // 1. Entries returned from opendal list operation are suffixed with "/".
            // 2. opendal list operation returns parent directory as well.
            let stripped_subfolder = cur_entry.name().strip_suffix("/").unwrap();
            if stripped_subfolder == subfolder_to_exclude {
                continue;
            }
            let table_ident =
                TableIdent::new(namespace_ident.clone(), stripped_subfolder.to_string());
            tables.push(table_ident);
        }

        Ok(tables)
    }

    async fn update_namespace(
        &self,
        _namespace_ident: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        todo!()
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let table_ident = TableIdent::new(namespace_ident.clone(), creation.name.clone());
        let warehouse_location = self.warehouse_location.clone();
        let metadata_directory = format!(
            "{}/{}/{}/metadata",
            warehouse_location,
            namespace_ident.to_url_string(),
            creation.name,
        );
        let version_hint_path = format!("{}/version-hint.text", metadata_directory,);

        // Create the initial table metadata.
        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;

        // Write the initial metadata file.
        let metadata_file_path = format!("{metadata_directory}/v0.metadata.json");
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
    async fn load_table(&self, table_ident: &TableIdent) -> IcebergResult<Table> {
        // Construct the path to the version hint file
        let metadata_directory = format!(
            "{}/{}/{}/metadata",
            self.warehouse_location,
            table_ident.namespace().to_url_string(),
            table_ident.name(),
        );
        let version_hint_path = format!("{}/version-hint.text", metadata_directory,);

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
        let metadata_path = format!("{}/v{}.metadata.json", metadata_directory, version,);

        // Read and parse table metadata.
        let input_file = self.file_io.new_input(&metadata_path)?;
        let metadata_content = input_file.read().await?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_content)
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        // Build and return the table
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
        // Construct the path to the table directory.
        let table_directory = format!(
            "{}/{}/{}",
            self.warehouse_location,
            table.namespace().to_url_string(),
            table.name()
        );
        let table_metadata_filepath = format!("{}/metadata/version-hint.text", table_directory);

        // Check if path exists first.
        let file_metadata = self.op.stat(&table_metadata_filepath).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Table version hint does not exist {}: {:?}",
                    table_metadata_filepath, e
                ),
            )
        })?;

        if file_metadata.mode() != EntryMode::FILE {
            return Err(IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Table version hint file {} doesn't exist.",
                    table_metadata_filepath
                ),
            ));
        }

        // Remove the directory and all its contents.
        self.op.remove_all(&table_directory).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "failed to remove namespace directory {}: {}",
                    table_directory, e
                ),
            )
        })?;

        Ok(())
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table_ident: &TableIdent) -> IcebergResult<bool> {
        let mut metadata_directory = self.get_namespace_path(Some(table_ident.namespace()));
        metadata_directory.push(table_ident.name());
        metadata_directory.push("metadata");

        let metadata_directory_stats = self
            .op
            .stat(&normalize_directory(metadata_directory.clone()))
            .await;
        if metadata_directory_stats.is_err() {
            let stats_error = metadata_directory_stats.unwrap_err();
            if stats_error.kind() == OpendalErrorKind::NotFound {
                return Ok(false);
            }
            return Err(IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to stat metadata directory {:?} indicated by table {:?}: {:?}",
                    metadata_directory, table_ident, stats_error
                ),
            ));
        }

        // Check if version hint file exists.
        let mut version_hint_file = metadata_directory;
        version_hint_file.push("version-hint.text");
        let version_hint_stats = self.op.stat(version_hint_file.to_str().unwrap()).await;
        if version_hint_stats.is_err() {
            let stats_error = version_hint_stats.unwrap_err();
            if stats_error.kind() == OpendalErrorKind::NotFound {
                // When create the table, it's possible to succeed on directory creation, but fail on version hint file persistence.
                return Ok(false);
            }
            return Err(IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to stat metadata file {:?} indicated by table {:?}: {:?}",
                    version_hint_file, table_ident, stats_error
                ),
            ));
        }

        let version_hint_stats = version_hint_stats.unwrap();
        if version_hint_stats.mode() != EntryMode::FILE {
            return Err(IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "Metadata file {:?} is supposed to be of file, but actually is of type {:?}",
                    version_hint_file,
                    version_hint_stats.mode()
                ),
            ));
        }

        Ok(true)
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        todo!()
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

        // Manifest files and manifest list has persisted into storage, update metadata and persist.
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

        // Manifest files and manifest list has persisted into storage, make modifications based on puffin blobs.
        //
        // TODO(hjiang): Add unit test for update and check manifest population.
        for (puffin_filepath, puffin_blob_metadata) in self.puffin_blobs.iter() {
            append_puffin_metadata_and_rewrite(
                &metadata,
                &self.file_io,
                puffin_filepath,
                puffin_blob_metadata.clone(),
            )
            .await?;
        }

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

    #[test]
    fn test_get_parent_directory() {
        // Root directory.
        assert_eq!(get_parent_directory("/"), "/");
        // Directory without slash suffix.
        assert_eq!(get_parent_directory("/a/b/c"), "/a/b/");
        // Directory with slash suffix.
        assert_eq!(get_parent_directory("/a/b/c/"), "/a/b/");
    }

    #[test]
    fn test_normalize_directory() {
        // Root directory.
        assert_eq!(normalize_directory(PathBuf::from("/")), "/");
        // Directory without slash suffix.
        assert_eq!(normalize_directory(PathBuf::from("/a/b/c")), "/a/b/c/");
        // Directory with slash suffix.
        assert_eq!(normalize_directory(PathBuf::from("/a/b/c/")), "/a/b/c/");
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
        assert_eq!(err.kind(), iceberg::ErrorKind::NamespaceNotFound);

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
        let table_already_exists = catalog.table_exists(&table_ident).await?;
        assert!(
            !table_already_exists,
            "Table should not exist before creation"
        );
        let results = catalog.list_tables(&namespace).await;
        let err = results.unwrap_err();
        assert_eq!(err.kind(), iceberg::ErrorKind::NamespaceNotFound);

        create_test_table(&catalog).await?;
        let table_already_exists = catalog.table_exists(&table_ident).await?;
        assert!(table_already_exists, "Table should exist after creation");

        let tables = catalog.list_tables(&namespace).await?;
        assert_eq!(
            tables.len(),
            1,
            "Expects one table, but actually have {:?}",
            tables
        );
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

        // Check metadata files and manifest files are correctly created.
        let table_path = format!(
            "{warehouse_path}/{}/{table_name}",
            namespace.to_url_string()
        );
        let metadata_filepath_v0 = format!("{table_path}/metadata/v0.metadata.json");
        assert!(
            Path::new(&metadata_filepath_v0).exists(),
            "{metadata_filepath_v0} doesn't exist"
        ); // created at table creation
        let metadata_filepath_v1 = format!("{table_path}/metadata/v1.metadata.json");
        assert!(
            Path::new(&metadata_filepath_v1).exists(),
            "{metadata_filepath_v1} doesn't exist"
        ); // created at table update

        let version_hint_filepath = format!("{table_path}/metadata/version-hint.text");
        assert!(
            Path::new(&version_hint_filepath).exists(),
            "{version_hint_filepath} doesn't exist"
        ); // created at table creation
        assert_eq!(std::fs::read_to_string(version_hint_filepath).unwrap(), "1"); // updated at table update

        Ok(())
    }
}
