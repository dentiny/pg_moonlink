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

/// This module contains the filesystem catalog implementation, which serves for local development and hermetic unit test purpose, so for initial versions, it's focusing more on simplicity and correctness rather than performance.
/// Compared with `MemoryCatalog`, `FileSystemCatalog` could be used in production environment.
///
/// TODO(hjiang):
/// 1. Implement features necessary for concurrent accesses.
/// 2. The initial version access everything via filesystem, for performance considerartion we should cache metadata in memory.

#[derive(Debug)]
pub struct FileSystemCatalog {
    file_io: FileIO,
    table_name: String,
    warehouse_location: String,
    metadata: Option<TableMetadata>,
    metadata_content: Vec<u8>,
    metadata_file_path: String,
    table: Option<Table>,
}

impl FileSystemCatalog {
    /// Creates a rest catalog from config.
    pub fn new(name: String, warehouse_location: String) -> Self {
        Self {
            file_io: FileIO::from_path(warehouse_location.clone())
                .unwrap()
                .build()
                .unwrap(),
            table_name: name,
            warehouse_location: warehouse_location,
            metadata: None,
            metadata_content: vec![],
            metadata_file_path: "".to_string(),
            table: None,
        }
    }

    pub async fn load_metadata(mut self) -> IcebergResult<Self> {
        let version_hint_path = format!("{}/metadata/version-hint.text", self.warehouse_location);
        let input_file: iceberg::io::InputFile = self.file_io.new_input(&version_hint_path)?;
        let version = String::from_utf8(input_file.read().await?.to_vec()).expect("");
        self.metadata_file_path = format!(
            "{}/metadata/v{}.metadata.json",
            self.warehouse_location, version
        );
        let input_file: iceberg::io::InputFile =
            self.file_io.new_input(&self.metadata_file_path)?;
        self.metadata_content = input_file.read().await?.to_vec();
        let metadata = serde_json::from_slice::<TableMetadata>(&self.metadata_content)?;
        let table_id: TableIdent = TableIdent::from_strs(["default", &self.table_name]).unwrap();
        self.table = Some(
            Table::builder()
                .file_io(self.file_io.clone())
                .metadata_location(self.metadata_file_path.clone())
                .metadata(metadata.clone())
                .identifier(table_id.clone())
                .build()
                .unwrap(),
        );
        self.metadata = Some(metadata);
        Ok(self)
    }
}

#[async_trait]
impl Catalog for FileSystemCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&iceberg::NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        todo!()
    }

    /// Create a new namespace inside the catalog.
    ///
    /// TODO(hjiang): Implement properties handling.
    async fn create_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        // Build full path from warehouse_location and namespace components
        let mut path = PathBuf::from(&self.warehouse_location);
        for part in namespace.as_ref().iter() {
            path.push(part);
        }

        // Attempt to create all directories (no-op if already exists)
        std::fs::create_dir_all(&path).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create namespace directory: {}", e),
            )
        })?;

        // Return the created namespace
        Ok(Namespace::new(namespace.clone()))
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        todo!()
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> IcebergResult<bool> {
        todo!()
    }
    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
        todo!()
    }
    /// List tables from namespace.
    async fn list_tables(&self, _namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
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
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let warehouse_location = creation.location.clone().unwrap();
        // Create the metadata directory.
        // Most client implicitly expects metadata in PATH/metadata directory.
        //
        let metadata_path: String = format!("{warehouse_location}/metadata");
        // Create the initial table metadata
        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;
        // Write the initial metadata file
        let metadata_file_path = format!("{metadata_path}/v0.metadata.json");
        let metadata_json = serde_json::to_string(&table_metadata.metadata)?;
        let output = self.file_io.new_output(&metadata_file_path)?;
        output.write(metadata_json.into()).await?;
        // Write the version hint file
        let version_hint_path = format!("{warehouse_location}/metadata/version-hint.text");
        let version_hint_output = self.file_io.new_output(&version_hint_path)?;
        version_hint_output.write("0".into()).await?;
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(table_ident)
            .file_io(self.file_io.clone())
            .build()
            .unwrap();
        Ok(table)
    }
    /// Load table from the catalog.
    async fn load_table(&self, _table: &TableIdent) -> IcebergResult<Table> {
        todo!()
    }
    /// Drop a table from the catalog.
    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        todo!()
    }
    /// Check if a table exists in the catalog.
    async fn table_exists(&self, _table: &TableIdent) -> IcebergResult<bool> {
        todo!()
    }
    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        todo!()
    }
    /// Update a table to the catalog.
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        let version = self.metadata.as_ref().unwrap().next_sequence_number();
        let builder = TableMetadataBuilder::new_from_metadata(
            self.metadata.clone().unwrap(),
            Some(self.metadata_file_path.clone()),
        );
        let update = commit.take_updates().to_vec();
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &update[0] {
            snapshot
        } else {
            unreachable!()
        };
        let metadata = builder
            .add_snapshot(new_snapshot.clone())
            .unwrap()
            .build()
            .unwrap();
        // Write the initial metadata file
        let metadata_file_path = format!(
            "{}/metadata/v{}.metadata.json",
            self.warehouse_location, version
        );
        let metadata_json = serde_json::to_string(&metadata.metadata)?;
        let output = self.file_io.new_output(&metadata_file_path)?;
        output.write(metadata_json.into()).await?;
        let version_hint_path = format!("{}/metadata/version-hint.text", self.warehouse_location);
        let version_hint_output = self.file_io.new_output(&version_hint_path)?;
        version_hint_output
            .write(format!("{version}").into())
            .await?;
        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(self.file_io.clone())
            .metadata(metadata.metadata)
            .metadata_location(metadata_file_path)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::Result as IcebergResult;

    #[tokio::test]
    async fn test_file_catalog() -> IcebergResult<()> {
        assert!(false, "Test not implemented yet");
        Ok(())
    }
}
