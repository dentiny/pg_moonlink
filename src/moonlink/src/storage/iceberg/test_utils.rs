/// This module provides a few test util functions.
#[cfg(test)]
pub(crate) mod catalog_test_utils {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type as IcebergType};
    use iceberg::Result as IcebergResult;
    use iceberg::{NamespaceIdent, TableCreation};

    // Test util function to get test table schema.
    pub(crate) fn create_test_table_schema() -> IcebergResult<Schema> {
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

    // Test util function to get table schema for the given table.
    pub(crate) fn create_test_table_creation(
        namespace_ident: &NamespaceIdent,
        table_name: &str,
    ) -> IcebergResult<TableCreation> {
        let schema = create_test_table_schema()?;
        let table_creation = TableCreation::builder()
            .name(table_name.to_string())
            .location(format!(
                "file:///tmp/iceberg-test/{}/{}",
                namespace_ident.to_url_string(),
                table_name
            ))
            .schema(schema.clone())
            .build();
        Ok(table_creation)
    }
}

use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::puffin_utils;
use crate::storage::mooncake_table::Snapshot;
use crate::storage::mooncake_table::{DiskFileDeletionVector, TableConfig as MooncakeTableConfig};
use iceberg::io::FileIOBuilder;
use std::collections::HashSet;

/// Test util function to check consistency for snapshot batch deletion vector and deletion puffin blob.
async fn check_deletion_vector_consistency(disk_dv_entry: &DiskFileDeletionVector) {
    if disk_dv_entry.puffin_deletion_blob.is_none() {
        assert!(disk_dv_entry
            .batch_deletion_vector
            .collect_deleted_rows()
            .is_empty());
        return;
    }

    let local_fileio = FileIOBuilder::new_fs_io().build().unwrap();
    let blob = puffin_utils::load_blob_from_puffin_file(
        local_fileio,
        &disk_dv_entry
            .puffin_deletion_blob
            .as_ref()
            .unwrap()
            .puffin_filepath,
    )
    .await
    .unwrap();
    let iceberg_deletion_vector = DeletionVector::deserialize(blob).unwrap();
    let batch_deletion_vector = iceberg_deletion_vector
        .take_as_batch_delete_vector(MooncakeTableConfig::default().batch_size());
    assert_eq!(batch_deletion_vector, disk_dv_entry.batch_deletion_vector);
}

/// Test util function to check deletion vector consistency for the given snapshot.
pub(crate) async fn check_deletion_vector_consistency_for_snapshot(snapshot: &Snapshot) {
    for disk_deletion_vector in snapshot.disk_files.values() {
        check_deletion_vector_consistency(disk_deletion_vector).await;
    }
}

/// Test util functions to check recovered snapshot only contains remote filepaths and they do exist.
pub(crate) async fn validate_recovered_snapshot(snapshot: &Snapshot, warehouse_uri: &str) {
    let warehouse_directory = tokio::fs::canonicalize(&warehouse_uri).await.unwrap();
    let mut data_filepaths: HashSet<String> = HashSet::new();

    // Check data files and their puffin blobs.
    for (cur_disk_file, cur_deletion_vector) in snapshot.disk_files.iter() {
        let cur_disk_pathbuf = std::path::PathBuf::from(cur_disk_file.file_path());
        assert!(cur_disk_pathbuf.starts_with(&warehouse_directory));
        assert!(tokio::fs::try_exists(cur_disk_pathbuf).await.unwrap());
        assert!(data_filepaths.insert(cur_disk_file.file_path().clone()));

        if cur_deletion_vector.puffin_deletion_blob.is_none() {
            continue;
        }
        let puffin_filepath = &cur_deletion_vector
            .puffin_deletion_blob
            .as_ref()
            .unwrap()
            .puffin_filepath;
        let puffin_pathbuf = std::path::PathBuf::from(puffin_filepath);
        assert!(puffin_pathbuf.starts_with(&warehouse_directory));
        assert!(tokio::fs::try_exists(puffin_filepath).await.unwrap());
    }

    // Check file indices.
    let mut index_referenced_data_filepaths: HashSet<String> = HashSet::new();
    for cur_file_index in snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            let index_pathbuf = std::path::PathBuf::from(&cur_index_block.file_path);
            assert!(index_pathbuf.starts_with(&warehouse_directory));
            assert!(tokio::fs::try_exists(&index_pathbuf).await.unwrap());
        }

        for cur_data_filepath in cur_file_index.files.iter() {
            index_referenced_data_filepaths.insert(cur_data_filepath.file_path().clone());
        }
    }

    assert_eq!(index_referenced_data_filepaths, data_filepaths);
}
