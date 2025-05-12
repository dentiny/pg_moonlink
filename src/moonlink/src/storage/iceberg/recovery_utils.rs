/// This module defines util functions and structs for recovery from iceberg.
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
use crate::storage::iceberg::index::FileIndexBlob;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::utils;
use crate::storage::iceberg::validation as IcebergValidation;
use crate::storage::index::{FileIndex as MooncakeFileIndex, MooncakeIndex};
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::mooncake_table::{self, Snapshot as MooncakeSnapshot};

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::vec;

use iceberg::io::FileIO;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::DataFileFormat;
use iceberg::spec::{DataFile, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::Result as IcebergResult;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DataFileEntry {
    /// Iceberg data file, used to decide what to persist at new commit requests.
    pub(crate) data_file: DataFile,
    /// In-memory deletion vector.
    pub(crate) deletion_vector: BatchDeletionVector,
}

#[allow(dead_code)]
pub(crate) struct RecoveredItems {
    /// Mooncake table snapshot.
    pub(crate) snapshot: MooncakeSnapshot,

    /// Maps from already persisted data file filepath to its deletion vector, and iceberg `DataFile`.
    pub(crate) persisted_data_files: HashMap<PathBuf, DataFileEntry>,

    /// A set of file index id which has been managed by the iceberg table.
    pub(crate) persisted_file_index_ids: HashSet<u32>,
}

fn transform_to_mooncake_snapshot(
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    persisted_file_indices: Vec<MooncakeFileIndex>,
    iceberg_table: &IcebergTable,
    persisted_data_files: &HashMap<PathBuf, DataFileEntry>,
) -> MooncakeSnapshot {
    let mut mooncake_snapshot = MooncakeSnapshot::new(mooncake_table_metadata.clone());

    // Assign snapshot version.
    let iceberg_table_metadata = iceberg_table.metadata();
    mooncake_snapshot.snapshot_version =
        if let Some(ver) = iceberg_table_metadata.current_snapshot_id() {
            ver as u64
        } else {
            0
        };

    // Fill in disk files.
    mooncake_snapshot.disk_files = HashMap::with_capacity(persisted_data_files.len());
    for (data_filepath, data_file_entry) in persisted_data_files.iter() {
        mooncake_snapshot.disk_files.insert(
            data_filepath.clone(),
            data_file_entry.deletion_vector.clone(),
        );
    }

    // Fill in indices.
    mooncake_snapshot.indices = MooncakeIndex {
        in_memory_index: HashSet::new(),
        file_indices: persisted_file_indices,
    };

    mooncake_snapshot
}

/// Load deletion vector into table manager from the current manifest entry.
async fn load_deletion_vector_from_manifest_entry(
    entry: &ManifestEntry,
    file_io: &FileIO,
    persisted_data_files: &mut HashMap<PathBuf, DataFileEntry>,
) -> IcebergResult<()> {
    // Skip data files and file indices.
    if !utils::is_deletion_vector_entry(entry) {
        return Ok(());
    }

    let data_file = entry.data_file();
    let referenced_path_buf: PathBuf = data_file.referenced_data_file().unwrap().into();
    let data_file_entry = persisted_data_files.get_mut(&referenced_path_buf);
    assert!(
        data_file_entry.is_some(),
        "At recovery, the data file path for {:?} doesn't exist",
        referenced_path_buf
    );

    IcebergValidation::validate_puffin_manifest_entry(entry)?;
    let deletion_vector = DeletionVector::load_from_dv_blob(file_io.clone(), data_file).await?;
    let batch_deletion_vector = deletion_vector.take_as_batch_delete_vector();
    data_file_entry.unwrap().deletion_vector = batch_deletion_vector;

    Ok(())
}

/// Load index file into table manager from the current manifest entry.
///
/// TODO(hjiang): Parallelize blob read and recovery.
async fn load_file_indices_from_manifest_entry(
    entry: &ManifestEntry,
    file_io: &FileIO,
    persisted_file_index_ids: &mut HashSet<u32>,
) -> IcebergResult<Vec<MooncakeFileIndex>> {
    if !utils::is_file_index(entry) {
        return Ok(vec![]);
    }

    let mut file_index_blob =
        FileIndexBlob::load_from_index_blob(file_io.clone(), entry.data_file()).await?;
    let mut file_indices = Vec::with_capacity(file_index_blob.file_indices.len());
    file_index_blob
        .file_indices
        .iter_mut()
        .for_each(|cur_file_index| {
            let mooncake_file_index = cur_file_index.as_mooncake_file_index();
            persisted_file_index_ids.insert(mooncake_file_index.global_index_id);
            file_indices.push(mooncake_file_index);
        });

    Ok(file_indices)
}

/// Load data file into table manager from the current manifest entry.
async fn load_data_file_from_manifest_entry(
    entry: &ManifestEntry,
    persisted_data_files: &mut HashMap<PathBuf, DataFileEntry>,
) -> IcebergResult<()> {
    if !utils::is_data_file_entry(entry) {
        return Ok(());
    }

    let data_file = entry.data_file();
    let file_path = PathBuf::from(data_file.file_path().to_string());
    assert_eq!(
        data_file.file_format(),
        DataFileFormat::Parquet,
        "Data file is of file format parquet for entry {:?}.",
        entry,
    );
    let new_data_file_entry = DataFileEntry {
        data_file: data_file.clone(),
        deletion_vector: BatchDeletionVector::new(/*max_rows=*/ 0),
    };
    let old_entry = persisted_data_files.insert(file_path, new_data_file_entry);
    assert!(old_entry.is_none());
    Ok(())
}

/// Load latest mooncake table snapshot from iceberg table.
pub(crate) async fn load_snapshot_from_table(
    iceberg_table: &mut IcebergTable,
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
) -> IcebergResult<RecoveredItems> {
    let table_metadata = iceberg_table.metadata();

    // Maps from already persisted data file filepath to its deletion vector, and iceberg `DataFile`.
    let mut persisted_data_files: HashMap<PathBuf, DataFileEntry> = HashMap::new();

    // A set of file index id which has been managed by the iceberg table.
    let mut persisted_file_index_ids: HashSet<u32> = HashSet::new();

    // There's nothing stored in iceberg table (aka, first time initialization).
    if table_metadata.current_snapshot().is_none() {
        return Ok(RecoveredItems {
            snapshot: MooncakeSnapshot::new(mooncake_table_metadata),
            persisted_data_files,
            persisted_file_index_ids,
        });
    }

    // Load table state into iceberg table manager.
    let snapshot_meta = table_metadata.current_snapshot().unwrap();
    let manifest_list = snapshot_meta
        .load_manifest_list(iceberg_table.file_io(), table_metadata)
        .await?;

    let file_io = iceberg_table.file_io().clone();
    let mut loaded_file_indices = vec![];
    for manifest_file in manifest_list.entries().iter() {
        // All files (i.e. data files, deletion vector, manifest files) under the same snapshot are assigned with the same sequence number.
        // Reference: https://iceberg.apache.org/spec/?h=content#sequence-numbers
        let manifest = manifest_file.load_manifest(&file_io).await?;
        let (manifest_entries, _) = manifest.into_parts();

        // On load, we do two pass on all entries, to check whether all deletion vector has a corresponding data file.
        for entry in manifest_entries.iter() {
            load_data_file_from_manifest_entry(entry.as_ref(), &mut persisted_data_files).await?;
            let file_indices = load_file_indices_from_manifest_entry(
                entry.as_ref(),
                &file_io,
                &mut persisted_file_index_ids,
            )
            .await?;
            loaded_file_indices.extend(file_indices);
        }
        for entry in manifest_entries.into_iter() {
            load_deletion_vector_from_manifest_entry(
                entry.as_ref(),
                &file_io,
                &mut persisted_data_files,
            )
            .await?;
        }
    }

    let mooncake_snapshot = transform_to_mooncake_snapshot(
        mooncake_table_metadata.clone(),
        loaded_file_indices,
        &iceberg_table,
        &persisted_data_files,
    );
    Ok(RecoveredItems {
        snapshot: mooncake_snapshot,
        persisted_data_files,
        persisted_file_index_ids,
    })
}
