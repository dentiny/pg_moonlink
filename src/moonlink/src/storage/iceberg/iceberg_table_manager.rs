use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
use crate::storage::iceberg::index::FileIndexBlob;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::iceberg::utils;
use crate::storage::iceberg::validation as IcebergValidation;
use crate::storage::index::{FileIndex as MooncakeFileIndex, MooncakeIndex};
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::{
    DiskFileDeletionVector, Snapshot as MooncakeSnapshot, TableMetadata as MooncakeTableMetadata,
};

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

#[cfg(test)]
use mockall::*;

/// Key for iceberg table property, to record flush lsn.
const MOONCAKE_TABLE_FLUSH_LSN: &str = "mooncake-table-flush-lsn";

#[derive(Clone, Debug, TypedBuilder)]
pub struct IcebergTableConfig {
    /// Table warehouse location.
    #[builder(default = "/tmp/moonlink_iceberg".to_string())]
    pub warehouse_uri: String,
    /// Namespace for the iceberg table.
    #[builder(default = vec!["default".to_string()])]
    pub namespace: Vec<String>,
    /// Iceberg table name.
    #[builder(default = "table".to_string())]
    pub table_name: String,
}

#[allow(dead_code)]
#[cfg_attr(test, automock)]
pub(crate) trait IcebergOperation {
    /// Write a new snapshot to iceberg table.
    /// It could be called for multiple times to write and commit multiple snapshots.
    ///
    /// - Apart from data files, it also supports deletion vector (which is introduced in v3) and self-defined hash index,
    ///   both of which are stored in puffin files.
    /// - For deletion vectors, we store one blob in one puffin file.
    /// - For hash index, we store one mooncake file index in one puffin file.
    ///
    /// # Arguments
    ///
    /// * disk_files: new data files to be managed by iceberg
    /// * file_indices: all file indexes which have been persisted, could be either local or remote.
    ///
    /// Please notice, it takes full content to commit snapshot.
    ///
    /// TODO(hjiang): We're storing the iceberg table status in two places, one for iceberg table manager, another at snapshot.
    /// Provide delta change interface, so snapshot doesn't need to store everything.
    async fn sync_snapshot(
        &mut self,
        flush_lsn: u64,
        disk_files: Vec<PathBuf>,
        new_deletion_vector: HashMap<PathBuf, BatchDeletionVector>,
        file_indices: &[MooncakeFileIndex],
    ) -> IcebergResult<HashMap<PathBuf, PuffinBlobRef>>;

    /// Load latest snapshot from iceberg table. Used for recovery and initialization.
    /// Notice this function is supposed to call **only once**.
    async fn load_snapshot_from_table(&mut self) -> IcebergResult<MooncakeSnapshot>
    where
        Self: Sized;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DataFileEntry {
    /// Iceberg data file, used to decide what to persist at new commit requests.
    pub(crate) data_file: DataFile,
    /// In-memory deletion vector.
    pub(crate) deletion_vector: BatchDeletionVector,
    /// Puffin blob for its deletion vector.
    persisted_deletion_vector: Option<PuffinBlobRef>,
}

/// TODO(hjiang):
/// 1. Support a data file handle, which is a remote file path, plus an optional local cache filepath.
/// 2. Support a deletion vector handle, which is a remote file path, with an optional in-memory buffer and a local cache filepath.
#[derive(Debug)]
pub struct IcebergTableManager {
    /// Iceberg table configuration.
    pub(crate) config: IcebergTableConfig,

    /// Mooncake table metadata.
    pub(crate) mooncake_table_metadata: Arc<MooncakeTableMetadata>,

    /// Iceberg catalog, which interacts with the iceberg table.
    pub(crate) catalog: Box<dyn MoonlinkCatalog>,

    /// The iceberg table it's managing.
    pub(crate) iceberg_table: Option<IcebergTable>,

    /// Maps from already persisted data file filepath to its deletion vector, and iceberg `DataFile`.
    pub(crate) persisted_data_files: HashMap<PathBuf, DataFileEntry>,

    /// A set of file index id which has been managed by the iceberg table.
    pub(crate) persisted_file_index_ids: HashSet<u32>,
}

impl IcebergTableManager {
    pub fn new(
        mooncake_table_metadata: Arc<MooncakeTableMetadata>,
        config: IcebergTableConfig,
    ) -> IcebergTableManager {
        let catalog = utils::create_catalog(&config.warehouse_uri).unwrap();
        Self {
            config,
            mooncake_table_metadata,
            catalog,
            iceberg_table: None,
            persisted_data_files: HashMap::new(),
            persisted_file_index_ids: HashSet::new(),
        }
    }

    /// Get a unique puffin filepath under table warehouse uri.
    fn get_unique_deletion_vector_filepath(&self) -> String {
        let location_generator =
            DefaultLocationGenerator::new(self.iceberg_table.as_ref().unwrap().metadata().clone())
                .unwrap();
        location_generator
            .generate_location(&format!("{}-deletion-vector-v1-puffin.bin", Uuid::new_v4()))
    }
    fn get_unique_hash_index_v1_filepath(&self) -> String {
        let location_generator =
            DefaultLocationGenerator::new(self.iceberg_table.as_ref().unwrap().metadata().clone())
                .unwrap();
        location_generator
            .generate_location(&format!("{}-hash-index-v1-puffin.bin", Uuid::new_v4()))
    }

    /// Get or create an iceberg table, and load full table status into table manager.
    async fn get_or_create_table(&mut self) -> IcebergResult<()> {
        if self.iceberg_table.is_none() {
            let table = utils::get_or_create_iceberg_table(
                &*self.catalog,
                &self.config.warehouse_uri,
                &self.config.namespace,
                &self.config.table_name,
                self.mooncake_table_metadata.schema.as_ref(),
            )
            .await?;
            self.iceberg_table = Some(table);
        }
        Ok(())
    }

    /// ---------- load snapshot ----------
    ///
    /// Load index file into table manager from the current manifest entry.
    ///
    /// TODO(hjiang): Parallelize blob read and recovery.
    async fn load_file_indices_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
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
                self.persisted_file_index_ids
                    .insert(mooncake_file_index.global_index_id);
                file_indices.push(mooncake_file_index);
            });

        Ok(file_indices)
    }

    /// Load data file into table manager from the current manifest entry.
    async fn load_data_file_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
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
            persisted_deletion_vector: None,
        };
        let old_entry = self
            .persisted_data_files
            .insert(file_path, new_data_file_entry);
        assert!(old_entry.is_none());
        Ok(())
    }

    /// Load deletion vector into table manager from the current manifest entry.
    async fn load_deletion_vector_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        // Skip data files and file indices.
        if !utils::is_deletion_vector_entry(entry) {
            return Ok(());
        }

        let data_file = entry.data_file();
        let referenced_path_buf: PathBuf = data_file.referenced_data_file().unwrap().into();
        let data_file_entry = self.persisted_data_files.get_mut(&referenced_path_buf);
        assert!(
            data_file_entry.is_some(),
            "At recovery, the data file path for {:?} doesn't exist",
            referenced_path_buf
        );

        IcebergValidation::validate_puffin_manifest_entry(entry)?;
        let deletion_vector = DeletionVector::load_from_dv_blob(file_io.clone(), data_file).await?;
        let batch_deletion_vector = deletion_vector
            .take_as_batch_delete_vector(self.mooncake_table_metadata.config.batch_size());
        data_file_entry.unwrap().deletion_vector = batch_deletion_vector;

        Ok(())
    }

    /// Util function to transform iceberg table status to mooncake table snapshot.
    fn transform_to_mooncake_snapshot(
        &self,
        persisted_file_indices: Vec<MooncakeFileIndex>,
        flush_lsn: Option<u64>,
    ) -> MooncakeSnapshot {
        let mut mooncake_snapshot = MooncakeSnapshot::new(self.mooncake_table_metadata.clone());

        // Assign snapshot version.
        let iceberg_table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
        mooncake_snapshot.snapshot_version =
            if let Some(ver) = iceberg_table_metadata.current_snapshot_id() {
                ver as u64
            } else {
                0
            };

        // Fill in disk files.
        mooncake_snapshot.disk_files = HashMap::with_capacity(self.persisted_data_files.len());
        for (data_filepath, data_file_entry) in self.persisted_data_files.iter() {
            mooncake_snapshot.disk_files.insert(
                data_filepath.clone(),
                DiskFileDeletionVector {
                    puffin_deletion_blob: None,
                    batch_deletion_vector: data_file_entry.deletion_vector.clone(),
                },
            );
        }

        // Fill in indices.
        mooncake_snapshot.indices = MooncakeIndex {
            in_memory_index: HashSet::new(),
            file_indices: persisted_file_indices,
        };

        // Fill in flush LSN.
        mooncake_snapshot.data_file_flush_lsn = flush_lsn;

        mooncake_snapshot
    }

    /// ---------- store snapshot ----------
    ///
    /// Write deletion vector to puffin file.
    /// Precondition: batch deletion vector is not empty.
    ///
    /// Puffin blob write condition:
    /// 1. No compression is performed, otherwise it's hard to get blob size without another read operation.
    /// 2. We put one deletion vector within one puffin file.
    async fn write_deletion_vector(
        &mut self,
        data_file: String,
        deletion_vector: BatchDeletionVector,
    ) -> IcebergResult<PuffinBlobRef> {
        let deleted_rows = deletion_vector.collect_deleted_rows();
        assert!(!deleted_rows.is_empty());

        let deleted_row_count = deleted_rows.len();
        let mut iceberg_deletion_vector = DeletionVector::new();
        iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
        let blob_properties = HashMap::from([
            (DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(), data_file),
            (
                DELETION_VECTOR_CADINALITY.to_string(),
                deleted_row_count.to_string(),
            ),
        ]);
        let blob = iceberg_deletion_vector.serialize(blob_properties);
        let blob_size = blob.data().len();
        let puffin_filepath = self.get_unique_deletion_vector_filepath();
        let mut puffin_writer = puffin_utils::create_puffin_writer(
            self.iceberg_table.as_ref().unwrap().file_io(),
            &puffin_filepath,
        )
        .await?;
        puffin_writer.add(blob, CompressionCodec::None).await?;

        self.catalog
            .record_puffin_metadata_and_close(puffin_filepath.clone(), puffin_writer)
            .await?;

        println!("\n\n==== write deletion vector puffin file ====\n\n");
        println!("file path = {}\n", puffin_filepath);
        println!("start offset = 4\n");
        println!("blob size = {}\n", blob_size);

        Ok(PuffinBlobRef {
            puffin_filepath,
            start_offset: 4_u32, // Puffin file starts with 4 magic bytes.
            blob_size: blob_size as u32,
        })
    }

    /// Dump local data files into iceberg table.
    /// Return new iceberg data files for append transaction.
    async fn sync_data_files(
        &mut self,
        new_data_files: Vec<PathBuf>,
    ) -> IcebergResult<Vec<DataFile>> {
        let mut new_iceberg_data_files = Vec::with_capacity(new_data_files.len());
        for local_data_file in new_data_files.into_iter() {
            let iceberg_data_file = utils::write_record_batch_to_iceberg(
                self.iceberg_table.as_ref().unwrap(),
                &local_data_file,
            )
            .await?;
            let old_entry = self.persisted_data_files.insert(
                local_data_file.clone(),
                DataFileEntry {
                    data_file: iceberg_data_file.clone(),
                    deletion_vector: BatchDeletionVector::new(
                        self.mooncake_table_metadata.config.batch_size(),
                    ),
                    persisted_deletion_vector: None,
                },
            );
            assert!(old_entry.is_none());
            new_iceberg_data_files.push(iceberg_data_file);
        }
        Ok(new_iceberg_data_files)
    }

    /// Dump committed deletion logs into iceberg table, only the changed part will be persisted.
    async fn sync_deletion_vector(
        &mut self,
        new_deletion_logs: HashMap<PathBuf, BatchDeletionVector>,
    ) -> IcebergResult<HashMap<PathBuf, PuffinBlobRef>> {
        let mut puffin_deletion_blobs = HashMap::new();
        for (local_data_filepath, new_deletion_vector) in new_deletion_logs.into_iter() {
            let mut entry = self
                .persisted_data_files
                .get(&local_data_filepath)
                .unwrap()
                .clone();
            entry.deletion_vector.merge_with(&new_deletion_vector);

            // Data filepath in iceberg table.
            let iceberg_data_file = entry.data_file.file_path();
            let puffin_blob = self
                .write_deletion_vector(iceberg_data_file.to_string(), entry.deletion_vector.clone())
                .await?;
            self.persisted_data_files
                .insert(local_data_filepath.clone(), entry);
            puffin_deletion_blobs.insert(local_data_filepath, puffin_blob);
        }
        Ok(puffin_deletion_blobs)
    }

    /// Dump file indexes into the iceberg table, only new file indexes will be persisted into the table.
    /// Return file index ids which should be added into iceberg table.
    ///
    /// TODO(hjiang): Need to configure (1) the number of blobs in a puffin file; and (2) the number of file index in a puffin blob.
    /// For implementation simpicity, put everything in a single file and a single blob.
    async fn sync_file_indices(&mut self, file_indices: &[MooncakeFileIndex]) -> IcebergResult<()> {
        if file_indices.len() == self.persisted_file_index_ids.len() {
            return Ok(());
        }

        let puffin_filepath = self.get_unique_hash_index_v1_filepath();
        let mut puffin_writer = puffin_utils::create_puffin_writer(
            self.iceberg_table.as_ref().unwrap().file_io(),
            &puffin_filepath,
        )
        .await?;

        // TODO(hjiang): Maps from local filepath to remote filepath.
        // After sync, file index still stores local index file location.
        // After cache design, we should be able to provide a "handle" abstraction, which could be either local or remote.
        // The hash map here is merely a workaround to pass remote path to iceberg file index structure.
        let mut local_index_file_to_remote = HashMap::new();

        let mut new_file_indices: Vec<&MooncakeFileIndex> =
            Vec::with_capacity(file_indices.len() - self.persisted_file_index_ids.len());
        for cur_file_index in file_indices.iter() {
            if self
                .persisted_file_index_ids
                .insert(cur_file_index.global_index_id)
            {
                // Record new index file id.
                new_file_indices.push(cur_file_index);
                // Upload new index file to iceberg table.
                for cur_index_block in cur_file_index.index_blocks.iter() {
                    let remote_index_block = utils::upload_index_file(
                        self.iceberg_table.as_ref().unwrap(),
                        &cur_index_block.file_path,
                    )
                    .await?;
                    local_index_file_to_remote
                        .insert(cur_index_block.file_path.clone(), remote_index_block);
                }
            }
        }

        let file_index_blob = FileIndexBlob::new(new_file_indices, local_index_file_to_remote);
        let puffin_blob = file_index_blob.as_blob()?;
        puffin_writer
            .add(puffin_blob, iceberg::puffin::CompressionCodec::None)
            .await?;
        self.catalog
            .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
            .await?;

        Ok(())
    }
}

/// TODO(hjiang): Parallelize all IO operations.
impl IcebergOperation for IcebergTableManager {
    async fn sync_snapshot(
        &mut self,
        flush_lsn: u64,
        new_disk_files: Vec<PathBuf>,
        new_deletion_vector: HashMap<PathBuf, BatchDeletionVector>,
        file_indices: &[MooncakeFileIndex],
    ) -> IcebergResult<HashMap<PathBuf, PuffinBlobRef>> {
        // Initialize iceberg table on access.
        self.get_or_create_table().await?;

        // Persist data files.
        let new_iceberg_data_files = self.sync_data_files(new_disk_files).await?;

        // Persist committed deletion logs.
        let deletion_puffin_blobs = self.sync_deletion_vector(new_deletion_vector).await?;

        // Persist file index changes.
        self.sync_file_indices(file_indices).await?;

        // Only start append action when there're new data files.
        let mut txn = Transaction::new(self.iceberg_table.as_ref().unwrap());
        if !new_iceberg_data_files.is_empty() {
            let mut action =
                txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;
            action.add_data_files(new_iceberg_data_files)?;
            txn = action.apply().await?;
        }

        // Persist flush lsn at table property, it's just a workaround.
        // The ideal solution is to store at snapshot summary additional properties.
        // Issue: https://github.com/apache/iceberg-rust/issues/1329
        let mut prop = HashMap::new();
        prop.insert(MOONCAKE_TABLE_FLUSH_LSN.to_string(), flush_lsn.to_string());
        txn = txn.set_properties(prop)?;

        self.iceberg_table = Some(txn.commit(&*self.catalog).await?);
        self.catalog.clear_puffin_metadata();

        Ok(deletion_puffin_blobs)
    }

    async fn load_snapshot_from_table(&mut self) -> IcebergResult<MooncakeSnapshot> {
        assert!(self.persisted_file_index_ids.is_empty());

        self.get_or_create_table().await?;
        let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
        let mut flush_lsn: Option<u64> = None;
        if let Some(lsn) = table_metadata.properties().get(MOONCAKE_TABLE_FLUSH_LSN) {
            flush_lsn = Some(lsn.parse().unwrap());
        }

        // There's nothing stored in iceberg table (aka, first time initialization).
        if table_metadata.current_snapshot().is_none() {
            return Ok(MooncakeSnapshot::new(self.mooncake_table_metadata.clone()));
        }

        // Load table state into iceberg table manager.
        let snapshot_meta = table_metadata.current_snapshot().unwrap();
        let manifest_list = snapshot_meta
            .load_manifest_list(
                self.iceberg_table.as_ref().unwrap().file_io(),
                table_metadata,
            )
            .await?;

        let file_io = self.iceberg_table.as_ref().unwrap().file_io().clone();
        let mut loaded_file_indices = vec![];
        for manifest_file in manifest_list.entries().iter() {
            // All files (i.e. data files, deletion vector, manifest files) under the same snapshot are assigned with the same sequence number.
            // Reference: https://iceberg.apache.org/spec/?h=content#sequence-numbers
            let manifest = manifest_file.load_manifest(&file_io).await?;
            let (manifest_entries, _) = manifest.into_parts();
            assert!(
                !manifest_entries.is_empty(),
                "Shouldn't have empty manifest file"
            );

            // On load, we do two pass on all entries, to check whether all deletion vector has a corresponding data file.
            for entry in manifest_entries.iter() {
                self.load_data_file_from_manifest_entry(entry.as_ref())
                    .await?;
                let file_indices = self
                    .load_file_indices_from_manifest_entry(entry.as_ref(), &file_io)
                    .await?;
                loaded_file_indices.extend(file_indices);
            }
            for entry in manifest_entries.into_iter() {
                self.load_deletion_vector_from_manifest_entry(entry.as_ref(), &file_io)
                    .await?;
            }
        }

        let mooncake_snapshot = self.transform_to_mooncake_snapshot(loaded_file_indices, flush_lsn);
        Ok(mooncake_snapshot)
    }
}
