use std::{path::PathBuf, sync::Arc};

/// This module defines the file index struct used for iceberg, which corresponds to in-memory mooncake table file index structs, and supports the serde between mooncake table format and iceberg format.
use crate::storage::index as MooncakeIndex;
use crate::storage::index::file_index_id::get_next_file_index_id;
use crate::storage::index::persisted_bucket_hash_map::IndexBlock as MooncakeIndexBlock;

use iceberg::puffin::Blob;
use serde::{Deserialize, Serialize};
use serde_json;
use std::path::Path;

/// Corresponds to [storage::index::IndexBlock], which records the metadata for each index block.
#[derive(Deserialize, Serialize)]
pub(crate) struct IndexBlock {
    bucket_start_idx: u32,
    bucket_end_idx: u32,
    bucket_start_offset: u64,
    filepath: String,
}

/// Corresponds to [storage::index::FileIndex], used to persist at iceberg table.
#[derive(Deserialize, Serialize)]
pub(crate) struct FileIndex {
    /// Data file paths at iceberg table.
    data_files: Vec<String>,
    /// Corresponds to [storage::index::IndexBlock].
    index_block_files: Vec<IndexBlock>,
    /// Hash related fields.
    num_rows: u32,
    hash_bits: u32,
    hash_upper_bits: u32,
    hash_lower_bits: u32,
    seg_id_bits: u32,
    row_id_bits: u32,
    bucket_bits: u32,
}

impl FileIndex {
    /// Convert from mooncake table [storage::index::FileIndex].
    pub(crate) fn from_mooncake_file_index(mooncake_index: &MooncakeIndex::FileIndex) -> Self {
        Self {
            data_files: mooncake_index
                .files
                .iter()
                .map(|path| path.to_str().unwrap().to_string())
                .collect(),
            index_block_files: mooncake_index
                .index_blocks
                .iter()
                .map(|cur_index_block| IndexBlock {
                    bucket_start_idx: cur_index_block.bucket_start_idx,
                    bucket_end_idx: cur_index_block.bucket_end_idx,
                    bucket_start_offset: cur_index_block.bucket_start_offset,
                    filepath: cur_index_block.file_name.clone(),
                })
                .collect(),
            num_rows: mooncake_index.num_rows,
            hash_bits: mooncake_index.hash_bits,
            hash_upper_bits: mooncake_index.hash_upper_bits,
            hash_lower_bits: mooncake_index.hash_lower_bits,
            seg_id_bits: mooncake_index.seg_id_bits,
            row_id_bits: mooncake_index.row_id_bits,
            bucket_bits: mooncake_index.bucket_bits,
        }
    }

    /// Transfer the ownership and convert into [storage::index::FileIndex].
    /// The file index id is generated on-the-fly.
    pub(crate) fn take_as_mooncake_file_index(&mut self) -> MooncakeIndex::FileIndex {
        MooncakeIndex::FileIndex {
            global_index_id: get_next_file_index_id(),
            files: self
                .data_files
                .iter()
                .map(|path| Arc::new(PathBuf::from(path)))
                .collect(),
            num_rows: self.num_rows,
            hash_bits: self.hash_bits,
            hash_upper_bits: self.hash_upper_bits,
            hash_lower_bits: self.hash_lower_bits,
            seg_id_bits: self.seg_id_bits,
            row_id_bits: self.row_id_bits,
            bucket_bits: self.bucket_bits,
            index_blocks: self
                .index_block_files
                .iter()
                .map(|cur_index_block| {
                    // TODO(hjiang): Need to figure out the remote path download and mmap.
                    MooncakeIndexBlock::new(
                        cur_index_block.bucket_start_idx,
                        cur_index_block.bucket_end_idx,
                        cur_index_block.bucket_start_offset,
                        /*directory=*/ Path::new("/tmp/iceberg-table"),
                        cur_index_block.filepath.clone(),
                    )
                })
                .collect(),
        }
    }
}

/// In-memory structure for one file index blob in the puffin file, which contains multiple `FileIndex` structs.
#[derive(Deserialize, Serialize)]
pub(crate) struct FileIndexBlob {
    /// A blob contains multiple file indexes.
    pub(crate) file_indexes: Vec<FileIndex>,
}

impl FileIndexBlob {
    /// Serialize the file index into iceberg puffin blob.
    pub(crate) fn serialize(&mut self) -> Blob {
        todo!()
    }

    /// Deserialize from iceberg puffin blob.
    pub(crate) fn deserialize(blob: Blob) -> Self {
        todo!()
    }
}
