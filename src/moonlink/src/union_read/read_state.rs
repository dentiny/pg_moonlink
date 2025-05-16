// A read state is a collection of objects that are shared between moonlink and readers
//
// Meant to be sent using either shared memory or network connection.
//

use super::table_metadata::TableMetadata;
use crate::storage::PuffinDeletionBlobAtRead;

use bincode::config;

const BINCODE_CONFIG: config::Configuration = config::standard();

#[derive(Debug)]
pub struct ReadState {
    pub data: Vec<u8>,
    associated_files: Vec<String>,
}

impl Drop for ReadState {
    fn drop(&mut self) {
        println!("Dropping files: {:?}", self.associated_files);
        for file in self.associated_files.iter() {
            std::fs::remove_file(file).unwrap();
        }
    }
}

impl ReadState {
    pub(super) fn new(
        data_files: Vec<String>,
        deletion_vectors_at_read: Vec<PuffinDeletionBlobAtRead>,
        positional_deletes: Vec<(u32 /*file_index*/, u32 /*row_index*/)>,
        associated_files: Vec<String>,
    ) -> Self {
        let metadata = TableMetadata {
            data_files,
            deletion_vector: deletion_vectors_at_read,
            positional_deletes,
        };
        let data = bincode::encode_to_vec(metadata, BINCODE_CONFIG).unwrap(); // TODO
        Self {
            data,
            associated_files,
        }
    }
}

#[cfg(test)]
pub fn decode_read_state_for_testing(read_state: &ReadState) -> (Vec<String>, Vec<(u32, u32)>) {
    let metadata = TableMetadata::decode(&read_state.data);
    // (metadata.data_files, metadata.position_deletes)
    (metadata.data_files, vec![])
}
