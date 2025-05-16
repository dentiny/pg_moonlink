use crate::storage::mooncake_table::PuffinDeletionBlobAtRead;

use bincode::enc::{write::Writer, Encode, Encoder};
use bincode::error::EncodeError;
use iceberg::puffin;
use bincode::config;

const BINCODE_CONFIG: config::Configuration = config::standard();

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TableMetadata {
    pub(super) data_files: Vec<String>,
    pub(super) deletion_vector: Vec<PuffinDeletionBlobAtRead>,
    pub(super) positional_deletes: Vec<(u32, u32)>,
}

impl Encode for TableMetadata {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let writer = encoder.writer();

        // Write data filepaths offsets.
        write_usize(writer, self.data_files.len())?;
        let mut offset = 0;
        for data_file in &self.data_files {
            write_usize(writer, offset)?;
            offset = offset.saturating_add(data_file.len());
        }
        write_usize(writer, offset)?;

        // Write deletion vector puffin blob filepaths offsets.
        // Arrange all offsets together (instead of mixing with blob start offset and blob size), so decode side could directly operate on `uint32_t` pointers.
        write_usize(writer, self.deletion_vector.len())?;
        let mut offset = 0;
        for cur_puffin_blob in self.deletion_vector.iter() {
            write_usize(writer, offset)?;
            offset = offset.saturating_add(cur_puffin_blob.puffin_filepath.len());
        }
        write_usize(writer, offset)?;

        // Write deletion vector puffin blob information.
        for cur_puffin_blob in self.deletion_vector.iter() {
            write_u32(writer, cur_puffin_blob.data_file_index)?;
            write_u32(writer, cur_puffin_blob.start_offset)?;
            write_u32(writer, cur_puffin_blob.blob_size)?;
        }

        // Write positional deletion records.
        write_usize(writer, self.positional_deletes.len())?;
        for position_delete in &self.positional_deletes {
            write_u32(writer, position_delete.0)?;
            write_u32(writer, position_delete.1)?;
        }

        // Write data filepaths.
        for data_file in &self.data_files {
            writer.write(data_file.as_bytes())?;
        }

        // Write puffin filepaths.
        for puffin_file in self.deletion_vector.iter() {
            writer.write(puffin_file.puffin_filepath.as_bytes())?;
        }

        Ok(())
    }
}

fn write_u32<W: Writer>(writer: &mut W, value: u32) -> Result<(), EncodeError> {
    writer.write(&value.to_ne_bytes())
}

fn write_usize<W: Writer>(writer: &mut W, value: usize) -> Result<(), EncodeError> {
    let value = u32::try_from(value).map_err(|_| EncodeError::Other("out of range"))?;
    write_u32(writer, value)
}

#[cfg(test)]
impl TableMetadata {
    pub fn decode(data: &[u8]) -> Self {
        use crate::storage::mooncake_table::PuffinDeletionBlobAtRead;
        use std::convert::TryInto;
        use bincode::config;

        let mut cursor = 0;

        fn read_u32(data: &[u8], cursor: &mut usize) -> u32 {
            let val = u32::from_ne_bytes(data[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            val
        }

        fn read_usize(data: &[u8], cursor: &mut usize) -> usize {
            read_u32(data, cursor) as usize
        }

        // Read data filepath offsets.
        let data_files_len = read_usize(data, &mut cursor);
        let mut data_file_offsets = Vec::with_capacity(data_files_len + 1);
        for _ in 0..=data_files_len {
            let data_file_offset = read_usize(data, &mut cursor);
            data_file_offsets.push(data_file_offset);
        }

        // Read puffin filepath offsets.
        let puffin_files_len = read_usize(data, &mut cursor);
        let mut puffin_file_offsets = Vec::with_capacity(puffin_files_len + 1);
        for _ in 0..=puffin_files_len {
            let puffin_file_offset = read_usize(data, &mut cursor);
            puffin_file_offsets.push(puffin_file_offset);
        }

        // Read deletion vector blobs.
        let mut deletion_vector_blobs = Vec::with_capacity(puffin_files_len);
        for _ in 0..puffin_files_len {
            let data_file_index = read_u32(data, &mut cursor);
            let start_offset = read_u32(data, &mut cursor);
            let blob_size = read_u32(data, &mut cursor);
            deletion_vector_blobs.push(PuffinDeletionBlobAtRead {
               data_file_index,
               start_offset,
               blob_size,
                puffin_filepath: "".to_string(), // Temporarily fill in empty string and decode later.
            });
        }

        // Read positional delete records.
        let position_deletes_len = read_usize(data, &mut cursor);
        let mut position_deletes = Vec::with_capacity(position_deletes_len);
        for _ in 0..position_deletes_len {
            let data_file_index = read_u32(data, &mut cursor);
            let row_index = read_u32(data, &mut cursor);
            position_deletes.push((data_file_index, row_index));
        }

        // Read data filepaths.
        let mut data_files = Vec::with_capacity(data_files_len);
        for i in 0..data_files_len {
            let start = data_file_offsets[i];
            let end = data_file_offsets[i + 1];
            let s = String::from_utf8(data[cursor + start..cursor + end].to_vec()).unwrap();
            data_files.push(s);
        }
        if data_files_len > 0 {
            cursor += data_file_offsets.last().unwrap();
        }

        // Read puffin filepaths.
        for i in 0..puffin_files_len {
            let start = puffin_file_offsets[i];
            let end = puffin_file_offsets[i + 1];
            let cur_puffin_filepath = String::from_utf8(data[cursor + start..cursor + end].to_vec()).unwrap();
            deletion_vector_blobs[i].puffin_filepath = cur_puffin_filepath;
        }
        if data_files_len > 0 {
            cursor += puffin_file_offsets.last().unwrap();
        }

        TableMetadata {
            data_files,
            deletion_vector: deletion_vector_blobs,
            positional_deletes: position_deletes,
        }
    }
}

#[cfg(test)]
mod tests {
    use iceberg::table::Table;

    use super::*;

    /// Util function to create a puffin deletion blob.
    fn create_puffin_deletion_blob_1() -> PuffinDeletionBlobAtRead {
        PuffinDeletionBlobAtRead {
            data_file_index: 0,
            puffin_filepath: "/tmp/iceberg_test/1-puffin.bin".to_string(),
            start_offset: 4,
            blob_size: 10,
        }
    }
    fn create_puffin_deletion_blob_2() -> PuffinDeletionBlobAtRead {
        PuffinDeletionBlobAtRead {
            data_file_index: 0,
            puffin_filepath: "/tmp/iceberg_test/2-puffin.bin".to_string(),
            start_offset: 4,
            blob_size: 20,
        }
    }

    #[test]
    fn test_table_metadata_serde() {
        let table_metadata = TableMetadata {
            data_files: vec![
                "/tmp/iceberg_test/data/1.parquet".to_string(),
                "/tmp/iceberg_test/data/2.parquet".to_string(),
                "/tmp/iceberg-rust/data/temp.parquet".to_string(), // associate file
            ],
            deletion_vector: vec![
                create_puffin_deletion_blob_1(),
                create_puffin_deletion_blob_2(),  
            ],
            positional_deletes: vec![(2, 2)],
        };
        let data = bincode::encode_to_vec(table_metadata.clone(), BINCODE_CONFIG).unwrap();

        let decoded_metadata = TableMetadata::decode(data.as_slice());
        assert_eq!(table_metadata, decoded_metadata);
    }

}