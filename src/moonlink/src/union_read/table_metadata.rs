use crate::storage::mooncake_table::PuffinDeletionBlob;

use bincode::enc::{write::Writer, Encode, Encoder};
use bincode::error::EncodeError;

#[derive(Debug)]
pub(super) struct TableMetadata {
    pub(super) data_files: Vec<String>,
    pub(super) deletion_vector: Vec<PuffinDeletionBlob>,
    pub(super) position_deletes: Vec<(u32, u32)>,
}

impl Encode for TableMetadata {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let writer = encoder.writer();
        write_usize(writer, self.data_files.len())?;
        let mut offset = 0;
        for data_file in &self.data_files {
            write_usize(writer, offset)?;
            offset = offset.saturating_add(data_file.len());
        }
        write_usize(writer, offset)?;
        write_usize(writer, self.position_deletes.len())?;
        for position_delete in &self.position_deletes {
            write_u32(writer, position_delete.0)?;
            write_u32(writer, position_delete.1)?;
        }
        for data_file in &self.data_files {
            writer.write(data_file.as_bytes())?;
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
        use std::convert::TryInto;

        let mut cursor = 0;

        fn read_u32(data: &[u8], cursor: &mut usize) -> u32 {
            let val = u32::from_ne_bytes(data[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            val
        }

        fn read_usize(data: &[u8], cursor: &mut usize) -> usize {
            read_u32(data, cursor) as usize
        }

        let data_files_len = read_usize(data, &mut cursor);

        let mut offsets = Vec::with_capacity(data_files_len + 1);
        for _ in 0..=data_files_len {
            offsets.push(read_usize(data, &mut cursor));
        }

        let position_deletes_len = read_usize(data, &mut cursor);

        let mut position_deletes = Vec::with_capacity(position_deletes_len);
        for _ in 0..position_deletes_len {
            let a = read_u32(data, &mut cursor);
            let b = read_u32(data, &mut cursor);
            position_deletes.push((a, b));
        }

        let mut data_files = Vec::with_capacity(data_files_len);
        for i in 0..data_files_len {
            let start = offsets[i];
            let end = offsets[i + 1];
            let s = String::from_utf8(data[cursor + start..cursor + end].to_vec()).unwrap();
            data_files.push(s);
        }

        TableMetadata {
            data_files,
            deletion_vector: vec![],
            position_deletes,
        }
    }
}
