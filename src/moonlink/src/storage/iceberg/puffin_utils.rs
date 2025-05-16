/// This module contains util structs and functions for puffin access.
use std::collections::HashMap;

use iceberg::io::FileIO;
use iceberg::puffin::PuffinWriter;
use iceberg::puffin::{Blob, PuffinReader};
use iceberg::{Error as IcebergError, Result as IcebergResult};

/// Reference to puffin blob.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PuffinBlobRef {
    /// Path for the puffin file.
    pub(crate) puffin_filepath: String,
    /// Start offset for the blob.
    pub(crate) start_offset: u32,
    /// Blob size.
    pub(crate) blob_size: u32,
}

/// Get puffin writer with the given file io.
pub(crate) async fn create_puffin_writer(
    file_io: &FileIO,
    puffin_filepath: String,
) -> IcebergResult<PuffinWriter> {
    let out_file = file_io.new_output(puffin_filepath)?;
    let puffin_writer = PuffinWriter::new(
        &out_file,
        /*properties=*/ HashMap::new(),
        /*compress_footer=*/ false,
    )
    .await?;
    Ok(puffin_writer)
}

/// Load blob from the given puffin data entry.
/// Note: this function assumes there's only one blob in the puffin file.
pub(crate) async fn load_blob_from_puffin_file(
    file_io: FileIO,
    file_path: &str,
) -> IcebergResult<Blob> {
    let input_file = file_io.new_input(file_path)?;
    let puffin_reader = PuffinReader::new(input_file);
    let puffin_file_metadata = puffin_reader.file_metadata().await?;

    // Moonlink places one deletion vector in each puffin file.
    if puffin_file_metadata.blobs().len() != 1 {
        return Err(IcebergError::new(
            iceberg::ErrorKind::DataInvalid,
            format!(
                "Puffin file expects to have one blob, but has {} blobs",
                puffin_file_metadata.blobs().len()
            ),
        ));
    }

    let blob_metadata = &puffin_file_metadata.blobs()[0];
    puffin_reader.blob(blob_metadata).await
}
