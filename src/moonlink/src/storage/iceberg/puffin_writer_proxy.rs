// iceberg-rust currently doesn't support puffin related features, to write deletion vector into iceberg metadata, we need two things at least:
// 1. the start offset and blob size for each deletion vector
// 2. append blob metadata into metadata
// So here to workaround the limitation and to avoid/reduce changes to iceberg-rust ourselves, we use a proxy to reinterpret the memory directly.

use futures::future::ok;
use iceberg::io::FileIO;
use iceberg::puffin::CompressionCodec;
use iceberg::puffin::PuffinWriter;
use iceberg::Result as IcebergResult;
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum PuffinFlagProxy {
    FooterPayloadCompressed = 0,
}

#[derive(Debug)]
pub(crate) struct PuffinBlobMetadataProxy {
    r#type: String,
    fields: Vec<i32>,
    snapshot_id: i64,
    sequence_number: i64,
    offset: u64,
    length: u64,
    compression_codec: CompressionCodec,
    properties: HashMap<String, String>,
}

struct PuffinWriterProxy {
    writer: Box<dyn iceberg::io::FileWrite>,
    is_header_written: bool,
    num_bytes_written: u64,
    written_blobs_metadata: Vec<PuffinBlobMetadataProxy>,
    properties: HashMap<String, String>,
    footer_compression_codec: CompressionCodec,
    flags: std::collections::HashSet<PuffinFlagProxy>,
}

// Get puffin metadata.
pub(crate) fn get_puffin_metadata(puffin_writer: PuffinWriter) -> Vec<PuffinBlobMetadataProxy> {
    let puffin_writer_proxy =
        unsafe { std::mem::transmute::<PuffinWriter, PuffinWriterProxy>(puffin_writer) };
    puffin_writer_proxy.written_blobs_metadata
}

// manifest file write logic: https://github.com/apache/iceberg-rust/blob/841ef0a3dc87a3ecffaeb1aa62ca9ca4ea4c1712/crates/iceberg/src/spec/manifest/writer.rs#L336-L417
//
// Note: this function should be called before catalog transaction commit.
pub(crate) async fn append_puffin_metadata_and_rewrite(
    file_io: FileIO,
    path: String,
    blob_metadata: Vec<PuffinBlobMetadataProxy>,
) -> IcebergResult<()> {
    let content = String::from_utf8(file_io.new_input(path)?.read().await?.to_vec())
        .expect("Failed to read in utf-8");
    Ok(())
}
