// iceberg-rust currently doesn't support puffin related features, to write deletion vector into iceberg metadata, we need two things at least:
// 1. the start offset and blob size for each deletion vector
// 2. append blob metadata into manifest file
// So here to workaround the limitation and to avoid/reduce changes to iceberg-rust ourselves, we use a few proxy types to reinterpret the memory directly.
//
// deletion vector spec:
// issue collection: https://github.com/apache/iceberg/issues/11122
// deletion vector table spec: https://github.com/apache/iceberg/pull/11240
//
// puffin blob spec: https://iceberg.apache.org/puffin-spec/?h=deletion#deletion-vector-v1-blob-type

use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};

use std::collections::HashMap;

use iceberg::io::FileIO;
use iceberg::puffin::CompressionCodec;
use iceberg::puffin::PuffinWriter;
use iceberg::spec::TableMetadata;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Datum, ManifestWriterBuilder, Struct,
};
use iceberg::Result as IcebergResult;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[allow(dead_code)]
enum PuffinFlagProxy {
    FooterPayloadCompressed = 0,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
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

#[allow(dead_code)]
struct PuffinWriterProxy {
    writer: Box<dyn iceberg::io::FileWrite>,
    is_header_written: bool,
    num_bytes_written: u64,
    written_blobs_metadata: Vec<PuffinBlobMetadataProxy>,
    properties: HashMap<String, String>,
    footer_compression_codec: CompressionCodec,
    flags: std::collections::HashSet<PuffinFlagProxy>,
}

/// Data file carries data file path, partition tuple, metrics, …
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct DataFileProxy {
    /// field id: 134
    ///
    /// Type of content stored by the data file: data, equality deletes,
    /// or position deletes (all v1 files are data files)
    content: DataContentType,
    /// field id: 100
    ///
    /// Full URI for the file with FS scheme
    file_path: String,
    /// field id: 101
    ///
    /// String file format name, `avro`, `orc`, `parquet`, or `puffin`
    file_format: DataFileFormat,
    /// field id: 102
    ///
    /// Partition data tuple, schema based on the partition spec output using
    /// partition field ids for the struct field ids
    partition: Struct,
    /// field id: 103
    ///
    /// Number of records in this file, or the cardinality of a deletion vector
    record_count: u64,
    /// field id: 104
    ///
    /// Total file size in bytes
    file_size_in_bytes: u64,
    /// field id: 108
    /// key field id: 117
    /// value field id: 118
    ///
    /// Map from column id to the total size on disk of all regions that
    /// store the column. Does not include bytes necessary to read other
    /// columns, like footers. Leave null for row-oriented formats (Avro)
    column_sizes: HashMap<i32, u64>,
    /// field id: 109
    /// key field id: 119
    /// value field id: 120
    ///
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    value_counts: HashMap<i32, u64>,
    /// field id: 110
    /// key field id: 121
    /// value field id: 122
    ///
    /// Map from column id to number of null values in the column
    null_value_counts: HashMap<i32, u64>,
    /// field id: 137
    /// key field id: 138
    /// value field id: 139
    ///
    /// Map from column id to number of NaN values in the column
    nan_value_counts: HashMap<i32, u64>,
    /// field id: 125
    /// key field id: 126
    /// value field id: 127
    ///
    /// Map from column id to lower bound in the column serialized as binary.
    /// Each value must be less than or equal to all non-null, non-NaN values
    /// in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    lower_bounds: HashMap<i32, Datum>,
    /// field id: 128
    /// key field id: 129
    /// value field id: 130
    ///
    /// Map from column id to upper bound in the column serialized as binary.
    /// Each value must be greater than or equal to all non-null, non-Nan
    /// values in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    upper_bounds: HashMap<i32, Datum>,
    /// field id: 131
    ///
    /// Implementation-specific key metadata for encryption
    key_metadata: Option<Vec<u8>>,
    /// field id: 132
    /// element field id: 133
    ///
    /// Split offsets for the data file. For example, all row group offsets
    /// in a Parquet file. Must be sorted ascending
    split_offsets: Vec<i64>,
    /// field id: 135
    /// element field id: 136
    ///
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and should be null
    /// otherwise. Fields with ids listed in this column must be present
    /// in the delete file
    equality_ids: Vec<i32>,
    /// field id: 140
    ///
    /// ID representing sort order for this file.
    ///
    /// If sort order ID is missing or unknown, then the order is assumed to
    /// be unsorted. Only data files and equality delete files should be
    /// written with a non-null order id. Position deletes are required to be
    /// sorted by file and position, not a table order, and should set sort
    /// order id to null. Readers must ignore sort order id for position
    /// delete files.
    sort_order_id: Option<i32>,
    /// field id: 142
    ///
    /// The _row_id for the first row in the data file.
    /// For more details, refer to https://github.com/apache/iceberg/blob/main/format/spec.md#first-row-id-inheritance
    pub(crate) first_row_id: Option<i64>,
    /// This field is not included in spec. It is just store in memory representation used
    /// in process.
    partition_spec_id: i32,
    /// field id: 143
    ///
    /// Fully qualified location (URI with FS scheme) of a data file that all deletes reference.
    /// Position delete metadata can use `referenced_data_file` when all deletes tracked by the
    /// entry are in a single data file. Setting the referenced file is required for deletion vectors.
    referenced_data_file: Option<String>,
    /// field: 144
    ///
    /// The offset in the file where the content starts.
    /// The `content_offset` and `content_size_in_bytes` fields are used to reference a specific blob
    /// for direct access to a deletion vector. For deletion vectors, these values are required and must
    /// exactly match the `offset` and `length` stored in the Puffin footer for the deletion vector blob.
    content_offset: Option<i64>,
    /// field: 145
    ///
    /// The length of a referenced content stored in the file; required if `content_offset` is present
    content_size_in_bytes: Option<i64>,
}

/// Get puffin blob metadata within the puffin write, and close the writer.
/// This function is supposed to be called after all blobs added.
pub(crate) async fn get_puffin_metadata_and_close(
    puffin_writer: PuffinWriter,
) -> IcebergResult<Vec<PuffinBlobMetadataProxy>> {
    let puffin_writer_proxy =
        unsafe { std::mem::transmute::<PuffinWriter, PuffinWriterProxy>(puffin_writer) };
    let puffin_metadata = puffin_writer_proxy.written_blobs_metadata.clone();
    let puffin_writer =
        unsafe { std::mem::transmute::<PuffinWriterProxy, PuffinWriter>(puffin_writer_proxy) };
    puffin_writer.close().await?;
    Ok(puffin_metadata)
}

// manifest file write logic: https://github.com/apache/iceberg-rust/blob/841ef0a3dc87a3ecffaeb1aa62ca9ca4ea4c1712/crates/iceberg/src/spec/manifest/writer.rs#L336-L417
//
/// Get manifest with the given sequence number, append with puffion deletion vector blob and rewrite it.
/// Note: this function should be called before catalog transaction commit.
//
/// # Arguments
/// path: filepath for the puffin file.
pub(crate) async fn append_puffin_metadata_and_rewrite(
    table_metadata: &TableMetadata,
    file_io: &FileIO,
    puffin_filepath: &str,
    blob_metadata: Vec<PuffinBlobMetadataProxy>,
) -> IcebergResult<()> {
    let latest_seq_no = table_metadata.last_sequence_number();
    let cur_snapshot = table_metadata.current_snapshot().unwrap();
    let manifest_list = cur_snapshot
        .load_manifest_list(file_io, table_metadata)
        .await?;
    let manifest_file = manifest_list
        .entries()
        .iter()
        .find(|cur_manifest| cur_manifest.sequence_number == latest_seq_no)
        .unwrap();
    let manifest_file_path = manifest_file.manifest_path.clone();
    let manifest = manifest_file.load_manifest(file_io).await?;
    let (manifest_entries, manifest_metadata) = manifest.into_parts();

    // Rewrite the manifest file.
    // There's only one moonlink process/thread writing manifest file, so we don't need to write to temporary file and rename.
    let output_file = file_io.new_output(manifest_file_path)?;
    let mut manifest_writer = ManifestWriterBuilder::new(
        output_file,
        table_metadata.current_snapshot_id(),
        /*key_metadata=*/ vec![],
        manifest_metadata.schema,
        manifest_metadata.partition_spec,
    )
    .build_v2_data();

    // Add existing data files to manifest writer.
    for cur_manifest_entry in manifest_entries.iter() {
        manifest_writer.add_file(
            cur_manifest_entry.data_file.clone(),
            cur_manifest_entry.sequence_number().unwrap(),
        )?;
    }

    // Append puffin blobs into existing manifest entries.
    //
    // TODO(hjiang): Add sanity check for sequence number between table metadata and deletion vector.
    for cur_blob_metadata in blob_metadata.iter() {
        let new_data_file = DataFileProxy {
            content: DataContentType::Data,
            file_path: puffin_filepath.to_string(),
            file_format: DataFileFormat::Puffin,
            partition: Struct::empty(),
            record_count: cur_blob_metadata
                .properties
                .get(DELETION_VECTOR_CADINALITY)
                .unwrap()
                .parse()
                .unwrap(),
            file_size_in_bytes: 0, // TODO(hjiang): Not necessary for puffin blob, but worth double confirm.
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            split_offsets: Vec::new(),
            equality_ids: Vec::new(),
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: Some(
                cur_blob_metadata
                    .properties
                    .get(DELETION_VECTOR_REFERENCED_DATA_FILE)
                    .unwrap()
                    .clone(),
            ),
            content_offset: Some(cur_blob_metadata.offset as i64),
            content_size_in_bytes: Some(cur_blob_metadata.length as i64),
        };
        let data_file = unsafe { std::mem::transmute::<DataFileProxy, DataFile>(new_data_file) };
        manifest_writer.add_file(data_file, cur_blob_metadata.sequence_number)?;
    }

    // Flush manifest file.
    manifest_writer.write_manifest_file().await?;

    Ok(())
}
