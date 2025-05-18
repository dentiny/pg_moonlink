/// This module defines a few iceberg table property related constants and utils.
/// Reference: https://iceberg.apache.org/docs/latest/configuration/#table-properties
///
/// Compression codec for parquet files.
pub(crate) const PARQUET_COMPRESSION: &str = "write.parquet.compression-codec";
pub(crate) const PARQUET_COMPRESSION_DEFAULT: &str = "none";

/// Compression codec for metadata.
pub(crate) const METADATA_COMPRESSION: &str = "write.metadata.compression-codec";
pub(crate) const METADATA_COMPRESSION_DEFAULT: &str = "none";
