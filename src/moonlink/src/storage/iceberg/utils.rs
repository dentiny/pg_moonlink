/// Util functions related to iceberg.
use iceberg::spec::{DataContentType, DataFileFormat, ManifestEntry};

/// Return whether the given manifest entry represents data files.
pub fn is_data_file_entry(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_data_file =
        f.content_type() == DataContentType::Data && f.file_format() == DataFileFormat::Parquet;
    if !is_data_file {
        return false;
    }
    assert!(f.referenced_data_file().is_none());
    assert!(f.content_offset().is_none());
    assert!(f.content_size_in_bytes().is_none());
    true
}

/// Return whether the given manifest entry represents deletion vector.
pub fn is_deletion_vector_entry(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_deletion_vector = f.content_type() == DataContentType::PositionDeletes;
    if !is_deletion_vector {
        return false;
    }
    assert_eq!(f.file_format(), DataFileFormat::Puffin);
    assert!(f.referenced_data_file().is_some());
    assert!(f.content_offset().is_some());
    assert!(f.content_size_in_bytes().is_some());
    true
}

/// Return whether the given manifest entry represents file index.
pub fn is_file_index(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_file_index =
        f.content_type() == DataContentType::Data && f.file_format() == DataFileFormat::Puffin;
    if !is_file_index {
        return false;
    }
    assert!(f.referenced_data_file().is_none());
    assert!(f.content_offset().is_none());
    assert!(f.content_size_in_bytes().is_none());
    true
}
