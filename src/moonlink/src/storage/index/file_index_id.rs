/// This module generates unique id for file indexes.
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

// File index id generator.
static FILE_INDEX_ID_GENERATOR: OnceLock<AtomicU32> = OnceLock::new();
pub(crate) fn get_next_file_index_id() -> u32 {
    FILE_INDEX_ID_GENERATOR
        .get_or_init(|| AtomicU32::new(0))
        .fetch_add(1, Ordering::Relaxed)
}
