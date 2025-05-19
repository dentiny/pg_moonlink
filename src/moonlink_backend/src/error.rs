use moonlink_connectors::PostgresSourceError;
use std::result;
use thiserror::Error;

/// Custom error type for moonlink_backend
#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres source error: {0}")]
    PostgresSource(#[from] PostgresSourceError),
    #[error("Moonlink error: {0}")]
    Moonlink(#[from] moonlink::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = result::Result<T, Error>;
