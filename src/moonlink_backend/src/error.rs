use moonlink_connectors::Error as MoonlinkConnectorError;
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
    #[error("Moonlink connector error: {source}")]
    MoonlinkConnectorError { source: MoonlinkConnectorError },
}

pub type Result<T> = result::Result<T, Error>;

impl From<MoonlinkConnectorError> for Error {
    fn from(source: MoonlinkConnectorError) -> Self {
        Error::MoonlinkConnectorError { source }
    }
}
