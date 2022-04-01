use std::array::TryFromSliceError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid format")]
    InvalidFormat,
    #[error("Checksum mismatch")]
    ChecksumMismatch,
}

impl From<TryFromSliceError> for Error {
    #[inline]
    fn from(_src: TryFromSliceError) -> Error {
        Error::InvalidFormat
    }
}
