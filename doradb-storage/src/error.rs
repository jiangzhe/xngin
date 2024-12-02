use doradb_datatype::error::Error as DataTypeError;
use std::array::TryFromSliceError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("invalid argument")]
    InvalidArgument,
    #[error("internal error")]
    InternalError,
    #[error("invalid state")]
    InvalidState,
    #[error("Invalid format")]
    InvalidFormat,
    #[error("Checksum mismatch")]
    ChecksumMismatch,
    #[error("IO Error")]
    IOError,
    #[error("Data type not supported")]
    DataTypeNotSupported,
    #[error("Index out of bound")]
    IndexOutOfBound,
    #[error("Invalid codec for selection")]
    InvalidCodecForSel,
    #[error("Value count mismatch")]
    ValueCountMismatch,
    #[error("Invalid datatype")]
    InvalidDatatype,
    // buffer pool errors
    #[error("insufficient memory({0})")]
    InsufficientMemory(usize),
    #[error("insufficient buffer pool({0})")]
    InsufficientBufferPool(u64),
    #[error("page id out of bound({0})")]
    PageIdOutOfBound(u64),
    #[error("empty free list of buffer pool")]
    EmptyFreeListOfBufferPool,
    // latch errors
    #[error("retry latch")]
    RetryLatch,
}

impl From<TryFromSliceError> for Error {
    #[inline]
    fn from(_src: TryFromSliceError) -> Error {
        Error::InvalidFormat
    }
}

impl From<DataTypeError> for Error {
    #[inline]
    fn from(src: DataTypeError) -> Self {
        match src {
            DataTypeError::InvalidFormat => Error::InvalidFormat,
            DataTypeError::IOError => Error::IOError,
        }
    }
}

impl From<std::io::Error> for Error {
    #[inline]
    fn from(_src: std::io::Error) -> Self {
        Error::IOError
    }
}
