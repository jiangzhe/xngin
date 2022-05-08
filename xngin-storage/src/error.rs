use std::array::TryFromSliceError;
use thiserror::Error;
use xngin_datatype::error::Error as DataTypeError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("invalid argument")]
    InvalidArgument,
    #[error("internal error")]
    InternalError,
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
