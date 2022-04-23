use std::array::TryFromSliceError;
use thiserror::Error;
use xngin_common::error::Error as CommonError;
use xngin_datatype::error::Error as DataTypeError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid format")]
    InvalidFormat,
    #[error("Checksum mismatch")]
    ChecksumMismatch,
    #[error("IO Error")]
    IOError,
    #[error("Data type not supported")]
    DataTypeNotSupported,
    #[error("Internal error")]
    InternalError,
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

impl From<CommonError> for Error {
    #[inline]
    fn from(src: CommonError) -> Self {
        match src {
            CommonError::InvalidFormat => Error::InvalidFormat,
            _ => Error::InternalError,
        }
    }
}
