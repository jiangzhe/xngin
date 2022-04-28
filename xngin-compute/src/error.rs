use thiserror::Error;
use xngin_common::error::Error as CommonError;
use xngin_storage::error::Error as StorageError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Row number mismtach")]
    RowNumberMismatch,
    #[error("Missing attribute")]
    MissingAttr,
    #[error("Unsupported arithmetic operation")]
    UnsupportedArithOp,
    #[error("Invalid attribute to evaluate")]
    InvalidAttrEval,
    #[error("Failed to build eval arguments")]
    FailToBuildEvalArgs,
    #[error("Failed to build eval output")]
    FailToBuildEvalOutput,
    #[error("Failed to fetch attribute")]
    FailToFetchAttr,
    #[error("Failed to fetch eval cache")]
    FailToFetchEvalCache,
    #[error("Invalid codec for selection")]
    InvalidCodecForSel,
    #[error("Index out of bound")]
    IndexOutOfBound,
}

impl From<StorageError> for Error {
    #[inline]
    fn from(src: StorageError) -> Self {
        match src {
            StorageError::IndexOutOfBound => Error::IndexOutOfBound,
            _ => todo!(),
        }
    }
}

impl From<CommonError> for Error {
    #[inline]
    fn from(src: CommonError) -> Self {
        match src {
            CommonError::IndexOutOfBound(_) => Error::IndexOutOfBound,
            _ => todo!(),
        }
    }
}
