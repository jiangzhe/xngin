pub type Result<T> = std::result::Result<T, Error>;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Row number mismtach")]
    RowNumberMismatch,
    #[error("Missing codec")]
    MissingCodec,
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
}
