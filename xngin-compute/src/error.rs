pub type Result<T> = std::result::Result<T, Error>;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Row number mismtach")]
    RowNumberMismatch,
}
