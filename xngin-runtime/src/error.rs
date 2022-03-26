use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Syntax(#[from] xngin_frontend::error::Error),
    #[error("{0}")]
    Plan(#[from] xngin_plan::error::Error),
}
