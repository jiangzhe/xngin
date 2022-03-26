use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Syntax error: {0}")]
    SyntaxError(Box<String>),
}
