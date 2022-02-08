use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Value out of range")]
    ValueOutOfRange,
    #[error("Invalid Value format")]
    InvalidValueFormat,
}

impl From<std::num::ParseFloatError> for Error {
    fn from(_: std::num::ParseFloatError) -> Self {
        Error::InvalidValueFormat
    }
}
