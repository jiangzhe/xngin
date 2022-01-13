use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Schema '{0}' already exists")]
    SchemaAlreadyExists(String),
    #[error("Schema '{0}' not exists")]
    SchemaNotExists(String),
    #[error("Table '{0}' already exists")]
    TableAlreadyExists(String),
    #[error("Column name '{0}' is not unique")]
    ColumnNameNotUnique(String),
}
