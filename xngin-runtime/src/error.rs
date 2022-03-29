use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Syntax(#[from] xngin_frontend::error::Error),
    #[error("{0}")]
    Plan(#[from] xngin_plan::error::Error),
    #[error("{0}")]
    Compute(#[from] xngin_compute::error::Error),
    #[error("Empty logical plan")]
    EmptyLgcPlan,
    #[error("Unsupported physical table scan")]
    UnsupportedPhyTableScan,
    #[error("Invalid logical node")]
    InvalidLgcNode,
    #[error("Buffer closed")]
    BufferClosed,
    #[error("Cancelled")]
    Cancelled,
    #[error("Dispatch error")]
    DispatchError,
    #[error("Rerun not allowed")]
    RerunNotAllowed,
}
