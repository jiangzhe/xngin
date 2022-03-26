use crate::cancel::{Cancellable, Cancellation};
use futures_lite::Stream;
use xngin_datatype::PreciseType;
use xngin_storage::block::Block;

pub struct ResultSet {
    fields: Vec<Field>,
    data: BlockStream,
    cancel: Cancellation,
}

#[derive(Debug)]
pub struct Field {
    pub name: String,
    pub ty: PreciseType,
}

/// Type alias of query result set
pub type BlockStream = Box<dyn Stream<Item = Cancellable<Block>>>;
