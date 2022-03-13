use crate::attr::Attr;

/// Block collects multiple tuples and aggregate synopses for analytical query.
pub struct Block {
    pub len: usize,
    pub attrs: Vec<Attr>,
}
