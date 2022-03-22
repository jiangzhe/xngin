use crate::attr::Attr;

/// Block collects multiple tuples and aggregate synopses for analytical query.
pub struct Block {
    pub len: usize,
    pub attrs: Vec<Attr>,
}

impl Block {
    #[inline]
    pub fn fetch_attr(&self, attr_idx: usize) -> Option<Attr> {
        self.attrs.get(attr_idx).map(Attr::to_owned)
    }
}
