use crate::epoch::{Atomic, Pointable, Shared, Guard};
use super::node::{NodeTemplate, NodeOps};
use super::key::ExtractKey;

pub trait ValueLoader {
    type Value: ExtractKey;

    fn load_leaf(&self, node: Shared<'_, NodeTemplate>) -> Self::Value;
}

pub struct EmbeddedU32;

impl ValueLoader for EmbeddedU32 {
    type Value = u32;

    #[inline]
    fn load_leaf(&self, node: Shared<'_, NodeTemplate>) -> Self::Value {
        node.tid() as u32
    }
}
