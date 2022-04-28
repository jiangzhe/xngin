use crate::error::Result;
use crate::sel::Sel;
use xngin_storage::attr::Attr;

pub struct Not;

impl Not {
    /// evaluate single attribute.
    #[inline]
    pub fn eval_bool(&self, input: &Attr) -> Result<Attr> {
        todo!()
    }

    #[inline]
    pub fn eval_bool_sel(&self, input: &Attr, sel: Sel) -> Result<(Attr, Sel)> {
        todo!()
    }
}
