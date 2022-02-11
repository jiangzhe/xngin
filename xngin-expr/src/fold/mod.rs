mod add;
mod cmp;
mod neg;
mod not;
mod sub;

use crate::error::Result;
use crate::Const;

pub use add::FoldAdd;
pub use cmp::*;
pub use neg::FoldNeg;
pub use not::FoldNot;
pub use sub::FoldSub;

pub trait ConstFold {
    fn fold(self) -> Result<Option<Const>>;
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::Const::{self, *};
    use std::sync::Arc;

    pub(crate) fn new_f64(v: f64) -> Const {
        Const::new_f64(v).unwrap()
    }

    pub(crate) fn new_decimal(s: &str) -> Const {
        let d: xngin_datatype::Decimal = s.parse().unwrap();
        Decimal(d)
    }

    pub(crate) fn new_str(s: &str) -> Const {
        Const::String(Arc::from(s))
    }

    pub(crate) fn new_bytes(bs: &[u8]) -> Const {
        Const::Bytes(Arc::from(bs))
    }
}
