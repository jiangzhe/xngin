mod add;
mod neg;
mod not;

use crate::error::Result;
use crate::Const;

pub use add::FoldAdd;
pub use neg::FoldNeg;
pub use not::FoldNot;

pub trait ConstFold {
    fn fold(self) -> Result<Option<Const>>;
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::Const::{self, *};

    pub(crate) fn new_f64(v: f64) -> Const {
        Const::new_f64(v).unwrap()
    }

    pub(crate) fn new_decimal(s: &str) -> Const {
        let d: xngin_datatype::Decimal = s.parse().unwrap();
        Decimal(d)
    }
}
