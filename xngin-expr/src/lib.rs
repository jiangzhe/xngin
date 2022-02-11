pub mod error;
pub mod expr;
pub mod fold;
pub mod func;
pub mod pred;
// pub mod konst;

pub use crate::expr::*;
pub use crate::func::*;
pub use crate::pred::*;
// pub use crate::konst::*;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_size_of_expr() {
        println!("size of Expr {}", std::mem::size_of::<Expr>());
        println!("size of Const {}", std::mem::size_of::<Const>());
        println!("size of Col {}", std::mem::size_of::<Col>());
        println!("size of AggrFunc {}", std::mem::size_of::<Aggf>());
        println!("size of Func {}", std::mem::size_of::<Func>());
    }
}
