pub mod controlflow;
pub mod error;
pub mod expr;
pub mod fold;
pub mod func;
pub mod infer;
pub mod pred;

pub use crate::expr::*;
pub use crate::func::*;
pub use crate::pred::*;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_size_of_expr() {
        println!("size of Expr {}", std::mem::size_of::<ExprKind>());
        println!("size of Const {}", std::mem::size_of::<Const>());
        println!("size of Col {}", std::mem::size_of::<Col>());
    }
}
