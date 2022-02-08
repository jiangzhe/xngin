macro_rules! impl_typed {
    ($ty:ty, $p:path) => {
        impl $crate::Typed for $ty {
            fn ty() -> $crate::DataType {
                $p
            }
        }
    };
}
