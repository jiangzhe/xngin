#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum DataType {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    Decimal,
    Date,
    Datetime,
    Char,
    String,
    Bytes,
    Bool,
}

impl DataType {
    #[inline]
    pub const fn is_fixed_len(self) -> bool {
        !matches!(self, DataType::String | DataType::Bytes)
    }
}

pub trait Typed {
    /// Returns type of data
    fn ty() -> DataType;
}

macro_rules! impl_typed {
    ($ty:ty, $p:path) => {
        impl $crate::types::Typed for $ty {
            fn ty() -> DataType {
                $p
            }
        }
    };
}

impl_typed!(i8, DataType::I8);
impl_typed!(u8, DataType::U8);
impl_typed!(i16, DataType::I16);
impl_typed!(u16, DataType::U16);
impl_typed!(i32, DataType::I32);
impl_typed!(u32, DataType::U32);
impl_typed!(i64, DataType::I64);
impl_typed!(u64, DataType::U64);
impl_typed!(f32, DataType::F32);
impl_typed!(f64, DataType::F64);
impl_typed!(str, DataType::String);
impl_typed!([u8], DataType::Bytes);
impl_typed!(bool, DataType::Bool);
