pub use fxd::{Error as DecimalError, FixedDecimal as Decimal};
pub use time::format_description::{self, FormatItem};
pub use time::PrimitiveDateTime as Datetime;
pub use time::{error::Parse as DatetimeParseError, Error as DatetimeError};
pub use time::{Date, Time};

use static_init::dynamic;

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
    Bool,
    String,
    Bytes,
    Char,
    Date,
    Time,
    Datetime,
    Interval,
    Decimal,
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
        impl Typed for $ty {
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
impl_typed!(bool, DataType::Bool);
impl_typed!(str, DataType::String);
impl_typed!([u8], DataType::Bytes);
impl_typed!(Date, DataType::Date);
impl_typed!(Time, DataType::Time);
impl_typed!(Datetime, DataType::Datetime);
impl_typed!(Interval, DataType::Interval);
impl_typed!(Decimal, DataType::Decimal);

impl<const N: usize> Typed for [char; N] {
    fn ty() -> DataType {
        DataType::Char
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Microsecond = 1,
    Second = 2,
    Minute = 3,
    Hour = 4,
    Day = 5,
    Week = 6,
    Month = 7,
    Quarter = 8,
    Year = 9,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interval {
    pub value: i32,
    pub unit: TimeUnit,
}

#[dynamic]
pub static DEFAULT_DATE_FORMAT: Vec<FormatItem<'static>> =
    format_description::parse("[year]-[month]-[day]").unwrap();
