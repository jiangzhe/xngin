#[macro_use]
mod macros;
pub mod align;
pub mod konst;
pub mod precise;
pub mod runtime;

pub use align::*;
pub use konst::*;
pub use precise::*;
pub use runtime::*;

pub use fxd::{Error as DecimalError, FixedDecimal as Decimal};
pub use time::format_description::{self, FormatItem};
pub use time::PrimitiveDateTime as Datetime;
pub use time::{error::Parse as DatetimeParseError, Error as DatetimeError};
pub use time::{Date, Time};

use static_init::dynamic;

pub type DataType = RuntimeType;

pub trait Typed {
    /// Returns type of data
    fn ty() -> DataType;
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

impl TimeUnit {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            TimeUnit::Microsecond => "microsecond",
            TimeUnit::Second => "second",
            TimeUnit::Minute => "minute",
            TimeUnit::Hour => "hour",
            TimeUnit::Day => "day",
            TimeUnit::Week => "week",
            TimeUnit::Month => "month",
            TimeUnit::Quarter => "quarter",
            TimeUnit::Year => "year",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interval {
    pub value: i32,
    pub unit: TimeUnit,
}

#[dynamic]
pub static DEFAULT_DATE_FORMAT: Vec<FormatItem<'static>> =
    format_description::parse("[year]-[month]-[day]").unwrap();
