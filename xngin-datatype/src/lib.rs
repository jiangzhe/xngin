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

pub trait Typed {
    /// Returns runtime type
    fn rty(&self) -> RuntimeType;

    /// Returns precise type
    fn pty(&self) -> PreciseType;
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
