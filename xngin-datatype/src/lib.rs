pub mod align;
pub mod konst;

pub use align::*;
pub use konst::*;

pub use fxd::{Error as DecimalError, FixedDecimal as Decimal};
pub use time::format_description::{self, FormatItem};
pub use time::PrimitiveDateTime as Datetime;
pub use time::{error::Parse as DatetimeParseError, Error as DatetimeError};
pub use time::{Date, Time};

use static_init::dynamic;
use std::borrow::Cow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PreciseType {
    /// Most expressions are initialized with unknown type.
    /// After type inference, the precise type will be assigned.
    Unknown,
    /// Only constant null will have null type.
    Null,
    /// Integer type.
    /// First argument is byte number.
    /// Second argument is unsigned flag.
    Int(u8, bool),
    Decimal(u8, u8),
    Float(u8),
    Bool,
    Date,
    Time(u8),
    Datetime(u8),
    Interval,
    /// Note: Char and Varchar length is not same as bytes.
    /// It depends on collation, e.g. commonly used utf8mb4
    /// uses at most 4 bytes to store single character.
    Char(u16, Collation),
    Varchar(u16, Collation),
    // multiple types, only for tuple value.
    Compound,
}

impl Default for PreciseType {
    fn default() -> Self {
        PreciseType::Unknown
    }
}

impl PreciseType {
    #[inline]
    pub fn null() -> Self {
        PreciseType::Null
    }

    #[inline]
    pub fn bool() -> Self {
        PreciseType::Bool
    }

    #[inline]
    pub fn int(bytes: u8, unsigned: bool) -> Self {
        PreciseType::Int(bytes, unsigned)
    }

    #[inline]
    pub fn i32() -> Self {
        PreciseType::Int(4, false)
    }

    #[inline]
    pub fn u32() -> Self {
        PreciseType::Int(4, true)
    }

    #[inline]
    pub fn i64() -> Self {
        PreciseType::Int(8, false)
    }

    #[inline]
    pub fn u64() -> Self {
        PreciseType::Int(8, true)
    }

    #[inline]
    pub fn f32() -> Self {
        PreciseType::Float(4)
    }

    #[inline]
    pub fn f64() -> Self {
        PreciseType::Float(8)
    }

    #[inline]
    pub fn decimal(max_prec: u8, max_frac: u8) -> Self {
        PreciseType::Decimal(max_prec, max_frac)
    }

    #[inline]
    pub fn char(len: u16, collation: Collation) -> Self {
        PreciseType::Char(len, collation)
    }

    #[inline]
    pub fn varchar(max_len: u16, collation: Collation) -> Self {
        PreciseType::Varchar(max_len, collation)
    }

    #[inline]
    pub fn ascii(len: u16) -> Self {
        PreciseType::Char(len, Collation::Ascii)
    }

    #[inline]
    pub fn var_ascii(max_len: u16) -> Self {
        PreciseType::Varchar(max_len, Collation::Ascii)
    }

    #[inline]
    pub fn utf8(len: u16) -> Self {
        PreciseType::Char(len, Collation::Utf8mb4)
    }

    #[inline]
    pub fn var_utf8(max_len: u16) -> Self {
        PreciseType::Varchar(max_len, Collation::Utf8mb4)
    }

    #[inline]
    pub fn bytes(len: u16) -> Self {
        PreciseType::Char(len, Collation::Binary)
    }

    #[inline]
    pub fn var_bytes(max_len: u16) -> Self {
        PreciseType::Varchar(max_len, Collation::Binary)
    }

    #[inline]
    pub fn date() -> Self {
        PreciseType::Date
    }

    #[inline]
    pub fn time(frac: u8) -> Self {
        PreciseType::Time(frac)
    }

    #[inline]
    pub fn datetime(frac: u8) -> Self {
        PreciseType::Datetime(frac)
    }

    #[inline]
    pub fn interval() -> Self {
        PreciseType::Interval
    }

    #[inline]
    pub fn runtime_ty(&self) -> RuntimeType {
        match self {
            PreciseType::Int(n, false) => match n {
                1..=4 => RuntimeType::I32,
                8 => RuntimeType::I64,
                _ => unreachable!(),
            },
            PreciseType::Int(n, true) => match n {
                1..=4 => RuntimeType::U32,
                8 => RuntimeType::U64,
                _ => unreachable!(),
            },
            PreciseType::Float(n) => match n {
                4 => RuntimeType::F32,
                8 => RuntimeType::F64,
                _ => unreachable!(),
            },
            PreciseType::Decimal { .. } => RuntimeType::Decimal,
            PreciseType::Bool => RuntimeType::Bool,
            PreciseType::Char(_, Collation::Binary)
            | PreciseType::Varchar(_, Collation::Binary) => RuntimeType::Bytes,
            PreciseType::Char(..) | PreciseType::Varchar(..) => RuntimeType::String,
            PreciseType::Date => RuntimeType::Date,
            PreciseType::Time(..) => RuntimeType::Time,
            PreciseType::Datetime(..) => RuntimeType::Datetime,
            PreciseType::Interval => RuntimeType::Interval,
            PreciseType::Null => RuntimeType::Null,
            PreciseType::Unknown | PreciseType::Compound => RuntimeType::Unknown,
        }
    }

    #[inline]
    pub fn is_unknown(&self) -> bool {
        matches!(self, PreciseType::Unknown)
    }

    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, PreciseType::Bool)
    }

    #[inline]
    pub fn to_lower(&self) -> Cow<'_, str> {
        match self {
            PreciseType::Unknown => Cow::Borrowed("unknown"),
            PreciseType::Null => Cow::Borrowed("null"),
            PreciseType::Int(bytes, unsigned) => {
                if *unsigned {
                    Cow::Owned(format!("uint({})", bytes))
                } else {
                    Cow::Owned(format!("int({})", bytes))
                }
            }
            PreciseType::Decimal(max_prec, max_frac) => {
                Cow::Owned(format!("decimal({}, {})", max_prec, max_frac))
            }
            PreciseType::Float(bytes) => Cow::Owned(format!("float({})", bytes)),
            PreciseType::Bool => Cow::Borrowed("bool"),
            PreciseType::Date => Cow::Borrowed("date"),
            PreciseType::Time(frac) => Cow::Owned(format!("time({})", frac)),
            PreciseType::Datetime(frac) => Cow::Owned(format!("datetime({})", frac)),
            PreciseType::Interval => Cow::Borrowed("interval"),
            PreciseType::Char(n, c) => Cow::Owned(format!("char({}, {:?})", n, c)),
            PreciseType::Varchar(n, c) => Cow::Owned(format!("varchar({}, {:?})", n, c)),
            PreciseType::Compound => Cow::Borrowed("compound"),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum RuntimeType {
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    Decimal,
    Bool,
    String,
    Bytes,
    Date,
    Time,
    Datetime,
    Interval,
    Null,
    Unknown,
}

pub trait Typed {
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

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Collation {
    Ascii,
    Utf8mb4,
    Binary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interval {
    pub value: i32,
    pub unit: TimeUnit,
}

#[dynamic]
pub static DEFAULT_DATE_FORMAT: Vec<FormatItem<'static>> =
    format_description::parse("[year]-[month]-[day]").unwrap();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_precise_type() {
        use std::mem::size_of;
        println!("size of PreciseType is {}", size_of::<PreciseType>());
    }
}
